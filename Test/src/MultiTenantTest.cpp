// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifdef ROCKSDB
#include "inc/Core/SPANN/TenantIndexManager.h"
#include "inc/Server/SearchExecutor.h"
#include "inc/Socket/RemoteSearchQuery.h"
#include "inc/Test.h"
#include <boost/filesystem.hpp>
#include <fstream>
#include <future>
#ifndef _MSC_VER
#include <sys/wait.h>
#endif

using namespace SPTAG;
using namespace SPTAG::SPANN;

namespace
{
struct TemporaryNode
{
    std::string root =
        (boost::filesystem::temp_directory_path() / boost::filesystem::unique_path("sptag-multitenant-%%%%-%%%%-%%%%"))
            .string();
    ~TemporaryNode()
    {
        std::error_code error;
        std::filesystem::remove_all(root, error);
    }
};

ByteArray Vectors(SizeType count, DimensionType dimension, float base)
{
    auto result = ByteArray::Alloc(static_cast<size_t>(count) * dimension * sizeof(float));
    auto *data = reinterpret_cast<float *>(result.Data());
    for (SizeType i = 0; i < count; ++i)
        for (DimensionType j = 0; j < dimension; ++j)
            data[i * dimension + j] = base + i * 0.1f + j * 0.01f;
    return result;
}

void RequireSuccess(ErrorCode status)
{
    BOOST_REQUIRE_EQUAL(static_cast<int>(status), static_cast<int>(ErrorCode::Success));
}

std::unique_ptr<TenantIndexManager> OpenNode(const std::string &path)
{
    TenantIndexManager::Limits limits;
    limits.maxTenants = 4;
    limits.maxVectorsPerTenant = 4096;
    limits.maxDimension = 32;
    std::unique_ptr<TenantIndexManager> node;
    RequireSuccess(TenantIndexManager::Open(path, limits, node));
    return node;
}

struct SnapshotProbe
{
    std::atomic<int> saves{0};
    std::atomic<bool> block{false};
    std::atomic<bool> failLoad{false};
    std::atomic<bool> failSave{false};
    std::promise<void> entered;
    std::promise<void> release;
    std::shared_future<void> released = release.get_future().share();
};

class SnapshotFileIO : public Helper::SimpleFileIO
{
  public:
    explicit SnapshotFileIO(std::shared_ptr<SnapshotProbe> probe) : m_probe(std::move(probe))
    {
    }
    bool Initialize(const char *path, int mode, uint64_t maxIO = 1 << 20, uint32_t reads = 2, uint32_t writes = 2,
                    uint16_t threads = 4, uint64_t maxFile = 300ULL << 30) override
    {
        if (std::filesystem::path(path).filename() == "indexloader.ini" &&
            (((mode & std::ios::in) && m_probe->failLoad) || ((mode & std::ios::out) && m_probe->failSave)))
            return false;
        if ((mode & std::ios::out) && std::filesystem::path(path).filename() == "indexloader.ini")
        {
            ++m_probe->saves;
            if (m_probe->block.exchange(false))
            {
                m_probe->entered.set_value();
                m_probe->released.wait();
            }
        }
        return SimpleFileIO::Initialize(path, mode, maxIO, reads, writes, threads, maxFile);
    }

  private:
    std::shared_ptr<SnapshotProbe> m_probe;
};

struct SnapshotFactory
{
    inline static std::shared_ptr<SnapshotProbe> active;
    decltype(f_createIO) original = f_createIO;
    explicit SnapshotFactory(std::shared_ptr<SnapshotProbe> probe)
    {
        active = std::move(probe);
        f_createIO = []() -> std::shared_ptr<Helper::DiskIO> { return std::make_shared<SnapshotFileIO>(active); };
    }
    ~SnapshotFactory()
    {
        f_createIO = original;
        active.reset();
    }
};
} // namespace

BOOST_AUTO_TEST_SUITE(MultiTenant)

BOOST_AUTO_TEST_CASE(TenantWireBoundsAndRoundTrip)
{
    Socket::RemoteTenantRequest request;
    request.operation = Socket::TenantOperation::Create;
    request.tenant = "tenant / with delimiters";
    request.valueType = VectorValueType::Float;
    request.metric = DistCalcMethod::L2;
    request.dimension = 4;
    request.count = 64;
    request.vectors = Vectors(64, 4, 0);
    ByteArray body;
    BOOST_REQUIRE(request.Write(body));
    Socket::RemoteTenantRequest decoded;
    BOOST_REQUIRE(decoded.Read(body.Data(), body.Length()));
    BOOST_CHECK_EQUAL(decoded.tenant, request.tenant);
    BOOST_CHECK_EQUAL(decoded.count, 64);
    BOOST_CHECK_EQUAL(memcmp(decoded.vectors.Data(), request.vectors.Data(), request.vectors.Length()), 0);
    for (size_t size = 0; size < body.Length(); ++size)
        BOOST_CHECK(!decoded.Read(body.Data(), size));
    BOOST_CHECK(!decoded.Read(nullptr, body.Length()));
    auto invalid = ByteArray::Alloc(body.Length() + 1);
    memcpy(invalid.Data(), body.Data(), body.Length());
    BOOST_CHECK(!decoded.Read(invalid.Data(), invalid.Length()));
    invalid[0] = 2;
    BOOST_CHECK(!decoded.Read(invalid.Data(), body.Length()));
    memcpy(invalid.Data(), body.Data(), body.Length());
    invalid[2] = 255;
    BOOST_CHECK(!decoded.Read(invalid.Data(), body.Length()));
    memcpy(invalid.Data(), body.Data(), body.Length());
    std::uint32_t oversized = UINT32_MAX;
    memcpy(invalid.Data() + 25, &oversized, sizeof(oversized));
    BOOST_CHECK(!decoded.Read(invalid.Data(), body.Length()));

    Socket::RemoteTenantResult result;
    result.status = ErrorCode::Success;
    result.count = 1;
    result.results.resize(1);
    result.results[0].VID = 42;
    result.results[0].Dist = 0.25f;
    BOOST_REQUIRE(result.Write(body));
    Socket::RemoteTenantResult reply;
    BOOST_REQUIRE(reply.Read(body.Data(), body.Length()));
    RequireSuccess(reply.status);
    BOOST_CHECK_EQUAL(reply.results[0].VID, 42);
    BOOST_CHECK_EQUAL(reply.results[0].Dist, 0.25f);
    for (size_t size = 0; size < body.Length(); ++size)
        BOOST_CHECK(!reply.Read(body.Data(), size));
    memcpy(body.Data() + 12, &oversized, sizeof(oversized));
    BOOST_CHECK(!reply.Read(body.Data(), body.Length()));
    result.status = ErrorCode::Key_NotFound;
    result.results.clear();
    result.count = 0;
    BOOST_REQUIRE(result.Write(body));
    BOOST_REQUIRE(reply.Read(body.Data(), body.Length()));
    BOOST_CHECK_EQUAL(std::string(reply.StatusName()), "Key_NotFound");
    BOOST_CHECK(reply.results.empty());
}

BOOST_AUTO_TEST_CASE(DedicatedRootAndExclusiveOwner)
{
    TemporaryNode nonempty;
    std::filesystem::create_directories(nonempty.root);
    {
        std::ofstream sentinel(nonempty.root + "/keep.txt");
        sentinel << "existing data";
    }
    std::unique_ptr<TenantIndexManager> node;
    BOOST_CHECK(TenantIndexManager::Open(nonempty.root, {}, node) != ErrorCode::Success);
    BOOST_CHECK(std::filesystem::exists(nonempty.root + "/keep.txt"));
    BOOST_CHECK(!std::filesystem::exists(nonempty.root + "/recovery"));
    TemporaryNode directory;
    node = OpenNode(directory.root);
    std::unique_ptr<TenantIndexManager> duplicate;
    BOOST_CHECK(TenantIndexManager::Open(directory.root, {}, duplicate) != ErrorCode::Success);
    BOOST_CHECK(!duplicate);
    RequireSuccess(node->Checkpoint());
}

BOOST_AUTO_TEST_CASE(NamespacesAndViewOwnership)
{
    TemporaryNode directory;
    std::filesystem::create_directories(directory.root);
    std::shared_ptr<SharedRocksDB> engine;
    RequireSuccess(
        SharedRocksDB::Open(directory.root + "/state", {"postings"}, engine, true, rocksdb::NewLRUCache(1 << 20)));
    auto a = engine->PostingView("a/b", 1);
    auto b = engine->PostingView("a", 1);
    RequireSuccess(a->Put(1, "alpha", MaxTimeout, nullptr));
    RequireSuccess(b->Put(1, "beta", MaxTimeout, nullptr));
    RequireSuccess(a->Merge(1, "-tail", MaxTimeout, nullptr, nullptr));
    std::string value;
    RequireSuccess(a->Get(1, &value, MaxTimeout, nullptr));
    BOOST_CHECK_EQUAL(value, "alpha-tail");
    RequireSuccess(a->DeleteRange(0, 2));
    BOOST_CHECK(a->Get(1, &value, MaxTimeout, nullptr) == ErrorCode::Key_NotFound);
    a->ShutDown();
    RequireSuccess(b->Get(1, &value, MaxTimeout, nullptr));
    BOOST_CHECK_EQUAL(value, "beta");
    RequireSuccess(b->Merge(2, "first", MaxTimeout, nullptr, nullptr));
    std::vector<Helper::PageBuffer<uint8_t>> buffers(2);
    RequireSuccess(b->MultiGet({1, 2}, buffers, MaxTimeout, nullptr));
    BOOST_CHECK_EQUAL(buffers[1].GetAvailableSize(), 5);
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char *>(buffers[1].GetBuffer()), 5), "first");
    std::vector<std::string> missing;
    BOOST_CHECK(b->MultiGet({1, 999}, &missing, MaxTimeout, nullptr) == ErrorCode::Key_NotFound);
    RequireSuccess(engine->Checkpoint(directory.root + "/snapshot"));
    BOOST_CHECK(engine->Checkpoint(directory.root + "/snapshot") != ErrorCode::Success);
}

BOOST_AUTO_TEST_CASE(OrderedLogKeysAndBoundedScan)
{
    TemporaryNode directory;
    std::filesystem::create_directories(directory.root);
    std::shared_ptr<SharedRocksDB> engine;
    RequireSuccess(SharedRocksDB::Open(directory.root + "/recovery", {"recovery_log"}, engine, true,
                                       rocksdb::NewLRUCache(1 << 20)));
    const auto prefix = SharedRocksDB::Namespace("tenant", 1);
    for (auto number : {65536ULL, 256ULL, 1ULL, 255ULL})
        RequireSuccess(engine->Put("recovery_log", prefix + SharedRocksDB::EncodeNumber(number), "record"));
    RequireSuccess(
        engine->Put("recovery_log", SharedRocksDB::Namespace("tenant", 2) + SharedRocksDB::EncodeNumber(1), "other"));
    std::vector<uint64_t> seen;
    RequireSuccess(engine->Scan("recovery_log", prefix, [&](const std::string &key, const std::string &) {
        uint64_t number;
        BOOST_REQUIRE(SharedRocksDB::DecodeNumber(key.substr(prefix.size()), number));
        seen.push_back(number);
        return ErrorCode::Success;
    }));
    const std::vector<uint64_t> expected{1, 255, 256, 65536};
    BOOST_CHECK_EQUAL_COLLECTIONS(seen.begin(), seen.end(), expected.begin(), expected.end());
}

BOOST_AUTO_TEST_CASE(IndependentIndexesCheckpointAndReload)
{
    TemporaryNode directory;
    auto node = OpenNode(directory.root);
    auto a = Vectors(128, 4, 0);
    auto b = Vectors(128, 7, 1000);
    RequireSuccess(node->Create("A", VectorValueType::Float, 4, DistCalcMethod::L2, a, 128));
    RequireSuccess(node->Create("B", VectorValueType::Float, 7, DistCalcMethod::L2, b, 128));
    BOOST_CHECK(node->Create("A", VectorValueType::Float, 4, DistCalcMethod::L2, a, 128) != ErrorCode::Success);
    std::vector<BasicResult> results;
    RequireSuccess(node->Search("A", Vectors(1, 4, 0), 3, results));
    BOOST_CHECK_EQUAL(results[0].VID, 0);
    RequireSuccess(node->Search("B", Vectors(1, 7, 1000), 3, results));
    BOOST_CHECK_EQUAL(results[0].VID, 0);
    BOOST_CHECK(node->Search("A", Vectors(1, 7, 0), 3, results) != ErrorCode::Success);
    BOOST_CHECK(node->Search("missing", Vectors(1, 4, 0), 3, results) == ErrorCode::Key_NotFound);
    SizeType id = -1;
    BOOST_CHECK(node->Insert("A", Vectors(1, 4, 500), 1, id, VectorValueType::UInt8, 16) != ErrorCode::Success);
    BOOST_CHECK(node->Insert("A", Vectors(1, 4, 500), 1, id, VectorValueType::Float, 5) != ErrorCode::Success);
    BOOST_CHECK(node->Search("A", Vectors(1, 4, 0), 3, results, VectorValueType::UInt8, 16) != ErrorCode::Success);
    BOOST_CHECK(node->Search("A", Vectors(1, 4, 0), 3, results, VectorValueType::Float, 5) != ErrorCode::Success);
    RequireSuccess(node->Insert("A", Vectors(1, 4, 500), 1, id));
    BOOST_CHECK_EQUAL(id, 128);
    RequireSuccess(node->DeleteVector("A", 0));
    RequireSuccess(node->Unload("A"));
    RequireSuccess(node->Search("A", Vectors(1, 4, 500), 3, results));
    BOOST_CHECK_EQUAL(results[0].VID, 128);
    RequireSuccess(node->Search("B", Vectors(1, 7, 1000), 3, results));
    BOOST_CHECK_EQUAL(results[0].VID, 0);
    RequireSuccess(node->Checkpoint());
    RequireSuccess(node->Close());
    node.reset();
    node = OpenNode(directory.root);
    RequireSuccess(node->Search("A", Vectors(1, 4, 500), 3, results));
    BOOST_CHECK_EQUAL(results[0].VID, 128);
    RequireSuccess(node->DeleteTenant("A"));
    RequireSuccess(node->Create("A", VectorValueType::Float, 4, DistCalcMethod::L2, Vectors(128, 4, -500), 128));
    RequireSuccess(node->Search("A", Vectors(1, 4, -500), 3, results));
    BOOST_CHECK_EQUAL(results[0].VID, 0);
    RequireSuccess(node->Close());
}

BOOST_AUTO_TEST_CASE(ConcurrentTenantsAndBoundedArtifacts)
{
    TemporaryNode directory;
    auto node = OpenNode(directory.root);
    for (const auto *name : {"A", "B"})
        RequireSuccess(node->Create(name, VectorValueType::Float, 4, DistCalcMethod::L2, Vectors(128, 4, 0), 128));
    auto update = [&](const char *name, float base) {
        SizeType first;
        auto status = node->Insert(name, Vectors(1024, 4, base), 1024, first);
        if (status != ErrorCode::Success)
            return status;
        std::vector<BasicResult> results;
        status = node->Search(name, Vectors(1, 4, base), 1, results);
        if (status != ErrorCode::Success)
            return status;
        return results.front().VID == 128 ? ErrorCode::Success : ErrorCode::Fail;
    };
    auto a = std::async(std::launch::async, update, "A", 500);
    auto b = std::async(std::launch::async, update, "B", -500);
    RequireSuccess(a.get());
    RequireSuccess(b.get());
    RequireSuccess(node->Checkpoint());
    RequireSuccess(node->Close());
    node.reset();
    node = OpenNode(directory.root);
    std::vector<BasicResult> results;
    RequireSuccess(node->Search("A", Vectors(1, 4, 500), 1, results));
    BOOST_CHECK_EQUAL(results[0].VID, 128);
    RequireSuccess(node->Search("B", Vectors(1, 4, -500), 1, results));
    BOOST_CHECK_EQUAL(results[0].VID, 128);
    size_t work = 0, checkpoint = 0;
    for (const auto &entry : std::filesystem::directory_iterator(directory.root))
    {
        const auto name = entry.path().filename().string();
        if (name.find("work-") == 0)
            ++work;
        if (name.find("checkpoint-") == 0)
            ++checkpoint;
    }
    BOOST_CHECK_EQUAL(work, 1);
    BOOST_CHECK_EQUAL(checkpoint, 1);
}

BOOST_AUTO_TEST_CASE(SameTenantConcurrentReadersWritersAndReload)
{
    TemporaryNode directory;
    auto node = OpenNode(directory.root);
    RequireSuccess(node->Create("A", VectorValueType::Float, 4, DistCalcMethod::L2, Vectors(128, 4, 0), 128));
    RequireSuccess(node->Unload("A"));
    std::promise<void> start;
    auto gate = start.get_future().share();
    std::vector<std::future<std::pair<ErrorCode, SizeType>>> writers;
    for (int worker = 0; worker < 4; ++worker)
        writers.emplace_back(std::async(std::launch::async, [&, worker] {
            gate.wait();
            SizeType first = -1;
            auto status = node->Insert("A", Vectors(256, 4, 1000 + worker * 100), 256, first);
            return std::make_pair(status, first);
        }));
    std::vector<std::future<ErrorCode>> readers;
    for (int worker = 0; worker < 3; ++worker)
        readers.emplace_back(std::async(std::launch::async, [&] {
            gate.wait();
            for (int i = 0; i < 200; ++i)
            {
                std::vector<BasicResult> results;
                auto status = node->Search("A", Vectors(1, 4, 0), 3, results);
                if (status != ErrorCode::Success)
                    return status;
                if (results[0].VID != 0)
                    return ErrorCode::Fail;
            }
            return ErrorCode::Success;
        }));
    start.set_value();
    std::vector<SizeType> firstIDs;
    for (auto &writer : writers)
    {
        auto result = writer.get();
        RequireSuccess(result.first);
        firstIDs.push_back(result.second);
    }
    for (auto &reader : readers)
        RequireSuccess(reader.get());
    auto sorted = firstIDs;
    std::sort(sorted.begin(), sorted.end());
    for (int i = 0; i < 4; ++i)
        BOOST_CHECK_EQUAL(sorted[i], 128 + i * 256);
    std::promise<void> deleteStart;
    auto deleteGate = deleteStart.get_future().share();
    auto remove = [&] {
        deleteGate.wait();
        return node->DeleteVector("A", 0);
    };
    auto a = std::async(std::launch::async, remove);
    auto b = std::async(std::launch::async, remove);
    deleteStart.set_value();
    auto sa = a.get(), sb = b.get();
    BOOST_CHECK((sa == ErrorCode::Success && sb == ErrorCode::VectorNotFound) ||
                (sb == ErrorCode::Success && sa == ErrorCode::VectorNotFound));
    // Reopen without publishing: replay must retain all independently allocated IDs.
    node.reset();
    for (int attempt = 0; attempt < 2; ++attempt)
    {
        node = OpenNode(directory.root);
        for (int worker = 0; worker < 4; ++worker)
        {
            std::vector<BasicResult> results;
            RequireSuccess(node->Search("A", Vectors(1, 4, 1000 + worker * 100), 1, results));
            BOOST_CHECK_EQUAL(results[0].VID, firstIDs[worker]);
        }
        BOOST_CHECK(node->DeleteVector("A", 0) == ErrorCode::VectorNotFound);
        node.reset();
    }
}

BOOST_AUTO_TEST_CASE(CheckpointReusesCleanSnapshotsAndAllowsQueries)
{
    TemporaryNode directory;
    auto node = OpenNode(directory.root);
    for (auto name : {"A", "B"})
        RequireSuccess(node->Create(name, VectorValueType::Float, 4, DistCalcMethod::L2, Vectors(128, 4, 0), 128));
    auto probe = std::make_shared<SnapshotProbe>();
    SnapshotFactory factory(probe);
    RequireSuccess(node->Checkpoint());
    BOOST_CHECK_EQUAL(probe->saves.load(), 0);
    SizeType first;
    RequireSuccess(node->Insert("A", Vectors(1, 4, 500), 1, first));
    probe->block = true;
    auto entered = probe->entered.get_future();
    auto checkpoint = std::async(std::launch::async, [&] { return node->Checkpoint(); });
    bool saving = entered.wait_for(std::chrono::seconds(10)) == std::future_status::ready;
    auto query = std::async(std::launch::async, [&] {
        std::vector<BasicResult> results;
        return node->Search("A", Vectors(1, 4, 500), 1, results);
    });
    bool queryFinished = query.wait_for(std::chrono::seconds(2)) == std::future_status::ready;
    auto writer = std::async(std::launch::async, [&] {
        SizeType id;
        return node->Insert("A", Vectors(1, 4, 600), 1, id);
    });
    bool writerBlocked = writer.wait_for(std::chrono::milliseconds(100)) == std::future_status::timeout;
    probe->release.set_value();
    RequireSuccess(checkpoint.get());
    RequireSuccess(query.get());
    RequireSuccess(writer.get());
    BOOST_CHECK(saving);
    BOOST_CHECK(queryFinished);
    BOOST_CHECK(writerBlocked);
    BOOST_CHECK_EQUAL(probe->saves.load(), 1);
    RequireSuccess(node->Checkpoint());
    BOOST_CHECK_EQUAL(probe->saves.load(), 2);
    RequireSuccess(node->Unload("A"));
    std::vector<BasicResult> results;
    RequireSuccess(node->Search("A", Vectors(1, 4, 600), 1, results));
    RequireSuccess(node->Checkpoint());
    BOOST_CHECK_EQUAL(probe->saves.load(), 2);
    node.reset();
    node = OpenNode(directory.root);
    probe->saves = 0;
    RequireSuccess(node->Checkpoint());
    BOOST_CHECK_EQUAL(probe->saves.load(), 0);
}

BOOST_AUTO_TEST_CASE(ResidentCacheLRUDirtyEvictionAndRecovery)
{
    TemporaryNode directory;
    TenantIndexManager::Limits limits;
    limits.maxResidentTenants = 2;
    std::unique_ptr<TenantIndexManager> node;
    RequireSuccess(TenantIndexManager::Open(directory.root, limits, node));
    for (auto name : {"A", "B", "C"})
        RequireSuccess(node->Create(name, VectorValueType::Float, 4, DistCalcMethod::L2, Vectors(128, 4, 0), 128));
    std::vector<std::filesystem::path> configs;
    auto oldTime = std::filesystem::file_time_type::clock::now() - std::chrono::hours(1);
    for (const auto &entry : std::filesystem::recursive_directory_iterator(directory.root))
        if (entry.path().filename() == "indexloader.ini")
        {
            configs.push_back(entry.path());
            std::filesystem::last_write_time(entry.path(), oldTime);
        }
    BOOST_REQUIRE(!configs.empty());
    TenantIndexManager::NodeStats state;
    std::vector<TenantIndexManager::TenantStats> tenants;
    auto refresh = [&] { node->GetStats(state, tenants); };
    auto search = [&](const char *name, float value = 0) {
        std::vector<BasicResult> results;
        RequireSuccess(node->Search(name, Vectors(1, 4, value), 1, results));
        return results[0].VID;
    };
    refresh();
    BOOST_CHECK_EQUAL(state.residentTenants, 0);
    search("A");
    search("B");
    search("A");
    search("C");
    for (const auto &config : configs)
        BOOST_CHECK(std::filesystem::last_write_time(config) == oldTime);
    refresh();
    BOOST_CHECK_EQUAL(state.residentTenants, 2);
    BOOST_CHECK(tenants[1].state == TenantIndexManager::Residency::Unloaded);
    BOOST_CHECK_EQUAL(tenants[1].evictions, 1);
    SizeType first;
    RequireSuccess(node->Insert("A", Vectors(1, 4, 500), 1, first));
    refresh();
    BOOST_CHECK(tenants[0].dirty);
    search("B");
    search("C");
    refresh();
    BOOST_CHECK(tenants[0].state == TenantIndexManager::Residency::Unloaded);
    BOOST_CHECK(!tenants[0].dirty);
    BOOST_CHECK_EQUAL(search("A", 500), first);
    node.reset();
    RequireSuccess(TenantIndexManager::Open(directory.root, limits, node));
    refresh();
    BOOST_CHECK_EQUAL(state.residentTenants, 0);
    BOOST_CHECK_EQUAL(search("A", 500), first);
    RequireSuccess(node->DeleteTenant("A"));
    refresh();
    BOOST_CHECK_EQUAL(state.residentTenants, 0);
    RequireSuccess(node->Close());
    refresh();
    BOOST_CHECK(state.closed);
    BOOST_CHECK(tenants.empty());
}

BOOST_AUTO_TEST_CASE(ResidentByteTargetAndConcurrentMisses)
{
    TemporaryNode directory;
    TenantIndexManager::Limits limits;
    limits.maxResidentTenants = 2;
    limits.maxResidentBytes = 1;
    std::unique_ptr<TenantIndexManager> node;
    RequireSuccess(TenantIndexManager::Open(directory.root, limits, node));
    for (auto name : {"A", "B"})
        RequireSuccess(node->Create(name, VectorValueType::Float, 4, DistCalcMethod::L2, Vectors(128, 4, 0), 128));
    std::promise<void> start;
    auto gate = start.get_future().share();
    std::vector<std::future<ErrorCode>> readers;
    for (int i = 0; i < 8; ++i)
        readers.emplace_back(std::async(std::launch::async, [&] {
            gate.wait();
            std::vector<BasicResult> results;
            return node->Search("A", Vectors(1, 4, 0), 1, results);
        }));
    start.set_value();
    for (auto &reader : readers)
        RequireSuccess(reader.get());
    TenantIndexManager::NodeStats state;
    std::vector<TenantIndexManager::TenantStats> tenants;
    node->GetStats(state, tenants);
    BOOST_CHECK_EQUAL(tenants[0].loads, 1);
    BOOST_CHECK_EQUAL(state.residentTenants, 1);
    BOOST_CHECK(state.overByteTarget);
    BOOST_CHECK(state.residentBytes > 1);
    readers.clear();
    for (int worker = 0; worker < 4; ++worker)
        readers.emplace_back(std::async(std::launch::async, [&, worker] {
            for (int i = 0; i < 10; ++i)
            {
                std::vector<BasicResult> results;
                auto status = node->Search((i + worker) % 2 ? "A" : "B", Vectors(1, 4, 0), 1, results);
                if (status != ErrorCode::Success)
                    return status;
                if (results[0].VID != 0)
                    return ErrorCode::Fail;
            }
            return ErrorCode::Success;
        }));
    for (auto &reader : readers)
        RequireSuccess(reader.get());
    node->GetStats(state, tenants);
    BOOST_CHECK_EQUAL(state.residentTenants, 1);
    BOOST_CHECK_EQUAL(tenants[0].activeRequests + tenants[1].activeRequests, 0);
    BOOST_CHECK(!state.failed);
}

BOOST_AUTO_TEST_CASE(UnloadAndPrivateCreateDoNotBlockOtherTenant)
{
    TemporaryNode directory;
    auto node = OpenNode(directory.root);
    for (auto name : {"A", "B"})
        RequireSuccess(node->Create(name, VectorValueType::Float, 4, DistCalcMethod::L2, Vectors(128, 4, 0), 128));
    SizeType id;
    RequireSuccess(node->Insert("A", Vectors(1, 4, 500), 1, id));
    std::vector<BasicResult> results;
    RequireSuccess(node->Search("B", Vectors(1, 4, 0), 1, results));
    for (bool creating : {false, true})
    {
        auto probe = std::make_shared<SnapshotProbe>();
        SnapshotFactory factory(probe);
        probe->block = true;
        auto entered = probe->entered.get_future();
        auto slow = std::async(std::launch::async, [&] {
            return creating ? node->Create("C", VectorValueType::Float, 4, DistCalcMethod::L2, Vectors(128, 4, 0), 128)
                            : node->Unload("A");
        });
        bool saving = entered.wait_for(std::chrono::seconds(10)) == std::future_status::ready;
        auto other = std::async(std::launch::async, [&] {
            std::vector<BasicResult> found;
            auto status = node->Search("B", Vectors(1, 4, 0), 1, found);
            if (status != ErrorCode::Success)
                return status;
            SizeType first;
            return node->Insert("B", Vectors(1, 4, 700), 1, first);
        });
        bool concurrent = other.wait_for(std::chrono::seconds(2)) == std::future_status::ready;
        probe->release.set_value();
        RequireSuccess(slow.get());
        RequireSuccess(other.get());
        BOOST_CHECK(saving);
        BOOST_CHECK(concurrent);
    }
}

BOOST_AUTO_TEST_CASE(CachePressureWithConcurrentWritesAndCheckpoints)
{
    TemporaryNode directory;
    TenantIndexManager::Limits limits;
    limits.maxResidentTenants = 1;
    std::unique_ptr<TenantIndexManager> node;
    RequireSuccess(TenantIndexManager::Open(directory.root, limits, node));
    for (auto name : {"A", "B"})
        RequireSuccess(node->Create(name, VectorValueType::Float, 4, DistCalcMethod::L2, Vectors(128, 4, 0), 128));
    std::promise<void> start;
    auto gate = start.get_future().share();
    std::vector<std::future<ErrorCode>> workers;
    std::vector<SizeType> ids[4];
    for (int worker = 0; worker < 4; ++worker)
        workers.emplace_back(std::async(std::launch::async, [&, worker] {
            gate.wait();
            for (int i = 0; i < 8; ++i)
            {
                SizeType first;
                auto status = node->Insert(worker % 2 ? "A" : "B", Vectors(1, 4, 500 + worker * 100 + i), 1, first);
                if (status != ErrorCode::Success)
                    return status;
                ids[worker].push_back(first);
            }
            return ErrorCode::Success;
        }));
    auto checkpoint = std::async(std::launch::async, [&] {
        gate.wait();
        for (int i = 0; i < 3; ++i)
        {
            auto status = node->Checkpoint();
            if (status != ErrorCode::Success)
                return status;
        }
        return ErrorCode::Success;
    });
    start.set_value();
    for (auto &worker : workers)
        RequireSuccess(worker.get());
    RequireSuccess(checkpoint.get());
    for (int tenant = 0; tenant < 2; ++tenant)
    {
        auto combined = ids[tenant];
        combined.insert(combined.end(), ids[tenant + 2].begin(), ids[tenant + 2].end());
        std::sort(combined.begin(), combined.end());
        for (int i = 0; i < 16; ++i)
            BOOST_CHECK_EQUAL(combined[i], 128 + i);
    }
    node.reset();
    RequireSuccess(TenantIndexManager::Open(directory.root, limits, node));
    for (auto name : {"A", "B"})
    {
        SizeType first;
        RequireSuccess(node->Insert(name, Vectors(1, 4, 900), 1, first));
        BOOST_CHECK_EQUAL(first, 144);
    }
}

BOOST_AUTO_TEST_CASE(CacheLoadFailureReleasesReservationAndWakesWaiters)
{
    TemporaryNode directory;
    auto node = OpenNode(directory.root);
    RequireSuccess(node->Create("A", VectorValueType::Float, 4, DistCalcMethod::L2, Vectors(128, 4, 0), 128));
    {
        auto probe = std::make_shared<SnapshotProbe>();
        SnapshotFactory factory(probe);
        probe->failLoad = true;
        std::promise<void> start;
        auto gate = start.get_future().share();
        std::vector<std::future<ErrorCode>> readers;
        for (int i = 0; i < 4; ++i)
            readers.emplace_back(std::async(std::launch::async, [&] {
                gate.wait();
                std::vector<BasicResult> results;
                return node->Search("A", Vectors(1, 4, 0), 1, results);
            }));
        start.set_value();
        for (auto &reader : readers)
            BOOST_CHECK(reader.get() != ErrorCode::Success);
        TenantIndexManager::NodeStats state;
        std::vector<TenantIndexManager::TenantStats> tenants;
        node->GetStats(state, tenants);
        BOOST_CHECK(state.failed);
        BOOST_CHECK_EQUAL(state.residentTenants, 0);
        BOOST_CHECK_EQUAL(state.residentBytes, 0);
        BOOST_CHECK_EQUAL(tenants[0].activeRequests, 0);
    }
    node.reset();
    node = OpenNode(directory.root);
    std::vector<BasicResult> results;
    RequireSuccess(node->Search("A", Vectors(1, 4, 0), 1, results));
}

BOOST_AUTO_TEST_CASE(FailedDirtyEvictionRetainsRecoveryLog)
{
    TemporaryNode directory;
    TenantIndexManager::Limits limits;
    limits.maxResidentTenants = 1;
    std::unique_ptr<TenantIndexManager> node;
    RequireSuccess(TenantIndexManager::Open(directory.root, limits, node));
    for (auto name : {"A", "B"})
        RequireSuccess(node->Create(name, VectorValueType::Float, 4, DistCalcMethod::L2, Vectors(128, 4, 0), 128));
    SizeType first;
    RequireSuccess(node->Insert("A", Vectors(1, 4, 500), 1, first));
    std::vector<BasicResult> results;
    {
        auto probe = std::make_shared<SnapshotProbe>();
        SnapshotFactory factory(probe);
        probe->failSave = true;
        BOOST_CHECK(node->Search("B", Vectors(1, 4, 0), 1, results) != ErrorCode::Success);
        TenantIndexManager::NodeStats state;
        std::vector<TenantIndexManager::TenantStats> tenants;
        node->GetStats(state, tenants);
        BOOST_CHECK(state.failed);
        BOOST_CHECK_EQUAL(state.residentTenants, 1);
        BOOST_CHECK(tenants[0].dirty);
    }
    node.reset();
    RequireSuccess(TenantIndexManager::Open(directory.root, limits, node));
    RequireSuccess(node->Search("A", Vectors(1, 4, 500), 1, results));
    BOOST_CHECK_EQUAL(results[0].VID, first);
}

BOOST_AUTO_TEST_CASE(FullyDeletedSnapshotAndAdmissionLimits)
{
    TemporaryNode directory;
    TenantIndexManager::Limits limits;
    limits.maxTenants = 1;
    limits.maxVectorsPerTenant = 64;
    limits.maxDimension = 4;
    limits.maxResults = 5;
    std::unique_ptr<TenantIndexManager> node;
    RequireSuccess(TenantIndexManager::Open(directory.root, limits, node));
    auto seed = Vectors(64, 4, 0);
    RequireSuccess(node->Create("A", VectorValueType::Float, 4, DistCalcMethod::L2, seed, 64));
    BOOST_CHECK(node->Create("B", VectorValueType::Float, 4, DistCalcMethod::L2, seed, 64) != ErrorCode::Success);
    SizeType first;
    BOOST_CHECK(node->Insert("A", Vectors(1, 4, 0), 1, first) != ErrorCode::Success);
    std::vector<BasicResult> results;
    BOOST_CHECK(node->Search("A", Vectors(1, 4, 0), 6, results) != ErrorCode::Success);
    auto invalid = Vectors(1, 4, 0);
    reinterpret_cast<float *>(invalid.Data())[0] = std::numeric_limits<float>::quiet_NaN();
    BOOST_CHECK(node->Search("A", invalid, 1, results) != ErrorCode::Success);
    for (SizeType i = 0; i < 64; ++i)
        RequireSuccess(node->DeleteVector("A", i));
    RequireSuccess(node->Close());
    node.reset();
    RequireSuccess(TenantIndexManager::Open(directory.root, limits, node));
    RequireSuccess(node->Search("A", Vectors(1, 4, 0), 3, results));
    for (const auto &result : results)
        BOOST_CHECK_EQUAL(result.VID, -1);
}

BOOST_AUTO_TEST_CASE(CosineInputOwnershipAndIntegerTenant)
{
    TemporaryNode directory;
    auto node = OpenNode(directory.root);
    auto seed = Vectors(128, 4, 1);
    std::string before(reinterpret_cast<char *>(seed.Data()), seed.Length());
    RequireSuccess(node->Create("cos", VectorValueType::Float, 4, DistCalcMethod::Cosine, seed, 128));
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char *>(seed.Data()), seed.Length()), before);
    auto integers = ByteArray::Alloc(128 * 5);
    for (size_t i = 0; i < integers.Length(); ++i)
        integers[i] = static_cast<uint8_t>((i * 13 + i / 5) % 251);
    RequireSuccess(node->Create("int", VectorValueType::UInt8, 5, DistCalcMethod::L2, integers, 128));
    std::vector<BasicResult> results;
    RequireSuccess(node->Search("int", ByteArray(integers.Data(), 5, false), 1, results));
    BOOST_CHECK_EQUAL(results[0].VID, 0);
    RequireSuccess(node->Close());
    node.reset();
    node = OpenNode(directory.root);
    RequireSuccess(node->Search("cos", ByteArray(seed.Data(), 4 * sizeof(float), false), 1, results));
    BOOST_CHECK_SMALL(results[0].Dist, 0.0001f);
    RequireSuccess(node->Search("int", ByteArray(integers.Data(), 5, false), 1, results));
    BOOST_CHECK_EQUAL(results[0].VID, 0);
}

BOOST_AUTO_TEST_CASE(TrustedServerRoutesExactlyOneTenant)
{
    TemporaryNode directory;
    auto node = OpenNode(directory.root);
    RequireSuccess(node->Create("A", VectorValueType::Float, 4, DistCalcMethod::L2, Vectors(128, 4, 0), 128));
    RequireSuccess(node->Create("B", VectorValueType::Float, 4, DistCalcMethod::L2, Vectors(128, 4, 1000), 128));
    RequireSuccess(node->Close());
    node.reset();
    const auto config = directory.root + "/server.ini";
    {
        std::ofstream output(config);
        output << "[MultiTenant]\nRoot=" << directory.root << "\n";
    }
    auto service = std::make_shared<Service::ServiceContext>(config);
    BOOST_REQUIRE(service->IsInitialized());
    auto execute = [&](const std::string &query) {
        std::shared_ptr<Service::SearchExecutionContext> result;
        Service::SearchExecutor request(query, service, [&](auto response) { result = std::move(response); });
        request.Execute();
        return result;
    };
    auto response = execute("$tenantid:A $resultnum:1 0|0.01|0.02|0.03");
    BOOST_REQUIRE(response);
    BOOST_REQUIRE_EQUAL(response->GetResults().size(), 1);
    BOOST_CHECK_EQUAL(response->GetResults()[0].m_indexName, "A");
    BOOST_CHECK_EQUAL(response->GetResults()[0].m_results.GetResult(0)->VID, 0);
    for (const auto *bad : {"0|0.01|0.02|0.03", "$tenantid:missing 0|0|0|0", "$tenantid:A $indexname:B 0|0|0|0",
                            "$tenantid:A 0|0", "$tenantid:A $tenantid:B 0|0|0|0", "$tenantid:A $resultnum:1001 0|0|0|0",
                            "$tenantid:A $resultnum:invalid 0|0|0|0"})
        BOOST_CHECK(!execute(bad));
}

BOOST_AUTO_TEST_CASE(PublishedWatermarkSkipsOldLogsAndCorruptionFailsClosed)
{
    TemporaryNode directory;
    auto node = OpenNode(directory.root);
    RequireSuccess(node->Create("A", VectorValueType::Float, 4, DistCalcMethod::L2, Vectors(128, 4, 0), 128));
    SizeType first;
    RequireSuccess(node->Insert("A", Vectors(1, 4, 500), 1, first));
    node.reset();
    const auto openRecovery = [&] {
        std::shared_ptr<SharedRocksDB> engine;
        RequireSuccess(SharedRocksDB::Open(directory.root + "/recovery", {"recovery_log", "catalog", "recovery_meta"},
                                           engine, false, rocksdb::NewLRUCache(1 << 20)));
        return engine;
    };
    std::string logKey, logRecord;
    {
        auto recovery = openRecovery();
        RequireSuccess(recovery->Scan("recovery_log", "", [&](const auto &key, const auto &value) {
            logKey = key;
            logRecord = value;
            return ErrorCode::Success;
        }));
        BOOST_REQUIRE(!logKey.empty());
    }
    node = OpenNode(directory.root);
    RequireSuccess(node->Checkpoint());
    node.reset();
    {
        auto recovery = openRecovery();
        // Simulate a crash after publishing a checkpoint, before covered-log deletion.
        RequireSuccess(recovery->Put("recovery_log", logKey, logRecord));
    }
    node = OpenNode(directory.root);
    RequireSuccess(node->Insert("A", Vectors(1, 4, 600), 1, first));
    BOOST_CHECK_EQUAL(first, 129);
    node.reset();
    {
        auto recovery = openRecovery();
        auto prefix = logKey.substr(0, logKey.size() - 8);
        RequireSuccess(recovery->Put("recovery_log", prefix + SharedRocksDB::EncodeNumber(2), "truncated"));
    }
    BOOST_CHECK(TenantIndexManager::Open(directory.root, {}, node) != ErrorCode::Success);
    BOOST_CHECK(!node);
}

#ifndef _MSC_VER
BOOST_AUTO_TEST_CASE(ConcurrentCrashWriter)
{
    const char *root = std::getenv("SPTAG_TEST_CONCURRENT_CRASH_ROOT");
    if (!root)
        return;
    auto node = OpenNode(root);
    std::promise<void> start;
    auto gate = start.get_future().share();
    std::vector<std::future<ErrorCode>> writers;
    for (int i = 0; i < 4; ++i)
        writers.emplace_back(std::async(std::launch::async, [&, i] {
            gate.wait();
            SizeType id;
            return node->Insert("A", Vectors(32, 4, 500 + i * 100), 32, id);
        }));
    start.set_value();
    for (auto &writer : writers)
        RequireSuccess(writer.get());
    RequireSuccess(node->DeleteVector("A", 0));
    _exit(0);
}

BOOST_AUTO_TEST_CASE(ConcurrentProcessCrashAndRepeatedReplay)
{
    TemporaryNode directory;
    {
        auto node = OpenNode(directory.root);
        RequireSuccess(node->Create("A", VectorValueType::Float, 4, DistCalcMethod::L2, Vectors(128, 4, 0), 128));
        RequireSuccess(node->Close());
    }
    auto child = fork();
    BOOST_REQUIRE(child >= 0);
    if (child == 0)
    {
        setenv("SPTAG_TEST_CONCURRENT_CRASH_ROOT", directory.root.c_str(), 1);
        execl("/proc/self/exe", "/proc/self/exe", "--run_test=MultiTenant/ConcurrentCrashWriter", nullptr);
        _exit(127);
    }
    int status;
    BOOST_REQUIRE_EQUAL(waitpid(child, &status, 0), child);
    BOOST_REQUIRE(WIFEXITED(status));
    BOOST_REQUIRE_EQUAL(WEXITSTATUS(status), 0);
    std::vector<SizeType> firstIDs;
    for (int attempt = 0; attempt < 2; ++attempt)
    {
        auto node = OpenNode(directory.root);
        std::set<SizeType> unique;
        for (int i = 0; i < 4; ++i)
        {
            std::vector<BasicResult> results;
            RequireSuccess(node->Search("A", Vectors(1, 4, 500 + i * 100), 1, results));
            auto id = results[0].VID;
            BOOST_CHECK(id >= 128 && id < 256);
            unique.insert(id);
            if (attempt == 0)
                firstIDs.push_back(id);
            else
                BOOST_CHECK_EQUAL(id, firstIDs[i]);
        }
        BOOST_CHECK_EQUAL(unique.size(), 4);
        BOOST_CHECK(node->DeleteVector("A", 0) == ErrorCode::VectorNotFound);
    }
}

BOOST_AUTO_TEST_CASE(CrashWriter)
{
    const char *root = std::getenv("SPTAG_TEST_CRASH_ROOT");
    if (!root)
        return;
    auto node = OpenNode(root);
    SizeType id;
    RequireSuccess(node->Insert("A", Vectors(1, 4, 500), 1, id));
    BOOST_REQUIRE_EQUAL(id, 128);
    RequireSuccess(node->DeleteVector("A", 0));
    _exit(0);
}

BOOST_AUTO_TEST_CASE(ProcessCrashAndRepeatedReplay)
{
    TemporaryNode directory;
    {
        auto node = OpenNode(directory.root);
        RequireSuccess(node->Create("A", VectorValueType::Float, 4, DistCalcMethod::L2, Vectors(128, 4, 0), 128));
        RequireSuccess(node->Close());
    }
    auto child = fork();
    BOOST_REQUIRE(child >= 0);
    if (child == 0)
    {
        setenv("SPTAG_TEST_CRASH_ROOT", directory.root.c_str(), 1);
        execl("/proc/self/exe", "/proc/self/exe", "--run_test=MultiTenant/CrashWriter", nullptr);
        _exit(127);
    }
    int status;
    BOOST_REQUIRE_EQUAL(waitpid(child, &status, 0), child);
    BOOST_REQUIRE(WIFEXITED(status));
    BOOST_REQUIRE_EQUAL(WEXITSTATUS(status), 0);
    for (int attempt = 0; attempt < 2; ++attempt)
    {
        auto node = OpenNode(directory.root);
        std::vector<BasicResult> results;
        RequireSuccess(node->Search("A", Vectors(1, 4, 500), 3, results));
        BOOST_CHECK_EQUAL(results[0].VID, 128);
        RequireSuccess(node->Search("A", Vectors(1, 4, 0), 3, results));
        for (const auto &result : results)
            BOOST_CHECK_NE(result.VID, 0);
        // No Close(): reopening must replay the original log against a fresh checkpoint copy.
    }
}
#endif

BOOST_AUTO_TEST_SUITE_END()
#endif
