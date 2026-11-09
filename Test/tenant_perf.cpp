// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

// Standalone in-process benchmark; compile the same source against both revisions.
#include "inc/Core/VectorIndex.h"
#include "inc/Helper/SimpleIniReader.h"
#ifdef TENANT_BENCH_CURRENT
#include "inc/Core/SPANN/ExtraRocksDBController.h"
#include "inc/Core/SPANN/TenantIndexManager.h"
#endif
#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <mutex>
#include <random>
#include <sys/resource.h>
#include <thread>

using namespace SPTAG;
using Clock = std::chrono::steady_clock;

namespace
{
void Check(ErrorCode status)
{
    if (status != ErrorCode::Success)
        throw std::runtime_error("SPTAG status " + std::to_string(static_cast<int>(status)));
}

uint64_t EnvironmentNumber(const char *name, uint64_t fallback)
{
    const char *value = std::getenv(name);
    if (!value)
        return fallback;
    std::string text(value);
    size_t end = 0;
    auto result = std::stoull(text, &end);
    if (text.empty() || text[0] == '-' || end != text.size())
        throw std::runtime_error(std::string("Invalid ") + name);
    return result;
}

const Helper::IniReader *BenchmarkProfile()
{
    static Helper::IniReader profile;
    static const char *path = std::getenv("TENANT_PERF_CONFIG");
    static const bool loaded = [&] {
        if (path)
            Check(profile.LoadIniFile(path));
        return true;
    }();
    return loaded && path ? &profile : nullptr;
}

#ifdef TENANT_BENCH_CURRENT
SPANN::TenantIndexManager::Limits BenchmarkLimits(int slots)
{
    SPANN::TenantIndexManager::Limits limits;
    limits.maxTenants = 128;
    limits.maxResidentTenants = slots;
    auto cacheMiB = EnvironmentNumber("TENANT_PERF_CACHE_MIB", limits.cacheBytes >> 20);
    if (cacheMiB < 1 || cacheMiB > 4096)
        throw std::runtime_error("Cache budget must be between 1 and 4096 MiB");
    limits.cacheBytes = cacheMiB << 20;
    limits.maxResidentBytes = EnvironmentNumber("TENANT_PERF_RESIDENT_BYTES", 0);
    return limits;
}
#endif

struct Vectors
{
    int32_t count = 0, dimension = 0;
    ByteArray data;
    explicit Vectors(const std::string &path)
    {
        std::ifstream in(path, std::ios::binary);
        in.read(reinterpret_cast<char *>(&count), 4);
        in.read(reinterpret_cast<char *>(&dimension), 4);
        if (!in || count <= 0 || dimension != 128 ||
            std::filesystem::file_size(path) != 8 + uint64_t(count) * dimension)
            throw std::runtime_error("Expected DEFAULT UInt8 128D input: " + path);
        data = ByteArray::Alloc(uint64_t(count) * dimension);
        in.read(reinterpret_cast<char *>(data.Data()), data.Length());
        if (!in)
            throw std::runtime_error("Incomplete input: " + path);
    }
    ByteArray Row(int id, int rows = 1) const
    {
        if (id < 0 || rows < 1 || int64_t(id) + rows > count)
            throw std::runtime_error("Vector range out of bounds");
        return ByteArray(data.Data() + uint64_t(id) * dimension, uint64_t(rows) * dimension, false);
    }
};

struct Engine
{
    std::shared_ptr<VectorIndex> index;
#ifdef TENANT_BENCH_CURRENT
    std::shared_ptr<SPANN::SharedRocksDB> directState, directRecovery;
    std::map<std::string, std::shared_ptr<VectorIndex>> directIndexes;
    std::unique_ptr<SPANN::TenantIndexManager> node;
#endif
    void Open(const std::string &mode, const std::string &root, int slots)
    {
        if (mode == "legacy")
            Check(VectorIndex::LoadIndex(root, index));
#ifdef TENANT_BENCH_CURRENT
        else if (mode == "tenant" || mode == "direct")
        {
            auto limits = BenchmarkLimits(slots);
            Check(SPANN::TenantIndexManager::Open(root, limits, node));
            if (mode == "direct")
            {
                SPANN::TenantIndexManager::NodeStats state;
                std::vector<SPANN::TenantIndexManager::TenantStats> tenants;
                node->GetStats(state, tenants);
                if (tenants.empty() || size_t(slots) < tenants.size() || limits.maxResidentBytes)
                    throw std::runtime_error("Direct control requires all tenants resident without a byte limit");
                std::filesystem::path work;
                for (const auto &entry : std::filesystem::directory_iterator(root))
                    if (entry.is_directory() && entry.path().filename().string().rfind("work-", 0) == 0)
                    {
                        if (!work.empty())
                            throw std::runtime_error("Ambiguous recovered work directory");
                        work = entry.path();
                    }
                if (work.empty())
                    throw std::runtime_error("Missing recovered work directory");
                Check(node->Close());
                node.reset();
                auto cache = std::shared_ptr<rocksdb::Cache>(rocksdb::NewLRUCache(limits.cacheBytes));
                Check(SPANN::SharedRocksDB::Open(root + "/recovery", {"recovery_log", "catalog", "recovery_meta"},
                                                 directRecovery, false, cache));
                Check(SPANN::SharedRocksDB::Open((work / "state").string(),
                                                 {"postings", "head_snapshots", "checkpoint_meta"}, directState, false,
                                                 cache));
                for (const auto &tenant : tenants)
                    Check(VectorIndex::LoadIndex((work / ("index-" + std::to_string(tenant.indexID))).string(),
                                                 directIndexes[tenant.name],
                                                 directState->PostingView(tenant.name, tenant.indexID)));
            }
        }
#endif
        else
            throw std::runtime_error("Unsupported mode: " + mode);
    }
    ErrorCode Search(const std::string &name, const ByteArray &query, std::vector<BasicResult> &results)
    {
#ifdef TENANT_BENCH_CURRENT
        if (node)
            return node->Search(name, query, 5, results, VectorValueType::UInt8, 128);
        if (!directIndexes.empty())
        {
            auto found = directIndexes.find(name);
            if (found == directIndexes.end())
                return ErrorCode::VectorNotFound;
            QueryResult q(query.Data(), 5, false);
            auto status = found->second->SearchIndex(q);
            results.assign(q.GetResults(), q.GetResults() + 5);
            return status;
        }
#endif
        QueryResult q(query.Data(), 5, false);
        auto status = index->SearchIndex(q);
        results.assign(q.GetResults(), q.GetResults() + 5);
        return status;
    }
    void Stats(std::ostream &out)
    {
#ifdef TENANT_BENCH_CURRENT
        if (node)
        {
            SPANN::TenantIndexManager::NodeStats state;
            std::vector<SPANN::TenantIndexManager::TenantStats> tenants;
            node->GetStats(state, tenants);
            out << "node," << state.residentTenants << "," << state.maxResidentTenants << "," << state.residentBytes
                << "," << state.maxResidentBytes << "," << state.overByteTarget << "," << state.failed << "\n";
            for (const auto &t : tenants)
                out << "cache," << t.name << "," << t.cacheHits << "," << t.cacheMisses << "," << t.loads << ","
                    << t.evictions << "," << t.snapshotBytes << "," << t.lastLoadMs << "," << t.lastEvictionMs << "\n";
        }
#endif
    }
};

void BuildLegacy(const std::string &root, const Vectors &vectors, int count)
{
    if (std::filesystem::exists(root))
        throw std::runtime_error("Build requires a new root");
    std::filesystem::create_directories(root);
    auto index = VectorIndex::CreateInstance(IndexAlgoType::SPANN, VectorValueType::UInt8);
    if (const auto *profile = BenchmarkProfile())
        for (const auto *section : {"Base", "SelectHead", "BuildHead", "BuildSSDIndex"})
            for (const auto &parameter : profile->GetParameters(section))
                Check(index->SetParameter(parameter.first.c_str(), parameter.second.c_str(), section));
    const auto set = [&](const char *section, const char *key, const std::string &value) {
        Check(index->SetParameter(key, value.c_str(), section));
    };
    // Use SPANN defaults apart from the tenant-compatible build and lifecycle settings.
    set("Base", "ValueType", "UInt8");
    set("Base", "Dim", "128");
    set("Base", "DistCalcMethod", "L2");
    set("Base", "IndexDirectory", root);
    set("Base", "DataCapacity", "1000000");
    set("SelectHead", "isExecute", "true");
    set("BuildHead", "isExecute", "true");
    set("BuildHead", "DataCapacity", "1000000");
    set("BuildHead", "DeletePercentageForRefine", "2");
    set("BuildSSDIndex", "isExecute", "true");
    set("BuildSSDIndex", "BuildSsdIndex", "true");
    set("BuildSSDIndex", "TmpDir", root);
    set("BuildSSDIndex", "Storage", "ROCKSDBIO");
    set("BuildSSDIndex", "Update", "true");
    // Read-only measurements must not trigger background merges.
    set("BuildSSDIndex", "AsyncMergeInSearch", "false");
    Check(index->BuildIndex(vectors.Row(0, count).Data(), count, 128));
    Check(index->SaveIndex(root));
}

double CpuSeconds(const rusage &r)
{
    return r.ru_utime.tv_sec + r.ru_utime.tv_usec * 1e-6 + r.ru_stime.tv_sec + r.ru_stime.tv_usec * 1e-6;
}

#ifdef TENANT_BENCH_CURRENT
void ClonePrivate(const std::string &root, const std::string &destination, const std::string &name)
{
    if (std::filesystem::exists(destination))
        throw std::runtime_error("Private clone requires a new root");
    Engine engine;
    engine.Open("tenant", root, 128);
    SPANN::TenantIndexManager::NodeStats state;
    std::vector<SPANN::TenantIndexManager::TenantStats> tenants;
    engine.node->GetStats(state, tenants);
    auto found = std::find_if(tenants.begin(), tenants.end(), [&](const auto &tenant) { return tenant.name == name; });
    if (found == tenants.end())
        throw std::runtime_error("Missing tenant to clone");
    auto prefix = SPANN::SharedRocksDB::Namespace(name, found->indexID);
    Check(engine.node->Close());
    engine.node.reset();
    engine.Open("direct", root, 128);
    auto index = engine.directIndexes.at(name);
    Check(index->SetParameter("IndexDirectory", destination.c_str(), "Base"));
    Check(index->SetParameter("TmpDir", destination.c_str(), "BuildSSDIndex"));
    std::filesystem::create_directories(destination);
    Check(index->SaveIndex(destination, nullptr, true));
    auto kv = destination + "/" + index->GetParameter("KVFile", "BuildSSDIndex") + "_0";
    SPANN::RocksDBIO privateStore(kv.c_str(), false);
    if (!privateStore.Available())
        throw std::runtime_error("Cannot open private RocksDB");
    uint64_t postings = 0, bytes = 0;
    Check(engine.directState->Scan("postings", prefix, [&](const std::string &key, const std::string &value) {
        uint64_t postingID;
        if (!SPANN::SharedRocksDB::DecodeNumber(key.substr(prefix.size()), postingID) || postingID > MaxSize)
            throw std::runtime_error("Invalid source posting key");
        // Match upstream construction; Put can change RocksDB's SST/blob placement.
        int mergedBytes = 0;
        Check(privateStore.MergeWithCAS(SizeType(postingID), value, std::chrono::microseconds::max(), nullptr,
                                        mergedBytes));
        if (mergedBytes < 0 || size_t(mergedBytes) != value.size())
            throw std::runtime_error("Private clone changed posting size");
        ++postings;
        bytes += value.size();
        return ErrorCode::Success;
    }));
    std::cout << "cloned_postings," << postings << "," << bytes << "\n";
}

void BuildMany(const std::string &root, const std::string &manifest)
{
    if (std::filesystem::exists(root))
        throw std::runtime_error("Build requires a new root");
    std::ifstream input(manifest);
    if (!input)
        throw std::runtime_error("Cannot open tenant input manifest");
    Engine engine;
    engine.Open("tenant", root, 8);
    std::string name, path;
    int count, tenants = 0;
    auto started = Clock::now();
    while (input >> name)
    {
        if (!(input >> path >> count))
            throw std::runtime_error("Malformed tenant input manifest");
        Vectors vectors(path);
        auto start = Clock::now();
        Check(engine.node->Create(name, VectorValueType::UInt8, 128, DistCalcMethod::L2, vectors.Row(0, count), count,
                                  IndexAlgoType::Undefined, 0, BenchmarkProfile()));
        std::cout << "build," << name << "," << count << ","
                  << std::chrono::duration<double>(Clock::now() - start).count() << std::endl;
        ++tenants;
    }
    if (!input.eof() || !tenants)
        throw std::runtime_error("Malformed or empty tenant input manifest");
    Check(engine.node->Close());
    std::cout << "build_seconds," << std::chrono::duration<double>(Clock::now() - started).count() << std::endl;
}

void Updates(int argc, char **argv)
{
    if (argc != 11)
        throw std::runtime_error("updates ROOT VECTORS QUERIES OUT INITIAL TOTAL BATCH WRITERS READERS");
    const std::string root = argv[2], output = argv[5];
    Vectors vectors(argv[3]), queries(argv[4]);
    int initial = std::stoi(argv[6]), total = std::stoi(argv[7]), batch = std::stoi(argv[8]);
    int writers = std::stoi(argv[9]), readers = std::stoi(argv[10]);
    if (initial < 64 || total < 1 || batch < 1 || total % batch || writers < 1 || readers < 0 || writers > 16 ||
        readers > 16 || int64_t(initial) + total > vectors.count || std::filesystem::exists(output))
        throw std::runtime_error("Invalid update workload or existing output");
    std::filesystem::create_directories(output);
    Engine engine;
    engine.Open("tenant", root, 8);
    Check(engine.node->Create("only", VectorValueType::UInt8, 128, DistCalcMethod::L2, vectors.Row(0, initial), initial,
                              IndexAlgoType::Undefined, 0, BenchmarkProfile()));
    std::ofstream metrics(output + "/updates.csv"), mapping(output + "/id-map.csv");
    std::ofstream batches(output + "/query-batches.csv"), reads(output + "/read-timeline.csv"),
        writes(output + "/write-timeline.csv");
    metrics.exceptions(std::ios::badbit | std::ios::failbit);
    mapping.exceptions(std::ios::badbit | std::ios::failbit);
    for (auto *stream : {&batches, &reads, &writes})
    {
        stream->exceptions(std::ios::badbit | std::ios::failbit);
        *stream << std::setprecision(10);
    }
    metrics << "phase,count,insert_s,checkpoint_s,vectors_per_s,query_count,query_errors,p50_ms,p95_ms,p99_ms\n";
    mapping << "input_id,assigned_id\n";
    batches << "phase,queries,threads,elapsed_s,qps,p50_ms,p95_ms,p99_ms\n";
    reads << "phase,thread,start_ms,end_ms\n";
    writes << "phase,input_id,assigned_id,start_ms,end_ms\n";
    std::vector<int> sourceID(initial + total, -1);
    for (int i = 0; i < initial; ++i)
        sourceID[i] = i;
    auto accuracy = [&](int phase) {
        std::ofstream out(output + "/phase-" + std::to_string(phase) + ".csv");
        out.exceptions(std::ios::badbit | std::ios::failbit);
        out << std::setprecision(10) << "tenant,query,rank,id,distance\n";
        const auto *profile = BenchmarkProfile();
        int threads = profile ? profile->GetParameter("Benchmark", "NumSearchThreads", 1) : 1;
        if (threads < 1 || threads > 128)
            throw std::runtime_error("Invalid query-driver thread count");
        std::atomic<int> next{0}, errors{0};
        std::vector<std::vector<BasicResult>> results(queries.count);
        std::vector<double> latencies(queries.count);
        std::vector<std::thread> workers;
        auto started = Clock::now();
        for (int t = 0; t < threads; ++t)
            workers.emplace_back([&] {
                for (int q = next++; q < queries.count; q = next++)
                {
                    auto start = Clock::now();
                    if (engine.Search("only", queries.Row(q), results[q]) != ErrorCode::Success)
                        ++errors;
                    latencies[q] = std::chrono::duration<double, std::milli>(Clock::now() - start).count();
                }
            });
        for (auto &worker : workers)
            worker.join();
        auto elapsed = std::chrono::duration<double>(Clock::now() - started).count();
        if (errors)
            throw std::runtime_error("Accuracy query failures");
        for (int q = 0; q < queries.count; ++q)
        {
            for (int k = 0; k < int(results[q].size()); ++k)
            {
                int vid = results[q][k].VID;
                if (vid >= int(sourceID.size()) || (vid >= 0 && sourceID[vid] < 0))
                    throw std::runtime_error("Unexpected inserted VID");
                out << "only," << q << "," << k << "," << (vid < 0 ? -1 : sourceID[vid]) << "," << results[q][k].Dist
                    << "\n";
            }
        }
        std::sort(latencies.begin(), latencies.end());
        batches << phase << "," << queries.count << "," << threads << "," << elapsed << "," << queries.count / elapsed
                << "," << latencies[size_t(.5 * latencies.size())] << "," << latencies[size_t(.95 * latencies.size())]
                << "," << latencies[size_t(.99 * latencies.size())] << "\n";
        batches.flush();
    };
    accuracy(0);
    for (int phase = 1; phase <= total / batch; ++phase)
    {
        std::atomic<bool> stop{false};
        std::atomic<int> next{0}, errors{0};
        std::vector<std::vector<double>> latency(readers);
        std::vector<std::vector<std::pair<double, double>>> intervals(readers);
        std::vector<std::pair<double, double>> acknowledgements(batch);
        std::vector<uint64_t> queryErrors(readers, 0);
        std::vector<std::thread> searchers, inserters;
        std::vector<int> assigned(batch, -1);
        auto start = Clock::now();
        for (int t = 0; t < readers; ++t)
            searchers.emplace_back([&, t] {
                std::mt19937 rng(20261104 + t);
                std::vector<BasicResult> results;
                while (!stop)
                {
                    auto start = Clock::now();
                    auto status = engine.Search("only", queries.Row(rng() % queries.count), results);
                    if (status == ErrorCode::Success)
                    {
                        auto end = Clock::now();
                        latency[t].push_back(std::chrono::duration<double, std::milli>(end - start).count());
                        intervals[t].emplace_back(
                            std::chrono::duration<double, std::milli>(start.time_since_epoch()).count(),
                            std::chrono::duration<double, std::milli>(end.time_since_epoch()).count());
                    }
                    else
                        ++queryErrors[t];
                }
            });
        for (int t = 0; t < writers; ++t)
            inserters.emplace_back([&] {
                for (int offset = next++; offset < batch; offset = next++)
                {
                    SizeType id = -1;
                    auto before = Clock::now();
                    auto status = engine.node->Insert("only", vectors.Row(initial + (phase - 1) * batch + offset), 1,
                                                      id, VectorValueType::UInt8, 128);
                    acknowledgements[offset] = {
                        std::chrono::duration<double, std::milli>(before - start).count(),
                        std::chrono::duration<double, std::milli>(Clock::now() - start).count()};
                    assigned[offset] = id;
                    if (status != ErrorCode::Success)
                        ++errors;
                }
            });
        for (auto &t : inserters)
            t.join();
        auto inserted = Clock::now();
        auto checkpointStatus = engine.node->Checkpoint();
        auto checkpointed = Clock::now();
        stop = true;
        for (auto &t : searchers)
            t.join();
        Check(checkpointStatus);
        if (errors)
            throw std::runtime_error("Insertion failures");
        for (int offset = 0; offset < batch; ++offset)
        {
            auto id = assigned[offset];
            if (id < initial || id >= int(sourceID.size()) || sourceID[id] >= 0)
                throw std::runtime_error("Invalid or duplicated reserved VID");
            auto input = initial + (phase - 1) * batch + offset;
            sourceID[id] = input;
            mapping << input << "," << id << "\n";
            writes << phase << "," << input << "," << id << "," << acknowledgements[offset].first << ","
                   << acknowledgements[offset].second << "\n";
        }
        mapping.flush();
        std::vector<double> all;
        uint64_t failedQueries = 0;
        for (int t = 0; t < readers; ++t)
        {
            all.insert(all.end(), latency[t].begin(), latency[t].end());
            failedQueries += queryErrors[t];
            const auto origin = std::chrono::duration<double, std::milli>(start.time_since_epoch()).count();
            for (const auto &interval : intervals[t])
                reads << phase << "," << t << "," << interval.first - origin << "," << interval.second - origin << "\n";
        }
        reads.flush();
        writes.flush();
        std::sort(all.begin(), all.end());
        auto percentile = [&](double p) {
            return all.empty() ? -1 : all[std::min(all.size() - 1, size_t(p * all.size()))];
        };
        auto insertSeconds = std::chrono::duration<double>(inserted - start).count();
        metrics << phase << "," << initial + phase * batch << "," << insertSeconds << ","
                << std::chrono::duration<double>(checkpointed - inserted).count() << "," << batch / insertSeconds << ","
                << all.size() << "," << failedQueries << "," << percentile(.5) << "," << percentile(.95) << ","
                << percentile(.99) << "\n";
        metrics.flush();
        if (failedQueries)
            throw std::runtime_error("Concurrent query failures");
        accuracy(phase);
    }
    Check(engine.node->Close());
    engine.node.reset();
    engine.Open("tenant", root, 8);
    accuracy(total / batch + 1);
    Check(engine.node->Close());
}
#endif

void Measure(Engine &engine, const Vectors &queries, const std::vector<std::string> &names, const std::string &mix,
             int threads, double warmup, double seconds, std::ostream &out)
{
    struct Row
    {
        int tenant;
        double ms;
        bool success, inWindow;
    };
    std::array<std::vector<std::vector<Row>>, 2> rows;
    for (auto &phase : rows)
        phase.resize(threads);
    std::mutex mutex;
    std::condition_variable changed;
    int ready = 0, finished = 0, go = -1;
    Clock::time_point stop;
    std::vector<std::thread> workers;
    bool failed = false;
    for (int t = 0; t < threads; ++t)
        workers.emplace_back([&, t] {
            std::mt19937 rng(20261104 + t);
            std::vector<double> weights(names.size(), 1);
            if (mix == "hot" && names.size() > 1)
                weights[0] = (7.0 / 3) * (names.size() - 1);
            std::discrete_distribution<int> tenantID(weights.begin(), weights.end());
            std::uniform_int_distribution<int> queryID(0, queries.count - 1);
            std::vector<BasicResult> results;
            {
                std::lock_guard<std::mutex> lock(mutex);
                ++ready;
            }
            changed.notify_all();
            for (int phase = 0; phase < 2; ++phase)
            {
                {
                    std::unique_lock<std::mutex> lock(mutex);
                    changed.wait(lock, [&] { return go == phase; });
                }
                rng.seed(20261104 + t);
                while (Clock::now() < stop)
                {
                    auto tenant = tenantID(rng);
                    auto query = queries.Row(queryID(rng));
                    auto start = Clock::now();
                    auto status = engine.Search(names[tenant], query, results);
                    auto end = Clock::now();
                    rows[phase][t].push_back({tenant, std::chrono::duration<double, std::milli>(end - start).count(),
                                              status == ErrorCode::Success, end <= stop});
                }
                {
                    std::lock_guard<std::mutex> lock(mutex);
                    ++finished;
                }
                changed.notify_all();
            }
        });
    {
        std::unique_lock<std::mutex> lock(mutex);
        changed.wait(lock, [&] { return ready == threads; });
    }
    for (int phase = 0; phase < 2; ++phase)
    {
        double duration = phase == 0 ? warmup : seconds;
        rusage before{}, after{};
        getrusage(RUSAGE_SELF, &before);
        auto start = Clock::now();
        {
            std::lock_guard<std::mutex> lock(mutex);
            stop = start + std::chrono::duration_cast<Clock::duration>(std::chrono::duration<double>(duration));
            go = phase;
        }
        changed.notify_all();
        {
            std::unique_lock<std::mutex> lock(mutex);
            changed.wait(lock, [&] { return finished == (phase + 1) * threads; });
        }
        if (phase == 1)
            for (auto &worker : workers)
                worker.join();
        getrusage(RUSAGE_SELF, &after);
        double elapsed = std::chrono::duration<double>(Clock::now() - start).count();
        out << (phase == 0 ? "# warmup\n" : "# measured\n");
        for (int tenant = -1; tenant < int(names.size()); ++tenant)
        {
            std::vector<double> latencies;
            uint64_t completed = 0, errors = 0;
            for (const auto &thread : rows[phase])
                for (const auto &r : thread)
                    if (tenant < 0 || tenant == r.tenant)
                    {
                        if (r.success)
                        {
                            latencies.push_back(r.ms);
                            completed += r.inWindow;
                        }
                        else
                            ++errors;
                    }
            std::sort(latencies.begin(), latencies.end());
            failed = failed || errors != 0;
            auto percentile = [&](double p) {
                return latencies.empty() ? -1 : latencies[std::min(latencies.size() - 1, size_t(p * latencies.size()))];
            };
            out << "query," << (tenant < 0 ? "all" : names[tenant]) << "," << threads << "," << duration << ","
                << completed << "," << errors << "," << completed / duration << "," << percentile(.5) << ","
                << percentile(.95) << "," << percentile(.99) << "," << elapsed << ","
                << (CpuSeconds(after) - CpuSeconds(before)) / elapsed << "," << after.ru_maxrss << "\n";
        }
        engine.Stats(out);
        out.flush();
        rows[phase].clear();
    }
    if (failed)
        throw std::runtime_error("Query failures; see the result CSV");
}
} // namespace

int main(int argc, char **argv)
{
    try
    {
        if (argc < 2)
            throw std::runtime_error("build legacy|tenant ROOT VECTORS COUNT NAME; "
                                     "accuracy MODE ROOT QUERIES OUT SLOTS NAME...; "
                                     "query MODE ROOT QUERIES OUT SLOTS THREADS SECONDS WARMUP uniform|hot NAME...");
        const std::string command = argv[1];
#ifdef TENANT_BENCH_CURRENT
        if (command == "clone-private" && argc == 5)
        {
            ClonePrivate(argv[2], argv[3], argv[4]);
            return 0;
        }
        if (command == "build-many" && argc == 4)
        {
            BuildMany(argv[2], argv[3]);
            return 0;
        }
        if (command == "updates")
        {
            Updates(argc, argv);
            return 0;
        }
#endif
        if (command == "build" && argc == 7)
        {
            Vectors vectors(argv[4]);
            int count = std::stoi(argv[5]);
            vectors.Row(0, count);
            auto start = Clock::now();
            if (std::string(argv[2]) == "legacy")
                BuildLegacy(argv[3], vectors, count);
#ifdef TENANT_BENCH_CURRENT
            else if (std::string(argv[2]) == "tenant")
            {
                Engine engine;
                engine.Open("tenant", argv[3], 8);
                Check(engine.node->Create(argv[6], VectorValueType::UInt8, 128, DistCalcMethod::L2,
                                          vectors.Row(0, count), count, IndexAlgoType::Undefined, 0,
                                          BenchmarkProfile()));
                Check(engine.node->Close());
            }
#endif
            else
                throw std::runtime_error("Unsupported builder");
            std::cout << "build_seconds," << std::chrono::duration<double>(Clock::now() - start).count() << "\n";
            return 0;
        }
        if ((command != "accuracy" && command != "query") || argc < (command == "accuracy" ? 8 : 12))
            throw std::runtime_error("Invalid arguments");
        if (std::string(argv[2]) == "legacy" && argc != (command == "accuracy" ? 8 : 12))
            throw std::runtime_error("Legacy mode has one index and requires exactly one name");
        int slots = std::stoi(argv[6]);
        if (slots < 1)
            throw std::runtime_error("Slots must be positive");
        Vectors queries(argv[4]);
        Engine engine;
        engine.Open(argv[2], argv[3], slots);
        if (std::filesystem::exists(argv[5]))
            throw std::runtime_error("Refusing to overwrite result");
        std::ofstream out(argv[5]);
        out.exceptions(std::ios::badbit | std::ios::failbit);
        out << std::setprecision(10);
        if (command == "accuracy")
        {
            out << "tenant,query,rank,id,distance\n";
            for (int name = 7; name < argc; ++name)
                for (int q = 0; q < queries.count; ++q)
                {
                    std::vector<BasicResult> results;
                    Check(engine.Search(argv[name], queries.Row(q), results));
                    for (int k = 0; k < int(results.size()); ++k)
                        out << argv[name] << "," << q << "," << k << "," << results[k].VID << "," << results[k].Dist
                            << "\n";
                }
        }
        else
        {
            int threads = std::stoi(argv[7]);
            double seconds = std::stod(argv[8]), warmup = std::stod(argv[9]);
            std::string mix = argv[10];
            if (threads < 1 || threads > 128 || seconds <= 0 || warmup <= 0 || (mix != "uniform" && mix != "hot"))
                throw std::runtime_error("Invalid measurement settings");
            std::vector<std::string> names(argv + 11, argv + argc);
            out << "# query,name,threads,seconds,completed_in_window,errors,qps,p50_ms,p95_ms,p99_ms,"
                   "elapsed_with_drain_s,cpu_cores,process_peak_rss_kib\n"
                   "# cache,name,hits,misses,loads,evictions,snapshot_bytes,last_load_ms,last_eviction_ms\n";
            if (size_t(slots) >= names.size())
            {
                auto start = Clock::now();
                std::vector<BasicResult> results;
                for (const auto &name : names)
                    Check(engine.Search(name, queries.Row(0), results));
                out << "# prefill_seconds," << std::chrono::duration<double>(Clock::now() - start).count() << "\n";
            }
            Measure(engine, queries, names, mix, threads, warmup, seconds, out);
        }
        return 0;
    }
    catch (const std::exception &error)
    {
        std::cerr << "Benchmark failed: " << error.what() << "\n";
        return 1;
    }
}
