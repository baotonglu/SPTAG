// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/Core/SPANN/TenantIndexManager.h"
#include "inc/Core/SPANN/UpdateContext.h"
#ifdef ROCKSDB
#include <fstream>
#include <set>

namespace SPTAG::SPANN
{
namespace
{
const std::vector<std::string> StateFamilies{"postings", "head_snapshots", "checkpoint_meta"};
const std::vector<std::string> RecoveryFamilies{"recovery_log", "catalog", "recovery_meta"};
constexpr size_t ChunkSize = 1 << 20;

ErrorCode Failure(const char *message, ErrorCode status = ErrorCode::Fail)
{
    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Tenant manager: %s\n", message);
    return status;
}

ErrorCode MakeDirectory(const std::string &path)
{
    std::error_code error;
    std::filesystem::create_directories(path, error);
    return error ? Failure(error.message().c_str(), ErrorCode::DiskIOFail) : ErrorCode::Success;
}

void Number(std::string &output, std::uint64_t number)
{
    output += SharedRocksDB::EncodeNumber(number);
}
void String(std::string &output, const std::string &value)
{
    Number(output, value.size());
    output += value;
}

class Reader
{
  public:
    explicit Reader(const std::string &data) : m_data(data)
    {
    }
    bool Number(std::uint64_t &value)
    {
        if (m_data.size() - m_pos < 8)
            return false;
        bool result = SharedRocksDB::DecodeNumber(m_data.substr(m_pos, 8), value);
        m_pos += 8;
        return result;
    }
    bool String(std::string &value)
    {
        std::uint64_t size;
        if (!Number(size) || size > m_data.size() - m_pos)
            return false;
        value.assign(m_data, m_pos, static_cast<size_t>(size));
        m_pos += static_cast<size_t>(size);
        return true;
    }
    bool End() const
    {
        return m_pos == m_data.size();
    }

  private:
    const std::string &m_data;
    size_t m_pos = 0;
};

std::string FileKey(const std::string &prefix, const std::string &file, std::uint64_t chunk)
{
    std::string key = prefix;
    String(key, file);
    Number(key, chunk);
    return key;
}

bool SafeRelativePath(const std::string &file)
{
    std::filesystem::path path(file);
    if (file.empty() || file.find('\0') != std::string::npos || path.is_absolute())
        return false;
    for (const auto &component : path)
        if (component == ".." || component == ".")
            return false;
    return true;
}
} // namespace

TenantIndexManager::TenantIndexManager(std::string root, Limits limits)
    : m_root(std::move(root)), m_limits(limits), m_cache(rocksdb::NewLRUCache(limits.cacheBytes))
{
}

TenantIndexManager::~TenantIndexManager()
{
    // Destruction does not publish a checkpoint: the last acknowledged log must remain replayable.
    for (auto &entry : m_tenants)
        if (entry.second->index)
            entry.second->index->DrainUpdates();
}

ErrorCode TenantIndexManager::Open(const std::string &root, const Limits &limits,
                                   std::unique_ptr<TenantIndexManager> &result)
{
    result.reset();
    if (root.empty() || !limits.maxTenants || limits.maxVectorsPerTenant < 64 || limits.maxDimension <= 0 ||
        limits.maxResults <= 0 || !limits.cacheBytes || !limits.maxResidentTenants)
        return Failure("Invalid node limits", ErrorCode::FailedParseValue);
    auto manager = std::unique_ptr<TenantIndexManager>(new TenantIndexManager(root, limits));
    auto status = manager->Initialize();
    if (status != ErrorCode::Success)
        return status;
    result = std::move(manager);
    return ErrorCode::Success;
}

ErrorCode TenantIndexManager::AllocateID(std::uint64_t &id)
{
    std::lock_guard<std::mutex> allocation(m_idMutex);
    std::string value;
    auto status = m_recovery->Get("recovery_meta", "next_id", value);
    id = 1;
    if (status == ErrorCode::Success)
    {
        if (!SharedRocksDB::DecodeNumber(value, id) || id == 0 || id == UINT64_MAX)
            return Failure("Invalid or exhausted node ID counter");
    }
    else if (status != ErrorCode::Key_NotFound)
        return status;
    return m_recovery->Put("recovery_meta", "next_id", SharedRocksDB::EncodeNumber(id + 1));
}

ErrorCode TenantIndexManager::Initialize()
{
    auto status = MakeDirectory(m_root);
    if (status != ErrorCode::Success)
        return status;
    std::error_code error;
    const bool existing = std::filesystem::exists(m_root + "/recovery", error);
    if (error)
        return Failure(error.message().c_str(), ErrorCode::DiskIOFail);
    if (!existing)
    {
        bool empty = std::filesystem::is_empty(m_root, error);
        if (error)
            return Failure(error.message().c_str(), ErrorCode::DiskIOFail);
        if (!empty)
            return Failure("A new tenant node requires an empty, dedicated root directory");
    }
    // Recovery DB's LOCK excludes a second node owner before any state is restored.
    status = SharedRocksDB::Open(m_root + "/recovery", RecoveryFamilies, m_recovery, !existing, m_cache);
    if (status != ErrorCode::Success)
        return status;
    std::string format;
    status = m_recovery->Get("recovery_meta", "format", format);
    if (status == ErrorCode::Key_NotFound)
    {
        status = m_recovery->Put("recovery_meta", "format", "SPTAG-Tenant-1");
    }
    else if (status == ErrorCode::Success && format != "SPTAG-Tenant-1")
        return Failure("Unsupported node format", ErrorCode::FailedParseValue);
    if (status != ErrorCode::Success)
        return status;
    std::uint64_t workID;
    if ((status = AllocateID(workID)) != ErrorCode::Success)
        return status;
    m_workID = workID;
    m_work = m_root + "/work-" + std::to_string(workID);
    bool collision = std::filesystem::exists(m_work, error);
    if (error)
        return Failure(error.message().c_str(), ErrorCode::DiskIOFail);
    if (collision)
        return Failure("Work directory already exists");
    if ((status = m_recovery->Put("recovery_meta", "artifact/" + SharedRocksDB::EncodeNumber(workID), "work")) !=
        ErrorCode::Success)
        return status;
    if ((status = MakeDirectory(m_work)) != ErrorCode::Success)
        return status;

    std::string current;
    status = m_recovery->Get("recovery_meta", "current", current);
    const bool fresh = status == ErrorCode::Key_NotFound;
    if (!fresh && status != ErrorCode::Success)
        return status;
    if (!fresh)
    {
        std::uint64_t checkpoint;
        if (!SharedRocksDB::DecodeNumber(current, checkpoint))
            return Failure("Invalid checkpoint pointer");
        std::error_code error;
        std::filesystem::copy(m_root + "/checkpoint-" + std::to_string(checkpoint), m_work + "/state",
                              std::filesystem::copy_options::recursive, error);
        if (error)
            return Failure(error.message().c_str(), ErrorCode::DiskIOFail);
    }
    status = SharedRocksDB::Open(m_work + "/state", StateFamilies, m_state, fresh, m_cache);
    if (status != ErrorCode::Success)
        return status;
    if (fresh)
        return CheckpointLocked();

    std::string catalog;
    if ((status = m_state->Get("checkpoint_meta", "catalog", catalog)) != ErrorCode::Success)
        return status;
    std::string publishedCatalog;
    if ((status = m_recovery->Get("catalog", "tenants", publishedCatalog)) != ErrorCode::Success)
        return status;
    if (publishedCatalog != catalog)
        return Failure("Published catalog does not match checkpoint");
    if ((status = RestoreCatalog(catalog)) != ErrorCode::Success)
        return status;
    for (auto &entry : m_tenants)
    {
        if ((status = Load(*entry.second)) != ErrorCode::Success)
            return status;
        if ((status = Replay(*entry.second)) != ErrorCode::Success)
            return status;
        if ((status = Save(*entry.second)) != ErrorCode::Success)
            return status;
        entry.second->index.reset();
    }
    return CollectGarbage();
}

ErrorCode TenantIndexManager::Ready() const
{
    return (m_closed || m_failed.load()) ? Failure("Node closed or failed; reopen to recover") : ErrorCode::Success;
}

ErrorCode TenantIndexManager::Poison(ErrorCode status)
{
    if (status != ErrorCode::Success)
    {
        m_failed.store(true);
        m_cacheChanged.notify_all();
        Failure("State mutation failed; node fenced until recovery", status);
    }
    return status;
}

std::string TenantIndexManager::Directory(const Tenant &tenant) const
{
    return m_work + "/index-" + std::to_string(tenant.id);
}

bool TenantIndexManager::ValidVectors(const Tenant &tenant, const ByteArray &vectors, SizeType count) const
{
    if (count <= 0 || count > m_limits.maxVectorsPerTenant || !vectors.Data())
        return false;
    auto bytes = static_cast<std::uint64_t>(count) * tenant.dimension * GetValueTypeSize(tenant.type);
    if (bytes != vectors.Length())
        return false;
    if (tenant.type == VectorValueType::Float)
    {
        for (size_t i = 0; i < bytes / sizeof(float); ++i)
        {
            float value;
            memcpy(&value, vectors.Data() + i * sizeof(float), sizeof(float));
            if (!std::isfinite(value))
                return false;
        }
    }
    return true;
}

std::string TenantIndexManager::SerializeCatalog() const
{
    std::string result;
    Number(result, 1);
    Number(result, m_tenants.size());
    for (const auto &entry : m_tenants)
    {
        const auto &tenant = *entry.second;
        String(result, tenant.name);
        Number(result, tenant.id);
        Number(result, static_cast<std::uint64_t>(tenant.type));
        Number(result, tenant.dimension);
        Number(result, static_cast<std::uint64_t>(tenant.metric));
        Number(result, tenant.sequence);
    }
    return result;
}

ErrorCode TenantIndexManager::RestoreCatalog(const std::string &data)
{
    Reader reader(data);
    std::uint64_t version, count;
    if (!reader.Number(version) || version != 1 || !reader.Number(count) || count > m_limits.maxTenants)
        return Failure("Invalid catalog header or tenant limit");
    std::set<std::uint64_t> ids;
    for (std::uint64_t i = 0; i < count; ++i)
    {
        auto tenant = std::make_shared<Tenant>();
        std::uint64_t type, dimension, metric;
        if (!reader.String(tenant->name) || tenant->name.empty() || tenant->name.size() > 1024 ||
            !reader.Number(tenant->id) || !tenant->id || !ids.insert(tenant->id).second || !reader.Number(type) ||
            !reader.Number(dimension) || !reader.Number(metric) || !reader.Number(tenant->sequence) || dimension == 0 ||
            dimension > static_cast<uint64_t>(m_limits.maxDimension))
            return Failure("Invalid tenant catalog record");
        tenant->type = static_cast<VectorValueType>(type);
        tenant->dimension = static_cast<DimensionType>(dimension);
        tenant->metric = static_cast<DistCalcMethod>(metric);
        tenant->savedSequence = tenant->sequence;
        tenant->hasSnapshot = true;
        if (GetValueTypeSize(tenant->type) == 0 ||
            (tenant->metric != DistCalcMethod::L2 && tenant->metric != DistCalcMethod::Cosine) ||
            !m_tenants.emplace(tenant->name, tenant).second)
            return Failure("Unsupported or duplicate tenant catalog record");
    }
    return reader.End() ? ErrorCode::Success : Failure("Trailing catalog bytes");
}

ErrorCode TenantIndexManager::Create(const std::string &name, VectorValueType type, DimensionType dimension,
                                     DistCalcMethod metric, const ByteArray &vectors, SizeType count)
{
    std::lock_guard<std::mutex> management(m_management);
    std::shared_lock<std::shared_mutex> barrier(m_barrier);
    std::shared_lock<std::shared_mutex> updates(m_updates);
    auto status = Ready();
    if (status != ErrorCode::Success)
        return status;
    if (name.empty() || name.size() > 1024 || m_tenants.count(name) || m_tenants.size() >= m_limits.maxTenants ||
        dimension <= 0 || dimension > m_limits.maxDimension || GetValueTypeSize(type) == 0 ||
        (metric != DistCalcMethod::L2 && metric != DistCalcMethod::Cosine) || count < 64)
        return Failure("Invalid create request, duplicate tenant, or node capacity", ErrorCode::FailedParseValue);
    auto tenant = std::make_shared<Tenant>();
    tenant->name = name;
    tenant->type = type;
    tenant->dimension = dimension;
    tenant->metric = metric;
    if (!ValidVectors(*tenant, vectors, count))
        return Failure("Invalid seed vectors", ErrorCode::FailedParseValue);
    if ((status = AllocateID(tenant->id)) != ErrorCode::Success)
        return Poison(status);
    if ((status = MakeDirectory(Directory(*tenant))) != ErrorCode::Success)
        return Poison(status);
    tenant->index = VectorIndex::CreateInstance(IndexAlgoType::SPANN, type);
    if (!tenant->index)
        return Failure("Unsupported vector type", ErrorCode::FailedParseValue);
    const auto set = [&](const char *section, const char *key, const std::string &value) {
        return tenant->index->SetParameter(key, value.c_str(), section);
    };
    const std::vector<std::tuple<std::string, std::string, std::string>> parameters{
        {"Base", "IndexAlgoType", "BKT"},
        {"Base", "ValueType", Helper::Convert::ConvertToString(type)},
        {"Base", "Dim", std::to_string(dimension)},
        {"Base", "DistCalcMethod", Helper::Convert::ConvertToString(metric)},
        {"Base", "IndexDirectory", Directory(*tenant)},
        {"Base", "DataBlockSize", "1024"},
        {"Base", "DataCapacity", std::to_string(m_limits.maxVectorsPerTenant)},
        {"SelectHead", "isExecute", "true"},
        {"SelectHead", "NumberOfThreads", "1"},
        {"SelectHead", "Ratio", "0.2"},
        {"SelectHead", "SelectThreshold", "0"},
        {"SelectHead", "SplitFactor", "0"},
        {"SelectHead", "SplitThreshold", "0"},
        {"BuildHead", "isExecute", "true"},
        {"BuildHead", "NumberOfThreads", "1"},
        {"BuildHead", "DataBlockSize", "1024"},
        {"BuildHead", "DataCapacity", std::to_string(m_limits.maxVectorsPerTenant)},
        {"BuildHead", "BKTKmeansK", "8"},
        {"BuildHead", "NeighborhoodSize", "16"},
        {"BuildHead", "DeletePercentageForRefine", "2"},
        {"BuildSSDIndex", "isExecute", "true"},
        {"BuildSSDIndex", "BuildSsdIndex", "true"},
        {"BuildSSDIndex", "NumberOfThreads", "1"},
        {"BuildSSDIndex", "SearchThreadNum", "1"},
        {"BuildSSDIndex", "AppendThreadNum", "1"},
        {"BuildSSDIndex", "InternalResultNum", "32"},
        {"BuildSSDIndex", "SearchInternalResultNum", "32"},
        {"BuildSSDIndex", "ReassignK", "32"},
        {"BuildSSDIndex", "ReplicaCount", "2"},
        {"BuildSSDIndex", "ExcludeHead", "false"},
        {"BuildSSDIndex", "InPlace", "true"},
        {"BuildSSDIndex", "OneClusterCutMax", "true"},
        {"BuildSSDIndex", "TmpDir", Directory(*tenant)},
        {"BuildSSDIndex", "MaxCheck", "256"}};
    for (const auto &parameter : parameters)
    {
        status = set(std::get<0>(parameter).c_str(), std::get<1>(parameter).c_str(), std::get<2>(parameter));
        if (status != ErrorCode::Success)
            return Poison(status);
    }
    status = tenant->index->SetPostingStore(m_state->PostingView(name, tenant->id));
    if (status != ErrorCode::Success)
        return Poison(status);
    auto seed = ByteArray::Alloc(vectors.Length());
    memcpy(seed.Data(), vectors.Data(), vectors.Length());
    status = tenant->index->BuildIndex(seed.Data(), count, dimension);
    if (status != ErrorCode::Success)
        return Poison(status);
    if ((status = Save(*tenant)) != ErrorCode::Success)
        return Poison(status);
    tenant->index.reset();
    updates.unlock();
    barrier.unlock();
    std::unique_lock<std::shared_mutex> publish(m_barrier);
    if ((status = Ready()) != ErrorCode::Success)
        return status;
    m_tenants.emplace(name, tenant);
    return Poison(CheckpointLocked());
}

ErrorCode TenantIndexManager::Save(Tenant &tenant)
{
    if (!tenant.index)
        return ErrorCode::Success; // Unload already saved this exact, now immutable state.
    auto status = tenant.index->DrainUpdates();
    if (status != ErrorCode::Success)
        return status;
    if (tenant.hasSnapshot && tenant.sequence == tenant.savedSequence)
        return ErrorCode::Success;
    if ((status = tenant.index->SaveIndex(Directory(tenant), nullptr, true)) != ErrorCode::Success)
        return status;
    std::set<std::string> files{"indexloader.ini", "DeletedIDs.bin", "ssdinfo", "checksum"};
    auto indexFiles = tenant.index->GetIndexFiles();
    for (const auto &file : *indexFiles)
        files.insert(file);
    const auto prefix = SharedRocksDB::Namespace(tenant.name, tenant.id);
    if ((status = m_state->DeleteRange("head_snapshots", prefix, SharedRocksDB::PrefixEnd(prefix))) !=
        ErrorCode::Success)
        return status;
    std::string manifest;
    uint64_t snapshotBytes = 0;
    Number(manifest, files.size());
    std::vector<char> buffer(ChunkSize);
    for (const auto &file : files)
    {
        if (!SafeRelativePath(file))
            return Failure("Unsafe snapshot file path");
        std::ifstream input(Directory(tenant) + "/" + file, std::ios::binary);
        if (!input)
            return Failure("Cannot open snapshot component", ErrorCode::FailedOpenFile);
        std::uint64_t size = 0, chunk = 0;
        while (input)
        {
            input.read(buffer.data(), buffer.size());
            const auto bytes = input.gcount();
            if (bytes > 0)
            {
                status = m_state->Put("head_snapshots", FileKey(prefix, file, chunk++),
                                      std::string(buffer.data(), static_cast<size_t>(bytes)));
                if (status != ErrorCode::Success)
                    return status;
                size += bytes;
            }
        }
        if (!input.eof())
            return Failure("Snapshot component read failed", ErrorCode::DiskIOFail);
        String(manifest, file);
        Number(manifest, size);
        snapshotBytes += size;
    }
    status = m_state->Put("head_snapshots", prefix + "manifest", manifest);
    if (status == ErrorCode::Success)
    {
        std::lock_guard<std::mutex> cache(m_cacheMutex);
        tenant.savedSequence = tenant.sequence;
        tenant.hasSnapshot = true;
        tenant.filesReady = true;
        if (tenant.stats.state != Residency::Unloaded)
            m_residentBytes = m_residentBytes - tenant.stats.snapshotBytes + snapshotBytes;
        tenant.stats.snapshotBytes = snapshotBytes;
    }
    return status;
}

TenantIndexManager::Lease::~Lease()
{
    if (owner)
        owner->Release(*tenant);
}

void TenantIndexManager::Release(Tenant &tenant)
{
    std::lock_guard<std::mutex> cache(m_cacheMutex);
    --tenant.stats.activeRequests;
    tenant.lastAccess = std::chrono::steady_clock::now();
    m_cacheChanged.notify_all();
}

ErrorCode TenantIndexManager::Evict(Tenant &tenant, std::unique_lock<std::mutex> &cacheLock)
{
    tenant.stats.state = Residency::Evicting;
    auto start = std::chrono::steady_clock::now();
    cacheLock.unlock();
    ErrorCode status;
    try
    {
        status = Save(tenant);
        if (status == ErrorCode::Success)
            tenant.index.reset();
    }
    catch (...)
    {
        status = ErrorCode::Fail;
    }
    cacheLock.lock();
    if (status == ErrorCode::Success)
    {
        --m_residentCount;
        m_residentBytes -= tenant.stats.snapshotBytes;
        tenant.stats.state = Residency::Unloaded;
        ++tenant.stats.evictions;
        tenant.stats.lastEvictionMs =
            std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - start).count();
    }
    else
    {
        tenant.stats.state = Residency::Resident;
        Poison(status);
    }
    m_cacheChanged.notify_all();
    return status;
}

ErrorCode TenantIndexManager::Acquire(Tenant &tenant, Lease &lease, bool updatesHeld)
{
    const auto pin = [&] {
        ++tenant.stats.activeRequests;
        tenant.lastAccess = std::chrono::steady_clock::now();
        lease.owner = this;
        lease.tenant = &tenant;
    };
    {
        std::lock_guard<std::mutex> cache(m_cacheMutex);
        auto status = Ready();
        if (status != ErrorCode::Success)
            return status;
        if (tenant.stats.state == Residency::Resident)
        {
            ++tenant.stats.cacheHits;
            pin();
            return ErrorCode::Success;
        }
        ++tenant.stats.cacheMisses;
    }
    std::shared_lock<std::shared_mutex> updates(m_updates, std::defer_lock);
    if (!updatesHeld)
        updates.lock();
    std::unique_lock<std::mutex> cache(m_cacheMutex);
    for (;;)
    {
        auto status = Ready();
        if (status != ErrorCode::Success)
            return status;
        if (tenant.stats.state == Residency::Resident)
        {
            pin();
            return ErrorCode::Success;
        }
        if (tenant.stats.state != Residency::Unloaded)
        {
            m_cacheChanged.wait(cache);
            continue;
        }
        const auto charge = tenant.stats.snapshotBytes;
        const bool bytesFit =
            !m_limits.maxResidentBytes || m_residentCount == 0 ||
            (charge <= m_limits.maxResidentBytes && m_residentBytes <= m_limits.maxResidentBytes - charge);
        if (m_residentCount < m_limits.maxResidentTenants && bytesFit)
        {
            tenant.stats.state = Residency::Loading;
            ++m_residentCount;
            m_residentBytes += charge;
            auto start = std::chrono::steady_clock::now();
            cache.unlock();
            try
            {
                status = Load(tenant);
            }
            catch (...)
            {
                status = ErrorCode::Fail;
            }
            cache.lock();
            if (status != ErrorCode::Success)
            {
                tenant.index.reset();
                --m_residentCount;
                m_residentBytes -= tenant.stats.snapshotBytes;
                tenant.stats.state = Residency::Unloaded;
                m_cacheChanged.notify_all();
                return Poison(status);
            }
            tenant.stats.state = Residency::Resident;
            ++tenant.stats.loads;
            tenant.stats.lastLoadMs =
                std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - start).count();
            pin();
            m_cacheChanged.notify_all();
            return ErrorCode::Success;
        }
        Tenant *victim = nullptr;
        for (auto &entry : m_tenants)
        {
            auto &candidate = *entry.second;
            if (candidate.stats.state == Residency::Resident && !candidate.stats.activeRequests &&
                (!victim || candidate.lastAccess < victim->lastAccess))
                victim = &candidate;
        }
        if (victim)
        {
            if ((status = Evict(*victim, cache)) != ErrorCode::Success)
                return status;
        }
        else
            m_cacheChanged.wait(cache);
    }
}

ErrorCode TenantIndexManager::Load(Tenant &tenant)
{
    if (tenant.index)
        return ErrorCode::Success;
    uint64_t snapshotBytes = tenant.stats.snapshotBytes;
    if (!tenant.filesReady)
    {
        auto status = MakeDirectory(Directory(tenant));
        if (status != ErrorCode::Success)
            return status;
        auto prefix = SharedRocksDB::Namespace(tenant.name, tenant.id);
        std::string manifest;
        if ((status = m_state->Get("head_snapshots", prefix + "manifest", manifest)) != ErrorCode::Success)
            return status;
        Reader reader(manifest);
        std::uint64_t count;
        snapshotBytes = 0;
        if (!reader.Number(count) || count > 1000)
            return Failure("Invalid snapshot manifest");
        for (std::uint64_t i = 0; i < count; ++i)
        {
            std::string file;
            std::uint64_t size;
            if (!reader.String(file) || !SafeRelativePath(file) || !reader.Number(size))
                return Failure("Invalid snapshot component");
            snapshotBytes += size;
            auto path = std::filesystem::path(Directory(tenant)) / file;
            if ((status = MakeDirectory(path.parent_path().string())) != ErrorCode::Success)
                return status;
            std::ofstream output(path, std::ios::binary | std::ios::trunc);
            if (!output)
                return Failure("Cannot restore snapshot component", ErrorCode::FailedCreateFile);
            for (std::uint64_t offset = 0, chunk = 0; offset < size; ++chunk)
            {
                std::string data;
                if ((status = m_state->Get("head_snapshots", FileKey(prefix, file, chunk), data)) != ErrorCode::Success)
                    return status;
                auto expected = (std::min)(static_cast<std::uint64_t>(ChunkSize), size - offset);
                if (data.size() != expected)
                    return Failure("Truncated snapshot component");
                output.write(data.data(), data.size());
                offset += data.size();
            }
            output.close();
            if (!output)
                return Failure("Snapshot write failed", ErrorCode::DiskIOFail);
        }
        if (!reader.End())
            return Failure("Trailing snapshot manifest data");
    }
    auto status = VectorIndex::LoadIndex(Directory(tenant), tenant.index, m_state->PostingView(tenant.name, tenant.id));
    if (status != ErrorCode::Success)
    {
        tenant.index.reset();
        return status;
    }
    if (tenant.index->GetFeatureDim() != tenant.dimension || tenant.index->GetVectorValueType() != tenant.type)
        return Failure("Snapshot configuration does not match catalog");
    {
        std::lock_guard<std::mutex> cache(m_cacheMutex);
        if (tenant.stats.state != Residency::Unloaded)
            m_residentBytes = m_residentBytes - tenant.stats.snapshotBytes + snapshotBytes;
        tenant.stats.snapshotBytes = snapshotBytes;
        tenant.filesReady = true;
    }
    return ErrorCode::Success;
}

ErrorCode TenantIndexManager::CheckpointLocked()
{
    for (auto &entry : m_tenants)
    {
        auto status = Save(*entry.second);
        if (status != ErrorCode::Success)
            return status;
    }
    const auto catalog = SerializeCatalog();
    auto status = m_state->Put("checkpoint_meta", "catalog", catalog);
    if (status != ErrorCode::Success)
        return status;
    std::uint64_t id;
    if ((status = AllocateID(id)) != ErrorCode::Success)
        return status;
    const auto path = m_root + "/checkpoint-" + std::to_string(id);
    std::error_code error;
    bool collision = std::filesystem::exists(path, error);
    if (error)
        return Failure(error.message().c_str(), ErrorCode::DiskIOFail);
    if (collision)
        return Failure("Checkpoint directory already exists");
    if ((status = m_recovery->Put("recovery_meta", "artifact/" + SharedRocksDB::EncodeNumber(id), "checkpoint")) !=
        ErrorCode::Success)
        return status;
    if ((status = m_state->Checkpoint(path)) != ErrorCode::Success)
        return status;
    status = m_recovery->Write(
        {{"recovery_meta", "current", SharedRocksDB::EncodeNumber(id)}, {"catalog", "tenants", catalog}});
    if (status != ErrorCode::Success)
        return status;
    if ((status = m_recovery->FlushWAL()) != ErrorCode::Success)
        return status;
    for (const auto &entry : m_tenants)
    {
        auto prefix = SharedRocksDB::Namespace(entry.first, entry.second->id);
        status = m_recovery->DeleteRange("recovery_log", prefix,
                                         prefix + SharedRocksDB::EncodeNumber(entry.second->sequence + 1));
        if (status != ErrorCode::Success)
            return status;
    }
    return CollectGarbage();
}

ErrorCode TenantIndexManager::CollectGarbage()
{
    std::set<std::string> active;
    for (const auto &entry : m_tenants)
        active.insert(SharedRocksDB::Namespace(entry.first, entry.second->id));
    std::string removed;
    auto status = m_recovery->Scan("recovery_log", "", [&](const std::string &key, const std::string &) {
        std::uint64_t length;
        if (key.size() < 25 || key[0] != '\1' || !SharedRocksDB::DecodeNumber(key.substr(1, 8), length) ||
            length == 0 || length > 1024 || key.size() != length + 25)
            return Failure("Invalid recovery log namespace");
        auto prefix = key.substr(0, key.size() - 8);
        if (active.count(prefix) || prefix == removed)
            return ErrorCode::Success;
        removed = prefix;
        return m_recovery->DeleteRange("recovery_log", prefix, SharedRocksDB::PrefixEnd(prefix));
    });
    if (status != ErrorCode::Success)
        return status;
    std::string current;
    status = m_recovery->Get("recovery_meta", "current", current);
    if (status != ErrorCode::Success)
        return status;
    std::uint64_t checkpoint;
    if (!SharedRocksDB::DecodeNumber(current, checkpoint))
        return Failure("Invalid checkpoint during cleanup");
    // Only delete explicitly registered artifacts; never enumerate/remove arbitrary root contents.
    return m_recovery->Scan("recovery_meta", "artifact/", [&](const std::string &key, const std::string &kind) {
        std::uint64_t id;
        if (!SharedRocksDB::DecodeNumber(key.substr(9), id) || (kind != "work" && kind != "checkpoint"))
            return Failure("Invalid artifact registry");
        if ((kind == "work" && id == m_workID) || (kind == "checkpoint" && id == checkpoint))
            return ErrorCode::Success;
        auto path = std::filesystem::path(m_root) / (kind + "-" + std::to_string(id));
        std::error_code error;
        if (std::filesystem::is_symlink(path, error))
            return Failure("Refusing to remove symlink artifact");
        if (error && error != std::errc::no_such_file_or_directory)
            return Failure(error.message().c_str(), ErrorCode::DiskIOFail);
        error.clear();
        std::filesystem::remove_all(path, error);
        if (error)
            return Failure(error.message().c_str(), ErrorCode::DiskIOFail);
        return m_recovery->Delete("recovery_meta", key);
    });
}

ErrorCode TenantIndexManager::Apply(Tenant &tenant, const std::string &record)
{
    Reader reader(record);
    std::uint64_t version, operation, id, count;
    std::string bytes;
    if (!reader.Number(version) || version != 1 || !reader.Number(operation) || !reader.Number(id) || id > MaxSize)
        return Failure("Invalid recovery record");
    ErrorCode status;
    if (operation == 1)
    {
        if (!reader.Number(count) || count == 0 || count > static_cast<uint64_t>(m_limits.maxVectorsPerTenant) ||
            !reader.String(bytes) || !reader.End() || tenant.index->GetNumSamples() != static_cast<SizeType>(id) ||
            id + count > static_cast<uint64_t>(m_limits.maxVectorsPerTenant))
            return Failure("Invalid insertion recovery record or VID sequence");
        ByteArray data = ByteArray::Alloc(bytes.size());
        memcpy(data.Data(), bytes.data(), bytes.size());
        if (!ValidVectors(tenant, data, static_cast<SizeType>(count)))
            return Failure("Invalid recovery vector size");
        SizeType first;
        status = tenant.index->ReserveVectorIDs(static_cast<SizeType>(count), m_limits.maxVectorsPerTenant, first);
        if (status != ErrorCode::Success || first != static_cast<SizeType>(id))
            return status == ErrorCode::Success ? Failure("Recovery VID reservation mismatch") : status;
        status = tenant.index->InsertAt(data.Data(), static_cast<SizeType>(count), tenant.dimension, first);
    }
    else if (operation == 2 && reader.End())
    {
        status = tenant.index->DeleteIndex(static_cast<SizeType>(id));
    }
    else
        return Failure("Unsupported recovery operation");
    if (status != ErrorCode::Success)
        return status;
    return tenant.index->DrainUpdates();
}

ErrorCode TenantIndexManager::Record(Tenant &tenant, const std::string &record)
{
    if (tenant.sequence == UINT64_MAX - 1)
        return Poison(Failure("Operation sequence exhausted"));
    const auto key =
        SharedRocksDB::Namespace(tenant.name, tenant.id) + SharedRocksDB::EncodeNumber(tenant.sequence + 1);
    auto status = m_recovery->Put("recovery_log", key, record);
    if (status != ErrorCode::Success)
        return Poison(status);
    ++tenant.sequence;
    return ErrorCode::Success;
}

ErrorCode TenantIndexManager::Replay(Tenant &tenant)
{
    const auto prefix = SharedRocksDB::Namespace(tenant.name, tenant.id);
    return m_recovery->Scan("recovery_log", prefix, [&](const std::string &key, const std::string &record) {
        std::uint64_t sequence;
        if (!SharedRocksDB::DecodeNumber(key.substr(prefix.size()), sequence))
            return Failure("Invalid recovery sequence key");
        if (sequence <= tenant.sequence)
            return ErrorCode::Success;
        if (sequence != tenant.sequence + 1)
            return Failure("Gap in recovery sequence");
        auto status = Apply(tenant, record);
        if (status == ErrorCode::Success)
            tenant.sequence = sequence;
        return status;
    });
}

ErrorCode TenantIndexManager::Insert(const std::string &name, const ByteArray &vectors, SizeType count,
                                     SizeType &firstID, VectorValueType expectedType, DimensionType expectedDimension)
{
    std::shared_lock<std::shared_mutex> barrier(m_barrier);
    std::shared_lock<std::shared_mutex> updates(m_updates);
    auto status = Ready();
    if (status != ErrorCode::Success)
        return status;
    auto it = m_tenants.find(name);
    if (it == m_tenants.end())
        return Failure("Tenant not found", ErrorCode::Key_NotFound);
    auto &tenant = *it->second;
    if (!ValidVectors(tenant, vectors, count) ||
        (expectedType != VectorValueType::Undefined && expectedType != tenant.type) ||
        (expectedDimension != 0 && expectedDimension != tenant.dimension))
        return Failure("Invalid insertion data", ErrorCode::FailedParseValue);
    Lease lease;
    if ((status = Acquire(tenant, lease, true)) != ErrorCode::Success)
        return status;
    SizeType begin = -1;
    auto completion = std::make_shared<UpdateContext>();
    try
    {
        std::string record;
        record.reserve(vectors.Length() + 40);
        Number(record, 1);
        Number(record, 1);
        const auto vidOffset = record.size();
        Number(record, 0);
        Number(record, count);
        Number(record, vectors.Length());
        record.append(reinterpret_cast<const char *>(vectors.Data()), vectors.Length());
        {
            std::lock_guard<std::mutex> lock(tenant.submitMutex);
            if ((status = Ready()) != ErrorCode::Success)
                return status;
            if (static_cast<int64_t>(tenant.index->GetNumSamples()) + count > m_limits.maxVectorsPerTenant)
                return ErrorCode::MemoryOverFlow;
            try
            {
                status = tenant.index->ReserveVectorIDs(count, m_limits.maxVectorsPerTenant, begin);
                if (status != ErrorCode::Success)
                    return Poison(status);
                tenant.pendingInserts.emplace(begin, count);
                auto encoded = SharedRocksDB::EncodeNumber(begin);
                record.replace(vidOffset, encoded.size(), encoded);
                if ((status = Record(tenant, record)) != ErrorCode::Success)
                    return status;
            }
            catch (...)
            {
                // Fence before another submitter can record a VID after an unlogged reservation.
                return Poison(ErrorCode::Fail);
            }
        }
        UpdateContext::Scope scope(completion);
        status = tenant.index->InsertAt(vectors.Data(), count, tenant.dimension, begin);
    }
    catch (...)
    {
        status = ErrorCode::Fail;
    }
    if (status != ErrorCode::Success)
        Poison(status);
    if (!completion->Wait())
        status = ErrorCode::Fail;
    if (status == ErrorCode::Success)
        status = tenant.index->GetUpdateStatus();
    {
        std::lock_guard<std::mutex> lock(tenant.submitMutex);
        tenant.pendingInserts.erase(begin);
        if (status != ErrorCode::Success)
            return Poison(status);
        if ((status = Ready()) != ErrorCode::Success)
            return status;
        firstID = begin;
    }
    return ErrorCode::Success;
}

ErrorCode TenantIndexManager::DeleteVector(const std::string &name, SizeType id)
{
    std::shared_lock<std::shared_mutex> barrier(m_barrier);
    std::shared_lock<std::shared_mutex> updates(m_updates);
    auto status = Ready();
    if (status != ErrorCode::Success)
        return status;
    auto it = m_tenants.find(name);
    if (it == m_tenants.end())
        return Failure("Tenant not found", ErrorCode::Key_NotFound);
    auto &tenant = *it->second;
    Lease lease;
    if ((status = Acquire(tenant, lease, true)) != ErrorCode::Success)
        return status;
    std::lock_guard<std::mutex> lock(tenant.submitMutex);
    if ((status = Ready()) != ErrorCode::Success)
        return status;
    auto pending = tenant.pendingInserts.upper_bound(id);
    if (pending != tenant.pendingInserts.begin())
    {
        --pending;
        if (static_cast<int64_t>(id) < static_cast<int64_t>(pending->first) + pending->second)
            return ErrorCode::ExternalAbort;
    }
    if (!tenant.index->ContainSample(id))
        return Failure("Vector not found", ErrorCode::VectorNotFound);
    std::string record;
    Number(record, 1);
    Number(record, 2);
    Number(record, id);
    try
    {
        if ((status = Record(tenant, record)) != ErrorCode::Success)
            return status;
        return Poison(tenant.index->DeleteIndex(id));
    }
    catch (...)
    {
        return Poison(ErrorCode::Fail);
    }
}

ErrorCode TenantIndexManager::Search(const std::string &name, const ByteArray &query, SizeType k,
                                     std::vector<BasicResult> &results, VectorValueType expectedType,
                                     DimensionType expectedDimension)
{
    std::shared_lock<std::shared_mutex> barrier(m_barrier);
    auto status = Ready();
    if (status != ErrorCode::Success)
        return status;
    auto it = m_tenants.find(name);
    if (it == m_tenants.end())
        return Failure("Tenant not found", ErrorCode::Key_NotFound);
    auto &tenant = *it->second;
    if (!ValidVectors(tenant, query, 1) || k <= 0 || k > m_limits.maxResults ||
        (expectedType != VectorValueType::Undefined && expectedType != tenant.type) ||
        (expectedDimension != 0 && expectedDimension != tenant.dimension))
        return Failure("Invalid query or result limit", ErrorCode::FailedParseValue);
    Lease lease;
    if ((status = Acquire(tenant, lease)) != ErrorCode::Success)
        return status;
    auto target = ByteArray::Alloc(query.Length());
    memcpy(target.Data(), query.Data(), query.Length());
    if (tenant.metric == DistCalcMethod::Cosine)
        BasicVectorSet(target, tenant.type, tenant.dimension, 1).Normalize(1);
    QueryResult result(target.Data(), k, false);
    status = tenant.index->SearchIndex(result);
    if (status != ErrorCode::Success)
        return status;
    results.assign(result.GetResults(), result.GetResults() + k);
    return ErrorCode::Success;
}

ErrorCode TenantIndexManager::DeleteTenant(const std::string &name)
{
    std::lock_guard<std::mutex> management(m_management);
    std::unique_lock<std::shared_mutex> barrier(m_barrier);
    auto status = Ready();
    if (status != ErrorCode::Success)
        return status;
    auto it = m_tenants.find(name);
    if (it == m_tenants.end())
        return Failure("Tenant not found", ErrorCode::Key_NotFound);
    if (it->second->index && (status = it->second->index->DrainUpdates()) != ErrorCode::Success)
        return Poison(status);
    const auto prefix = SharedRocksDB::Namespace(name, it->second->id);
    const auto directory = Directory(*it->second);
    {
        std::lock_guard<std::mutex> cache(m_cacheMutex);
        if (it->second->stats.state == Residency::Resident)
        {
            --m_residentCount;
            m_residentBytes -= it->second->stats.snapshotBytes;
        }
    }
    m_tenants.erase(it);
    if ((status = m_state->DeleteRange("postings", prefix, SharedRocksDB::PrefixEnd(prefix))) != ErrorCode::Success)
        return Poison(status);
    if ((status = m_state->DeleteRange("head_snapshots", prefix, SharedRocksDB::PrefixEnd(prefix))) !=
        ErrorCode::Success)
        return Poison(status);
    if ((status = CheckpointLocked()) != ErrorCode::Success)
        return Poison(status);
    std::error_code error;
    std::filesystem::remove_all(directory, error);
    return error ? Poison(Failure(error.message().c_str(), ErrorCode::DiskIOFail)) : ErrorCode::Success;
}

ErrorCode TenantIndexManager::Unload(const std::string &name)
{
    std::shared_lock<std::shared_mutex> barrier(m_barrier);
    std::shared_lock<std::shared_mutex> updates(m_updates);
    auto status = Ready();
    if (status != ErrorCode::Success)
        return status;
    auto it = m_tenants.find(name);
    if (it == m_tenants.end())
        return Failure("Tenant not found", ErrorCode::Key_NotFound);
    auto &tenant = *it->second;
    std::unique_lock<std::mutex> cache(m_cacheMutex);
    while (tenant.stats.state == Residency::Loading || tenant.stats.state == Residency::Evicting)
    {
        m_cacheChanged.wait(cache);
        if ((status = Ready()) != ErrorCode::Success)
            return status;
    }
    if (tenant.stats.state == Residency::Unloaded)
        return ErrorCode::Success;
    tenant.stats.state = Residency::Evicting;
    while (tenant.stats.activeRequests)
        m_cacheChanged.wait(cache);
    if ((status = Ready()) != ErrorCode::Success)
    {
        tenant.stats.state = Residency::Resident;
        m_cacheChanged.notify_all();
        return status;
    }
    return Evict(tenant, cache);
}

ErrorCode TenantIndexManager::Checkpoint()
{
    std::lock_guard<std::mutex> management(m_management);
    std::shared_lock<std::shared_mutex> barrier(m_barrier);
    std::unique_lock<std::shared_mutex> updates(m_updates);
    auto status = Ready();
    return status == ErrorCode::Success ? Poison(CheckpointLocked()) : status;
}

ErrorCode TenantIndexManager::Close()
{
    std::lock_guard<std::mutex> management(m_management);
    std::unique_lock<std::shared_mutex> barrier(m_barrier);
    if (m_closed)
        return ErrorCode::Success;
    auto status = Ready();
    if (status == ErrorCode::Success)
        status = Poison(CheckpointLocked());
    m_closed = true;
    for (auto &entry : m_tenants)
        if (entry.second->index)
            entry.second->index->DrainUpdates();
    m_tenants.clear();
    m_residentCount = 0;
    m_residentBytes = 0;
    m_state.reset();
    m_recovery.reset();
    return status;
}

void TenantIndexManager::GetStats(NodeStats &node, std::vector<TenantStats> &tenants) const
{
    std::shared_lock<std::shared_mutex> barrier(m_barrier);
    {
        std::lock_guard<std::mutex> cache(m_cacheMutex);
        node.failed = m_failed.load();
        node.closed = m_closed;
        node.residentTenants = m_residentCount;
        node.maxResidentTenants = m_limits.maxResidentTenants;
        node.residentBytes = m_residentBytes;
        node.maxResidentBytes = m_limits.maxResidentBytes;
        node.overByteTarget = m_limits.maxResidentBytes && m_residentBytes > m_limits.maxResidentBytes;
    }
    tenants.clear();
    for (const auto &entry : m_tenants)
    {
        const auto &tenant = *entry.second;
        std::lock_guard<std::mutex> submit(entry.second->submitMutex);
        std::lock_guard<std::mutex> cache(m_cacheMutex);
        auto stats = tenant.stats;
        stats.name = tenant.name;
        stats.indexID = tenant.id;
        stats.dirty = !tenant.hasSnapshot || tenant.sequence != tenant.savedSequence;
        tenants.emplace_back(std::move(stats));
    }
}

std::vector<std::string> TenantIndexManager::List() const
{
    std::shared_lock<std::shared_mutex> barrier(m_barrier);
    std::vector<std::string> result;
    for (const auto &entry : m_tenants)
        result.push_back(entry.first);
    return result;
}

ErrorCode TenantIndexManager::Describe(const std::string &name, VectorValueType &type, DimensionType &dimension) const
{
    std::shared_lock<std::shared_mutex> barrier(m_barrier);
    auto status = Ready();
    if (status != ErrorCode::Success)
        return status;
    auto it = m_tenants.find(name);
    if (it == m_tenants.end())
        return Failure("Tenant not found", ErrorCode::Key_NotFound);
    type = it->second->type;
    dimension = it->second->dimension;
    return ErrorCode::Success;
}
} // namespace SPTAG::SPANN
#endif
