// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#pragma once

#ifdef ROCKSDB
#include "inc/Core/SPANN/SharedRocksDB.h"
#include "inc/Core/VectorIndex.h"
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <map>
#include <shared_mutex>

namespace SPTAG::SPANN
{
// Trusted, in-process API. No index pointer escapes the lifecycle barrier.
class TenantIndexManager
{
  public:
    struct Limits
    {
        size_t maxTenants = 64;
        SizeType maxVectorsPerTenant = 1000000;
        DimensionType maxDimension = 4096;
        SizeType maxResults = 1000;
        size_t cacheBytes = 64ULL << 20;
        size_t maxResidentTenants = 8;
        uint64_t maxResidentBytes = 0;
    };

    enum class Residency
    {
        Unloaded,
        Loading,
        Resident,
        Evicting
    };
    struct TenantStats
    {
        std::string name;
        uint64_t indexID = 0;
        Residency state = Residency::Unloaded;
        size_t activeRequests = 0;
        uint64_t snapshotBytes = 0;
        uint64_t cacheHits = 0, cacheMisses = 0, loads = 0, evictions = 0;
        double lastLoadMs = 0, lastEvictionMs = 0;
        bool dirty = false;
    };
    struct NodeStats
    {
        bool failed = false, closed = false;
        size_t residentTenants = 0, maxResidentTenants = 0;
        uint64_t residentBytes = 0, maxResidentBytes = 0;
        bool overByteTarget = false;
    };

    static ErrorCode Open(const std::string &root, const Limits &limits, std::unique_ptr<TenantIndexManager> &result);
    ~TenantIndexManager();

    ErrorCode Create(const std::string &tenant, VectorValueType type, DimensionType dimension, DistCalcMethod metric,
                     const ByteArray &vectors, SizeType count);
    ErrorCode Insert(const std::string &tenant, const ByteArray &vectors, SizeType count, SizeType &firstID,
                     VectorValueType expectedType = VectorValueType::Undefined, DimensionType expectedDimension = 0);
    ErrorCode DeleteVector(const std::string &tenant, SizeType id);
    ErrorCode Search(const std::string &tenant, const ByteArray &query, SizeType k, std::vector<BasicResult> &results,
                     VectorValueType expectedType = VectorValueType::Undefined, DimensionType expectedDimension = 0);
    ErrorCode DeleteTenant(const std::string &tenant);
    ErrorCode Unload(const std::string &tenant);
    ErrorCode Checkpoint();
    ErrorCode Close();
    ErrorCode Describe(const std::string &tenant, VectorValueType &type, DimensionType &dimension) const;
    std::vector<std::string> List() const;
    void GetStats(NodeStats &node, std::vector<TenantStats> &tenants) const;

  private:
    struct Tenant
    {
        std::string name;
        std::uint64_t id = 0;
        VectorValueType type = VectorValueType::Undefined;
        DimensionType dimension = 0;
        DistCalcMethod metric = DistCalcMethod::Undefined;
        std::uint64_t sequence = 0;
        std::uint64_t savedSequence = 0;
        bool hasSnapshot = false;
        bool filesReady = false;
        std::shared_ptr<VectorIndex> index;
        std::mutex submitMutex;
        std::map<SizeType, SizeType> pendingInserts;
        TenantStats stats;
        std::chrono::steady_clock::time_point lastAccess;
    };

    struct Lease
    {
        TenantIndexManager *owner = nullptr;
        Tenant *tenant = nullptr;
        Lease() = default;
        Lease(const Lease &) = delete;
        Lease &operator=(const Lease &) = delete;
        ~Lease();
    };

    TenantIndexManager(std::string root, Limits limits);
    ErrorCode Initialize();
    ErrorCode AllocateID(std::uint64_t &id);
    ErrorCode Load(Tenant &tenant);
    ErrorCode Acquire(Tenant &tenant, Lease &lease, bool updatesHeld = false);
    void Release(Tenant &tenant);
    ErrorCode Evict(Tenant &tenant, std::unique_lock<std::mutex> &cacheLock);
    ErrorCode Save(Tenant &tenant);
    ErrorCode CheckpointLocked();
    ErrorCode Replay(Tenant &tenant);
    ErrorCode Apply(Tenant &tenant, const std::string &record);
    ErrorCode Record(Tenant &tenant, const std::string &record);
    ErrorCode Poison(ErrorCode status);
    std::string Directory(const Tenant &tenant) const;
    std::string SerializeCatalog() const;
    ErrorCode RestoreCatalog(const std::string &data);
    bool ValidVectors(const Tenant &tenant, const ByteArray &vectors, SizeType count) const;
    ErrorCode Ready() const;
    ErrorCode CollectGarbage();

    std::string m_root;
    std::string m_work;
    std::uint64_t m_workID = 0;
    Limits m_limits;
    std::shared_ptr<rocksdb::Cache> m_cache;
    std::shared_ptr<SharedRocksDB> m_state;
    std::shared_ptr<SharedRocksDB> m_recovery;
    std::map<std::string, std::shared_ptr<Tenant>> m_tenants;
    mutable std::shared_mutex m_barrier;
    std::shared_mutex m_updates;
    std::mutex m_management;
    std::mutex m_idMutex;
    mutable std::mutex m_cacheMutex;
    std::condition_variable m_cacheChanged;
    size_t m_residentCount = 0;
    uint64_t m_residentBytes = 0;
    std::atomic<bool> m_failed{false};
    bool m_closed = false;
};
} // namespace SPTAG::SPANN
#endif
