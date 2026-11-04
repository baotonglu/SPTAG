// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

// Standalone in-process benchmark; compile the same source against both revisions.
#include "inc/Core/VectorIndex.h"
#ifdef TENANT_BENCH_CURRENT
#include "inc/Core/SPANN/TenantIndexManager.h"
#endif
#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
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
    std::unique_ptr<SPANN::TenantIndexManager> node;
#endif
    void Open(const std::string &mode, const std::string &root, int slots)
    {
        if (mode == "legacy")
            Check(VectorIndex::LoadIndex(root, index));
#ifdef TENANT_BENCH_CURRENT
        else if (mode == "tenant")
        {
            SPANN::TenantIndexManager::Limits limits;
            limits.maxResidentTenants = slots;
            Check(SPANN::TenantIndexManager::Open(root, limits, node));
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
    const auto set = [&](const char *section, const char *key, const std::string &value) {
        Check(index->SetParameter(key, value.c_str(), section));
    };
    // Match the shipped tenant builder, including its non-reference search budget.
    set("Base", "IndexAlgoType", "BKT");
    set("Base", "ValueType", "UInt8");
    set("Base", "Dim", "128");
    set("Base", "DistCalcMethod", "L2");
    set("Base", "IndexDirectory", root);
    set("Base", "DataBlockSize", "1024");
    set("Base", "DataCapacity", "1000000");
    set("SelectHead", "isExecute", "true");
    set("SelectHead", "NumberOfThreads", "1");
    set("SelectHead", "Ratio", "0.2");
    set("SelectHead", "SelectThreshold", "0");
    set("SelectHead", "SplitFactor", "0");
    set("SelectHead", "SplitThreshold", "0");
    set("BuildHead", "isExecute", "true");
    set("BuildHead", "NumberOfThreads", "1");
    set("BuildHead", "DataBlockSize", "1024");
    set("BuildHead", "DataCapacity", "1000000");
    set("BuildHead", "BKTKmeansK", "8");
    set("BuildHead", "NeighborhoodSize", "16");
    set("BuildHead", "DeletePercentageForRefine", "2");
    set("BuildSSDIndex", "isExecute", "true");
    set("BuildSSDIndex", "BuildSsdIndex", "true");
    set("BuildSSDIndex", "NumberOfThreads", "1");
    set("BuildSSDIndex", "SearchThreadNum", "1");
    set("BuildSSDIndex", "AppendThreadNum", "1");
    set("BuildSSDIndex", "InternalResultNum", "32");
    set("BuildSSDIndex", "SearchInternalResultNum", "32");
    set("BuildSSDIndex", "ReassignK", "32");
    set("BuildSSDIndex", "ReplicaCount", "2");
    set("BuildSSDIndex", "ExcludeHead", "false");
    set("BuildSSDIndex", "InPlace", "true");
    set("BuildSSDIndex", "OneClusterCutMax", "true");
    set("BuildSSDIndex", "TmpDir", root);
    set("BuildSSDIndex", "MaxCheck", "256");
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
    Check(
        engine.node->Create("only", VectorValueType::UInt8, 128, DistCalcMethod::L2, vectors.Row(0, initial), initial));
    std::ofstream metrics(output + "/updates.csv"), mapping(output + "/id-map.csv");
    metrics.exceptions(std::ios::badbit | std::ios::failbit);
    mapping.exceptions(std::ios::badbit | std::ios::failbit);
    metrics << "phase,count,insert_s,checkpoint_s,vectors_per_s,query_count,query_errors,p50_ms,p95_ms,p99_ms\n";
    mapping << "input_id,assigned_id\n";
    std::vector<int> sourceID(initial + total, -1);
    for (int i = 0; i < initial; ++i)
        sourceID[i] = i;
    auto accuracy = [&](int phase) {
        std::ofstream out(output + "/phase-" + std::to_string(phase) + ".csv");
        out.exceptions(std::ios::badbit | std::ios::failbit);
        out << "tenant,query,rank,id,distance\n";
        for (int q = 0; q < queries.count; ++q)
        {
            std::vector<BasicResult> results;
            Check(engine.Search("only", queries.Row(q), results));
            for (int k = 0; k < int(results.size()); ++k)
            {
                int vid = results[k].VID;
                if (vid >= int(sourceID.size()) || (vid >= 0 && sourceID[vid] < 0))
                    throw std::runtime_error("Unexpected inserted VID");
                out << "only," << q << "," << k << "," << (vid < 0 ? -1 : sourceID[vid]) << "," << results[k].Dist
                    << "\n";
            }
        }
    };
    accuracy(0);
    for (int phase = 1; phase <= total / batch; ++phase)
    {
        std::atomic<bool> stop{false};
        std::atomic<int> next{0}, errors{0};
        std::vector<std::vector<double>> latency(readers);
        std::vector<uint64_t> queryErrors(readers, 0);
        std::vector<std::thread> searchers, inserters;
        std::vector<int> assigned(batch, -1);
        for (int t = 0; t < readers; ++t)
            searchers.emplace_back([&, t] {
                std::mt19937 rng(20261104 + t);
                std::vector<BasicResult> results;
                while (!stop)
                {
                    auto start = Clock::now();
                    auto status = engine.Search("only", queries.Row(rng() % queries.count), results);
                    if (status == ErrorCode::Success)
                        latency[t].push_back(std::chrono::duration<double, std::milli>(Clock::now() - start).count());
                    else
                        ++queryErrors[t];
                }
            });
        auto start = Clock::now();
        for (int t = 0; t < writers; ++t)
            inserters.emplace_back([&] {
                for (int offset = next++; offset < batch; offset = next++)
                {
                    SizeType id = -1;
                    auto status = engine.node->Insert("only", vectors.Row(initial + (phase - 1) * batch + offset), 1,
                                                      id, VectorValueType::UInt8, 128);
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
        }
        mapping.flush();
        std::vector<double> all;
        uint64_t failedQueries = 0;
        for (int t = 0; t < readers; ++t)
        {
            all.insert(all.end(), latency[t].begin(), latency[t].end());
            failedQueries += queryErrors[t];
        }
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
             int threads, double seconds, std::ostream &out)
{
    struct Row
    {
        int tenant;
        double ms;
        bool success, inWindow;
    };
    std::vector<std::vector<Row>> rows(threads);
    std::atomic<int> ready{0};
    std::atomic<bool> go{false};
    Clock::time_point stop;
    std::vector<std::thread> workers;
    for (int t = 0; t < threads; ++t)
        workers.emplace_back([&, t] {
            std::mt19937 rng(20261104 + t);
            std::vector<double> weights(names.size(), 1);
            if (mix == "hot" && names.size() > 1)
                weights[0] = 7;
            std::discrete_distribution<int> tenantID(weights.begin(), weights.end());
            std::uniform_int_distribution<int> queryID(0, queries.count - 1);
            std::vector<BasicResult> results;
            ++ready;
            while (!go.load(std::memory_order_acquire))
                std::this_thread::yield();
            while (Clock::now() < stop)
            {
                auto tenant = tenantID(rng);
                auto query = queries.Row(queryID(rng));
                auto start = Clock::now();
                auto status = engine.Search(names[tenant], query, results);
                auto end = Clock::now();
                rows[t].push_back({tenant, std::chrono::duration<double, std::milli>(end - start).count(),
                                   status == ErrorCode::Success, end <= stop});
            }
        });
    while (ready != threads)
        std::this_thread::yield();
    rusage before{}, after{};
    getrusage(RUSAGE_SELF, &before);
    auto start = Clock::now();
    stop = start + std::chrono::duration_cast<Clock::duration>(std::chrono::duration<double>(seconds));
    go.store(true, std::memory_order_release);
    for (auto &worker : workers)
        worker.join();
    getrusage(RUSAGE_SELF, &after);
    double elapsed = std::chrono::duration<double>(Clock::now() - start).count();
    for (int tenant = -1; tenant < int(names.size()); ++tenant)
    {
        std::vector<double> latencies;
        uint64_t completed = 0, errors = 0;
        for (const auto &thread : rows)
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
        auto percentile = [&](double p) {
            return latencies.empty() ? -1 : latencies[std::min(latencies.size() - 1, size_t(p * latencies.size()))];
        };
        out << "query," << (tenant < 0 ? "all" : names[tenant]) << "," << threads << "," << seconds << "," << completed
            << "," << errors << "," << completed / seconds << "," << percentile(.5) << "," << percentile(.95) << ","
            << percentile(.99) << "," << elapsed << "," << (CpuSeconds(after) - CpuSeconds(before)) / elapsed << ","
            << after.ru_maxrss << "\n";
    }
    engine.Stats(out);
    out.flush();
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
                                          vectors.Row(0, count), count));
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
            out << "# warmup\n";
            Measure(engine, queries, names, mix, threads, warmup, out);
            out << "# measured\n";
            Measure(engine, queries, names, mix, threads, seconds, out);
        }
        return 0;
    }
    catch (const std::exception &error)
    {
        std::cerr << "Benchmark failed: " << error.what() << "\n";
        return 1;
    }
}
