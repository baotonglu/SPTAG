// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/Core/SPANN/TenantIndexManager.h"
#include <fstream>
#include <iostream>

using namespace SPTAG;

namespace
{
bool ReadVectors(const char *path, VectorValueType type, ByteArray &data, SizeType &count, DimensionType &dimension)
{
    std::ifstream input(path, std::ios::binary);
    input.read(reinterpret_cast<char *>(&count), sizeof(count));
    input.read(reinterpret_cast<char *>(&dimension), sizeof(dimension));
    SPANN::TenantIndexManager::Limits limits;
    if (!input || count <= 0 || count > limits.maxVectorsPerTenant || dimension <= 0 ||
        dimension > limits.maxDimension || GetValueTypeSize(type) == 0)
        return false;
    const uint64_t bytes = static_cast<uint64_t>(count) * dimension * GetValueTypeSize(type);
    input.seekg(0, std::ios::end);
    if (input.tellg() != static_cast<std::streamoff>(bytes + 8))
        return false;
    input.seekg(8);
    data = ByteArray::Alloc(bytes);
    input.read(reinterpret_cast<char *>(data.Data()), bytes);
    return static_cast<bool>(input);
}
} // namespace

int main(int argc, char **argv)
{
    if (argc < 3)
    {
        std::cerr << "Trusted offline administration (stop server first):\n"
                  << "tenanttool ROOT create TENANT TYPE METRIC VECTORS.bin [BKT|KDT]\n"
                  << "tenanttool ROOT insert TENANT VECTORS.bin\n"
                  << "tenanttool ROOT search TENANT QUERY.bin K\n"
                  << "tenanttool ROOT delete-vector TENANT VID\n"
                  << "tenanttool ROOT delete-tenant TENANT\n"
                  << "tenanttool ROOT list|stats|checkpoint\n"
                  << "Binary input: int32 count, int32 dimension, packed vectors. Query count must be 1.\n";
        return 2;
    }
    const std::string command = argv[2];
    bool valid = (command == "create" && (argc == 7 || argc == 8)) || (command == "insert" && argc == 5) ||
                 (command == "search" && argc == 6) || (command == "delete-vector" && argc == 5) ||
                 (command == "delete-tenant" && argc == 4) ||
                 ((command == "list" || command == "stats" || command == "checkpoint") && argc == 3);
    if (!valid)
    {
        std::cerr << "Invalid command or arguments\n";
        return 2;
    }
    std::unique_ptr<SPANN::TenantIndexManager> node;
    auto status = SPANN::TenantIndexManager::Open(argv[1], {}, node);
    if (status != ErrorCode::Success)
        return 1;
    ByteArray vectors;
    SizeType count = 0;
    DimensionType dimension = 0;
    VectorValueType type = VectorValueType::Undefined;
    if (command == "create")
    {
        DistCalcMethod metric;
        IndexAlgoType headIndexAlgo = IndexAlgoType::Undefined;
        if (!Helper::Convert::ConvertStringTo<VectorValueType>(argv[4], type) ||
            !Helper::Convert::ConvertStringTo<DistCalcMethod>(argv[5], metric) ||
            (argc == 8 && (!Helper::Convert::ConvertStringTo<IndexAlgoType>(argv[7], headIndexAlgo) ||
                           (headIndexAlgo != IndexAlgoType::BKT && headIndexAlgo != IndexAlgoType::KDT))) ||
            !ReadVectors(argv[6], type, vectors, count, dimension))
        {
            std::cerr << "Invalid type, metric, head algorithm or vector file\n";
            return 2;
        }
        status = node->Create(argv[3], type, dimension, metric, vectors, count, headIndexAlgo);
    }
    else if (command == "insert" || command == "search")
    {
        DimensionType expected;
        status = node->Describe(argv[3], type, expected);
        if (status != ErrorCode::Success)
            return 1;
        if (!ReadVectors(argv[4], type, vectors, count, dimension) || dimension != expected)
        {
            std::cerr << "Invalid vector file or dimension\n";
            return 2;
        }
        if (command == "insert")
        {
            SizeType first;
            status = node->Insert(argv[3], vectors, count, first);
            if (status == ErrorCode::Success)
                std::cout << "first_id=" << first << "\n";
        }
        else
        {
            SizeType k;
            if (count != 1 || !Helper::Convert::ConvertStringTo<SizeType>(argv[5], k))
            {
                std::cerr << "Search requires one query and integer K\n";
                return 2;
            }
            std::vector<BasicResult> results;
            status = node->Search(argv[3], vectors, k, results, type);
            if (status == ErrorCode::Success)
                for (const auto &result : results)
                    std::cout << result.VID << " " << result.Dist << "\n";
        }
    }
    else if (command == "delete-vector")
    {
        SizeType id;
        if (!Helper::Convert::ConvertStringTo<SizeType>(argv[4], id))
        {
            std::cerr << "Invalid vector ID\n";
            return 2;
        }
        status = node->DeleteVector(argv[3], id);
    }
    else if (command == "delete-tenant")
        status = node->DeleteTenant(argv[3]);
    else if (command == "checkpoint")
        status = node->Checkpoint();
    else if (command == "stats")
    {
        SPANN::TenantIndexManager::NodeStats state;
        std::vector<SPANN::TenantIndexManager::TenantStats> tenants;
        node->GetStats(state, tenants);
        std::cout << "resident_tenants=" << state.residentTenants
                  << " max_resident_tenants=" << state.maxResidentTenants
                  << " resident_snapshot_bytes=" << state.residentBytes
                  << " max_resident_bytes=" << state.maxResidentBytes << "\n";
        const char *names[] = {"Unloaded", "Loading", "Resident", "Evicting"};
        for (const auto &tenant : tenants)
            std::cout << tenant.name << " index_id=" << tenant.indexID
                      << " state=" << names[static_cast<int>(tenant.state)]
                      << " snapshot_bytes=" << tenant.snapshotBytes << " dirty=" << tenant.dirty << "\n";
    }
    else
        for (const auto &tenant : node->List())
            std::cout << tenant << "\n";

    if (status != ErrorCode::Success)
    {
        std::cerr << "Operation failed: " << static_cast<int>(status) << "\n";
        return 1;
    }
    return node->Close() == ErrorCode::Success ? 0 : 1;
}
