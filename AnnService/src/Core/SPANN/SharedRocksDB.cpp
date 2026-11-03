// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/Core/SPANN/SharedRocksDB.h"
#ifdef ROCKSDB
#include <rocksdb/merge_operator.h>
#include <rocksdb/table.h>
#include <rocksdb/utilities/checkpoint.h>
#include <rocksdb/write_batch.h>

namespace SPTAG::SPANN
{
namespace
{
ErrorCode Status(const rocksdb::Status &status)
{
    if (status.ok())
        return ErrorCode::Success;
    if (status.IsNotFound())
        return ErrorCode::Key_NotFound;
    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Shared RocksDB: %s\n", status.ToString().c_str());
    return ErrorCode::DiskIOFail;
}

class PostingMerge : public rocksdb::AssociativeMergeOperator
{
  public:
    bool Merge(const rocksdb::Slice &, const rocksdb::Slice *old, const rocksdb::Slice &operand, std::string *result,
               rocksdb::Logger *) const override
    {
        result->clear();
        if (old)
            result->assign(old->data(), old->size());
        result->append(operand.data(), operand.size());
        return true;
    }
    const char *Name() const override
    {
        return "SPTAG.NamespacedPostingAppend.v1";
    }
};

class PostingStore : public Helper::KeyValueIO
{
  public:
    PostingStore(std::shared_ptr<SharedRocksDB> engine, std::string prefix)
        : m_engine(std::move(engine)), m_prefix(std::move(prefix))
    {
    }

    void ShutDown() override
    {
        m_engine.reset();
    }
    bool Available() override
    {
        return m_engine != nullptr;
    }
    ErrorCode Get(SizeType key, std::string *value, const std::chrono::microseconds &,
                  std::vector<Helper::AsyncReadRequest> *) override
    {
        if (!m_engine || key < 0 || !value)
            return Invalid();
        return m_engine->Get("postings", Key(key), *value);
    }
    ErrorCode Put(SizeType key, const std::string &value, const std::chrono::microseconds &,
                  std::vector<Helper::AsyncReadRequest> *) override
    {
        if (!m_engine || key < 0)
            return Invalid();
        return m_engine->Put("postings", Key(key), value);
    }
    ErrorCode Merge(SizeType key, const std::string &value, const std::chrono::microseconds &,
                    std::vector<Helper::AsyncReadRequest> *, std::function<bool(const void *, const int)>) override
    {
        if (!m_engine || key < 0 || value.empty())
            return Invalid();
        return m_engine->Merge("postings", Key(key), value);
    }
    ErrorCode Delete(SizeType key) override
    {
        if (!m_engine || key < 0)
            return Invalid();
        return m_engine->Delete("postings", Key(key));
    }
    ErrorCode DeleteRange(SizeType begin, SizeType end) override
    {
        if (!m_engine || begin < 0 || end < begin)
            return Invalid();
        return m_engine->DeleteRange("postings", Key(begin), Key(end));
    }
    ErrorCode MultiGet(const std::vector<SizeType> &keys, std::vector<std::string> *values,
                       const std::chrono::microseconds &, std::vector<Helper::AsyncReadRequest> *) override
    {
        if (!m_engine || !values)
            return Invalid();
        std::vector<std::string> encoded;
        encoded.reserve(keys.size());
        for (auto key : keys)
        {
            if (key < 0)
                return Invalid();
            encoded.push_back(Key(key));
        }
        return m_engine->MultiGet("postings", encoded, *values);
    }
    ErrorCode MultiGet(const std::vector<SizeType> &keys, std::vector<Helper::PageBuffer<std::uint8_t>> &values,
                       const std::chrono::microseconds &timeout,
                       std::vector<Helper::AsyncReadRequest> *requests) override
    {
        if (values.size() < keys.size())
            return Invalid();
        std::vector<std::string> found;
        auto status = MultiGet(keys, &found, timeout, requests);
        if (status != ErrorCode::Success)
            return status;
        for (size_t i = 0; i < found.size(); ++i)
        {
            values[i].ReservePageBuffer(found[i].size());
            if (!found[i].empty())
                memcpy(values[i].GetBuffer(), found[i].data(), found[i].size());
            values[i].SetAvailableSize(found[i].size());
        }
        return ErrorCode::Success;
    }
    ErrorCode Checkpoint(std::string) override
    {
        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Index views cannot checkpoint a shared engine\n");
        return ErrorCode::Fail;
    }

  private:
    static ErrorCode Invalid()
    {
        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Invalid or closed posting view operation\n");
        return ErrorCode::FailedParseValue;
    }
    std::string Key(SizeType key) const
    {
        return m_prefix + SharedRocksDB::EncodeNumber(key);
    }
    std::shared_ptr<SharedRocksDB> m_engine;
    const std::string m_prefix;
};
} // namespace

ErrorCode SharedRocksDB::Open(const std::string &path, const std::vector<std::string> &families,
                              std::shared_ptr<SharedRocksDB> &result, bool create,
                              std::shared_ptr<rocksdb::Cache> cache)
{
    result.reset();
    auto engine = std::shared_ptr<SharedRocksDB>(new SharedRocksDB());
    engine->m_names = {rocksdb::kDefaultColumnFamilyName};
    engine->m_names.insert(engine->m_names.end(), families.begin(), families.end());
    std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;
    for (const auto &name : engine->m_names)
    {
        rocksdb::ColumnFamilyOptions options;
        options.write_buffer_size = 4 << 20;
        options.max_write_buffer_number = 2;
        rocksdb::BlockBasedTableOptions table;
        table.block_cache = cache;
        options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table));
        if (name == "postings")
            options.merge_operator = std::make_shared<PostingMerge>();
        descriptors.emplace_back(name, options);
    }
    rocksdb::DBOptions options;
    options.create_if_missing = create;
    options.create_missing_column_families = create;
    options.max_background_jobs = 2;
    rocksdb::DB *raw = nullptr;
    auto status = rocksdb::DB::Open(options, path, descriptors, &engine->m_handles, &raw);
    engine->m_db.reset(raw);
    if (!status.ok())
        return Status(status);
    result = std::move(engine);
    return ErrorCode::Success;
}

SharedRocksDB::~SharedRocksDB()
{
    for (auto *handle : m_handles)
        delete handle;
}

rocksdb::ColumnFamilyHandle *SharedRocksDB::Family(const std::string &name) const
{
    for (size_t i = 0; i < m_names.size(); ++i)
        if (m_names[i] == name)
            return m_handles[i];
    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Unknown RocksDB CF: %s\n", name.c_str());
    return nullptr;
}

ErrorCode SharedRocksDB::Get(const std::string &family, const std::string &key, std::string &value) const
{
    auto *cf = Family(family);
    return cf ? Status(m_db->Get(rocksdb::ReadOptions(), cf, key, &value)) : ErrorCode::ParamNotFound;
}

ErrorCode SharedRocksDB::Put(const std::string &family, const std::string &key, const std::string &value)
{
    auto *cf = Family(family);
    return cf ? Status(m_db->Put(rocksdb::WriteOptions(), cf, key, value)) : ErrorCode::ParamNotFound;
}

ErrorCode SharedRocksDB::MultiGet(const std::string &family, const std::vector<std::string> &keys,
                                  std::vector<std::string> &values) const
{
    auto *cf = Family(family);
    if (!cf)
        return ErrorCode::ParamNotFound;
    std::vector<rocksdb::Slice> slices;
    slices.reserve(keys.size());
    for (const auto &key : keys)
        slices.emplace_back(key);
    std::vector<rocksdb::ColumnFamilyHandle *> families(keys.size(), cf);
    std::vector<std::string> found;
    const auto statuses = m_db->MultiGet(rocksdb::ReadOptions(), families, slices, &found);
    for (const auto &status : statuses)
        if (!status.ok())
            return Status(status);
    values = std::move(found);
    return ErrorCode::Success;
}

ErrorCode SharedRocksDB::Merge(const std::string &family, const std::string &key, const std::string &value)
{
    auto *cf = Family(family);
    return cf ? Status(m_db->Merge(rocksdb::WriteOptions(), cf, key, value)) : ErrorCode::ParamNotFound;
}

ErrorCode SharedRocksDB::Delete(const std::string &family, const std::string &key)
{
    auto *cf = Family(family);
    return cf ? Status(m_db->Delete(rocksdb::WriteOptions(), cf, key)) : ErrorCode::ParamNotFound;
}

ErrorCode SharedRocksDB::DeleteRange(const std::string &family, const std::string &begin, const std::string &end)
{
    auto *cf = Family(family);
    return cf ? Status(m_db->DeleteRange(rocksdb::WriteOptions(), cf, begin, end)) : ErrorCode::ParamNotFound;
}

ErrorCode SharedRocksDB::Write(const std::vector<std::tuple<std::string, std::string, std::string>> &entries)
{
    rocksdb::WriteBatch batch;
    for (const auto &entry : entries)
    {
        auto *cf = Family(std::get<0>(entry));
        if (!cf)
            return ErrorCode::ParamNotFound;
        batch.Put(cf, std::get<1>(entry), std::get<2>(entry));
    }
    return Status(m_db->Write(rocksdb::WriteOptions(), &batch));
}

ErrorCode SharedRocksDB::Scan(const std::string &family, const std::string &prefix,
                              const std::function<ErrorCode(const std::string &, const std::string &)> &visitor) const
{
    auto *cf = Family(family);
    if (!cf)
        return ErrorCode::ParamNotFound;
    std::unique_ptr<rocksdb::Iterator> iterator(m_db->NewIterator(rocksdb::ReadOptions(), cf));
    for (iterator->Seek(prefix); iterator->Valid() && iterator->key().starts_with(prefix); iterator->Next())
    {
        auto status = visitor(iterator->key().ToString(), iterator->value().ToString());
        if (status != ErrorCode::Success)
            return status;
    }
    return Status(iterator->status());
}

ErrorCode SharedRocksDB::Checkpoint(const std::string &directory) const
{
    rocksdb::Checkpoint *raw = nullptr;
    auto status = rocksdb::Checkpoint::Create(m_db.get(), &raw);
    if (!status.ok())
        return Status(status);
    std::unique_ptr<rocksdb::Checkpoint> checkpoint(raw);
    return Status(checkpoint->CreateCheckpoint(directory));
}

ErrorCode SharedRocksDB::FlushWAL()
{
    return Status(m_db->FlushWAL(false));
}

std::string SharedRocksDB::EncodeNumber(std::uint64_t value)
{
    std::string result(8, '\0');
    for (int i = 7; i >= 0; --i)
    {
        result[i] = static_cast<char>(value & 255);
        value >>= 8;
    }
    return result;
}

bool SharedRocksDB::DecodeNumber(const std::string &value, std::uint64_t &number)
{
    if (value.size() != 8)
        return false;
    number = 0;
    for (unsigned char byte : value)
        number = (number << 8) | byte;
    return true;
}

std::string SharedRocksDB::Namespace(const std::string &tenant, std::uint64_t index)
{
    return std::string(1, '\1') + EncodeNumber(tenant.size()) + tenant + EncodeNumber(index);
}

std::string SharedRocksDB::PrefixEnd(std::string prefix)
{
    for (size_t i = prefix.size(); i > 0; --i)
    {
        auto byte = static_cast<unsigned char>(prefix[i - 1]);
        if (byte != 255)
        {
            prefix[i - 1] = static_cast<char>(byte + 1);
            prefix.resize(i);
            return prefix;
        }
    }
    return {};
}

std::shared_ptr<Helper::KeyValueIO> SharedRocksDB::PostingView(const std::string &tenant, std::uint64_t index)
{
    return std::make_shared<PostingStore>(shared_from_this(), Namespace(tenant, index));
}
} // namespace SPTAG::SPANN
#endif
