#include <string>
#include <glog/logging.h>

#include "leveldb/db.h"
#include "leveldb/cache.h"
#include "protobufs/serialization.pb.h"

#include "db.h"

namespace simpledb::db
{
    static leveldb::DB *db;
};

int simpledb::db::init(std::string path, bool create, size_t cache_size)
{
    leveldb::Options options;
    options.create_if_missing = create;
    options.block_cache = leveldb::NewLRUCache(cache_size);

    leveldb::Status status = leveldb::DB::Open(options, path, &db);
    if (!status.ok())
    {
        LOG(FATAL) << "Cannot create database. Error: " << status.ToString();
        return -1;
    }

    return 0;
}

int simpledb::db::get(const std::string key, std::string& value)
{
    if (db == nullptr)
        return -STATUS_NOTFOUND;

    std::string val;
    leveldb::Status s = db->Get(leveldb::ReadOptions(), key, &val);

    if (!s.ok())
    {
        if (s.IsNotFound())
            return -STATUS_NOTFOUND;
        
        LOG(ERROR) << "Get Key=" << key << " Error: " << s.ToString();
        return -STATUS_IOERROR;
    }

    simpledb::proto::FileMetadata file;
    if (!file.ParseFromString(val))
    {
        LOG(ERROR) << "Get Key=" << key << " Error: Protobuf Parse error";
        return -STATUS_IOERROR;
    }

    value = file.content();
    return STATUS_OK;
}

int simpledb::db::set(const std::string key, const std::string value, const bool immutable)
{
    if (db == nullptr)
        return -STATUS_IOERROR;

    std::string val;
    simpledb::proto::FileMetadata file;
    
    leveldb::Status s = db->Get(leveldb::ReadOptions(), key, &val);
    if (!s.IsNotFound())
    {
        if (!file.ParseFromString(val))
        {
            LOG(ERROR) << "Set Key=" << key << " Error: Protobuf Parse error";
            return -STATUS_IOERROR;
        }

        if (file.immutable())
        {
            LOG(ERROR) << "Set Key=" << key << " Error: Cannot modify immutable file";
            return -STATUS_IMMUTABLE;
        }
    }

    file.set_immutable(immutable);
    file.set_content(value);
    val = file.SerializeAsString();

    s = db->Put(leveldb::WriteOptions(), key, file.SerializeAsString());
    if (!s.ok())
    {
        LOG(ERROR) << "Set Key=" << key << " Error: " << s.ToString();
        return -STATUS_IOERROR;
    }

    return STATUS_OK;
}

int simpledb::db::remove(const std::string key)
{
    if (db == nullptr)
        return -STATUS_NOTFOUND;

    leveldb::Status s = db->Delete(leveldb::WriteOptions(), key);
    if (s.IsNotFound())
    {
        LOG(ERROR) << "Remove Key=" << key << " Error: " << s.ToString();
        return -STATUS_NOTFOUND;
    }
    else if (!s.ok())
    {
        LOG(ERROR) << "Remove Key=" << key << " Error: " << s.ToString();
        return -STATUS_IOERROR;
    }

    return STATUS_OK;
}