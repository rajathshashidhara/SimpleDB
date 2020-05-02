#include <string>
#include <sstream>
#include <exception>
#include <thread>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>

#include <glog/logging.h>
#include <errno.h>

#include "leveldb/db.h"
#include "leveldb/cache.h"
#include "formats/serialization.pb.h"

#include "storage/db.h"
#include "util/exception.h"
#include "util/crc16.h"

#include "config.h"

using namespace simpledb::storage;
using namespace std;

SimpleDB::SimpleDB(const SimpleDBConfig& config)
    : config_(config), db(nullptr), conns_(config.num_, nullptr)
{
    leveldb::Options options;
    options.create_if_missing = config_.create_if_not_exists;
    options.block_cache = leveldb::NewLRUCache(config_.cache_size);
    options.error_if_exists = false;

    leveldb::Status status = leveldb::DB::Open(options,
                                            config_.db_.string(),
                                            &db);
    if (!status.ok())
    {
        LOG(FATAL) << "Cannot create database. Error: " << status.ToString();
        throw runtime_error("Cannot create database. Error: " + status.ToString());
    }

    for (unsigned idx = 0; idx < config_.num_; idx++)
    {
        unsigned retry = 0;
        if (idx == config_.replica_idx)
            continue;

        while (retry < config_.conn_max_retry)
        {
            try
            {
                conns_[idx] = new TCPSocket();
                conns_[idx]->connect(config_.address_[idx]);
                break;
            }
            catch (system_error& e)
            {
                LOG(ERROR) << "Failed to connect to replica " << idx << " " << e.what();

                if (retry == config_.conn_max_retry)
                    rethrow_exception(current_exception());
            }
            retry++;
            sleep(config_.conn_timeout_seconds);
        }
    }
}

void SimpleDB::get(vector<GetRequest>& download_requests,
            const function<void(const GetRequest&,
                                    const DbOpStatus,
                                    const std::string&)>& callback)
{
    const size_t bucket_count = config_.num_;

    vector<vector<GetRequest>> buckets(bucket_count);
    for (size_t r_id = 0; r_id < download_requests.size(); r_id++)
    {
        const string& object_key = download_requests.at(r_id).object_key;
        const auto bIdx = crc16(object_key) % bucket_count;
        buckets[bIdx].emplace_back(move(download_requests.at(r_id)));
    }

    for (size_t bIdx = 0; bIdx < bucket_count; bIdx++)
    {
        if (bIdx == config_.replica_idx)
        {
            /* Local */
            for (auto &req: buckets[bIdx])
            {
                string content;
                auto status = local_get(req, content);
                callback(req, status, content);
            }
        }
        else
        {
            throw runtime_error("Not implemented!");
        }
    }
}

void SimpleDB::put(vector<PutRequest>& upload_requests,
            const function<void(const PutRequest&,
                                    const DbOpStatus)>& callback)
{
    const size_t bucket_count = config_.num_;

    vector<vector<PutRequest>> buckets(bucket_count);
    for (size_t r_id = 0; r_id < upload_requests.size(); r_id++)
    {
        const string& object_key = upload_requests.at(r_id).object_key;
        const auto bIdx = crc16(object_key) % bucket_count;
        buckets[bIdx].emplace_back(move(upload_requests.at(r_id)));
    }

    for (size_t bIdx = 0; bIdx < bucket_count; bIdx++)
    {
        if (bIdx == config_.replica_idx)
        {
            /* Local */
            for (auto &req: buckets[bIdx])
            {
                auto status = local_put(req);
                callback(req, status);
            }
        }
        else
        {
            throw runtime_error("Not implemented!");
        }
    }
}

void SimpleDB::del(const vector<string>& object_keys,
    const function<void(const string&,
                            const DbOpStatus)>& callback)
{
    const size_t bucket_count = config_.num_;

    vector<vector<string>> buckets(bucket_count);
    for (size_t r_id = 0; r_id < object_keys.size(); r_id++)
    {
        const string& object_key = object_keys.at(r_id);
        const auto bIdx = crc16(object_key) % bucket_count;
        buckets[bIdx].push_back(object_key);
    }

    for (size_t bIdx = 0; bIdx < bucket_count; bIdx++)
    {
        if (bIdx == config_.replica_idx)
        {
            /* Local */
            for (auto &req: buckets[bIdx])
            {
                auto status = local_del(req);
                callback(req, status);
            }
        }
        else
        {
            throw runtime_error("Not implemented!");
        }
    }
}

DbOpStatus SimpleDB::local_get(const GetRequest& request, std::string& data)
{
    std::string val;
    leveldb::Status s = db->Get(leveldb::ReadOptions(), request.object_key, &val);

    if (!s.ok())
    {
        if (s.IsNotFound())
        {
            LOG(INFO) << "Get Key=" << request.object_key << " Not found";
            return DbOpStatus::STATUS_NOTFOUND;
        }

        LOG(INFO) << "Get Key=" << request.object_key << " Error: " << s.ToString();
        return DbOpStatus::STATUS_IOERROR;
    }

    simpledb::proto::FileMetadata file;
    if (!file.ParseFromString(val))
    {
        LOG(ERROR) << "Get Key=" << request.object_key << " Error: Protobuf Parse error";
        return DbOpStatus::STATUS_IOERROR;
    }

    if (request.exec && !file.executable())
        return DbOpStatus::STATUS_NOTFOUND;

    data = std::move(file.content());

    if (request.filename.initialized())
    {
        roost::atomic_create(data, request.filename.get(),
            request.mode.initialized(),
            request.mode.get_or(0));
    }

    return DbOpStatus::STATUS_OK;
}

DbOpStatus SimpleDB::local_put(const PutRequest& request)
{
    std::string val;
    simpledb::proto::FileMetadata file;

    leveldb::Status s = db->Get(leveldb::ReadOptions(), request.object_key, &val);
    if (!s.IsNotFound())
    {
        if (!file.ParseFromString(val))
        {
            LOG(ERROR) << "Set Key=" << request.object_key << " Error: Protobuf Parse error";
            return DbOpStatus::STATUS_IOERROR;
        }

        if (file.immutable())
        {
            LOG(INFO) << "Set Key=" << request.object_key << " Error: Cannot modify immutable file";
            return DbOpStatus::STATUS_IMMUTABLE;
        }
    }

    val.clear();
    if (not request.object_data.initialized())
    {
        FileDescriptor file {
            CheckSystemCall("open " + request.filename.get().string(),
                open(request.filename.get().string().c_str(), O_RDONLY))
        };
        while (not file.eof())
            { val.append(file.read()); }
        file.close();
    }

    file.set_content(request.object_data.get_or(val));
    file.set_immutable(request.immutable);
    file.set_executable(request.executable);
    val = file.SerializeAsString();

    s = db->Put(leveldb::WriteOptions(), request.object_key, val);
    if (!s.ok())
    {
        LOG(ERROR) << "Set Key=" << request.object_key << " Error: " << s.ToString();
        return DbOpStatus::STATUS_IOERROR;
    }

    return DbOpStatus::STATUS_OK;
}

DbOpStatus SimpleDB::local_del(const std::string& key)
{
    leveldb::Status s = db->Delete(leveldb::WriteOptions(), key);
    if (s.IsNotFound())
    {
        LOG(INFO) << "Remove Key=" << key << " Error: " << s.ToString();
        return DbOpStatus::STATUS_NOTFOUND;
    }
    else if (!s.ok())
    {
        LOG(ERROR) << "Remove Key=" << key << " Error: " << s.ToString();
        return DbOpStatus::STATUS_IOERROR;
    }

    return DbOpStatus::STATUS_OK;
}