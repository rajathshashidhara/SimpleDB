#include <string>
#include <sstream>
#include <exception>
#include <thread>
#include <atomic>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <climits>
#include <errno.h>

#include <glog/logging.h>
#include <google/protobuf/io/coded_stream.h>

#include "leveldb/db.h"
#include "leveldb/cache.h"
#include "formats/serialization.pb.h"
#include "formats/netformats.pb.h"

#include "storage/db.h"
#include "util/exception.h"
#include "util/crc16.h"

#include "config.h"

using namespace simpledb::storage;
using namespace std;

static inline void send_request(TCPSocket* socket, simpledb::proto::KVRequest& req)
{
    std::string req_s = req.SerializeAsString();
    size_t len = req_s.length();
    static size_t slen = (ssize_t) sizeof(size_t);

    socket->write(string((char*) &len, slen));
    socket->write(req_s);
}

static inline bool receive_response(TCPSocket* socket, simpledb::proto::KVResponse& resp)
{
    size_t len;
    const static size_t slen = sizeof(size_t);

    len = *((size_t*) (&socket->read_exactly(slen)[0]));

    string data = move(socket->read_exactly(len));
    google::protobuf::io::CodedInputStream istream((const uint8_t*) data.c_str(), data.length());
    istream.SetTotalBytesLimit(INT_MAX, INT_MAX);
    if (!resp.ParseFromCodedStream(&istream))
        return false;

    return true;
}

SimpleDB::SimpleDB(const SimpleDBConfig& config)
    : config_(config), db(nullptr), conns_(config.num_, nullptr), immutable_object_cache_(config.immutable_cache_size)
{
    leveldb::Options options;
    options.create_if_missing = config_.create_if_not_exists;
    options.block_cache = leveldb::NewLRUCache(config_.backend_cache_size);
    options.error_if_exists = false;

    leveldb::Status status = leveldb::DB::Open(options,
                                            config_.db_.string(),
                                            &db);
    if (!status.ok())
    {
        LOG(FATAL) << "Cannot create database. Error: " << status.ToString();
        throw runtime_error("Cannot create database. Error: " + status.ToString());
    }
}

void SimpleDB::connect()
{
    for (unsigned idx = 0; idx < config_.num_; idx++)
    {
        unsigned retry = 0;
        conns_[idx] = new TCPSocket();

        if (idx == config_.replica_idx)
            continue;

        while (retry < config_.conn_max_retry)
        {
            try
            {
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

    init = true;
}

void SimpleDB::get(vector<GetRequest>& download_requests,
            const function<void(const GetRequest&,
                                    const DbOpStatus,
                                    const std::string&)>& callback)
{
    if (!init)
        connect();

    const size_t bucket_count = config_.num_;
    const size_t batch_size = config_.max_batch_size;
    vector<vector<GetRequest>> buckets(bucket_count);

    // LOG(ERROR) << "GET Size=" << download_requests.size();
    for (size_t r_id = 0; r_id < download_requests.size(); r_id++)
    {
        const string& object_key = download_requests.at(r_id).object_key;
        const auto bIdx = crc16(object_key) % bucket_count;
        buckets[bIdx].emplace_back(move(download_requests.at(r_id)));
    }

    vector<size_t> non_empty_buckets;
    for (size_t idx = 0; idx < bucket_count; idx++)
    {
        if (buckets[idx].size() > 0)
            non_empty_buckets.push_back(idx);
    }

    const size_t thread_count = non_empty_buckets.size();
    vector<thread> threads;
    for (size_t thread_index = 0;
            thread_index < thread_count;
            thread_index++)
    {
        threads.emplace_back(
            [&](const size_t index)
            {
                const size_t bIdx = non_empty_buckets[index];
                // LOG(ERROR) << "tID=" << index << " bIdx=" << bIdx << " Replica= " << config_.replica_idx << " Size=" << buckets[bIdx].size();
                /* Local */
                if (bIdx == config_.replica_idx)
                {
                    for (auto &req: buckets[bIdx])
                    {
                        string content;
                        auto status = local_get(req, content);
                        callback(req, status, content);
                    }
                }
                else
                {
                    for (size_t first_file_idx = 0;
                        first_file_idx < buckets[bIdx].size();
                        first_file_idx += batch_size)
                    {
                        size_t expected_responses = 0;

                        for (size_t file_id = first_file_idx;
                            file_id < min(buckets[bIdx].size(), first_file_idx + batch_size);
                            file_id += 1)
                        {
                            const string & object_key =
                                buckets[bIdx].at(file_id).object_key;

                            /* Check cache */
                            string content;
                            if (immutable_object_cache_.access(object_key, content))
                            {
                                if (buckets[bIdx].at(file_id).filename.initialized())
                                {
                                    roost::atomic_create(content,
                                        buckets[bIdx].at(file_id).filename.get(),
                                        buckets[bIdx].at(file_id).mode.initialized(),
                                        buckets[bIdx].at(file_id).mode.get_or(0));
                                }

                                callback(buckets[bIdx].at(file_id), DbOpStatus::STATUS_OK, content);

                                continue;
                            }

                            simpledb::proto::KVRequest req;
                            req.set_id(file_id);
                            auto get_req = req.mutable_get_request();
                            get_req->set_key(object_key);

                            // LOG(ERROR) << "GET " << object_key << " FROM " << bIdx;
                            send_request(this->conns_[bIdx], req);
                            expected_responses++;
                        }

                        size_t response_count = 0;
                        while (response_count != expected_responses)
                        {
                            simpledb::proto::KVResponse resp;

                            if (!receive_response(this->conns_[bIdx], resp))
                                throw runtime_error("failed to get response");

                            const size_t response_index = resp.id();
                            auto &req = buckets[bIdx].at(response_index);
                            string content;
                            if (resp.return_code() != 0){
                                callback(req, static_cast<DbOpStatus>(resp.return_code()), content);
                                response_count++;
                                continue;
                            }

                            // LOG(ERROR) << "GOT " << req.object_key << " FROM " << bIdx;
                            content = std::move(resp.val());
                            immutable_object_cache_.insert(req.object_key, content);
                            if (req.filename.initialized())
                            {
                                roost::atomic_create(content, req.filename.get(),
                                    req.mode.initialized(),
                                    req.mode.get_or(0));
                            }

                            callback(req, DbOpStatus::STATUS_OK, content);

                            response_count++;
                        }
                    }
                }
            },
            thread_index
        );
    }

    for (auto & thread : threads)
        thread.join();
}

void SimpleDB::put(vector<PutRequest>& upload_requests,
            const function<void(const PutRequest&,
                                    const DbOpStatus)>& callback)
{
    if (!init)
        connect();

    const size_t bucket_count = config_.num_;
    const size_t batch_size = config_.max_batch_size;
    vector<vector<PutRequest>> buckets(bucket_count);

    for (size_t r_id = 0; r_id < upload_requests.size(); r_id++)
    {
        const string& object_key = upload_requests.at(r_id).object_key;
        const auto bIdx = crc16(object_key) % bucket_count;
        buckets[bIdx].emplace_back(move(upload_requests.at(r_id)));
    }

    vector<size_t> non_empty_buckets;
    for (size_t idx = 0; idx < bucket_count; idx++)
    {
        if (buckets[idx].size() > 0)
            non_empty_buckets.push_back(idx);
    }

    const size_t thread_count = non_empty_buckets.size();
    vector<thread> threads;
    for (size_t thread_index = 0;
            thread_index < thread_count;
            thread_index++)
    {
        threads.emplace_back(
            [&](const size_t index)
            {
                const size_t bIdx = non_empty_buckets[index];

                /* Local */
                if (bIdx == config_.replica_idx)
                {
                    for (auto &req: buckets[bIdx])
                    {
                        auto status = local_put(req);
                        callback(req, status);
                    }
                }
                else
                {
                    for (size_t first_file_idx = 0;
                        first_file_idx < buckets[bIdx].size();
                        first_file_idx += batch_size)
                    {
                        size_t expected_responses = 0;

                        for (size_t file_id = first_file_idx;
                            file_id < min(buckets[bIdx].size(), first_file_idx + batch_size);
                            file_id += 1)
                        {
                            string content;
                            auto &request = buckets[bIdx].at(file_id);
                            if (not request.object_data.initialized())
                            {
                                FileDescriptor file {
                                    CheckSystemCall("open " + request.filename.get().string(),
                                        open(request.filename.get().string().c_str(), O_RDONLY))
                                };
                                while (not file.eof())
                                    { content.append(file.read()); }
                                file.close();
                            }

                            simpledb::proto::KVRequest req;
                            req.set_id(file_id);
                            auto put_req = req.mutable_put_request();
                            put_req->set_key(request.object_key);
                            put_req->set_val(request.object_data.get_or(content));
                            put_req->set_immutable(request.immutable);
                            put_req->set_executable(request.executable);

                            send_request(this->conns_[bIdx], req);
                            if (request.immutable)
                            {
                                immutable_object_cache_.insert(request.object_key, request.object_data.get_or(content));
                            }
                            expected_responses++;
                        }

                        size_t response_count = 0;
                        while (response_count != expected_responses)
                        {
                            simpledb::proto::KVResponse resp;

                            if (!receive_response(this->conns_[bIdx], resp))
                                throw runtime_error("failed to get response");

                            const size_t response_index = resp.id();
                            auto &req = buckets[bIdx].at(response_index);

                            if (DbOpStatus::STATUS_OK != static_cast<DbOpStatus>(resp.return_code()))
                            {
                                immutable_object_cache_.drop(req.object_key);
                            }
                            callback(req, static_cast<DbOpStatus>(resp.return_code()));

                            response_count++;
                        }
                    }
                }
            }, thread_index
        );
    }

    for (auto & thread : threads)
        thread.join();
}

void SimpleDB::del(const vector<string>& object_keys,
    const function<void(const string&,
                            const DbOpStatus)>& callback)
{
    if (!init)
        connect();

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
            size_t expected_responses = 0;
            for (auto & req: buckets[bIdx])
            {
                simpledb::proto::KVRequest request;
                request.set_id(expected_responses++);
                auto del_req = request.mutable_delete_request();
                del_req->set_key(req);

                send_request(this->conns_[bIdx], request);
                immutable_object_cache_.drop(req);

                simpledb::proto::KVResponse resp;

                if (!receive_response(this->conns_[bIdx], resp))
                    throw runtime_error("failed to get response");

                callback(req, static_cast<DbOpStatus>(resp.return_code()));
            }
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
    google::protobuf::io::CodedInputStream istream((const uint8_t*) val.c_str(), val.length());
    istream.SetTotalBytesLimit(INT_MAX, INT_MAX);
    if (!file.ParseFromCodedStream(&istream))
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
        google::protobuf::io::CodedInputStream istream((const uint8_t*) val.c_str(), val.length());
        istream.SetTotalBytesLimit(INT_MAX, INT_MAX);
        if (!file.ParseFromCodedStream(&istream))
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