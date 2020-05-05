#ifndef SIMPLEDB_DB_H
#define SIMPLEDB_DB_H

#include <string>
#include <deque>

#include "leveldb/db.h"
#include "leveldb/cache.h"

#include "config.h"
#include "util/path.h"
#include "util/optional.h"
#include "net/socket.h"
#include "net/address.h"

namespace simpledb::storage
{
    enum class DbOpStatus
    {
        STATUS_OK,
        STATUS_NOTFOUND,
        STATUS_IMMUTABLE,
        STATUS_IOERROR,
        STATUS_INVALID
    };

    struct GetRequest
    {
        std::string object_key;
        Optional<roost::path> filename;
        Optional<mode_t> mode;
        bool exec { false };

        GetRequest(const std::string& object_key, bool exec = false)
            :   object_key(object_key), filename(false),
                mode(false), exec(exec) {}
        GetRequest(const std::string& object_key,
                    const roost::path& filename,
                    mode_t mode,
                    bool exec = false)
            :   object_key(object_key), filename(true, filename),
                mode(true, mode), exec(exec) {}
    };

    struct PutRequest
    {
        std::string object_key;
        Optional<std::string> object_data;
        Optional<roost::path> filename;
        bool immutable { false };
        bool executable { false };

        PutRequest(const std::string& object_key,
                    const std::string data,
                    bool immutable = false,
                    bool executable = false)
                : object_key(object_key),
                object_data(true, data),
                immutable(immutable), executable(executable) {}
        PutRequest(const std::string& object_key,
                    const roost::path& filename,
                    bool immutable = false,
                    bool executable = false)
                : object_key(object_key),
                filename(true, filename),
                immutable(immutable), executable(executable) {}
    };

    struct SimpleDBConfig
    {
        unsigned num_ {0};
        std::vector<Address> address_;
        unsigned replica_idx {0};
        unsigned int conn_timeout_seconds { 5 };
        unsigned int conn_max_retry { 32 };

        roost::path db_;
        bool create_if_not_exists { true };
        size_t cache_size;

        size_t max_batch_size {32};

        SimpleDBConfig(): address_(0), db_("") {}
    };

    class SimpleDB
    {
    private:
        SimpleDBConfig config_;
        leveldb::DB *db;
        std::deque<TCPSocket*> conns_;
        bool init { false };


        void connect();

    public:
        SimpleDB(const SimpleDBConfig& config);
        ~SimpleDB() {}

        DbOpStatus local_get(const GetRequest& request,
            std::string& data);
        DbOpStatus local_put(const PutRequest& request);
        DbOpStatus local_del(const std::string& object_key);

        void get(std::vector<GetRequest>& download_requests,
            const std::function<void(const GetRequest&,
                                    const DbOpStatus,
                                    const std::string&)>& callback
                        = [](const GetRequest&,
                            const DbOpStatus,
                            const std::string&){});

        void put(std::vector<PutRequest>& upload_requests,
            const std::function<void(const PutRequest&,
                                    const DbOpStatus)>& callback
                        = [](const PutRequest&, const DbOpStatus){});

        void del(const std::vector<std::string>& object_keys,
            const std::function<void(const std::string&,
                                    const DbOpStatus)>& callback
                        = [](const std::string&, const DbOpStatus){});
    };
}

#endif /* SIMPLEDB_DB_H */