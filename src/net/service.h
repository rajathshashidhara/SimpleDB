#ifndef SIMPLEDB_NET_SERVICE
#define SIMPLEDB_NET_SERVICE

#include <cstdlib>

#include "execution/runtime.h"
#include "net/client.h"
#include "util/optional.h"
#include "storage/db.h"
#include "formats/netformats.pb.h"
#include "formats/execformats.pb.h"

extern "C" {
    #include "uv.h"
}

enum ExecError
{
    EXEC_STATUS_OK,
    EXEC_STATUS_LOOKUP_FAILED,
    EXEC_STATUS_ARGS_INVALID,
    EXEC_STATUS_ERR,
};

struct ExecCmd
{
    uint64_t id;
    std::string func_name;
    std::string func;
    std::string arguments;
    std::string output;
};

struct WorkRequest
{
    const uint64_t id;
    ClientState* const client;

    enum {KV, EXEC} const tag;
    simpledb::proto::KVRequest kv;
    simpledb::proto::ExecResponse exec;

    ~WorkRequest() {}
};

struct WorkResult
{
    const uint64_t id;
    ClientState* const client;

    enum {KV, EXEC} const tag;
    simpledb::proto::KVResponse kv;
    simpledb::proto::ExecArgs exec;

    ~WorkResult() {}
};

class Worker {
private:
    static simpledb::storage::SimpleDB* db;
    static roost::path cache_path;
    static void process_kv_request(const simpledb::proto::KVRequest& request,
                                    simpledb::proto::KVResponse& response);
    static void process_exec_request(const simpledb::proto::ExecRequest& request,
                                    simpledb::proto::ExecArgs& args);
    static void process_exec_result(const simpledb::proto::ExecResponse& result,
                                    simpledb::proto::KVResponse& response);
public:
    Worker(const simpledb::storage::SimpleDBConfig& config)
    {
        db = new simpledb::storage::SimpleDB(config);
        cache_path = config.db_ / "cache";
    }
    ~Worker()
    {
        if (db != nullptr)
            delete db;
    }
    static void process_work(uv_work_t* work);
};

#endif /* SIMPLEDB_NET_SERVICE */