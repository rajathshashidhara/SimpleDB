#ifndef SIMPLEDB_NET_SERVICE
#define SIMPLEDB_NET_SERVICE

#include <cstdlib>

#include "protobufs/netformats.pb.h"
#include "protobufs/execformats.pb.h"

extern "C" {
    #include "uv.h"
}

using namespace simpledb::proto;

#define WORK_ERROR 0x1
#define WORK_EXEC  0x2
struct work_request
{
    uv_handle_t*    handle;
    uv_file         exec_fd;
    uv_process_t    process;
    uv_pipe_t       input_pipe;
    uv_pipe_t       output_pipe;
    void*           buffer;
    void*           output_buffer;
    size_t          len;
    int             flags;
};

enum ExecError
{
    EXEC_STATUS_OK,
    EXEC_STATUS_LOOKUP_FAILED,
    EXEC_STATUS_ARGS_INVALID,
};

struct ExecCmd
{
    uint64_t id;
    std::string func_name;
    std::string func;
    std::string arguments;
    std::string output_key;
    bool put_output;
    std::string output;
};

void handle_request(uv_work_t* wq);

#endif /* SIMPLEDB_NET_SERVICE */