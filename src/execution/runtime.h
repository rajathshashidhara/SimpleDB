#ifndef SIMPLEDB_EXEC_STATE_
#define SIMPLEDB_EXEC_STATE_

#include <string>
#include <functional>

#include "net/client.h"
#include "net/protobuf_stream_parser.h"
#include "formats/execformats.pb.h"
extern "C" {
    #include "uv.h"
}

class ExecutionState;

typedef std::function<void(ExecutionState* state,
                        const simpledb::proto::ExecResponse&
                    )> ExecComplCallback;

class ExecutionState
{
private:
    std::string function;
    std::string s_input_;
    simpledb::proto::ExecArgs args;
    simpledb::proto::ExecResponse resp;
    ExecComplCallback exec_compl_cb_;
    std::string output_buffer_;
    ProtobufStreamParser<simpledb::proto::ExecResponse> parser;

    uv_process_t process;
    uv_pipe_t input_pipe;
    uv_pipe_t output_pipe;

public:
    const uint64_t req_id;
    ClientState* const client;

    ExecutionState(uint64_t id,
            ClientState* state,
            const std::string& function,
            simpledb::proto::ExecArgs&& args)
        : function(function), args(args),
            exec_compl_cb_([](
                ExecutionState*,
                const simpledb::proto::ExecResponse&){}),
            req_id(id), client(state)
        {
            process.data = this;
        }
    ~ExecutionState() {}
    void Spawn(const ExecComplCallback& exec_cb);
};

#endif /*  SIMPLEDB_EXEC_STATE_ */