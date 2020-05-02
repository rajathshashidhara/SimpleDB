#include <string>
#include <exception>
#include <glog/logging.h>

#include "net/server.h"
#include "net/service.h"
#include "net/client.h"
#include "storage/db.h"
#include "formats/netformats.pb.h"
#include "util/exception.h"

#include "config.h"

extern "C" {
    #include "uv.h"
}

using namespace std;
using namespace simpledb::proto;

static void on_execution_completion(
            ExecutionState* exec_state,
            const simpledb::proto::ExecResponse& response);
static void on_work_completion(uv_work_t* work, int status);

static void on_execution_completion(
            ExecutionState* exec_state,
            const simpledb::proto::ExecResponse& response)
{
    WorkRequest* work_request = new WorkRequest{exec_state->req_id,
                                                exec_state->client,
                                                WorkRequest::EXEC};
    delete exec_state;  // Dispose off execution context!

    work_request->exec = std::move(response);

    uv_work_t* work = new uv_work_t();
    work->data = work_request;

    int ret;
    if ((ret = uv_queue_work(uv_default_loop(), work,
                Worker::process_work, on_work_completion)) < 0)
    {
        LOG(ERROR) << "Failed to launch work handler. Error: " << uv_strerror(ret);
        throw uv_error("Failed to launch work handler", ret);
    }
}

static void on_work_completion(uv_work_t* work, int status)
{
    WorkResult* work_result = (WorkResult*) work->data;
    delete work;    // Dispose off work!

    if (status < 0)
    {
        LOG(ERROR) << "Work request error: " << uv_strerror(status);
        throw uv_error("Work Request failed. ", status);
    }

    ClientState* client = work_result->client;
    ExecutionState* exec = nullptr;
    string s_response(sizeof(size_t), 0);
    size_t response_len;

    switch (work_result->tag)
    {
        case WorkResult::KV:
            response_len = work_result->kv.ByteSize();
            *((size_t*)(&s_response[0])) = response_len;
            s_response.append(move(work_result->kv.SerializeAsString()));
            client->Write(move(s_response));
            break;

        case WorkResult::EXEC:
            exec = new ExecutionState(
                        work_result->id,
                        client,
                        work_result->exec.function(),
                        move(work_result->exec)
                );
            exec->Spawn(on_execution_completion);
            break;

        default:
            throw runtime_error("Unknown Work Result");
    }

    delete work_result;
}

static void on_new_request(const uv_tcp_t* handle, const KVRequest& request)
{
    ClientState* state = (ClientState*) handle->data;

    WorkRequest* work_request = new WorkRequest{request.id(), state, WorkRequest::KV};
    work_request->kv = std::move(request);

    uv_work_t* work = new uv_work_t();
    work->data = work_request;

    int ret;
    if ((ret = uv_queue_work(uv_default_loop(), work,
                Worker::process_work, on_work_completion)) < 0)
    {
        LOG(ERROR) << "Failed to launch work handler. Error: " << uv_strerror(ret);
        throw uv_error("Failed to launch work handler", ret);
    }
}

void ServerState::Listen()
{
    int ret;

    static auto on_new_connection = [](uv_stream_t* server, int status)
    {
        if (status < 0)
        {
            LOG(ERROR) << "New connection failed. Error: " << uv_strerror(status);
            return;
        }

        ClientState* client = new ClientState((uv_tcp_t*) server);
        client->Read(on_new_request);
    };

    if ((ret = uv_listen((uv_stream_t*) listen_handle, backlog, on_new_connection)) != 0)
    {
        LOG(FATAL) << "Listen failed. Error: " << uv_strerror(ret);
        throw uv_error("Listen failed.", ret);
    }
}

ServerState::ServerState(uv_loop_t* loop,
                const string& ip,
                const uint16_t& port,
                unsigned int backlog)
        : loop_(loop), address_(ip, port), backlog(backlog)
{
    int ret;

    listen_handle = new uv_tcp_t();
    if (listen_handle == nullptr)
        throw runtime_error("Out of Memory!");

    if ((ret = uv_tcp_init(loop, listen_handle)) != 0)
    {
        LOG(ERROR) << "Failed to initialize TCP connection handle. Error: " << uv_strerror(ret);
        throw uv_error("Failed to initialize TCP Connection handle", ret);
    }

    const auto addr = address_.to_sockaddr();
    if ((ret = uv_tcp_bind(listen_handle, &addr, 0)) != 0)
    {
        LOG(FATAL) << "Failed to bind TCP socket. Error: " << uv_strerror(ret);
        throw uv_error("Failed to bind TCP socket.", ret);
    }

    listen_handle->data = this;
}

ServerState::~ServerState()
{
    auto close_cb = [](uv_handle_t* handle) {};
    uv_close((uv_handle_t*) listen_handle, close_cb);

    delete listen_handle;
}