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

// enum client_parse_e
// {
//     CLIENT_RECV_INIT,
//     CLIENT_RECV_ALLOC_LEN,
//     CLIENT_RECV_LEN,
//     CLIENT_RECV_ALLOC_PAYLOAD,
//     CLIENT_RECV_PAYLOAD,
//     CLIENT_RECV_INVALID
// };

// struct client_state_t
// {
//     uv_tcp_t* connection;
//     char* req_buffer;
//     client_parse_e parse_state;
//     size_t req_buf_length;
//     size_t read_length;
//     size_t alloc_length;
// };

// static void on_close_connection(uv_handle_t* handle);
// static void on_shutdown(uv_shutdown_t* req, int status);
// static void on_msg_write(uv_write_t* req, int status);
// static void on_close_pipe(uv_handle_t* pipe_handle);
// static void on_handle_request(uv_work_t* req, int status);
// static void on_exec_compl(uv_process_t* process, int64_t exit_status, int term_signal);
// static void on_pipe_read(uv_stream_t* handle,
//         ssize_t nread, const uv_buf_t* buf);
// static void alloc_piperead_cb(uv_handle_t* handle,
//                                 size_t suggested_size,
//                                 uv_buf_t* buf);
// static void on_exec_write_compl(uv_write_t* req, int status);
// static void on_fsexec_close(uv_fs_t* req);
// static void on_fsexec_write(uv_fs_t* req);
// static void on_fsexec_open(uv_fs_t* req);
// static void on_msg_read(uv_stream_t* handle,
//         ssize_t nread, const uv_buf_t* buf);
// static void alloc_readbuffer_cb(uv_handle_t* handle,
//                                 size_t suggested_size,
//                                 uv_buf_t* buf);
// static void on_new_connection(uv_stream_t *server, int status);


// static void on_close_pipe(uv_handle_t* pipe_handle)
// {
//     /* Do nothing */
// }

// static void on_exec_compl(uv_process_t* process, int64_t exit_status, int term_signal)
// {
//     work_request* wr = (work_request*) process->data;
//     ExecCmd* cmd = (ExecCmd*) wr->buffer;
//     int ret;

//     if (exit_status < 0 || (term_signal > 0))
//     {
//         LOG(ERROR) << "Unable to launch process";
//         uv_close(wr->handle, on_close_connection);

//         delete cmd;
//         delete wr;

//         return;
//     }

//     uv_work_t* work_req = new uv_work_t();
//     work_req->data = wr;
//     wr->flags = 0;

//     if ((ret = uv_queue_work(uv_default_loop(), work_req, handle_exec_compl, on_handle_request)) < 0)
//     {
//         LOG(ERROR) << "Failed to launch work handler. Error: " << uv_strerror(ret);
//         uv_close((uv_handle_t*) wr->handle, on_close_connection);

//         delete work_req;
//         delete cmd;
//         delete wr;

//         return;
//     }

//     uv_close((uv_handle_t*) &wr->input_pipe, on_close_pipe);
//     uv_close((uv_handle_t*) &wr->output_pipe, on_close_pipe);
// }

// static void on_pipe_read(uv_stream_t* handle,
//         ssize_t nread, const uv_buf_t* buf)
// {
//     work_request* wr = (work_request*) handle->data;
//     ExecCmd* cmd = (ExecCmd*) wr->buffer;

//     if (nread == UV_EOF)
//     {
//         return;
//     }

//     if (nread < 0)
//     {
//         LOG(ERROR) << "Input error";
//         uv_close(wr->handle, on_close_connection);

//         delete cmd;
//         delete wr;

//         return;
//     }

//     cmd->output.append(std::move(std::string((char*) wr->output_buffer, nread)));
//     delete [] ((char*) wr->output_buffer);
//     wr->output_buffer = nullptr;

//     return;
// }

// static void alloc_piperead_cb(uv_handle_t* handle,
//                                 size_t suggested_size,
//                                 uv_buf_t* buf)
// {
//     work_request* wr = (work_request*) handle->data;
//     ExecCmd* cmd = (ExecCmd*) wr->buffer;
//     wr->output_buffer = new char[suggested_size];

//     if (wr->output_buffer == nullptr)
//     {
//         LOG(ERROR) << "Not enough memory";
//         uv_close(wr->handle, on_close_connection);

//         delete cmd;
//         delete wr;
//     }

//     buf->base = (char*) (wr->output_buffer);
//     buf->len = suggested_size;
// }

// static void on_exec_write_compl(uv_write_t* req, int status)
// {
//     work_request* wr = (work_request*) req->data;
//     ExecCmd* cmd = (ExecCmd*) wr->buffer;
//     delete req;

//     if (status < 0)
//     {
//         LOG(ERROR) << "Execution failed";
//         uv_close(wr->handle, on_close_connection);

//         uv_close((uv_handle_t*) &wr->input_pipe, on_close_pipe);
//         uv_close((uv_handle_t*) &wr->output_pipe, on_close_pipe);

//         delete cmd;
//         delete wr;

//         return;
//     }

//     int ret;
//     wr->output_pipe.data = (void*) wr;
//     if ((ret = uv_read_start((uv_stream_t*) &wr->output_pipe, alloc_piperead_cb, on_pipe_read)) < 0)
//     {
//         LOG(ERROR) << "Unable to read from process outstream";
//         uv_close(wr->handle, on_close_connection);

//         uv_close((uv_handle_t*) &wr->input_pipe, on_close_pipe);
//         uv_close((uv_handle_t*) &wr->output_pipe, on_close_pipe);

//         delete wr;
//         delete cmd;

//     }

//     return;
// }

// static void on_fsexec_close(uv_fs_t* req)
// {
//     work_request* wr = (work_request*) req->data;
//     ExecCmd* cmd = (ExecCmd*) wr->buffer;
//     uv_fs_req_cleanup(req);

//     int ret;
//     char* args[2];
//     args[0] = &cmd->func_name[0];
//     args[1] = NULL;
//     wr->process.data = (void*) wr;

//     uv_pipe_init(uv_default_loop(), &wr->input_pipe, 1);
//     uv_pipe_init(uv_default_loop(), &wr->output_pipe, 1);
//     uv_stdio_container_t child_stdio[3];
//     child_stdio[0].flags = (uv_stdio_flags) (UV_CREATE_PIPE | UV_READABLE_PIPE);
//     child_stdio[0].data.stream = (uv_stream_t*) &wr->input_pipe;
//     child_stdio[1].flags = (uv_stdio_flags) (UV_CREATE_PIPE | UV_WRITABLE_PIPE);
//     child_stdio[1].data.stream = (uv_stream_t*) &wr->output_pipe;
//     child_stdio[2].flags = UV_INHERIT_FD;
//     child_stdio[2].data.fd = 2;

//     uv_process_options_t options = {0};
//     options.exit_cb = on_exec_compl;
//     options.file = cmd->func_name.c_str();
//     options.args = args;
//     options.stdio_count = 3;
//     options.stdio = child_stdio;

//     if ((ret = uv_spawn(uv_default_loop(), &wr->process, &options)) < 0)
//     {
//         LOG(ERROR) << "Unable to spawn process: " << uv_strerror(ret);
//         uv_close(wr->handle, on_close_connection);

//         delete cmd;
//         delete wr;
//     }

//     /* Write data into stream */
//     auto input = uv_buf_init(&cmd->arguments[0], cmd->arguments.length());
//     uv_write_t* write_req = new uv_write_t();
//     write_req->data = wr;

//     if ((ret = uv_write(write_req, (uv_stream_t*) &wr->input_pipe, &input, 1, on_exec_write_compl)) < 0)
//     {
//         LOG(ERROR) << "Unable to send input: " << uv_strerror(ret);
//         uv_close(wr->handle, on_close_connection);

//         uv_close((uv_handle_t*) &wr->input_pipe, on_close_pipe);
//         uv_close((uv_handle_t*) &wr->output_pipe, on_close_pipe);

//         delete write_req;
//         delete cmd;
//         delete wr;
//     }

//     return;
// }

// static void on_handle_request(uv_work_t* req, int status)
// {
//     int ret;
//     work_request* wr = (work_request*) req->data;
//     delete req;

//     if (((wr->flags & WORK_ERROR) == WORK_ERROR) || status < 0)
//     {
//         LOG(ERROR) << "Handle request error: " << uv_strerror(status);
//         uv_close(wr->handle, on_close_connection);

//         if (wr->buffer != nullptr)
//             delete [] ((char*) wr->buffer);

//         delete wr;
//         return;
//     }

//     if ((wr->flags & WORK_EXEC) == WORK_EXEC)
//     {

//         ExecCmd* cmd = (ExecCmd*) wr->buffer;
//         uv_fs_t* open_req = new uv_fs_t();
//         open_req->data = (void*) wr;

//         if ((ret = uv_fs_open(uv_default_loop(), open_req,
//             cmd->func_name.c_str(), O_CREAT | O_TRUNC | O_RDWR, S_IRWXU | S_IRWXG | S_IRWXO, on_fsexec_open)) < 0)
//         {
//             LOG(ERROR) << "Unable to open file: " << uv_strerror(ret);
//             uv_close(wr->handle, on_close_connection);

//             uv_fs_req_cleanup(open_req);
//             delete wr;
//             delete cmd;
//         }

//         return;
//     }

//     uv_write_t* write_req = new uv_write_t();
//     uv_buf_t writebuf = uv_buf_init((char*) wr->buffer, wr->len);
//     write_req->data = wr->buffer;

//     if ((ret = uv_write(write_req, (uv_stream_t*) wr->handle, &writebuf, 1, on_msg_write)) < 0)
//     {
//         LOG(ERROR) << "Failed to send response. Error: " << uv_strerror(ret);
//         uv_close(wr->handle, on_close_connection);
//     }

//     delete wr;
// }

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
    std::string s_response(sizeof(size_t), 0);
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
            throw runtime_error("Not yet implemented!");
            break;

        default:
            throw runtime_error("Unknown Work Result");
    }

    delete work_result;
}

static void on_new_request(const uv_tcp_t* handle, const KVRequest& request)
{
    ClientState* state = (ClientState*) handle->data;

    WorkRequest* work_request = new WorkRequest{state, WorkRequest::KV};
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