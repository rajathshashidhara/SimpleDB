#include <string>
#include <glog/logging.h>

#include "net/service.h"
#include "net/connections.h"
#include "db/db.h"

extern "C" {
    #include "uv.h"

    #include "config.h"
}

enum client_parse_e
{
    CLIENT_RECV_INIT,
    CLIENT_RECV_ALLOC_LEN,
    CLIENT_RECV_LEN,
    CLIENT_RECV_ALLOC_PAYLOAD,
    CLIENT_RECV_PAYLOAD,
    CLIENT_RECV_INVALID
};

struct client_state_t
{
    uv_tcp_t* connection;
    char* req_buffer;
    client_parse_e parse_state;
    size_t req_buf_length;
    size_t read_length;
    size_t alloc_length;
};

static void on_close_connection(uv_handle_t* handle);
static void on_shutdown(uv_shutdown_t* req, int status);
static void on_msg_write(uv_write_t* req, int status);
static void on_close_pipe(uv_handle_t* pipe_handle);
static void on_handle_request(uv_work_t* req, int status);
static void on_exec_compl(uv_process_t* process, int64_t exit_status, int term_signal);
static void on_pipe_read(uv_stream_t* handle,
        ssize_t nread, const uv_buf_t* buf);
static void alloc_piperead_cb(uv_handle_t* handle,
                                size_t suggested_size,
                                uv_buf_t* buf);
static void on_exec_write_compl(uv_write_t* req, int status);
static void on_fsexec_close(uv_fs_t* req);
static void on_fsexec_write(uv_fs_t* req);
static void on_fsexec_open(uv_fs_t* req);
static void on_msg_read(uv_stream_t* handle,
        ssize_t nread, const uv_buf_t* buf);
static void alloc_readbuffer_cb(uv_handle_t* handle,
                                size_t suggested_size,
                                uv_buf_t* buf);
static void on_new_connection(uv_stream_t *server, int status);

static void on_close_connection(uv_handle_t* handle)
{
    uv_stream_t* stream = (uv_stream_t*) handle;
    client_state_t* state = (client_state_t*) stream->data;

    if (state->req_buffer != nullptr)
        delete [] state->req_buffer;

    delete state;
    delete stream;
}

static void on_shutdown(uv_shutdown_t* req, int status)
{
    if (status < 0)
    {
        LOG(ERROR) << "Shutdown failed. Error: " << uv_strerror(status);
    }

    on_close_connection((uv_handle_t*) req->handle);
    delete req;
}

static void on_msg_write(uv_write_t* req, int status)
{
    delete [] ((char*) req->data);

    if (status < 0)
    {
        LOG(ERROR) << "Write error: " << uv_strerror(status);
        uv_close((uv_handle_t*) req->handle, on_close_connection);
    }

    delete req;
}

static void on_close_pipe(uv_handle_t* pipe_handle)
{
    /* Do nothing */
}

static void on_handle_request(uv_work_t* req, int status)
{
    int ret;
    work_request* wr = (work_request*) req->data;
    delete req;

    if (((wr->flags & WORK_ERROR) == WORK_ERROR) || status < 0)
    {
        LOG(ERROR) << "Handle request error: " << uv_strerror(status);
        uv_close(wr->handle, on_close_connection);

        if (wr->buffer != nullptr)
            delete [] ((char*) wr->buffer);

        delete wr;
        return;
    }

    if ((wr->flags & WORK_EXEC) == WORK_EXEC)
    {

        ExecCmd* cmd = (ExecCmd*) wr->buffer;
        uv_fs_t* open_req = new uv_fs_t();
        open_req->data = (void*) wr;

        if ((ret = uv_fs_open(uv_default_loop(), open_req,
            cmd->func_name.c_str(), O_CREAT | O_TRUNC | O_RDWR, S_IRWXU | S_IRWXG | S_IRWXO, on_fsexec_open)) < 0)
        {
            LOG(ERROR) << "Unable to open file: " << uv_strerror(ret);
            uv_close(wr->handle, on_close_connection);

            uv_fs_req_cleanup(open_req);
            delete wr;
            delete cmd;
        }

        return;
    }

    uv_write_t* write_req = new uv_write_t();
    uv_buf_t writebuf = uv_buf_init((char*) wr->buffer, wr->len);
    write_req->data = wr->buffer;

    if ((ret = uv_write(write_req, (uv_stream_t*) wr->handle, &writebuf, 1, on_msg_write)) < 0)
    {
        LOG(ERROR) << "Failed to send response. Error: " << uv_strerror(ret);
        uv_close(wr->handle, on_close_connection);
    }

    delete wr;
}

static void on_exec_compl(uv_process_t* process, int64_t exit_status, int term_signal)
{
    work_request* wr = (work_request*) process->data;
    ExecCmd* cmd = (ExecCmd*) wr->buffer;
    int ret;

    if (exit_status < 0 || (term_signal > 0))
    {
        LOG(ERROR) << "Unable to launch process";
        uv_close(wr->handle, on_close_connection);

        delete cmd;
        delete wr;

        return;
    }

    uv_work_t* work_req = new uv_work_t();
    work_req->data = wr;
    wr->flags = 0;

    if ((ret = uv_queue_work(uv_default_loop(), work_req, handle_exec_compl, on_handle_request)) < 0)
    {
        LOG(ERROR) << "Failed to launch work handler. Error: " << uv_strerror(ret);
        uv_close((uv_handle_t*) wr->handle, on_close_connection);

        delete work_req;
        delete cmd;
        delete wr;

        return;
    }

    uv_close((uv_handle_t*) &wr->input_pipe, on_close_pipe);
    uv_close((uv_handle_t*) &wr->output_pipe, on_close_pipe);
}

static void on_pipe_read(uv_stream_t* handle,
        ssize_t nread, const uv_buf_t* buf)
{
    work_request* wr = (work_request*) handle->data;
    ExecCmd* cmd = (ExecCmd*) wr->buffer;

    if (nread == UV_EOF)
    {
        return;
    }

    if (nread < 0)
    {
        LOG(ERROR) << "Input error";
        uv_close(wr->handle, on_close_connection);

        delete cmd;
        delete wr;

        return;
    }

    cmd->output.append(std::move(std::string((char*) wr->output_buffer, nread)));
    delete [] ((char*) wr->output_buffer);
    wr->output_buffer = nullptr;

    return;
}

static void alloc_piperead_cb(uv_handle_t* handle,
                                size_t suggested_size,
                                uv_buf_t* buf)
{
    work_request* wr = (work_request*) handle->data;
    ExecCmd* cmd = (ExecCmd*) wr->buffer;
    wr->output_buffer = new char[suggested_size];

    if (wr->output_buffer == nullptr)
    {
        LOG(ERROR) << "Not enough memory";
        uv_close(wr->handle, on_close_connection);

        delete cmd;
        delete wr;
    }

    buf->base = (char*) (wr->output_buffer);
    buf->len = suggested_size;
}

static void on_exec_write_compl(uv_write_t* req, int status)
{
    work_request* wr = (work_request*) req->data;
    ExecCmd* cmd = (ExecCmd*) wr->buffer;
    delete req;

    if (status < 0)
    {
        LOG(ERROR) << "Execution failed";
        uv_close(wr->handle, on_close_connection);

        uv_close((uv_handle_t*) &wr->input_pipe, on_close_pipe);
        uv_close((uv_handle_t*) &wr->output_pipe, on_close_pipe);

        delete cmd;
        delete wr;

        return;
    }

    int ret;
    wr->output_pipe.data = (void*) wr;
    if ((ret = uv_read_start((uv_stream_t*) &wr->output_pipe, alloc_piperead_cb, on_pipe_read)) < 0)
    {
        LOG(ERROR) << "Unable to read from process outstream";
        uv_close(wr->handle, on_close_connection);

        uv_close((uv_handle_t*) &wr->input_pipe, on_close_pipe);
        uv_close((uv_handle_t*) &wr->output_pipe, on_close_pipe);

        delete wr;
        delete cmd;

    }

    return;
}

static void on_fsexec_close(uv_fs_t* req)
{
    work_request* wr = (work_request*) req->data;
    ExecCmd* cmd = (ExecCmd*) wr->buffer;
    uv_fs_req_cleanup(req);

    int ret;
    char* args[2];
    args[0] = &cmd->func_name[0];
    args[1] = NULL;
    wr->process.data = (void*) wr;

    uv_pipe_init(uv_default_loop(), &wr->input_pipe, 1);
    uv_pipe_init(uv_default_loop(), &wr->output_pipe, 1);
    uv_stdio_container_t child_stdio[3];
    child_stdio[0].flags = (uv_stdio_flags) (UV_CREATE_PIPE | UV_READABLE_PIPE);
    child_stdio[0].data.stream = (uv_stream_t*) &wr->input_pipe;
    child_stdio[1].flags = (uv_stdio_flags) (UV_CREATE_PIPE | UV_WRITABLE_PIPE);
    child_stdio[1].data.stream = (uv_stream_t*) &wr->output_pipe;
    child_stdio[2].flags = UV_INHERIT_FD;
    child_stdio[2].data.fd = 2;

    uv_process_options_t options = {0};
    options.exit_cb = on_exec_compl;
    options.file = cmd->func_name.c_str();
    options.args = args;
    options.stdio_count = 3;
    options.stdio = child_stdio;

    if ((ret = uv_spawn(uv_default_loop(), &wr->process, &options)) < 0)
    {
        LOG(ERROR) << "Unable to spawn process: " << uv_strerror(ret);
        uv_close(wr->handle, on_close_connection);

        delete cmd;
        delete wr;
    }

    /* Write data into stream */
    auto input = uv_buf_init(&cmd->arguments[0], cmd->arguments.length());
    uv_write_t* write_req = new uv_write_t();
    write_req->data = wr;

    if ((ret = uv_write(write_req, (uv_stream_t*) &wr->input_pipe, &input, 1, on_exec_write_compl)) < 0)
    {
        LOG(ERROR) << "Unable to send input: " << uv_strerror(ret);
        uv_close(wr->handle, on_close_connection);

        uv_close((uv_handle_t*) &wr->input_pipe, on_close_pipe);
        uv_close((uv_handle_t*) &wr->output_pipe, on_close_pipe);

        delete write_req;
        delete cmd;
        delete wr;
    }

    return;
}

static void on_fsexec_write(uv_fs_t* req)
{
    work_request* wr = (work_request*) req->data;
    ExecCmd* cmd = (ExecCmd*) wr->buffer;
    uv_fs_req_cleanup(req);

    int ret;
    uv_fs_t* close_req = new uv_fs_t();
    close_req->data = (void*) wr;
    ret = uv_fs_close(uv_default_loop(), close_req, wr->exec_fd, on_fsexec_close);
    if (ret < 0)
    {
        LOG(ERROR) << "Unable to spawn process: " << uv_strerror(ret);
        uv_close(wr->handle, on_close_connection);

        delete cmd;
        delete wr;        
    }

    return;
}

static void on_fsexec_open(uv_fs_t* req)
{
    work_request* wr = (work_request*) req->data;
    ExecCmd* cmd = (ExecCmd*) wr->buffer;
    int fd = req->result;
    uv_fs_req_cleanup(req);

    if (fd < 0)
    {
        LOG(ERROR) << "Unable to write executable to file: " << strerror(fd);
        uv_close(wr->handle, on_close_connection);

        delete cmd;
        delete wr;
    }

    wr->exec_fd = fd;

    uv_fs_t* write_req = new uv_fs_t();
    write_req->data = (void*) wr;

    int ret;
    auto iov = uv_buf_init(&(cmd->func[0]), cmd->func.length());
    if ((ret = uv_fs_write(uv_default_loop(), write_req,
            wr->exec_fd, &iov, 1, 0, on_fsexec_write)) < 0)
    {
        LOG(ERROR) << "File Write failed: " << uv_strerror(ret);
        uv_close(wr->handle, on_close_connection);

        delete cmd;
        delete wr;
        uv_fs_req_cleanup(write_req);
    }
}

static void on_msg_read(uv_stream_t* handle,
        ssize_t nread, const uv_buf_t* buf)
{
    int ret;
    client_state_t* state = (client_state_t*) handle->data;

    if (nread == UV_EOF)
    {
        LOG(INFO) << "Connection closed. ";

        uv_shutdown_t* req = new uv_shutdown_t();
        if (req == nullptr)
        {
            LOG(ERROR) << "Unable to allocate shutdown req.";
            return;
        }

        uv_shutdown(req, handle, on_shutdown);
        return;
    }

    if (nread == 0)
    {
        if (state->parse_state == CLIENT_RECV_ALLOC_LEN)
        {
            state->req_buf_length = 0;
            state->req_buffer = nullptr;
            state->parse_state = CLIENT_RECV_INIT;
        }
        else if (state->parse_state == CLIENT_RECV_ALLOC_PAYLOAD)
        {
            state->parse_state = CLIENT_RECV_LEN;
            state->alloc_length = 0;
            delete [] state->req_buffer;
            state->req_buffer = nullptr;
        }
        else
        {
            LOG(ERROR) << "Read callback with nread=0";
            uv_close((uv_handle_t*) handle, on_close_connection);
        }
        return;
    }

    if (nread < 0)
    {
        LOG(ERROR) << "Failed to read. Error: " << uv_strerror(nread);
        uv_close((uv_handle_t*) handle, on_close_connection);

        return;
    }

    state->read_length += nread;

    if (state->parse_state == CLIENT_RECV_ALLOC_LEN)
    {
        if (state->read_length < sizeof(state->req_buf_length))
            return;

        state->parse_state = CLIENT_RECV_LEN;
        return;
    }
    else if (state->parse_state == CLIENT_RECV_ALLOC_PAYLOAD)
    {
        if (state->read_length < state->req_buf_length)
            return;

        state->parse_state = CLIENT_RECV_PAYLOAD;
        LOG(INFO) << "Received message: len=" << state->read_length;

        uv_work_t* work_req = new uv_work_t();
        work_request* work_meta = new work_request();
        work_meta->handle = (uv_handle_t*) handle;
        work_meta->buffer = state->req_buffer;
        work_meta->len = state->req_buf_length;
        work_meta->flags = 0;
        work_req->data = work_meta;

        state->req_buffer = nullptr;
        state->req_buf_length = state->alloc_length = state->read_length = 0;

        if ((ret = uv_queue_work(uv_default_loop(), work_req, handle_request, on_handle_request)) < 0)
        {
            LOG(ERROR) << "Failed to launch work handler. Error: " << uv_strerror(ret);

            delete [] ((char*) work_meta->buffer);
            delete work_req;
            delete work_meta;

            uv_close((uv_handle_t*) handle, on_close_connection);
            return;
        }
    }
    else
    {
        LOG(ERROR) << "Parse state error";
        uv_close((uv_handle_t*) handle, on_close_connection);
    }
}

/**
 * TODO: Improve the allocation strategy
 */
static void alloc_readbuffer_cb(uv_handle_t* handle,
                                size_t suggested_size,
                                uv_buf_t* buf)
{
    (void) suggested_size;

    client_state_t* state = (client_state_t*) ((uv_stream_t*) handle)->data;

    if (state->parse_state == CLIENT_RECV_INIT || state->parse_state == CLIENT_RECV_PAYLOAD)
    {
        buf->base = (char*) &state->req_buf_length;
        buf->len = sizeof(state->req_buf_length);

        state->parse_state = CLIENT_RECV_ALLOC_LEN;
        state->read_length = 0;
    }
    else if (state->parse_state == CLIENT_RECV_LEN)
    {
        state->req_buffer = new char[state->req_buf_length];
        state->alloc_length = state->req_buf_length;

        if (state->req_buffer == nullptr)
        {
            LOG(ERROR) << "Cannot allocate recv memory buffers";
            uv_close((uv_handle_t*) handle, on_close_connection);

            return;
        }

        buf->base = state->req_buffer;
        buf->len = state->alloc_length;

        state->read_length = 0;
        state->parse_state = CLIENT_RECV_ALLOC_PAYLOAD;
    }
    else if (state->parse_state == CLIENT_RECV_ALLOC_PAYLOAD)
    {
        buf->base = state->req_buffer + state->read_length;
        buf->len = state->req_buf_length - state->read_length;
    }
    else if (state->parse_state == CLIENT_RECV_ALLOC_LEN)
    {
        buf->base = ((char*)&state->req_buf_length) + state->read_length;
        buf->len = sizeof(state->req_buf_length) - state->read_length;
    }
    else
    {
        LOG(ERROR) << "Invalid parse state: " << state->parse_state;
        uv_close((uv_handle_t*) handle, on_close_connection);

        return;
    }
}

static void on_new_connection(uv_stream_t *server, int status)
{
    int ret;

    if (status < 0)
    {
        LOG(ERROR) << "New connection failed. Error: " << uv_strerror(status);
        return;
    }

    uv_tcp_t* stream = new uv_tcp_t();
    if (stream == nullptr)
    {
        LOG(ERROR) << "Memory allocation failed.";
        return;
    }
    stream->data = nullptr;

    if ((ret = uv_tcp_init(uv_default_loop(), stream)) != 0)
    {
        LOG(ERROR) << "Failed to initialize TCP connection handle. Error: " << uv_strerror(ret);
        delete stream;
        return;
    }

    if ((ret = uv_accept(server, (uv_stream_t*) stream)) != 0)
    {
        LOG(ERROR) << "Accept failed. Error: " << uv_strerror(ret);
        delete stream;
        return;
    }

    client_state_t* state = new client_state_t();
    state->connection = stream;
    state->req_buffer = nullptr;
    state->req_buf_length = 0;
    state->read_length = 0;
    state->alloc_length = 0;
    state->parse_state = CLIENT_RECV_INIT;
    stream->data = state;

    if ((ret = uv_read_start((uv_stream_t*) stream, alloc_readbuffer_cb, on_msg_read)) != 0)
    {
        LOG(ERROR) << "Failed to start read. Error: " << uv_strerror(ret);
        delete stream;
        delete state;
        return;
    }
}

int simpledb::net::connections_main(unsigned port, unsigned backlog)
{

    struct sockaddr_in addr;
    int ret;

    if ((ret = uv_ip4_addr("0.0.0.0", port, &addr)) != 0)
    {
        LOG(FATAL) << "Fetching IP4 address failed. Error: " << uv_strerror(ret);
        return -1;
    }

    uv_tcp_t* server = new uv_tcp_t();
    if (server == nullptr)
    {
        LOG(FATAL) << "Memory allocation failed.";
        return -1;
    }

    if ((ret = uv_tcp_init(uv_default_loop(), server)) != 0)
    {
        LOG(FATAL) << "Failed to initialize TCP server handle. Error: " << uv_strerror(ret);
        return -1;
    }

    if ((ret = uv_tcp_bind(server, (const struct sockaddr*) &addr, 0)) != 0)
    {
        LOG(FATAL) << "Failed to bind TCP socket. Error: " << uv_strerror(ret);
        return -1;
    }

    if ((ret = uv_listen((uv_stream_t*) server, backlog, on_new_connection)) != 0)
    {
        LOG(FATAL) << "Listen failed. Error: " << uv_strerror(ret);
        return -1;
    }

    return 0;
}