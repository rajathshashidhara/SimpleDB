#include <string>
#include <cstdlib>
#include <glog/logging.h>
#include "gg.pb.h"

extern "C" {
    #include "uv.h"
}

using namespace gg::protobuf;

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

#define WORK_ERROR 0x1
#define WORK_EXEC  0x2

struct work_request
{
    uv_handle_t*    handle;
    void*           buffer;
    void*           output_buffer;
    size_t          len;
    int             flags;
};

extern void handler(const ExecutionRequest& req, ExecutionResponse& resp);

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

static void handle_request(uv_work_t* wq)
{
    work_request* wreq = (work_request*) wq->data;
    ExecutionRequest req;
    ExecutionResponse resp;

    if (!req.ParseFromArray(wreq->buffer, wreq->len))
    {
        LOG(ERROR) << "Failed to parse";
        wreq->flags |= WORK_ERROR;
        delete [] ((char*) wreq->buffer);
        wreq->buffer = nullptr;
        wreq->len = 0;

        return;
    }
    delete [] ((char*) wreq->buffer);
    wreq->buffer = nullptr;
    wreq->len = 0;

    handler(req, resp);

    size_t resp_len = resp.ByteSize();
    char* resp_buffer = new char[sizeof(resp_len) + resp_len];
    if (resp_buffer == nullptr)
    {
        LOG(ERROR) << "Failed to allocate memory";
        wreq->flags |= WORK_ERROR;

        return;
    }

    *((size_t*) resp_buffer) = resp_len;
    if (!resp.SerializeToArray(resp_buffer + sizeof(size_t), resp_len))
    {
        LOG(ERROR) << "Failed to serialize response.";
        delete [] resp_buffer;
        resp_buffer = nullptr;
        wreq->flags |= WORK_ERROR;

        return;
    }

    wreq->buffer = resp_buffer;
    wreq->len = resp_len + sizeof(size_t);

    return;
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

static int connections_main(unsigned port, unsigned backlog)
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

#define PORT 8080
#define BACKLOG 8

int main(int argc, char* argv[])
{
    GOOGLE_PROTOBUF_VERIFY_VERSION;
    google::InitGoogleLogging(argv[0]);

    unsigned port = PORT;
    if (argc > 1)
    {
        port = (unsigned) std::stoul(argv[1]);
    }

    if (connections_main(port, BACKLOG) < 0)
        return -1;

    if (uv_run(uv_default_loop(), UV_RUN_DEFAULT) < 0)
        return -1;

    google::protobuf::ShutdownProtobufLibrary();

    return 0;
}
