#include <string>
#include <glog/logging.h>

#include "net/service.h"
#include "net/connections.h"

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
    char* resp_buffer;
    client_parse_e parse_state;
    size_t req_buf_length;
    size_t resp_buf_length;
    size_t read_length;
    size_t alloc_length;
};

static void on_close_connection(uv_handle_t* handle)
{
    uv_stream_t* stream = (uv_stream_t*) handle;
    client_state_t* state = (client_state_t*) stream->data;

    if (state->req_buffer != nullptr)
        delete state->req_buffer;

    if (state->resp_buffer != nullptr)
        delete state->resp_buffer;

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
}

static void on_msg_write(uv_write_t* req, int status)
{
    client_state_t* state = (client_state_t*) req->data;
    delete state->resp_buffer;

    state->resp_buffer = nullptr;
    state->resp_buf_length = 0;

    if (status < 0)
    {
        LOG(ERROR) << "Write error: " << uv_strerror(status);
        uv_close((uv_handle_t*) req->handle, on_close_connection);
    }

    delete req;
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
            delete state->req_buffer;
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

        KVRequest req;
        KVResponse resp;
        if (!req.ParseFromArray(state->req_buffer, state->req_buf_length))
        {
            LOG(ERROR) << "Failed to parse";
            uv_close((uv_handle_t*) handle, on_close_connection);

            return;
        }
        if (process_request(req, resp) < 0)
        {
            LOG(ERROR) << "Failed to proess request";
            uv_close((uv_handle_t*) handle, on_close_connection);

            return;
        }
        /* Send response back */
        state->resp_buf_length = sizeof(size_t) + resp.ByteSize();
        state->resp_buffer = new char[state->resp_buf_length];
        *((size_t*) state->resp_buffer) = resp.ByteSize();
        if (!resp.SerializeToArray(state->resp_buffer + sizeof(size_t), resp.ByteSize()))
        {
            LOG(ERROR) << "Failed to serialize response.";
            uv_close((uv_handle_t*) handle, on_close_connection);
            return;
        }

        uv_write_t* write_req = new uv_write_t();
        uv_buf_t writebuf = uv_buf_init(state->resp_buffer, state->resp_buf_length);
        write_req->data = state;

        delete state->req_buffer;
        state->req_buffer = nullptr;
        state->req_buf_length = state->alloc_length = state->read_length = 0;

        if ((ret = uv_write(write_req, handle, &writebuf, 1, on_msg_write)) < 0)
        {
            LOG(ERROR) << "Failed to send response. Error: " << uv_strerror(ret);
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
    state->resp_buffer = nullptr;
    state->resp_buf_length = 0;
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