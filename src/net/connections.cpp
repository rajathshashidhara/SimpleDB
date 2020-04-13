#include <iostream>
#include <cstdlib>
#include <cstdint>

#include <glog/logging.h>

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
    char* msg_buffer;
    client_parse_e parse_state;
    size_t msg_length;
    size_t read_length;
    size_t alloc_length;
};

static void on_close_connection(uv_handle_t* handle)
{
    uv_stream_t* stream = (uv_stream_t*) handle;
    client_state_t* state = (client_state_t*) stream->data;

    if (state->msg_buffer != nullptr)
    {
        delete state->msg_buffer;
    }

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

static void on_msg_read(uv_stream_t* handle,
        ssize_t nread, const uv_buf_t* buf)
{
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

    if (nread < 0)
    {
        LOG(ERROR) << "Failed to read. Error: " << uv_strerror(nread);
        uv_tcp_close_reset((uv_tcp_t*) handle, on_close_connection);
                
        return;
    }

    state->read_length += nread;

    if (state->parse_state == CLIENT_RECV_ALLOC_LEN)
    {
        if (state->read_length < sizeof(state->msg_length))
            return;

        state->parse_state = CLIENT_RECV_LEN;
        return;
    }
    else if (state->parse_state == CLIENT_RECV_ALLOC_PAYLOAD)
    {
        if (state->read_length < state->msg_length)
            return;

        state->parse_state = CLIENT_RECV_PAYLOAD;
        LOG(INFO) << "Received message: len=" << state->read_length;

        /**
         * TODO: Use the data here
         */
        state->msg_buffer = NULL;
        state->msg_length = state->alloc_length = state->read_length = 0;
        delete buf->base;
    }
    else
    {
        LOG(ERROR) << "Parse state error";
        uv_tcp_close_reset((uv_tcp_t*) handle, on_close_connection);
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
        buf->base = (char*) &state->msg_length;
        buf->len = sizeof(state->msg_length);

        state->parse_state = CLIENT_RECV_ALLOC_LEN;
        state->read_length = 0;
    }
    else if(state->parse_state == CLIENT_RECV_LEN)
    {
        state->msg_buffer = new char[state->msg_length];
        state->alloc_length = state->msg_length;

        if (state->msg_buffer == nullptr)
        {
            LOG(ERROR) << "Cannot allocate recv memory buffers";
            uv_tcp_close_reset((uv_tcp_t*) handle, on_close_connection);
            
            return;
        }

        buf->base = state->msg_buffer;
        buf->len = state->alloc_length;

        state->read_length = 0;
        state->parse_state = CLIENT_RECV_ALLOC_PAYLOAD;
    }
    else
    {
        LOG(ERROR) << "Invalid parse state: " << state->parse_state;
        uv_tcp_close_reset((uv_tcp_t*) handle, on_close_connection);
            
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
    stream->data = NULL;

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
    state->msg_buffer = NULL;
    state->msg_length = 0;
    state->read_length = 0;
    state->alloc_length = 0;
    state->parse_state = CLIENT_RECV_INIT;
    stream->data = state;

    if ((ret = uv_read_start((uv_stream_t*) stream, alloc_readbuffer_cb, NULL)) != 0)
    {
        LOG(ERROR) << "Failed to start read. Error: " << uv_strerror(ret);
        delete stream;
        delete state;
        return;
    }
}

int connections_main(unsigned port, unsigned backlog)
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