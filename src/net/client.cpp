#include <exception>

#include <glog/logging.h>

#include "util/exception.h"
#include "net/client.h"

using namespace std;

ClientState::ClientState(const uv_tcp_t* listen_handle)
        : new_request_callback_([](const uv_tcp_t*,
                    const simpledb::proto::KVRequest&){})
{
    int ret;
    execution_loop = listen_handle->loop;

    tcp_handle = new uv_tcp_t();
    if (tcp_handle == nullptr)
        throw runtime_error("Out of Memory!");

    tcp_handle->data = this;

    if ((ret = uv_tcp_init(execution_loop, tcp_handle)) != 0)
    {
        LOG(ERROR) << "Failed to initialize TCP connection handle. Error: " << uv_strerror(ret);
        throw uv_error("Failed to initialize TCP Connection handle", ret);
    }

    if ((ret = uv_accept((uv_stream_t*) listen_handle, (uv_stream_t*) tcp_handle)) != 0)
    {
        LOG(ERROR) << "Accept failed. Error: " << uv_strerror(ret);
        throw uv_error("Accept failed", ret);
    }
}

ClientState::~ClientState()
{
    static auto close_cb = [](uv_handle_t* handle) {};
    uv_close((uv_handle_t*) tcp_handle, close_cb);

    delete tcp_handle;
}

void ClientState::Read(const KVRequestCallback& kv_cb)
{
    int ret;
    new_request_callback_ = kv_cb;

    static auto allocate_buffer_cb = [](uv_handle_t* handle,
                                size_t suggested_size,
                                uv_buf_t* buf)
    {
        auto state = (ClientState*) handle->data;
        state->read_buffer_ = string(suggested_size, 0);
        buf->base = &(state->read_buffer_[0]);
        buf->len = suggested_size;
    };

    static auto read_cb = [](uv_stream_t* handle,
        ssize_t nread, const uv_buf_t* buf)
    {
        ClientState* state = (ClientState*) handle->data;
        if (nread == UV_EOF)
        {
            LOG(INFO) << "Connection closed. ";

            delete state;
            return;
        }

        state->parser.parse(state->read_buffer_.substr(0, nread));
        state->read_buffer_.clear();

        while (not state->parser.empty())
        {
            auto req = move(state->parser.front());
            state->new_request_callback_(state->tcp_handle, req);

            state->parser.pop();
        }
    };

    if ((ret = uv_read_start((uv_stream_t*) tcp_handle, allocate_buffer_cb, read_cb)) < 0)
    {
        LOG(ERROR) << "Failed to start read. Error: " << uv_strerror(ret);
        throw uv_error("Failed to read.", ret);
    }
}

void ClientState::Write(string && data)
{
    int ret;
    write_buffer_.emplace_back(data);

    uv_write_t* write_req = new uv_write_t();
    uv_buf_t writebuf = uv_buf_init(&(write_buffer_.back()[0]),
                            write_buffer_.back().length());
    write_req->data = this;

    static auto write_cb = [](uv_write_t* req, int status)
    {
        auto *state = (ClientState*) req->data;
        delete req;

        if (status < 0)
        {
            LOG(ERROR) << "Write error: " << uv_strerror(status);

            delete state;
            return;
        }

        state->write_buffer_.pop_front();
    };

    if ((ret = uv_write(write_req,
                    (uv_stream_t*) tcp_handle,
                    &writebuf, 1, write_cb)) < 0)
    {
        LOG(ERROR) << "Failed to send response. Error: " << uv_strerror(ret);

        delete write_req;
        delete this;
    }
}
