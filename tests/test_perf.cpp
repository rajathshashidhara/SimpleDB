#include <iostream>
#include <string>
#include <sstream>
#include <random>
#include <cstdlib>
#include <vector>
#include <chrono>
#include <unordered_map>
#include "protobufs/netformats.pb.h"

extern "C" {
    #include "uv.h"
}

using namespace simpledb::proto;
using namespace std;

enum client_parse_e
{
    CLIENT_RECV_INIT,
    CLIENT_RECV_ALLOC_LEN,
    CLIENT_RECV_LEN,
    CLIENT_RECV_ALLOC_PAYLOAD,
    CLIENT_RECV_PAYLOAD,
    CLIENT_RECV_INVALID
};

struct write_request_t;

struct client_params {
    std::string ip;
    unsigned port;
    unsigned pipelined;
    size_t klen;
    size_t vlen;
    std::vector<std::string> keys;
    std::string val;
    std::unordered_map<uint64_t, chrono::time_point<chrono::high_resolution_clock> > reqs;
    uint64_t op_id;
    char* resp_buf;
    size_t resp_len;
    size_t alloc_len;
    size_t read_len;
    client_parse_e parse_state;

    double latency;
    uint64_t compl_req;
};

struct write_request_t {
    uv_write_t write_req;
    uint64_t op_id;
    chrono::time_point<chrono::high_resolution_clock> start;
    client_params* cfg;
};

static void on_write(uv_write_t* write, int status)
{
    if (status < 0)
    {
        cout << "Error in writing" << endl;
        exit(1);
    }

    /* Free the buffer & do nothing */
    delete [] ((char*) write->data);
    delete write;
}

static void on_read(uv_stream_t* handle,
        ssize_t nread, const uv_buf_t* buf)
{
    client_params* state = (client_params*) handle->data;

    if (nread == UV_EOF)
    {
        cout << "Connection closed. " << endl;
        exit(1);
        return;
    }

    if (nread == 0)
    {
        if (state->parse_state == CLIENT_RECV_ALLOC_LEN)
        {
            state->resp_len = 0;
            state->resp_buf = nullptr;
            state->parse_state = CLIENT_RECV_INIT;
        }
        else if (state->parse_state == CLIENT_RECV_ALLOC_PAYLOAD)
        {
            state->parse_state = CLIENT_RECV_LEN;
            state->alloc_len = 0;
            delete [] state->resp_buf;
            state->resp_buf = nullptr;
        }
        else
        {
            cout << "Read callback with nread=0" << endl;
            exit(1);
        }
        return;
    }

    if (nread < 0)
    {
        cout << "Failed to read. Error: " << endl;
        exit(1);
    }

    state->read_len += nread;

    if (state->parse_state == CLIENT_RECV_ALLOC_LEN)
    {
        if (state->read_len < sizeof(state->resp_len))
            return;

        state->parse_state = CLIENT_RECV_LEN;
        return;
    }
    else if (state->parse_state == CLIENT_RECV_ALLOC_PAYLOAD)
    {
        if (state->read_len < state->resp_len)
            return;

        state->parse_state = CLIENT_RECV_PAYLOAD;

        KVResponse resp;
        KVRequest req;
        if (!resp.ParseFromArray(state->resp_buf, state->resp_len))
        {
            cout << "Failed to parse" << endl;
            exit(1);
        }

        if (resp.return_code() != 0)
        {
            cout << "Op failed" << endl;
            exit(1);
        }

        auto wr_start = state->reqs[(uint64_t) resp.id()];
        state->latency += (chrono::high_resolution_clock::now() - wr_start).count();
        state->compl_req += 1;
        state->reqs.erase(resp.id());

        req.set_id(state->op_id++);
        GetRequest* get_req = req.mutable_get_request();
        get_req->set_key(state->keys[resp.id() % state->pipelined]);

        delete [] state->resp_buf;
        state->resp_buf = nullptr;
        state->resp_len = state->alloc_len = state->read_len = 0;

        char* req_buf = new char[sizeof(size_t) + req.ByteSize()];
        if (!req.SerializeToArray(req_buf + sizeof(size_t), req.ByteSize()))
        {
            cout << "Failed to serialize" << endl;
            exit(1);
        }

        uv_write_t* write_req = new uv_write_t();
        *((size_t*) req_buf) = req.ByteSize();
        uv_buf_t writebuf = uv_buf_init(req_buf, sizeof(size_t) + req.ByteSize());
        write_req->data = req_buf;
        state->reqs[state->op_id - 1] = chrono::high_resolution_clock::now();

        int ret;
        if ((ret = uv_write(write_req, handle, &writebuf, 1, on_write)) < 0)
        {
            cout << "Failed to send" << endl;
            exit(1);
        }
    }
    else
    {
        cout << "Parse state error";
        exit(1);
    }
}

static void alloc_readbuffer_cb(uv_handle_t* handle,
                                size_t suggested_size,
                                uv_buf_t* buf)
{
    (void) suggested_size;

    client_params* state = (client_params*) ((uv_stream_t*) handle)->data;

    if (state->parse_state == CLIENT_RECV_INIT || state->parse_state == CLIENT_RECV_PAYLOAD)
    {
        buf->base = (char*) &state->resp_len;
        buf->len = sizeof(state->resp_len);

        state->parse_state = CLIENT_RECV_ALLOC_LEN;
        state->read_len = 0;
    }
    else if (state->parse_state == CLIENT_RECV_LEN)
    {
        state->resp_buf = new char[state->resp_len];
        state->alloc_len = state->resp_len;

        if (state->resp_buf == nullptr)
        {
            cout << "Cannot allocate recv memory buffers" << endl;
            exit(1);
        }

        buf->base = state->resp_buf;
        buf->len = state->alloc_len;

        state->read_len = 0;
        state->parse_state = CLIENT_RECV_ALLOC_PAYLOAD;
    }
    else if (state->parse_state == CLIENT_RECV_ALLOC_PAYLOAD)
    {
        buf->base = state->resp_buf + state->read_len;
        buf->len = state->resp_len - state->read_len;
    }
    else if (state->parse_state == CLIENT_RECV_ALLOC_LEN)
    {
        buf->base = ((char*)&state->resp_len) + state->read_len;
        buf->len = sizeof(state->resp_len) - state->read_len;
    }
    else
    {
        cout << "Invalid parse state: " << state->parse_state;
        exit(1);
    }
}

static void on_connect(uv_connect_t* connect, int status)
{
    client_params* params = (client_params*) connect->data;

    /* Push keys to server */
    for (unsigned i = 0; i < params->pipelined; i++)
    {
        KVRequest req;
        req.set_id(params->op_id++);
        PutRequest* put_req = req.mutable_put_request();
        put_req->set_immutable(true);
        put_req->set_key(params->keys[i]);
        put_req->set_val(params->val);

        char* req_buf = new char[sizeof(size_t) + req.ByteSize()];
        if (!req.SerializeToArray(req_buf + sizeof(size_t), req.ByteSize()))
        {
            cout << "Failed to serialize" << endl;
            exit(1);
        }
        *((size_t*) req_buf) = req.ByteSize();

        uv_write_t* write_req = new uv_write_t();
        uv_buf_t writebuf = uv_buf_init(req_buf, sizeof(size_t) + req.ByteSize());
        write_req->data = req_buf;
        params->reqs[params->op_id - 1] = chrono::high_resolution_clock::now();;

        int ret;
        if ((ret = uv_write(write_req, connect->handle, &writebuf, 1, on_write)) < 0)
        {
            cout << "Failed to send" << endl;
            exit(1);
        }
    }

    params->resp_buf = nullptr;
    params->resp_len = 0;
    params->read_len = 0;
    params->alloc_len = 0;
    params->parse_state = CLIENT_RECV_INIT;
    connect->handle->data = params;

    if (uv_read_start(connect->handle, alloc_readbuffer_cb, on_read) < 0)
    {
        cout << "Failed to read" << endl;
        exit(1);
    }
}

static int connect_to_server(client_params* params)
{
    uv_tcp_t* socket = new uv_tcp_t();
    uv_tcp_init(uv_default_loop(), socket);

    struct sockaddr_in dest;
    uv_connect_t* connect = new uv_connect_t();
    connect->data = (void*) params;
    uv_ip4_addr(params->ip.c_str(), params->port, &dest);
    uv_tcp_connect(connect, socket,
        (const struct sockaddr*)&dest, on_connect);

    return 0;
}

static void timer_cb(uv_timer_t* handle)
{
    client_params* state = (client_params*) handle->data;

    cout << "GET throughput= " << state->compl_req << " ops/sec latency= " << (state->latency/state->compl_req)/(1000*1000*1000) << " sec" << endl;
    state->compl_req = 0;
    state->latency = 0;
}

int main(int argc, char* argv[])
{
    GOOGLE_PROTOBUF_VERIFY_VERSION;
    client_params* params = new client_params();
    params->op_id = 1;
    params->parse_state = CLIENT_RECV_INIT;
    params->resp_buf = nullptr;
    params->resp_len = 0;
    params->read_len = 0;
    params->alloc_len = 0;
    params->latency = 0;
    params->compl_req = 0;

    std::stringstream ss;
    params->ip = std::string(argv[1]);

    ss << argv[2];
    ss >> params->port;
    ss.clear();

    ss << argv[3];
    ss >> params->klen;
    ss.clear();

    ss << argv[4];
    ss >> params->vlen;
    ss.clear();

    ss << argv[5];
    ss >> params->pipelined;
    ss.clear();

    std::random_device engine;
    std::string key;
    for (size_t i = 0; i < params->klen; i += sizeof(unsigned))
    {
        unsigned x = engine();
        key.append((char*) &x);
    }
    for (size_t i = 0; i < params->vlen; i += sizeof(unsigned))
    {
        unsigned x = engine();
        params->val.append((char*) &x);
    }
    for (unsigned i = 0; i < params->pipelined; i++)
    {
        params->keys.push_back(key + std::to_string(i));
    }

    connect_to_server(params);
    uv_timer_t* timer_handle = new uv_timer_t();
    timer_handle->data = params;

    uv_timer_init(uv_default_loop(), timer_handle);
    uv_timer_start(timer_handle, timer_cb, 1000, 1000); /* timer every 1s */

    if (uv_run(uv_default_loop(), UV_RUN_DEFAULT) < 0)
        return -1;

    google::protobuf::ShutdownProtobufLibrary();
    return 0;
}