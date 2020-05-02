#ifndef SIMPLEDB_CLIENT_STATE_H
#define SIMPLEDB_CLIENT_STATE_H

#include <string>
#include <functional>
#include <deque>

#include "net/protobuf_stream_parser.h"
#include "formats/netformats.pb.h"

extern "C" {
    #include "uv.h"
}

class ClientState;

typedef std::function<void(const uv_tcp_t*,
            const simpledb::proto::KVRequest&)> KVRequestCallback;

class ClientState
{
private:
    uv_tcp_t* tcp_handle;
    ProtobufStreamParser<simpledb::proto::KVRequest> parser;
    std::string read_buffer_;
    std::deque<std::string> write_buffer_;
    KVRequestCallback new_request_callback_;

public:
    uv_loop_t* execution_loop;

    ClientState(const uv_tcp_t* listen_handle);
    ~ClientState();


    void Read(const KVRequestCallback& kv_cb);
    void Write(std::string && buf);
};

#endif /* SIMPLEDB_CLIENT_STATE_H */
