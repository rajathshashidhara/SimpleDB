#ifndef SIMPLEDB_NET_CONNECTIONS
#define SIMPLEDB_NET_CONNECTIONS

#include "config.h"
#include "net/address.h"
extern "C" {
    #include "uv.h"
}

class ServerState {
private:
    uv_loop_t* loop_;
    uv_tcp_t* listen_handle;
    Address address_;
    unsigned int backlog { 32 };
public:
    ServerState(uv_loop_t* loop,
            const std::string& ip = "0.0.0.0",
            const uint16_t& port = 8080,
            unsigned int backlog = 32);
    ~ServerState();
    void Listen();
};

#endif /* SIMPLEDB_NET_CONNECTIONS */