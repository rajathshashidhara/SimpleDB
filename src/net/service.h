#ifndef SIMPLEDB_NET_SERVICE
#define SIMPLEDB_NET_SERVICE

#include <cstdlib>

#include "protobufs/netformats.pb.h"

extern "C" {
    #include "uv.h"
}

using namespace simpledb::proto;

struct work_request
{
    uv_handle_t*    handle;
    void*           buffer;
    size_t          len;
    int             err;
};

void handle_request(uv_work_t* wq);

#endif /* SIMPLEDB_NET_SERVICE */