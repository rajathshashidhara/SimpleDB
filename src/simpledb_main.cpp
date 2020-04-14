#include <glog/logging.h>

#include "net/connections.h"
#include "db/db.h"
#include "protobufs/netformats.pb.h"
#include "config.h"

extern "C" {
    #include "uv.h"
}

int main(int argc, char* argv[])
{
    GOOGLE_PROTOBUF_VERIFY_VERSION;
    if (simpledb::db::init() < 0)
        return -1;

    if (simpledb::net::connections_main() < 0)
        return -1;

    if (uv_run(uv_default_loop(), UV_RUN_DEFAULT) < 0)
        return -1;

    google::protobuf::ShutdownProtobufLibrary();
    return 0;
}