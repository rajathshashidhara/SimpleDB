#include <string>
#include <cstdlib>
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
    google::InitGoogleLogging(argv[0]);

    if (argc > 1)
    {
        if (simpledb::db::init(argv[1]) < 0)
            return -1;
    }
    else
    {
        if (simpledb::db::init() < 0)
            return -1;
    }

    if (argc > 2)
    {
        if (simpledb::net::connections_main((unsigned int) std::stoul(argv[2])) < 0)
            return -1;
    }
    else
    {
        if (simpledb::net::connections_main() < 0)
            return -1;
    }

    if (uv_run(uv_default_loop(), UV_RUN_DEFAULT) < 0)
        return -1;

    google::protobuf::ShutdownProtobufLibrary();

    return 0;
}