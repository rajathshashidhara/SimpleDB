#include <string>
#include <cstdlib>
#include <glog/logging.h>

#include "net/server.h"
#include "net/service.h"
#include "storage/db.h"
#include "formats/netformats.pb.h"
#include "config.h"

extern "C" {
    #include "uv.h"
}

using namespace simpledb::storage;

int main(int argc, char* argv[])
{
    GOOGLE_PROTOBUF_VERIFY_VERSION;
    google::InitGoogleLogging(argv[0]);

    std::string path = "/tmp/simpledb";
    unsigned port = 8080;

    if (argc > 1)
    {
        path = std::string(argv[1]);
    }

    if (argc > 2)
    {
        port = (unsigned int) std::stoul(argv[2]);
    }

    SimpleDBConfig config;
    config.num_ = 1;
    config.address_.emplace_back("0.0.0.0", port);
    config.db_ = path;

    uv_loop_t* execution_loop = uv_default_loop();
    Worker worker_(config);
    ServerState server(execution_loop,
                        "0.0.0.0", port);
    server.Listen();

    if (uv_run(uv_default_loop(), UV_RUN_DEFAULT) < 0)
        return -1;

    google::protobuf::ShutdownProtobufLibrary();

    return 0;
}