#include <string>
#include <iostream>
#include <cstdlib>
#include <glog/logging.h>

#include "net/server.h"
#include "net/service.h"
#include "storage/db.h"
#include "formats/netformats.pb.h"
#include "util/tokenize.h"
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

    if (argc > 1)
    {
        path = std::string(argv[1]);
    }

    SimpleDBConfig config;
    if (argc > 2)
    {
        assert(argc == 4);

        config.num_ = 0;
        for (auto &addr : split(argv[2], "&"))
        {
            uint16_t port = 8080;
            std::string::size_type colonpos = addr.find( ':' );
            std::string host_ip = addr.substr( 0, colonpos );

            if ( colonpos != std::string::npos ) {
                port = stoi( addr.substr( colonpos + 1 ) );
            }

            std::cerr << host_ip << " " << port << std::endl;
            config.address_.emplace_back(host_ip, port);
            config.num_++;
        }

        config.replica_idx = std::stoi(argv[3]);
    }
    else
    {
        config.num_ = 1;
        config.address_.emplace_back("0.0.0.0", 8080);
        config.replica_idx = 0;
    }
    config.db_ = path;
    config.backend_cache_size = 100 * 1024 * 1024; // 100 M
    config.immutable_cache_size = 100 * 1024 * 1024; // 100 M

    uv_loop_t* execution_loop = uv_default_loop();
    Worker worker_(config);
    ServerState server(execution_loop,
                        "0.0.0.0",
                        config.address_[config.replica_idx].port());
    server.Listen();

    if (uv_run(uv_default_loop(), UV_RUN_DEFAULT) < 0)
        return -1;

    google::protobuf::ShutdownProtobufLibrary();

    return 0;
}