#include <iostream>
#include <string>
#include <cstdlib>
#include "protobufs/netformats.pb.h"

extern "C" {
    #include <sys/types.h>
    #include <sys/socket.h>
    #include <netinet/in.h>
    #include <arpa/inet.h>
}

using namespace simpledb::proto;

bool send_request(int fd, KVRequest& req)
{
    std::string req_s = req.SerializeAsString();
    size_t len = req_s.length();
    static ssize_t slen = (ssize_t) sizeof(size_t);

    if (send(fd, &len, sizeof(size_t), 0) < slen)
        return false;

    if (send(fd, req_s.c_str(), len, 0) < (ssize_t)len)
        return false;

    return true;
}

bool receive_response(int fd, KVResponse& resp)
{
    size_t len;
    char* s;
    static ssize_t slen = (ssize_t) sizeof(size_t);

    if (recv(fd, &len, sizeof(size_t), 0) < slen)
        return false;
    s = new char[len];
    if (s == nullptr)
        return false;

    if (recv(fd, s, len, 0) < (ssize_t)len)
        return false;
    
    if (!resp.ParseFromArray(s, len))
        return false;

    delete s;
    return true;
}

int main(int argc, char* argv[])
{
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    if (argc < 3)
        return 1;

    std::string ip(argv[1]);
    unsigned port = (unsigned) atoi(argv[2]);

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
    {
        std::cout << "Could not create socket." << std::endl;
        return 1;
    }

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_aton(ip.c_str(), &addr.sin_addr);
    if (connect(fd, (const struct sockaddr*)&addr, sizeof(struct sockaddr_in)) < 0)
    {
        std::cout << "Could not connect to db." << std::endl;
        return 1;
    }

    std::string key = "hello";
    std::string value = "world!";
    KVRequest req;
    KVResponse resp;

    int id = 1;
    while (1)
    {
        /* Set */
        req.set_id(id++);
        req.set_op(OpType::SET);
        req.set_key(key);
        req.set_val(value);
        req.set_immutable(true);

        if (!send_request(fd, req))
            return 1;
        
        if (!receive_response(fd, resp))
            return 1;
        
        std::cout << "key=" << key << " return_code=" << resp.return_code() << std::endl;

        /* Get */
        req.set_id(id++);
        req.set_op(OpType::GET);
        req.set_key(key);
        req.set_val("");
        req.set_immutable(false);

        if (!send_request(fd, req))
            return 1;
        
        if (!receive_response(fd, resp))
            return 1;
        
        std::cout << "key=" << key << " return_code=" << resp.return_code() << " value=" << resp.val() << std::endl;

        /* Delete */
        req.set_id(id++);
        req.set_op(OpType::DELETE);
        req.set_key(key);

        if (!send_request(fd, req))
            return 1;
        
        if (!receive_response(fd, resp))
            return 1;
        
        std::cout << "key=" << key << " return_code=" << resp.return_code() << std::endl;
    }

    google::protobuf::ShutdownProtobufLibrary();
    return 0;
}