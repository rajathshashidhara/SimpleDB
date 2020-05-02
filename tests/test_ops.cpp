#include <iostream>
#include <string>
#include <fstream>
#include <streambuf>
#include <cstdlib>
#include "formats/netformats.pb.h"

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

    size_t offset = 0;
    ssize_t ret;
    while (offset < len)
    {
        if ((ret = recv(fd, s + offset, len - offset, 0)) < 0)
            return false;

        offset += ret;
    }

    if (!resp.ParseFromArray(s, len))
        return false;

    delete s;
    return true;
}

int main(int argc, char* argv[])
{
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    if (argc < 4)
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

    std::string key = "Hello";
    std::string value = "World!";
    KVRequest req;
    KVResponse resp;
    int id = 1;

    /* Set */
    PutRequest* set_req = req.mutable_put_request();
    req.set_id(id++);
    set_req->set_key(key);
    set_req->set_val(value);
    set_req->set_immutable(true);

    if (!send_request(fd, req))
        return 1;

    if (!receive_response(fd, resp))
        return 1;

    std::cout << "SET key=" << key << " return_code=" << resp.return_code() << std::endl;

    /* Get */
    GetRequest* get_req = req.mutable_get_request();
    req.set_id(id++);
    get_req->set_key(key);

    if (!send_request(fd, req))
        return 1;

    if (!receive_response(fd, resp))
        return 1;

    std::cout << "GET key=" << key << " return_code=" << resp.return_code() << " value=" << resp.val() << std::endl;

    /* Delete */
    DeleteRequest* del_req = req.mutable_delete_request();
    req.set_id(id++);
    del_req->set_key(key);

    if (!send_request(fd, req))
        return 1;

    if (!receive_response(fd, resp))
        return 1;

    std::cout << "DEL key=" << key << " return_code=" << resp.return_code() << std::endl;

    /* Failure Get */
    get_req = req.mutable_get_request();
    req.set_id(id++);
    get_req->set_key(key);

    if (!send_request(fd, req))
        return 1;

    if (!receive_response(fd, resp))
        return 1;

    std::cout << "GET key=" << key << " return_code=" << resp.return_code() << " value=" << resp.val() << std::endl;

    /* Register */
    PutRequest* reg_req = req.mutable_put_request();
    req.set_id(id++);
    reg_req->set_key(argv[3]);
    std::ifstream t(argv[3]);
    std::string code((std::istreambuf_iterator<char>(t)), std::istreambuf_iterator<char>());
    reg_req->set_val(std::move(code));
    reg_req->set_immutable(true);
    reg_req->set_executable(true);

    if (!send_request(fd, req))
        return 1;

    if (!receive_response(fd, resp))
        return 1;
    std::cout << "SET key="<< reg_req->key() <<" return_code=" << resp.return_code() << std::endl;

    /* Exec - Immediate */
    ExecRequest* exec_req = req.mutable_exec_request();
    req.set_id(id++);
    exec_req->set_func(argv[3]);
    exec_req->add_immediate_args(std::to_string(2));
    exec_req->add_immediate_args(std::to_string(3));

    if (!send_request(fd, req))
        return 1;

    if (!receive_response(fd, resp))
        return 1;
    std::cout << "value="<< resp.val() << " return_code=" << resp.return_code() << std::endl;


    google::protobuf::ShutdownProtobufLibrary();
    return 0;
}
