#include <string>
#include <glog/logging.h>

#include "db/db.h"
#include "net/service.h"

#include "protobufs/serialization.pb.h"

static int process_register_request(const RegisterRequest& reg_request)
{
    FunctionMetadata function;
    std::string serializedfunc;

    function.set_binary(reg_request.func_binary());
    function.set_runtime(reg_request.runtime());
    function.set_list_args(reg_request.list_args());
    function.set_dict_args(reg_request.dict_args());

    serializedfunc = function.SerializeAsString();
    return simpledb::db::set(reg_request.func_name(), serializedfunc, true);
}

static int process_request(const KVRequest& request,
                    KVResponse& response)
{
    std::string value;
    int ret;

    switch (request.ReqOps_case())
    {
    case KVRequest::ReqOpsCase::kGetRequest:
        ret = simpledb::db::get(request.get_request().key(), value);
        break;

    case KVRequest::ReqOpsCase::kPutRequest:
        ret = simpledb::db::set(request.put_request().key(),
                request.put_request().val(), request.put_request().immutable());
        break;

    case KVRequest::ReqOpsCase::kDeleteRequest:
        ret = simpledb::db::remove(request.delete_request().key());
        break;
    
    case KVRequest::ReqOpsCase::kRegisterRequest:
        ret = process_register_request(request.register_request());
        break;

    default:
        return -1;
    }

    response.set_id(request.id());
    response.set_return_code(ret);
    response.set_val(value);

    return 0;
}

void handle_request(uv_work_t* wq)
{
    work_request* wreq = (work_request*) wq->data;

    KVRequest req;
    KVResponse resp;
    size_t resp_len = 0;
    char* resp_buffer = nullptr;

    if (!req.ParseFromArray(wreq->buffer, wreq->len))
    {
        LOG(ERROR) << "Failed to parse";
        wreq->err = 1;
        goto work_done;
    }

    if (process_request(req, resp) < 0)
    {
        LOG(ERROR) << "Failed to proess request";
        wreq->err = 1;
        goto work_done;
    }

    resp_len = resp.ByteSize();
    resp_buffer = new char[sizeof(resp_len) + resp_len];
    if (resp_buffer == nullptr)
    {
        LOG(ERROR) << "Failed to allocate memory";
        wreq->err = 1;
        goto work_done;
    }
    *((size_t*) resp_buffer) = resp_len;
    if (!resp.SerializeToArray(resp_buffer + sizeof(size_t), resp_len))
    {
        LOG(ERROR) << "Failed to serialize response.";
        delete [] resp_buffer;
        resp_buffer = nullptr;
        wreq->err = 1;
        goto work_done;
    }

work_done:
    delete [] ((char*) wreq->buffer);
    wreq->buffer = resp_buffer;
    wreq->len = resp_len + sizeof(size_t);

    return;
}