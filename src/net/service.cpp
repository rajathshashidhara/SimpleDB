#include <string>
#include <glog/logging.h>

#include "db/db.h"
#include "net/service.h"

#include "protobufs/serialization.pb.h"
#include "protobufs/execformats.pb.h"

extern "C" {
    #include "config.h"
}

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

static int process_exec_request(const ExecRequest& request,
        ExecCmd* cmd)
{
    FunctionMetadata function;
    std::string _func;
    CPPExecArgs eargs;
    int ret;

    ret = simpledb::db::get(request.func(), _func);
    if (ret < 0 || !function.ParseFromString(_func))
    {
        LOG(ERROR) << "Unable to fetch function: " << request.func();
        return -EXEC_STATUS_LOOKUP_FAILED;
    }

    if ((!function.list_args() && (request.list_args_size() > 0)) ||
        (!function.dict_args() && (request.dict_args_size() > 0)))
    {
        LOG(ERROR) << "Invalid arguments";
        return -EXEC_STATUS_ARGS_INVALID;
    }

    cmd->func_name = std::move(DEFAULT_EXEC_PATH + request.func());
    cmd->func = std::move(function.binary());
    auto args = eargs.mutable_args();
    for (auto &arg: request.list_args())
    {
        std::string val;
        if (arg.immediate())
        {
            val = arg.key();
        }
        else
        {
            ret = simpledb::db::get(arg.key(), val);
            if (ret < 0)
            {
                LOG(ERROR) << "Unable to fetch argument: " << arg.key();
                return -EXEC_STATUS_ARGS_INVALID;
            }
        }

        args->Add(std::move(val));
    }

    auto kwargs = eargs.mutable_kwargs();
    for (auto &kwarg: request.dict_args())
    {
        DictArg* new_arg = kwargs->Add();
        std::string val;
        if (kwarg.immediate())
        {
            val = kwarg.val();
        }
        else
        {
            ret = simpledb::db::get(kwarg.key(), val);
            if (ret < 0)
            {
                LOG(ERROR) << "Unable to fetch argument: " << kwarg.key();
                return -EXEC_STATUS_ARGS_INVALID;
            }
        }

        new_arg->set_key(kwarg.key());
        new_arg->set_val(std::move(val));
    }

    size_t argsize = eargs.ByteSize();
    cmd->arguments.append(std::string((char*) &argsize, sizeof(size_t)));
    cmd->arguments.append(std::move(eargs.SerializeAsString()));
    cmd->put_output = request.put_output();
    if (cmd->put_output)
        cmd->output_key = std::move(request.output_key());

    return EXEC_STATUS_OK;
}

static int process_kv_request(const KVRequest& request,
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

void handle_exec_compl(uv_work_t* wq)
{
    work_request* wr = (work_request*) wq->data;
    ExecCmd* cmd = (ExecCmd*) wr->buffer;
    CPPExecResponse exec_resp;
    KVResponse resp;

    if (!exec_resp.ParseFromString(cmd->output))
    {
        LOG(ERROR) << "Failed to parse output";
        goto ERROR_HANDLE_EXEC;
    }

    resp.set_id(cmd->id);
    resp.set_return_code(exec_resp.return_code());
    if (cmd->put_output)
    {
        if (simpledb::db::set(cmd->output_key, exec_resp.output(), true) < 0)
        {
            LOG(ERROR) << "Unable to write output to DB";
            goto ERROR_HANDLE_EXEC;
        }
    }
    else
        resp.set_val(exec_resp.output());

    delete cmd;
    wr->buffer = new char[sizeof(size_t) + resp.ByteSize()];
    wr->len = sizeof(size_t) + resp.ByteSize();
    *((size_t*) wr->buffer) = resp.ByteSize();

    if (!resp.SerializeToArray((char*) wr->buffer + sizeof(size_t), resp.ByteSize()))
    {
        LOG(ERROR) << "Unable to serialize response";
        goto ERROR_HANDLE_EXEC;
    }

    return;

ERROR_HANDLE_EXEC:
    wr->flags |= WORK_ERROR;
    wr->buffer = nullptr;
    wr->len = 0;

    return;
}

void handle_request(uv_work_t* wq)
{
    work_request* wreq = (work_request*) wq->data;
    KVRequest req;

    if (!req.ParseFromArray(wreq->buffer, wreq->len))
    {
        LOG(ERROR) << "Failed to parse";
        wreq->flags |= WORK_ERROR;
        delete [] ((char*) wreq->buffer);
        wreq->buffer = nullptr;
        wreq->len = 0;

        return;
    }
    delete [] ((char*) wreq->buffer);
    wreq->buffer = nullptr;
    wreq->len = 0;

    if (req.ReqOps_case() == KVRequest::ReqOpsCase::kExecRequest)
    {
        ExecCmd* cmd = new ExecCmd();
        cmd->id = req.id();
        int ret;

        ret = process_exec_request(req.exec_request(), cmd);
        if (ret < 0)
        {
            delete cmd;

            KVResponse resp;
            resp.set_id(req.id());
            resp.set_return_code(ret);

            size_t resp_len = resp.ByteSize();
            char* resp_buffer = new char[sizeof(resp_len) + resp_len];
            if (resp_buffer == nullptr)
            {
                LOG(ERROR) << "Failed to allocate memory";
                wreq->flags |= WORK_ERROR;

                return;
            }

            *((size_t*) resp_buffer) = resp_len;
            if (!resp.SerializeToArray(resp_buffer + sizeof(size_t), resp_len))
            {
                LOG(ERROR) << "Failed to serialize response.";
                delete [] resp_buffer;
                resp_buffer = nullptr;
                wreq->flags |= WORK_ERROR;

                return;
            }

            wreq->buffer = resp_buffer;
            wreq->len = resp_len + sizeof(size_t);

            return;
        }

        wreq->flags |= WORK_EXEC;
        wreq->buffer = (void*) cmd;
        wreq->len = sizeof(ExecCmd);
    }
    else
    {
        KVResponse resp;
        if (process_kv_request(req, resp) < 0)
        {
            LOG(ERROR) << "Failed to proess request";
            wreq->flags |= WORK_ERROR;

            return;
        }

        size_t resp_len = resp.ByteSize();
        char* resp_buffer = new char[sizeof(resp_len) + resp_len];
        if (resp_buffer == nullptr)
        {
            LOG(ERROR) << "Failed to allocate memory";
            wreq->flags |= WORK_ERROR;

            return;
        }
        *((size_t*) resp_buffer) = resp_len;
        if (!resp.SerializeToArray(resp_buffer + sizeof(size_t), resp_len))
        {
            LOG(ERROR) << "Failed to serialize response.";
            delete [] resp_buffer;
            resp_buffer = nullptr;
            wreq->flags |= WORK_ERROR;

            return;
        }

        wreq->buffer = resp_buffer;
        wreq->len = resp_len + sizeof(size_t);
    }

    return;
}