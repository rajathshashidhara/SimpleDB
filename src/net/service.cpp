#include <string>
#include <glog/logging.h>

#include "db/db.h"
#include "net/service.h"

#include "protobufs/serialization.pb.h"
#include "protobufs/execformats.pb.h"

extern "C" {
    #include "config.h"
}

static int process_exec_request(const ExecRequest& request,
        ExecCmd* cmd)
{
    std::string _func;
    CPPExecArgs eargs;
    int ret;

    ret = simpledb::db::get(request.func(), _func, true);
    if (ret < 0)
    {
        LOG(ERROR) << "Unable to fetch function: " << request.func();
        return -EXEC_STATUS_LOOKUP_FAILED;
    }

    cmd->func_name = std::move(DEFAULT_EXEC_PATH + request.func());
    cmd->func = std::move(_func);
    auto args = eargs.mutable_args();
    for (auto &arg: request.list_args())
    {
        std::string val;
        std::string path;

        auto larg = args->Add();
        switch (arg.type())
        {
            case ArgType::IMMEDIATE:
                val = arg.key();
                larg->set_val(val);
                break;

            case ArgType::BYTESTRING:
                ret = simpledb::db::get(arg.key(), val);
                if (ret < 0)
                {
                    LOG(ERROR) << "Unable to fetch argument: " << arg.key();
                    return -EXEC_STATUS_ARGS_INVALID;
                }
                larg->set_val(val);
                break;

            case ArgType::FILE:
                path = std::string(DEFAULT_CACHE_PATH "/");
                path.append(arg.key());

                ret = access(path.c_str(), O_RDONLY);
                if (ret < 0)
                {
                    LOG(WARNING) << "File " << arg.key() << " does not exist in cache: " << strerror(errno);

                    ret = simpledb::db::get(arg.key(), val);
                    if (ret < 0)
                    {
                        LOG(ERROR) << "Unable to fetch argument: " << arg.key();
                        return -EXEC_STATUS_ARGS_INVALID;
                    }

                    /* Create file */
                    int fd = open(path.c_str(), O_RDWR | O_CREAT, S_IRWXU | S_IRGRP | S_IXGRP);

                    if (fd < 0)
                    {
                        LOG(ERROR) << "Unable to open argument file: " << strerror(errno);
                        return -EXEC_STATUS_ERR;
                    }

                    ssize_t write_len;
                    size_t total_write_len = 0;
                    while (total_write_len < val.length())
                    {
                        write_len = write(fd, val.c_str() + total_write_len, val.length() - total_write_len);
                        if (write_len < 0)
                        {
                            LOG(ERROR) << "Failed to create CACHE file: " << strerror(errno);
                            return -EXEC_STATUS_ERR;
                        }

                        total_write_len += write_len;
                    }
                }

                larg->set_val(path);
                larg->set_is_file(true);
                break;

            default:
                LOG(ERROR) << "Unable to fetch argument: " << arg.key();
                return -EXEC_STATUS_ARGS_INVALID;
        }
    }

    auto kwargs = eargs.mutable_kwargs();
    for (auto &kwarg: request.dict_args())
    {
        DictArg* new_arg = kwargs->Add();
        std::string val;
        std::string path;

        switch (kwarg.type())
        {
            case ArgType::IMMEDIATE:
                val = kwarg.val();
                break;

            case ArgType::BYTESTRING:
                ret = simpledb::db::get(kwarg.key(), val);
                if (ret < 0)
                {
                    LOG(ERROR) << "Unable to fetch argument: " << kwarg.key();
                    return -EXEC_STATUS_ARGS_INVALID;
                }
                break;

            case ArgType::FILE:
                path = std::string(DEFAULT_CACHE_PATH "/");
                path.append(kwarg.key());

                ret = access(path.c_str(), O_RDONLY);
                if (ret < 0)
                {
                    LOG(WARNING) << "File " << kwarg.key() << " does not exist in cache: " << strerror(errno);

                    ret = simpledb::db::get(kwarg.key(), val);
                    if (ret < 0)
                    {
                        LOG(ERROR) << "Unable to fetch argument: " << kwarg.key();
                        return -EXEC_STATUS_ARGS_INVALID;
                    }

                    /* Create file */
                    int fd = open(path.c_str(), O_RDWR | O_CREAT, S_IRWXU | S_IRGRP | S_IXGRP);

                    if (fd < 0)
                    {
                        LOG(ERROR) << "Unable to open argument file: " << strerror(errno);
                        return -EXEC_STATUS_ERR;
                    }

                    ssize_t write_len;
                    size_t total_write_len = 0;
                    while (total_write_len < val.length())
                    {
                        write_len = write(fd, val.c_str() + total_write_len, val.length() - total_write_len);
                        if (write_len < 0)
                        {
                            LOG(ERROR) << "Failed to create CACHE file: " << strerror(errno);
                            return -EXEC_STATUS_ERR;
                        }

                        total_write_len += write_len;
                    }
                }

                val = std::move(path);
                new_arg->set_is_file(true);
                break;
            default:
                LOG(ERROR) << "Unable to fetch argument: " << kwarg.key();
                return -EXEC_STATUS_ARGS_INVALID;
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
                request.put_request().val(), request.put_request().immutable(),
                request.put_request().executable());
        break;

    case KVRequest::ReqOpsCase::kDeleteRequest:
        ret = simpledb::db::remove(request.delete_request().key());
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