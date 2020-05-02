#include <string>
#include <fstream>
#include <glog/logging.h>

#include "storage/db.h"
#include "net/service.h"
#include "util/path.h"
#include "util/file_descriptor.h"

#include "formats/serialization.pb.h"
#include "formats/execformats.pb.h"

extern "C" {
    #include "config.h"
}

using namespace std;
using namespace simpledb::proto;

simpledb::storage::SimpleDB* Worker::db {nullptr};

// static int process_exec_request(const ExecRequest& request,
//         ExecCmd* cmd)
// {
//     int ret;
//     ExecArgs exec_args;

//     /* Check if file exists in cache and is executable */
//     auto executable_path = roost::path(DEFAULT_CACHE_PATH) / request.func();
//     if (not roost::exists(executable_path, X_OK))
//     {
//         string function_binary;
//         ret = simpledb::db::get(request.func(), function_binary, true);
//         if (ret < 0)
//         {
//             LOG(WARNING) << "Unable to fetch function: " << request.func();
//             return -EXEC_STATUS_LOOKUP_FAILED;
//         }

//         roost::atomic_create(function_binary, executable_path, true, 0544);
//     }

//     /* Add Immediate Args */
//     auto imm_args = exec_args.mutable_args();
//     for (auto &arg : request.immediate_args())
//     {
//         imm_args->Add()->assign(arg);
//     }

//     /* Add File Args */
//     auto fargs = exec_args.mutable_fargs();
//     for (auto &arg: request.file_args())
//     {
//         auto file_path = roost::path(DEFAULT_CACHE_PATH) / arg;
//         if (not roost::exists(file_path))
//         {
//             LOG(INFO) << "File " << arg << " does not exist in cache";

//             string file_data;
//             ret = simpledb::db::get(arg, file_data);
//             if (ret < 0)
//             {
//                 LOG(ERROR) << "Unable to fetch argument: " << arg;
//                 return -EXEC_STATUS_ARGS_INVALID;
//             }

//             roost::atomic_create(file_data, file_path, true, 0444);
//         }
//         fargs->Add()->assign(arg);
//     }

//     /* Add KV Args */
//     auto kwargs = exec_args.mutable_kwargs();
//     for (auto &kwarg: request.dict_args())
//     {
//         std::string val;
//         ret = simpledb::db::get(kwarg.val(), val);
//         if (ret < 0)
//         {
//             LOG(ERROR) << "Unable to fetch argument: " << kwarg.key() << " " << kwarg.val();
//             return -EXEC_STATUS_ARGS_INVALID;
//         }

//         auto kv = kwargs->Add();
//         kv->set_key(kwarg.key());
//         kv->set_val(std::move(val));
//     }

// //     std::string _func;
// //     ExecArgs exec_args;
// //     int ret;

// //     ret = simpledb::db::get(request.func(), _func, true);
// //     if (ret < 0)
// //     {
// //         LOG(ERROR) << "Unable to fetch function: " << request.func();
// //         return -EXEC_STATUS_LOOKUP_FAILED;
// //     }

// //     cmd->func_name = std::move(DEFAULT_EXEC_PATH + request.func());
// //     cmd->func = std::move(_func);

// //     /* Add Immediate Args */
// //     auto imm_args = exec_args.mutable_args();
// //     for (auto arg : request.immediate_args())
// //     {
// //         imm_args->Add(std::move(arg));
// //     }

// //     /* Add File Args */
// //     auto fargs = exec_args.mutable_fargs();
// //     for (auto arg: request.file_args())
// //     {
// //         std::string path(DEFAULT_CACHE_PATH "/");
// //         std::string val;
// //         path.append(arg);

// //         ret = access(path.c_str(), R_OK);
// //         if (ret < 0)
// //         {
// //             LOG(WARNING) << "File " << arg << " does not exist in cache: " << strerror(errno);

// //             ret = simpledb::db::get(arg, val);
// //             if (ret < 0)
// //             {
// //                 LOG(ERROR) << "Unable to fetch argument: " << arg;
// //                 return -EXEC_STATUS_ARGS_INVALID;
// //             }

// //             try
// //             {
// //                 std::ofstream tout(path, std::ios::binary | std::ios::out);
// //                 tout << val;
// //             }
// //             catch (std::ostream::failure& e)
// //             {
// //                 LOG(ERROR) << "Failed to create CACHE file: " << e.code() << " " << e.what();
// //                 return -EXEC_STATUS_ERR;
// //             }
// //         }

// //         fargs->Add(std::move(arg));
// //     }

// //     /* Add KV Args */
// //     auto kwargs = exec_args.mutable_kwargs();
// //     for (auto &kwarg: request.dict_args())
// //     {
// //         std::string val;
// //         ret = simpledb::db::get(kwarg.val(), val);
// //         if (ret < 0)
// //         {
// //             LOG(ERROR) << "Unable to fetch argument: " << kwarg.key() << " " << kwarg.val();
// //             return -EXEC_STATUS_ARGS_INVALID;
// //         }

// //         auto kv = kwargs->Add();
// //         kv->set_key(kwarg.key());
// //         kv->set_val(std::move(val));
// //     }

// //     size_t argsize = exec_args.ByteSize();
// //     cmd->arguments.append(std::string((char*) &argsize, sizeof(size_t)));
// //     cmd->arguments.append(std::move(exec_args.SerializeAsString()));

//     return EXEC_STATUS_OK;
// }

// static int process_kv_request(const KVRequest& request,
//                             KVResponse& response)
// {
//     std::string value;
//     int ret;

//     switch (request.ReqOps_case())
//     {
//     case KVRequest::ReqOpsCase::kGetRequest:
//         ret = simpledb::db::get(request.get_request().key(), value);
//         break;

//     case KVRequest::ReqOpsCase::kPutRequest:
//         ret = simpledb::db::set(request.put_request().key(),
//                 request.put_request().val(), request.put_request().immutable(),
//                 request.put_request().executable());
//         break;

//     case KVRequest::ReqOpsCase::kDeleteRequest:
//         ret = simpledb::db::remove(request.delete_request().key());
//         break;

//     default:
//         return -1;
//     }

//     response.set_id(request.id());
//     response.set_return_code(ret);
//     response.set_val(value);

//     return 0;
// }

// void handle_exec_compl(uv_work_t* wq)
// {
//     work_request* wr = (work_request*) wq->data;
//     ExecCmd* cmd = (ExecCmd*) wr->buffer;
//     ExecResponse exec_resp;
//     KVResponse resp;

//     if (!exec_resp.ParseFromString(cmd->output))
//     {
//         LOG(ERROR) << "Failed to parse output";
//         goto ERROR_HANDLE_EXEC;
//     }

//     resp.set_id(cmd->id);
//     resp.set_return_code(exec_resp.return_code());
//     resp.set_val(exec_resp.return_output());

//     for (auto &kwarg: exec_resp.store_output())
//     {
//         if (simpledb::db::set(kwarg.key(), kwarg.val(), kwarg.immutable(), kwarg.executable()) < 0)
//         {
//             LOG(ERROR) << "Unable to write output to DB";
//             goto ERROR_HANDLE_EXEC;
//         }
//     }

//     delete cmd;
//     wr->buffer = new char[sizeof(size_t) + resp.ByteSize()];
//     wr->len = sizeof(size_t) + resp.ByteSize();
//     *((size_t*) wr->buffer) = resp.ByteSize();

//     if (!resp.SerializeToArray((char*) wr->buffer + sizeof(size_t), resp.ByteSize()))
//     {
//         LOG(ERROR) << "Unable to serialize response";
//         goto ERROR_HANDLE_EXEC;
//     }

//     return;

// ERROR_HANDLE_EXEC:
//     wr->flags |= WORK_ERROR;
//     wr->buffer = nullptr;
//     wr->len = 0;

//     return;
// }

// void handle_request(uv_work_t* wq)
// {
//     work_request* wreq = (work_request*) wq->data;
//     KVRequest req;
//     KVResponse resp;

//     if (!req.ParseFromArray(wreq->buffer, wreq->len))
//     {
//         LOG(ERROR) << "Failed to parse";
//         wreq->flags |= WORK_ERROR;
//         delete [] ((char*) wreq->buffer);
//         wreq->buffer = nullptr;
//         wreq->len = 0;

//         return;
//     }
//     delete [] ((char*) wreq->buffer);
//     wreq->buffer = nullptr;
//     wreq->len = 0;

//     if (req.ReqOps_case() == KVRequest::ReqOpsCase::kExecRequest)
//     {
//         ExecCmd* cmd = new ExecCmd();
//         cmd->id = req.id();
//         int ret;

//         ret = process_exec_request(req.exec_request(), cmd);
//         if (ret == 0)
//         {
//             wreq->flags |= WORK_EXEC;
//             wreq->buffer = (void*) cmd;
//             wreq->len = sizeof(ExecCmd);

//             return;
//         }

//         delete cmd;

//         resp.set_id(req.id());
//         resp.set_return_code(ret);
//     }
//     else
//     {
//         if (process_kv_request(req, resp) < 0)
//         {
//             LOG(ERROR) << "Failed to proess request";
//             wreq->flags |= WORK_ERROR;

//             return;
//         }
//     }

//     size_t resp_len = resp.ByteSize();
//     char* resp_buffer = new char[sizeof(resp_len) + resp_len];
//     if (resp_buffer == nullptr)
//     {
//         LOG(ERROR) << "Failed to allocate memory";
//         wreq->flags |= WORK_ERROR;

//         return;
//     }
//     *((size_t*) resp_buffer) = resp_len;
//     if (!resp.SerializeToArray(resp_buffer + sizeof(size_t), resp_len))
//     {
//         LOG(ERROR) << "Failed to serialize response.";
//         delete [] resp_buffer;
//         resp_buffer = nullptr;
//         wreq->flags |= WORK_ERROR;

//         return;
//     }

//     wreq->buffer = resp_buffer;
//     wreq->len = resp_len + sizeof(size_t);

//     return;
// }

void Worker::process_kv_request(const KVRequest& request,
                                    KVResponse& response)
{
    vector<simpledb::storage::GetRequest> get_requests;
    vector<simpledb::storage::PutRequest> put_requests;
    vector<string> delete_requests;

    switch(request.ReqOps_case())
    {
        case KVRequest::ReqOpsCase::kGetRequest:
            get_requests.emplace_back(request.get_request().key());
            db->get(get_requests,
                [&response](const simpledb::storage::GetRequest& request,
                        const simpledb::storage::DbOpStatus status,
                        const std::string& value)
                {
                    response.set_return_code(static_cast<uint32_t>(status));
                    response.set_val(value);
                }
            );
            break;

        case KVRequest::ReqOpsCase::kPutRequest:
            put_requests.emplace_back(request.put_request().key(),
                request.put_request().val(),
                request.put_request().immutable(),
                request.put_request().executable());
            db->put(put_requests,
                [&response](const simpledb::storage::PutRequest& request,
                    const simpledb::storage::DbOpStatus status)
                {
                    response.set_return_code(static_cast<uint32_t>(status));
                }
            );
            break;

        case KVRequest::ReqOpsCase::kDeleteRequest:
            delete_requests.push_back(request.delete_request().key());
            db->del(delete_requests,
                [&response](const string& request,
                    const simpledb::storage::DbOpStatus status)
                {
                    response.set_return_code(static_cast<uint32_t>(status));
                }
            );
            break;

        default:
            throw runtime_error("Invalid KVRequest type!");
    }

    response.set_id(request.id());
}

void Worker::process_exec_request(const ExecRequest& request,
                                    ExecCmd& command)
{
    throw runtime_error("Not yet implemented!");
}

void Worker::process_exec_result(const simpledb::proto::ExecResponse& result,
                                    simpledb::proto::KVResponse& response)
{
    throw runtime_error("Not yet implemented!");
}

void Worker::process_work(uv_work_t* work)
{
    WorkRequest* work_request = (WorkRequest*) work->data;
    WorkResult* work_result = nullptr;

    switch (work_request->tag)
    {
        case WorkRequest::KV:
            switch (work_request->kv.ReqOps_case())
            {
                case KVRequest::ReqOpsCase::kGetRequest:
                case KVRequest::ReqOpsCase::kPutRequest:
                case KVRequest::ReqOpsCase::kDeleteRequest:
                    work_result = new WorkResult{work_request->client, WorkResult::KV};
                    process_kv_request(work_request->kv, work_result->kv);
                    break;

                case KVRequest::ReqOpsCase::kExecRequest:
                    work_result = new WorkResult{work_request->client, WorkResult::EXEC};
                    process_exec_request(work_request->kv.exec_request(), work_result->exec);
                    break;

                default:
                    throw runtime_error("Unknown KVRequest Op!");
            }
            break;

        case WorkRequest::EXEC:
            work_result = new WorkResult{work_request->client, WorkResult::KV};
            process_exec_result(work_request->exec, work_result->kv);
            break;

        default:
            throw runtime_error("Unknown work type!");
    }

    work->data = work_result;
    delete work_request;
}