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
roost::path Worker::cache_path;

void Worker::process_kv_request(const KVRequest& request,
                                    KVResponse& response)
{
    string content;
    simpledb::storage::DbOpStatus status;

    switch(request.ReqOps_case())
    {
        case KVRequest::ReqOpsCase::kGetRequest:
            LOG(ERROR) << "GET " << request.get_request().key();
            status = db->local_get(simpledb::storage::GetRequest(request.get_request().key()), content);
            response.set_return_code(static_cast<uint32_t>(status));
            response.set_val(content);
            break;

        case KVRequest::ReqOpsCase::kPutRequest:
            LOG(ERROR) << "PUT " << request.put_request().key();
            status = db->local_put(simpledb::storage::PutRequest(request.put_request().key(),
                                                                request.put_request().val(),
                                                                request.put_request().immutable(),
                                                                request.put_request().executable()));
            response.set_return_code(static_cast<uint32_t>(status));
            break;

        case KVRequest::ReqOpsCase::kDeleteRequest:
            LOG(ERROR) << "DEL " << request.delete_request().key();
            status = db->local_del(request.delete_request().key());
            response.set_return_code(static_cast<uint32_t>(status));
            break;

        default:
            throw runtime_error("Invalid KVRequest type!");
    }

    response.set_id(request.id());
}

void Worker::process_exec_request(const ExecRequest& request,
                                    ExecArgs& cmd)
{
    LOG(ERROR) << "EXEC";
    vector<simpledb::storage::GetRequest> get_requests;

    auto executable_path = roost::path(cache_path) / request.func();
    cmd.set_function(executable_path.string());
    if (not roost::exists(executable_path, X_OK))
    {
        get_requests.emplace_back(request.func(), executable_path,
                                                        0544, true);
    }

    // Add Immediate Args
    auto imm_args = cmd.mutable_args();
    for (auto &arg : request.immediate_args())
    {
        imm_args->Add()->assign(arg);
    }

    // Add File Args
    auto fargs = cmd.mutable_fargs();
    for (auto &arg: request.file_args())
    {
        auto file_path = roost::path(cache_path) / arg;
        if (not roost::exists(file_path, F_OK))
        {
            get_requests.emplace_back(arg, file_path, 0444, false);
        }
        fargs->Add()->assign(arg);
    }

    // Add KV Args
    for (auto &kwarg: request.dict_args())
    {
        get_requests.emplace_back(kwarg, false);
    }

    db->get(get_requests,
        [&](const simpledb::storage::GetRequest& req,
        const simpledb::storage::DbOpStatus status,
        const string& data)
        {
            if (status != simpledb::storage::DbOpStatus::STATUS_OK)
                throw runtime_error(
                    string("Failed to download key: ") + req.object_key
                );

            // KV Args
            if (not req.filename.initialized())
            {
                auto kv = cmd.add_kwargs();
                kv->set_key(move(req.object_key));
                kv->set_val(move(data));
            }
        }
    );
}

void Worker::process_exec_result(const simpledb::proto::ExecResponse& result,
                                    simpledb::proto::KVResponse& response)
{
    vector<simpledb::storage::PutRequest> put_requests;

    response.set_return_code(result.return_code());
    response.set_val(result.return_output());

    // Add File Args
    for (auto &arg: result.f_output())
    {
        roost::path f_path(arg.val());
        put_requests.emplace_back(
            arg.key(),
            f_path,
            arg.immutable(),
            arg.executable()
        );
    }

    for (auto &arg: result.kw_output())
    {
        put_requests.emplace_back(
            arg.key(),
            arg.val(),
            arg.immutable(),
            arg.executable()
        );
    }

    db->put(put_requests,
        [](const simpledb::storage::PutRequest& req,
        const simpledb::storage::DbOpStatus status)
        {
            if (status != simpledb::storage::DbOpStatus::STATUS_OK)
                throw runtime_error(
                    string("Failed to upload key: ") + req.object_key
                );
        }
    );
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
                    work_result = new WorkResult{work_request->id, work_request->client,
                                                    WorkResult::KV};
                    work_result->kv.set_id(work_result->id);
                    process_kv_request(work_request->kv, work_result->kv);
                    break;

                case KVRequest::ReqOpsCase::kExecRequest:
                    work_result = new WorkResult{work_request->id, work_request->client,
                                                    WorkResult::EXEC};
                    process_exec_request(work_request->kv.exec_request(), work_result->exec);
                    break;

                default:
                    LOG(ERROR) << "Invalid Op: " << work_request->kv.ReqOps_case();
                    throw runtime_error("Unknown KVRequest Op!");
            }
            break;

        case WorkRequest::EXEC:
            work_result = new WorkResult{work_request->id, work_request->client,
                                                        WorkResult::KV};
            work_result->kv.set_id(work_result->id);
            process_exec_result(work_request->exec, work_result->kv);
            break;

        default:
            throw runtime_error("Unknown work type!");
    }

    work->data = work_result;
    delete work_request;
}