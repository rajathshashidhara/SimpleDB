#include <string>
#include <glog/logging.h>

#include "db/db.h"
#include "net/service.h"

int process_request(const KVRequest& request,
                    KVResponse& response)
{
    std::string value;
    int ret;

    switch (request.op())
    {
    case OpType::GET:
        ret = simpledb::db::get(request.key(), value);
        break;

    case OpType::SET:
        ret = simpledb::db::set(request.key(), request.val(),
                request.immutable());
        break;
    case OpType::DELETE:
        ret = simpledb::db::remove(request.key());
        break;
    default:
        return -1;
    }

    response.set_id(request.id());
    response.set_return_code(ret);
    response.set_val(value);

    return 0;
}
