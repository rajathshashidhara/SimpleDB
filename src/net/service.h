#ifndef SIMPLEDB_NET_SERVICE
#define SIMPLEDB_NET_SERVICE

#include "protobufs/netformats.pb.h"

using namespace simpledb::proto;

int process_request(const KVRequest& request,
                    KVResponse& response);

#endif /* SIMPLEDB_NET_SERVICE */