#ifndef SIMPLEDB_NET_CONNECTIONS
#define SIMPLEDB_NET_CONNECTIONS

#include "config.h"
#include "uv.h"

int connections_main(unsigned port = DEFAULT_SERVER_PORT, unsigned backlog = SOMAXCONN);

#endif /* SIMPLEDB_NET_CONNECTIONS */