#ifndef SIMPLEDB_CONFIG
#define SIMPLEDB_CONFIG

#define DEFAULT_SERVER_PORT             8080
#define DEFAULT_BUFFER_ALLOC_SIZE       128 * 1024  /* 128 KB */

#define DEFAULT_DB_PATH                 "/tmp/simpledb"
#define DEFAULT_DB_CACHE_SIZE           512 * 1024 * 1024   /* 512 MB */

#define DEFAULT_EXEC_PATH               DEFAULT_DB_PATH "/exec"
#define DEFAULT_CACHE_PATH              DEFAULT_DB_PATH "/cache"

#endif /* SIMPLEDB_CONFIG */
