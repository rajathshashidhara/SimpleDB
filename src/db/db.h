#ifndef SIMPLEDB_DB_H
#define SIMPLEDB_DB_H

#include <string>
#include "config.h"

namespace simpledb::db
{
    enum db_status_e
    {
        STATUS_OK,
        STATUS_NOTFOUND,
        STATUS_IMMUTABLE,
        STATUS_IOERROR
    };

    int init(const std::string path=DEFAULT_DB_PATH, const bool create=true, const size_t cache_size=DEFAULT_DB_CACHE_SIZE);
    int get(const std::string& key, std::string& value, const bool exec=false);
    int set(const std::string& key, const std::string& value, const bool immutable=false, const bool executable=false);
    int remove(const std::string& key);
}

#endif /* SIMPLEDB_DB_H */