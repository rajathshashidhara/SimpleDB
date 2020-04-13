#ifndef SIMPLEDB_DB_H
#define SIMPLEDB_DB_H

#include "config.h"
#include <string>

namespace simpledb::db
{
    enum db_status_e
    {
        STATUS_OK,
        STATUS_NOTFOUND,
        STATUS_IMMUTABLE,
        STATUS_IOERROR
    };

    int init(std::string path, bool create, size_t cache_size);
    int get(const std::string key, std::string& value);
    int set(const std::string key, const std::string value, const bool immutable=false);
    int remove(const std::string key);
}

#endif /* SIMPLEDB_DB_H */