#ifndef SIMPLEDB_CACHE_HH
#define SIMPLEDB_CACHE_HH

#include <string>
#include <list>
#include <mutex>
#include <unordered_map>

namespace simpledb::storage
{

    class Cache {
    private:
        const size_t capacity_;
        size_t size_ {0};
        std::list<std::pair<std::string, std::string>> lru_cache_;
        std::unordered_map<std::string, std::list<std::pair<std::string, std::string>>::iterator> lru_lookup_;
        std::mutex lock_;
    public:
        Cache(const size_t capacity): capacity_(capacity) {}
        ~Cache() {}
        size_t size() const { return size_; }
        size_t capacity() const { return capacity_; }

        bool access(const std::string& key, std::string& val);
        void insert(const std::string& key, const std::string& val);
        void drop(const std::string& key);
    };
}

#endif /* SIMPLEDB_CACHE_HH */