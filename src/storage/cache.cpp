#include "storage/cache.h"

using namespace std;
using namespace simpledb::storage;

bool Cache::access(const string& key, string& val)
{
    const lock_guard<mutex> lguard(lock_);

    unordered_map<string, list<pair<string, string>>::iterator>::iterator it;
    if ((it = lru_lookup_.find(key)) == lru_lookup_.end())
        return false;

    val = it->second->second;
    lru_cache_.erase(it->second);
    lru_cache_.push_front(make_pair(key, val));
    lru_lookup_[key] = lru_cache_.begin();

    return true;
}

void Cache::insert(const string& key, const string& val)
{
    const lock_guard<mutex> lguard(lock_);

    const auto len = key.length() + val.length();
    while (size_ + len > capacity_)
    {
        auto &drop = lru_cache_.back();
        size_ -= drop.first.length() + drop.second.length();

        lru_lookup_.erase(drop.first);
        lru_cache_.pop_back();
    }

    lru_cache_.push_front(make_pair(key, val));
    lru_lookup_[key] = lru_cache_.begin();
}

void Cache::drop(const string& key)
{
    const lock_guard<mutex> lguard(lock_);

    unordered_map<string, list<pair<string, string>>::iterator>::iterator it;
    if ((it = lru_lookup_.find(key)) == lru_lookup_.end())
        return;

    lru_cache_.erase(it->second);
    lru_lookup_.erase(key);
}