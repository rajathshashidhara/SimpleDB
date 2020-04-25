#ifndef GGPATHS_H_
#define GGPATHS_H_

#include <fstream>
#include <streambuf>
#include <string>
#include <cstdlib>

extern std::string GG_DIR;

extern "C" {
    #include <unistd.h>
}

class GGPaths {
public:
    static std::string blobs;
    static std::string reductions;

    GGPaths()
    {
        GG_DIR = std::string(getenv("GG_DIR"));
        blobs = std::string(GG_DIR) + "/blobs";
        reductions = std::string(GG_DIR) + "/reductions";
    }

    static inline std::string blob_path(const std::string& blob_hash)
    {
        return blobs + "/" + blob_hash;
    }

    static inline std::string reduction_path(const std::string& blob_hash)
    {
        return reductions + "/" + blob_hash;
    }
};

class GGCache {
    static GGPaths _paths;
    static std::string _check(const std::string& key)
    {
        std::string rpath = move(GGPaths::reduction_path(key));

        if (access(rpath.c_str(), R_OK | W_OK | X_OK) < 0)
            return "";

        std::ifstream tin;
        tin.open(rpath, std::ios::in | std::ios::binary);
        std::string output = std::string((std::istreambuf_iterator<char>(tin)),
            std::istreambuf_iterator<char>());
    }

public:
    static std::string check(const std::string& thunk_hash)
    {
        return _check(thunk_hash);
    }

    static std::string check(const std::string& thunk_hash, const std::string& output_tag)
    {
        std::string key = thunk_hash;
        key.append("#" + output_tag);

        return _check(key);
    }

    static void insert(const std::string& old_hash, const std::string& new_hash)
    {
        std::string rpath = move(GGPaths::reduction_path(old_hash));

        std::ofstream tout;
        tout.open(rpath, std::ios::out | std::ios::binary);
        tout.write(new_hash.c_str(), new_hash.length());
    }
};

void ensure_paths_init();
void make_gg_dirs();

#endif /* GGPATHS_H_ */