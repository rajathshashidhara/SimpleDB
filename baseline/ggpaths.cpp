#include "ggpaths.h"

using namespace std;

string GG_DIR;
std::string GGPaths::blobs;
std::string GGPaths::reductions;
// static GGPaths _paths;
// static GGCache _cache;

static bool paths_init = false;

void ensure_paths_init()
{
    if (!paths_init)
    {
        GG_DIR = std::string(getenv("GG_DIR"));
        GGPaths::blobs = std::string(GG_DIR) + "/blobs";
        GGPaths::reductions = std::string(GG_DIR) + "/reductions";
    }

    paths_init = true;
}

void make_gg_dirs()
{
    ensure_paths_init();

    system(("mkdir -p " + GGPaths::blobs).c_str());
    system(("mkdir -p " + GGPaths::reductions).c_str());
}