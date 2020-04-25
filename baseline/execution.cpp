#include <string>
#include <vector>
#include <cstdlib>
#include <fstream>
#include <glog/logging.h>

#include "gg.pb.h"
#include "ggpaths.h"
#include "base64.h"

using namespace std;
using namespace gg::protobuf;

extern "C" {
    #include <unistd.h>
    #include <sys/types.h>
    #include <fcntl.h>
}

static bool is_hash_for_thunk(const string& hash)
{
    return (hash.length() > 0 && hash[0] == 'T');
}

static std::ifstream::pos_type filesize(const string& filename)
{
    std::ifstream in(filename, std::ifstream::ate | std::ifstream::binary);
    return in.tellg();
}

static bool is_executable(const string& filename)
{
    return (access(filename.c_str(), X_OK) >= 0);
}

void handler(const ExecutionRequest& req, ExecutionResponse& resp)
{
    ensure_paths_init();

    auto &thunks = req.thunks();
    string storage_uri = req.storage_backend();
    bool timelog = req.timelog();

    setenv("GG_STORAGE_URI", storage_uri.c_str(), true);
    system("rm -rf /tmp/thunk-execute.*");

    bool tried_once = false;
    while (true)
    {
        try
        {
            for (auto& thunk: thunks)
            {
                string thunk_data = move(thunk.data());
                string thunk_path = move(GGPaths::blob_path(thunk.hash()));
                if (access(thunk_path.c_str(), O_RDWR) == 0)
                {
                    unlink(thunk_path.c_str());
                }

                ofstream thunk_file;
                thunk_file.exceptions(ofstream::failbit | ofstream::badbit);
                thunk_file.open(thunk_path, ios::out | ios::binary);
                thunk_file << thunk_data;
            }
        }
        catch (ostream::failure& e)
        {
            if (!tried_once && e.code().value() == ENOSPC)
            {
                tried_once = true;
                LOG(WARNING) << "Cleanup up GG dirs";

                system(("rm -rf " + GGPaths::blobs).c_str());
                system(("rm -rf " + GGPaths::reductions).c_str());
                make_gg_dirs();

                continue;
            }
            else
            {
                LOG(WARNING) << "Failed to create thunks";
                std::rethrow_exception(current_exception());
            }
        }
    }

    string command = "gg-execute-static --get-dependencies --put-output --cleanup";
    if (timelog)
        command.append(" --timelog");
    for (auto& thunk: thunks)
    {
        command.append(" ");
        command.append(thunk.hash());
    }

    int ret = system(command.c_str());

    resp.set_return_code(ret);
    resp.set_stdout("");

    auto executed_thunks = resp.mutable_executed_thunks();
    for (auto& thunk : thunks)
    {
        auto executed_thunk = executed_thunks->Add();
        auto executed_thunk_outputs = executed_thunk->mutable_outputs();
        for (auto &output_tag : thunk.outputs())
        {
            auto output_hash = GGCache::check(thunk.hash(), output_tag);

            if (!output_hash.empty())
            {
                resp.clear_executed_thunks();
                return;
            }

            string data;
            if (is_hash_for_thunk(output_hash))
            {
                ifstream tin;
                tin.open(GGPaths::blob_path(output_hash), ios::binary | ios::in);
                data = move(string((istreambuf_iterator<char>(tin)),
                istreambuf_iterator<char>()));
            }

            auto output = executed_thunk_outputs->Add();
            output->set_tag(output_tag);
            output->set_hash(output_hash);
            output->set_size(filesize(GGPaths::blob_path(output_hash)));
            output->set_executable(is_executable(GGPaths::blob_path(output_hash)));
            output->set_data(data);
        }
    }

    return;
}
