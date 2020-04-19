#include "execution/cpplambda.h"
#include "protobufs/execformats.pb.h"

static int parse_args(std::vector<std::string>& args,
            std::unordered_map<std::string, std::string>& kwargs)
{
    simpledb::proto::CPPExecArgs exec_args;
    if (!exec_args.ParseFromIstream(&std::cin))
        return -1;
    
    for (auto it = exec_args.args().begin(); 
            it != exec_args.args().end(); ++it)
    {
        args.push_back(*it);
    }

    for (auto it = exec_args.kwargs().begin();
            it != exec_args.kwargs().end(); ++it)
    {
        kwargs.insert(std::pair<std::string, std::string>(it->key(), it->val()));
    }

    return 0;
}

int return_output(int return_code, std::string& output)
{
    simpledb::proto::CPPExecResponse resp;
    resp.set_return_code(return_code);
    resp.set_output(std::move(output));

    if (!resp.SerializeToOstream(&std::cout))
        return -1;
    
    return 0;
}

int main(int argc, char* argv[])
{
    std::vector<std::string> args;
    std::unordered_map<std::string, std::string> kwargs;
    std::string output;

    if (parse_args(args, kwargs) < 0)
        return PARSE_FAILURE_CODE;

    int ret = lambda_exec(args, kwargs, output);
    
    if (return_output(ret, output) < 0)
        return OUTPUT_FAILURE_CODE;
    
    return 0;
}