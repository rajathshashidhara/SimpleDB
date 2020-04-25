#include "execution/cpplambda.h"
#include <sstream>

void lambda_exec(const simpledb::proto::ExecArgs& params,
                simpledb::proto::ExecResponse& resp)
{
    unsigned a, b;

    if (params.args_size() < 2)
        throw std::runtime_error("Incorrect arguments");

    a = (unsigned) std::stoi(params.args()[0]);
    b = (unsigned) std::stoi(params.args()[1]);

    resp.Clear();
    resp.set_return_code(0);
    resp.set_return_output(std::to_string(a + b));

    return;
}