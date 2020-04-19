#include "execution/cpplambda.h"
#include <sstream>

int lambda_exec(std::vector<std::string>& args,
        std::unordered_map<std::string, std::string>& kwargs,
        std::string& output)
{
    unsigned a, b;

    if (args.size() < 2)
        return -1;

    a = (unsigned) std::stoi(args[0]);
    b = (unsigned) std::stoi(args[1]);

    output = std::move(std::to_string(a + b));
    return 0;
}