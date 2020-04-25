#include "execution/cpplambda.h"
#include <sstream>

int lambda_exec(std::vector<std::pair<bool, std::string> >& args,
        std::unordered_map<std::string, std::pair<bool, std::string> >& kwargs,
        std::string& output)
{
    unsigned a, b;

    if (args.size() < 2)
        return -1;

    a = (unsigned) std::stoi(args[0].second);
    b = (unsigned) std::stoi(args[1].second);

    output = std::move(std::to_string(a + b));
    return 0;
}