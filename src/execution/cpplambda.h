#ifndef SIMPLEDB_CPP_LAMBDA
#define SIMPLEDB_CPP_LAMBDA

#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>

#define PARSE_FAILURE_CODE 0xdeadbeef
#define OUTPUT_FAILURE_CODE 0xbeefdead

extern int lambda_exec(std::vector<std::string>& args,
        std::unordered_map<std::string, std::string>& kwargs,
        std::string& output);

#endif /* SIMPLEDB_CPP_LAMBDA */