#include "module_run.h"

#include <algorithm>
#include <cstring>
#include <cstdio>
#include <string>

#define DOCTEST_CONFIG_IMPLEMENT
#include "doctest/doctest.h"

namespace run {

namespace {

std::string shell_quote(const std::string& value) {
    std::string quoted = "'";
    for (const char character : value) {
        if (character == '\'')
            quoted += "'\\''";
        else
            quoted += character;
    }
    quoted += "'";
    return quoted;
}

void log_command_line(const char* argv0, const std::vector<std::string>& args) {
    std::string command = shell_quote(argv0);
    for (const std::string& arg : args)
        command += " " + shell_quote(arg);
    std::fprintf(stdout, "%s\n", command.c_str());
}

bool is_listing_invocation(const std::vector<std::string>& args) {
    return std::find(args.begin(), args.end(), "--list-test-cases") != args.end();
}

}  // namespace

Command classify(int argc, char* const argv[], bool default_to_test) {
    Command command;
    int first_arg = 1;

    if (argc > 1) {
        if (std::strcmp(argv[1], "test") == 0) {
            command.kind = Command::Kind::Test;
            first_arg = 2;
        } else if (std::strcmp(argv[1], "bench") == 0) {
            command.kind = Command::Kind::Bench;
            first_arg = 2;
        }
    }

    // No subcommand. A test runner treats that as `test` so bare doctest flags
    // work; the shipping executable leaves it None and decides for itself.
    if (command.kind == Command::Kind::None && default_to_test)
        command.kind = Command::Kind::Test;

    for (int i = first_arg; i < argc; ++i)
        command.args.emplace_back(argv[i]);

    return command;
}

int execute(const char* argv0,
            const Command& command,
            const std::vector<std::string>& preset) {
    doctest::Context context;
    context.setAsDefaultForAssertsOutOfTestCases();
    context.setOption("no-intro", true);

    // Benchmarks are ordinary tests whose bodies select a minimal smoke run
    // unless BENCHMARK is set. The bench subcommand only scopes the run to that
    // suite. User filters passed after the subcommand still apply on top.
    std::vector<std::string> scoped;
    if (command.kind == Command::Kind::Bench)
        scoped = {"--test-suite=" BENCH_SUITE};

    // c_str() pointers below must outlive the run; the source vectors do.
    // Order is preset, then suite scoping, then the user's own arguments, so
    // each layer can override the one before it.
    std::vector<const char*> forwarded;
    forwarded.push_back(argv0);
    for (const std::string& arg : preset)
        forwarded.push_back(arg.c_str());
    for (const std::string& arg : scoped)
        forwarded.push_back(arg.c_str());
    for (const std::string& arg : command.args)
        forwarded.push_back(arg.c_str());

    {
        std::vector<std::string> logged_args;
        logged_args.reserve(preset.size() + scoped.size() + command.args.size());
        logged_args.insert(logged_args.end(), preset.begin(), preset.end());
        logged_args.insert(logged_args.end(), scoped.begin(), scoped.end());
        logged_args.insert(logged_args.end(), command.args.begin(), command.args.end());
        if (!is_listing_invocation(logged_args))
            log_command_line(argv0, logged_args);
    }

    context.applyCommandLine(static_cast<int>(forwarded.size()), forwarded.data());
    return context.run();
}

}  // namespace run
