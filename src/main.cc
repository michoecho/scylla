#include <print>
#include <string>
#include <vector>

#include <CLI/CLI.hpp>

#define DOCTEST_CONFIG_IMPLEMENT
#include "doctest/doctest.h"

#include "module_run.h"

int main(int argc, char* argv[]) {
    CLI::App app{"cpp_template"};
    app.require_subcommand(0, 1);

    CLI::App* test = app.add_subcommand("test", "Run the test suite");
    // Everything after `test` is handed to doctest verbatim, untouched by CLI11.
    test->prefix_command();

    CLI::App* bench = app.add_subcommand("bench", "Run the benchmark suite");
    // Same passthrough as `test`; we just scope doctest to the bench suite below.
    bench->prefix_command();

    CLI11_PARSE(app, argc, argv);

    // CLI11 is kept for the parse (and for `--help`, which lists the
    // subcommands and their descriptions), but what each one *does* lives in
    // run::execute, shared with the module test runners. The suite scoping that
    // bench needs is applied there, so it cannot drift between the two entry
    // points.
    //
    // There is deliberately no `fuzz` subcommand. A randomized test is an
    // ordinary test case whose engine is chosen by TEST_RNG, so fuzzing one is
    // `TEST_RNG=afl cpp_template test --test-case=<name>` under afl-fuzz.
    run::Command command;
    if (*test)
        command = {run::Command::Kind::Test, test->remaining()};
    else if (*bench)
        command = {run::Command::Kind::Bench, bench->remaining()};

    // No subcommand: this is a program that embeds its tests, not a test
    // runner, so a bare invocation runs the program. (A module test runner
    // defaults to `test` instead; see run::classify.)
    if (command.kind == run::Command::Kind::None) {
        std::println("Hello, world!");
        return 0;
    }

    return run::execute(argv[0], command);
}
