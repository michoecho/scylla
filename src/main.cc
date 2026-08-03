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

    // `fuzz` lists and runs the FUZZ_TARGETs, which are doctest cases in the
    // fuzz suite (see fuzz.h). Same passthrough as `test`/`bench`, so:
    //
    //     cpp_template fuzz --list-test-cases     # list the targets
    //     cpp_template fuzz --test-case=<name>    # run one under AFL
    //
    // afl-fuzz execs the latter form. In a non-AFL build a target instead
    // replays one stdin testcase, so it doubles as a crash reproducer.
    CLI::App* fuzz = app.add_subcommand("fuzz", "List or run the AFL++ fuzz targets");
    fuzz->prefix_command();

    CLI11_PARSE(app, argc, argv);

    // CLI11 is kept for the parse (and for `--help`, which lists the
    // subcommands and their descriptions), but what each one *does* lives in
    // run::execute, shared with the module test runners. The suite scoping that
    // bench and fuzz need is applied there, so it cannot drift between the two
    // entry points -- which is what broke the AFL self-test when it started
    // running from main_test as well as from here.
    run::Command command;
    if (*test)
        command = {run::Command::Kind::Test, test->remaining()};
    else if (*fuzz)
        command = {run::Command::Kind::Fuzz, fuzz->remaining()};
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
