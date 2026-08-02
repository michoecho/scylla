#include <print>
#include <string>
#include <vector>

#include <CLI/CLI.hpp>

#define DOCTEST_CONFIG_IMPLEMENT
#include "doctest/doctest.h"

#include "bench.h"
#include "fuzz.h"

TEST_CASE("sanity") {
    CHECK(1 + 1 == 2);
}

// Run doctest with `prepend` forced ahead of the user's args, then `user_args`.
// Benchmarks force --no-skip --test-suite=bench so only the bench suite runs;
// `test` prepends nothing. Returns doctest's exit code.
static int run_doctest(const char* argv0,
                       const std::vector<std::string>& prepend,
                       const std::vector<std::string>& user_args) {
    doctest::Context context;
    context.setAsDefaultForAssertsOutOfTestCases();

    // c_str() pointers below must outlive the run; the source vectors do.
    std::vector<const char*> forwarded;
    forwarded.push_back(argv0);
    for (const std::string& arg : prepend)
        forwarded.push_back(arg.c_str());
    for (const std::string& arg : user_args)
        forwarded.push_back(arg.c_str());
    context.applyCommandLine(static_cast<int>(forwarded.size()), forwarded.data());

    int res = context.run();
    if (context.shouldExit()) // important - query flags (and --exit) rely on the user doing this
        return res;
    return res;
}

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

    if (*test)
        return run_doctest(argv[0], {}, test->remaining());

    // Fuzz targets are skip()'d like benchmarks, for the same reason: a normal
    // test run must not start a fuzzer. --no-skip re-enables them and
    // --test-suite=fuzz keeps the run to targets only.
    if (*fuzz)
        return run_doctest(argv[0],
                           {"--no-skip", "--test-suite=" FUZZ_SUITE},
                           fuzz->remaining());

    if (*bench)
        // Benchmarks are skip()'d by default; --no-skip re-enables them and
        // --test-suite=bench keeps the run to benchmarks only. User filters
        // (passed after `bench`) still apply on top.
        return run_doctest(argv[0],
                           {"--no-skip", "--test-suite=" BENCH_SUITE},
                           bench->remaining());

    std::println("Hello, world!");
    return 0;
}
