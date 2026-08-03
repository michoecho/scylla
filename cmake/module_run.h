// The `test` / `bench` / `fuzz` subcommands, in one place.
//
// Two different binaries expose these: the shipping executable (src/main.cc)
// and every module test runner (cmake/module_test_main.cc). They used to
// implement the dispatch separately, and the AFL self-test is what made that a
// bug rather than a duplication -- it fuzzes its own executable with
// `<self> fuzz --test-case=deliberate_bug`, so a runner that understood the
// case but not the subcommand failed the moment the test moved to it.
//
// Sharing the dispatch means any binary carrying a fuzz target can serve as
// afl-fuzz's target, and a bench or fuzz suite behaves identically wherever it
// is run from.
//
// The one deliberate difference is the default with no subcommand, which is
// what `default_to_test` selects. A test runner exists to run tests, and CTest
// invokes it with bare doctest flags (`--test-case=...`), so it treats those as
// `test`. The shipping executable must not: it is a program that happens to
// embed its tests, and `cpp_template` with no arguments prints Hello, world!

#ifndef MODULE_RUN_H
#define MODULE_RUN_H

#include <string>
#include <vector>

// The doctest suites the `bench` and `fuzz` subcommands scope to. Defined here
// rather than beside the BENCHMARK/FUZZ_TARGET macros because they are a
// contract between those macros and this dispatcher: the macros put a case into
// the suite, and `execute` below is what scopes a run to it. Keeping the names
// here lets a module define benchmarks or fuzz targets without the shared
// runner having to include that module's headers.
#define BENCH_SUITE "bench"
#define FUZZ_SUITE  "fuzz"

namespace run {

// What a binary wants done, after the subcommand has been identified.
struct Command {
    enum class Kind {
        None,   // no subcommand and no default: the caller decides
        Test,
        Bench,
        Fuzz,
    };

    Kind kind = Kind::None;
    // Everything after the subcommand, handed to doctest verbatim.
    std::vector<std::string> args;
};

// Classify argv. `default_to_test` makes a bare invocation (and one carrying
// only doctest flags) mean `test`; without it that case is Kind::None.
//
// Only argv[1] is inspected: the subcommands are a prefix, and every remaining
// argument belongs to doctest. That is what lets `--test-case=x` pass through
// untouched whether or not a subcommand preceded it.
Command classify(int argc, char* const argv[], bool default_to_test);

// Run doctest for `command`, with `preset` forced ahead of the user's
// arguments -- the module filter, in a test runner's case. Bench and fuzz add
// their own suite scoping on top. Returns doctest's exit code.
int execute(const char* argv0,
            const Command& command,
            const std::vector<std::string>& preset = {});

}  // namespace run

#endif  // MODULE_RUN_H
