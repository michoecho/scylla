// The `test` / `bench` subcommands, in one place.
//
// Two different binaries expose these: the shipping executable and every
// module test runner. They used to implement the dispatch separately, and the
// AFL self-test is what made that a bug rather than a duplication -- it
// re-executes its own binary under afl-fuzz, so a runner that disagreed with
// the shipping executable about argument handling failed the moment the test
// moved to it.
//
// The one deliberate difference is the default with no subcommand, which is
// what `default_to_test` selects. A test runner exists to run tests, and its
// bare doctest flags therefore mean `test`. The shipping executable must not:
// it is a program that happens to embed its tests, and `cpp_template` with no
// arguments prints Hello, world!

#ifndef MODULE_RUN_H
#define MODULE_RUN_H

#include <string>
#include <vector>

// The doctest suite every benchmark lives in. Defined here rather than beside
// the BENCHMARK macro because this is the contract between that macro and the
// dispatcher: the macro puts a case into the suite, and execute() scopes a run
// to it. Keeping the name here lets a module define benchmarks without the
// shared runner having to include that module's headers.
#define BENCH_SUITE "bench"

namespace run {

struct Command {
    enum class Kind {
        None,   // no subcommand and no default: the caller decides
        Test,
        Bench,
    };

    Kind kind = Kind::None;
    // Everything after the subcommand, handed to doctest verbatim.
    std::vector<std::string> args;
};

// Classify argv. `default_to_test` makes a bare invocation (and one carrying
// only doctest flags) mean `test`; without it that case is Kind::None.
//
// Only argv[1] is inspected: the subcommands are a prefix, and every remaining
// argument belongs to doctest. That lets `--test-case=x` pass through
// untouched whether or not a subcommand preceded it.
Command classify(int argc, char* const argv[], bool default_to_test);

// Run doctest for `command`, with `preset` forced ahead of the user's
// arguments. Bench adds its own suite scoping on top. Returns doctest's exit
// code.
int execute(const char* argv0,
            const Command& command,
            const std::vector<std::string>& preset = {});

}  // namespace run

#endif  // MODULE_RUN_H
