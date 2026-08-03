#pragma once

#include <concepts>
#include <cstddef>
#include <span>
#include <string_view>

#include "doctest/doctest.h"

// FUZZ_SUITE, the doctest suite every FUZZ_TARGET lives in, comes from the
// shared runner (cmake/module_run.h): it is that dispatcher which scopes a
// `fuzz` run to the suite, so `fuzz --list-test-cases` lists the targets and
// `fuzz --test-case=<name>` runs one under AFL.
#include "module_run.h"

namespace fuzz {

// Drives `fn` with fuzzer input, once per input.
//
//   * afl-clang-fast++ build (the Fuzz preset): AFL persistent mode with shared
//     memory. __AFL_INIT() starts the fork server after process setup (CLI11 +
//     doctest have already run by then, so that cost is paid once), and each
//     __AFL_LOOP iteration hands over a fresh testcase from AFL's shared-memory
//     buffer without a fork/exec.
//   * plain build: reads one testcase from stdin and runs `fn` once, so a saved
//     crash can be replayed without AFL:
//         ./cpp_template fuzz --test-case=<name> < crash_input
//
// The callback is type-erased down to (const std::byte*, std::size_t) so that
// all of the AFL-macro machinery stays in one non-template function in
// fuzz.cc — see the comment there about why it cannot be a header template.
void run(void (*fn)(const std::byte*, std::size_t));

// Adapts a user target to the type-erased signature `run` wants. The target may
// take whichever of these shapes reads best at the call site; the input bytes
// are identical either way.
//
//   * std::span<const std::byte>
//   * std::string_view
//   * (const char*, std::size_t)
//
// `Fn` is a capture-less lambda or plain function, so it converts to a function
// pointer and needs no state carried alongside it.
template <auto Fn>
void invoke(const std::byte* data, std::size_t size) {
    if constexpr (std::invocable<decltype(Fn), std::span<const std::byte>>)
        Fn(std::span<const std::byte>(data, size));
    else if constexpr (std::invocable<decltype(Fn), std::string_view>)
        Fn(std::string_view(reinterpret_cast<const char*>(data), size));
    else if constexpr (std::invocable<decltype(Fn), const char*, std::size_t>)
        Fn(reinterpret_cast<const char*>(data), size);
    else
        static_assert(false,
                      "fuzz target must be callable with std::span<const std::byte>, "
                      "std::string_view, or (const char*, std::size_t)");
}

}  // namespace fuzz

// Define a fuzz target:
//
//     FUZZ_TARGET("my_parser", [](std::span<const std::byte> input) {
//         parse(input);
//     });
//
// It registers as a doctest case in FUZZ_SUITE, marked skip() so a normal test
// run (ctest / `cpp_template test`) never executes it and it stays out of the
// default test list. `cpp_template fuzz` flips --no-skip and scopes to the
// suite, so the targets are exactly what that subcommand lists and runs.
//
// Outside the fuzz build (BUILD_FUZZERS off) the target is still *compiled* —
// so it cannot rot — but not registered: the body goes into an unused static
// function instead of a doctest case, leaving the normal binary's test list
// clean and free of AFL-dependent cases. Both arms name `fn` identically, so a
// type error surfaces in every build, not only under the Fuzz preset.
#ifdef BUILD_FUZZERS
#define FUZZ_TARGET(name, fn)                                                  \
    TEST_CASE(name * doctest::test_suite(FUZZ_SUITE) * doctest::skip()) {      \
        ::fuzz::run(&::fuzz::invoke<fn>);                                      \
    }                                                                          \
    static_assert(true, "swallow the trailing semicolon")
#else
#define FUZZ_TARGET(name, fn)                                                  \
    [[maybe_unused]] static void DOCTEST_ANONYMOUS(FUZZ_UNREGISTERED_)() {     \
        ::fuzz::run(&::fuzz::invoke<fn>);                                      \
    }                                                                          \
    static_assert(true, "swallow the trailing semicolon")
#endif
