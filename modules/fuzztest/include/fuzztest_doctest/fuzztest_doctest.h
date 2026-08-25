// FuzzTest properties as ordinary doctest test cases, with no GoogleTest.
//
// A fuzz test is written the way FuzzTest documents it -- a plain function of
// typed parameters, plus a domain per parameter -- and registered with a macro
// that also declares the doctest TEST_CASE which drives it:
//
//     void SortIsIdempotent(std::vector<int> v) {
//         auto once = sorted(v);
//         CHECK(sorted(once) == once);
//     }
//     FUZZ_TEST_DOCTEST_WITH_DOMAINS(
//         MySuite, SortIsIdempotent,
//         fuzztest::VectorOf(fuzztest::InRange(-100, 100)).WithMaxSize(32));
//
//     void RoundTrips(std::uint64_t x, const std::string& tag) { ... }
//     FUZZ_TEST_DOCTEST(MySuite, RoundTrips);   // Arbitrary<> for every param
//
// Any signature FuzzTest accepts works, with the full combinator vocabulary
// from fuzztest/domain_core.h. What this header supplies is only the driver:
// upstream's own driver (InitFuzzTest / RegisterFuzzTestsAsGoogleTests) is the
// single place FuzzTest depends on GoogleTest, and it is the piece replaced
// here.
//
// --- why this is only a driver ---------------------------------------------
//
// The engine itself is framework-agnostic: fuzztest/internal/runtime.cc, which
// holds the fuzzing loop, the domains, mutation, shrinking and crash reporting,
// contains no reference to GoogleTest at all. Only init_fuzztest.cc and
// googletest_adaptor.{h,cc} do. So the Buck2 dep set below is the GoogleTest-
// free subset of the vendored cell, and the two jobs upstream's adaptor does
// are re-done here against doctest:
//
//   1. Enumerate what the registration macros registered, and run it --
//      internal::ForEachTest plus FuzzTestFuzzer::RunIn{UnitTest,Fuzzing}Mode.
//   2. Tell the engine when a *non-crashing* assertion failed. The engine finds
//      bugs through signal handlers, so a failing doctest CHECK -- which only
//      records and returns -- is invisible to it and the search would sail past
//      the bug. See the doctest IReporter in fuzztest_doctest.cc.
//
// Both reach into fuzztest/internal/, which carries no stability guarantee.
// That is the price of not using upstream's driver, and it is confined to this
// module so that a FuzzTest upgrade breaks in exactly one place.
//
// --- the two modes ---------------------------------------------------------
//
// Mode::UnitTest is the cheap one and needs no instrumentation: it replays
// seeds and then mutates for a bounded budget (upstream: 1s / 10k iterations,
// see FUZZTEST_FUZZ_FOR). Its mutation loop passes an empty MutationMetadata,
// so there is no coverage feedback and no table of recent compares -- it is a
// smoke test, not a search. Fine for `buck2 test` with no modifier.
//
// Mode::Fuzzing is the real coverage-guided search, and requires the binary to
// be instrumented:
//
//     buck2 test --modifier root//:fuzztest //modules/fuzztest:fuzztest_test
//
// It refuses to run otherwise rather than reporting a clean pass, for the
// reason every availability check in modules/test_rng exists: a coverage-guided
// search with no coverage is a slow random search that reports exactly like a
// passing one.
//
// --- what a found bug does -------------------------------------------------
//
// In fuzzing mode a failing property does NOT come back as a doctest failure.
// FuzzTest minimizes the counterexample and then deliberately re-runs it with
// should_terminate_on_non_fatal_failure set, so the process aborts carrying the
// full report and the reproducing input (runtime.cc, in RunInFuzzingMode). That
// is upstream's design and this module does not fight it: a found bug is meant
// to be loud and to hand you the input, not to be swallowed into a red line in
// a test log.
//
// The practical consequence is that a fuzzing-mode test case which finds
// something takes the test binary down with it. A property that wants to
// *demonstrate* a find while leaving the suite alive has to record the find
// instead of failing on it, and stop the search itself -- see
// magic_constant_test.cc, which does exactly that and explains the bargain.
//
// In unit-test mode nothing aborts: the loop breaks on the first external
// failure and the doctest CHECK that failed has already marked the case red on
// its own.
//
// --- exceptions ------------------------------------------------------------
//
// An exception must never escape a property body. FuzzTest catches nothing
// anywhere (there is not one `catch` in its non-test sources), and its
// per-iteration teardown -- SetIsTracing(false), OnTestIterationEnd,
// UnsetCurrentArgs -- is plain sequential code rather than RAII. An exception
// unwinding through it skips all of that, leaving coverage tracing on and
// Runtime::current_args_ dangling at a destroyed stack local that the crash
// reporter reads.
//
// std::terminate does not save us here the way it would in a bare fuzzing
// binary: doctest's test-case runner wraps every TEST_CASE body in
// `catch(const TestFailureException&)` / `catch(...)`, so a handler always
// exists up the stack. The exception unwinds the whole engine and is caught by
// doctest, which marks the case failed but has no idea which input did it.
//
// --- what a property translation unit may include --------------------------
//
// A file holding properties is coverage-instrumented, and must not reach into
// fuzztest/internal/. This is a hard constraint, not style: std::atomic<bool>
// ::load is a weak inline function, so an instrumented copy emitted by such a
// file can win COMDAT deduplication for the whole binary -- including for
// FuzzTest's own coverage.cc, which is built with -fsanitize-coverage=0
// precisely to stay out of its own instrumentation. IsTracing() then loads an
// atomic whose load calls __sanitizer_cov_trace_const_cmp4, which calls
// IsTracing(), and the process dies in that recursion before the first
// iteration runs.
//
// So a property file includes this header and fuzztest/fuzztest_core.h (domains
// and macros, no internals) and nothing else from FuzzTest. Everything a test
// needs from the runtime is re-exported here as an out-of-line function --
// request_stop(), fuzzing_available() -- compiled into the uninstrumented
// driver library. See modules/fuzztest/BUCK for the split.
//
// --- exceptions, continued -------------------------------------------------
//
// So both macros route the property through Guard below, which catches at the
// throw site -- inside the engine's iteration, before any of its frames unwind
// -- and aborts there instead. That turns a throw into an ordinary FuzzTest
// crash report, with the input intact. The cost is that doctest's REQUIRE
// (which aborts a test case by throwing) becomes a hard abort inside a property
// body; prefer CHECK, which only records and lets the run continue.

#pragma once

#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>

namespace fuzztest_doctest {

// Which of the two runs above `drive` should perform.
enum class Mode {
    // Bounded, uninstrumented, no coverage feedback. Safe everywhere.
    UnitTest,
    // Coverage-guided search. Requires the root//:fuzztest modifier.
    Fuzzing,
};

struct RunOptions {
    Mode mode = Mode::Fuzzing;

    // Wall-clock budget for the run, applied through Configuration::time_limit.
    // Bounds a search that finds nothing; a search that finds something stops
    // as soon as it does.
    std::chrono::milliseconds time_limit = std::chrono::seconds(10);

    // Cap on fuzzing-mode iterations, or 0 for no cap. Applied through
    // FUZZTEST_MAX_FUZZING_RUNS, which is the only channel the engine reads it
    // from -- it is not a Configuration field.
    std::uint64_t max_runs = 0;

    // When set, a run that cannot happen (no engine linked, no instrumentation)
    // fails the test case instead of skipping it with a message. Off by
    // default so an unmodified `buck2 test` stays green.
    bool require_engine = false;
};

// Why a Mode::Fuzzing run cannot happen here, or empty if it can.
std::string fuzzing_unusable_reason();
bool fuzzing_available();

// Drive the fuzz test registered under `full_name` ("Suite.Function").
//
// Returns true if the run actually happened. Note that "happened" is not
// "passed": a unit-mode property whose CHECK failed has already marked the
// doctest case red through the reporter bridge, and a fuzzing-mode property
// that failed took the process down before this could return at all.
bool drive(std::string_view full_name, const RunOptions& options = {});

// The engine's termination flag is process-wide and can be set but never
// cleared, so at most one fuzzing-mode search can run per process: a second
// would stop before its first mutation and report a clean pass -- a search that
// searched nothing. `drive` refuses the second and says so.
//
// Call this from a property that has found what it was looking for, to end the
// search early instead of burning the whole time budget.
void request_stop();

}  // namespace fuzztest_doctest

#ifdef BUILD_FUZZTEST

#include <cstdio>
#include <cstdlib>
#include <exception>

#include <doctest/doctest.h>

#include "fuzztest/fuzztest_core.h"
#include "fuzztest/internal/registration.h"
#include "fuzztest/internal/registry.h"

namespace fuzztest_doctest {

// Wraps a property so no exception can reach the engine. See the header note.
//
// The partial specialization deduces the property's parameter pack from its
// function type, so Run has the property's exact signature -- references and
// all -- which is what FuzzTest's GetRegistration expects to be handed.
template <auto F>
struct Guard;

template <typename... Args, void (*F)(Args...)>
struct Guard<F> {
    static void Run(Args... args) {
        try {
            F(args...);
        } catch (const std::exception& e) {
            // abort() rather than rethrow: this frame sits inside the engine's
            // iteration, so aborting here leaves Runtime::current_args_ valid
            // and the report names the input. Rethrowing would unwind past the
            // engine's teardown and lose it.
            std::fprintf(stderr,
                         "[!] uncaught exception in fuzz test body: %s\n",
                         e.what());
            std::abort();
        } catch (...) {
            std::fprintf(stderr,
                         "[!] uncaught non-std exception in fuzz test body\n");
            std::abort();
        }
    }
};

// Registration helper, used only by the macros below. Returns bool so it can
// initialize a file-scope variable, which is how registration is made to happen
// before main -- the same mechanism FUZZ_TEST itself uses.
template <typename Registration>
bool register_fuzz_test(Registration&& reg) {
    ::fuzztest::RegisterFuzzTest(std::forward<Registration>(reg));
    return true;
}

}  // namespace fuzztest_doctest

// Shared tail of the two macros: the doctest case that drives the run.
//
// Declared *before* the registration so the property's own name is still what
// the registration expression ends with, and so a caller can read the pair as
// "here is the test case, here is what it runs".
#define FUZZTEST_DOCTEST_CASE_(suite, func)                                    \
    TEST_CASE("fuzz: " #suite "." #func) {                                     \
        ::fuzztest_doctest::drive(#suite "." #func);                           \
    }

#define FUZZTEST_DOCTEST_REGISTER_(suite, func, ...)                           \
    [[maybe_unused]] static const bool fuzztest_doctest_reg_##suite##_##func = \
        ::fuzztest_doctest::register_fuzz_test(                                \
            ::fuzztest::GetRegistration(#suite, #func, __FILE__, __LINE__,     \
                                        &::fuzztest_doctest::Guard<func>::Run) \
                __VA_ARGS__)

// Register `func` as a fuzz test with Arbitrary<> for every parameter, and
// declare the doctest case that drives it.
#define FUZZ_TEST_DOCTEST(suite, func)                                         \
    FUZZTEST_DOCTEST_CASE_(suite, func)                                        \
    FUZZTEST_DOCTEST_REGISTER_(suite, func, )

// The same, with an explicit domain per parameter.
#define FUZZ_TEST_DOCTEST_WITH_DOMAINS(suite, func, ...)                       \
    FUZZTEST_DOCTEST_CASE_(suite, func)                                        \
    FUZZTEST_DOCTEST_REGISTER_(suite, func, .WithDomains(__VA_ARGS__))

// Register only, declaring no test case. For a property whose run needs
// non-default RunOptions, or an assertion *after* the search has finished --
// write the TEST_CASE by hand and call drive() from it.
#define FUZZ_TEST_DOCTEST_REGISTER(suite, func)                                \
    FUZZTEST_DOCTEST_REGISTER_(suite, func, )

#define FUZZ_TEST_DOCTEST_REGISTER_WITH_DOMAINS(suite, func, ...)              \
    FUZZTEST_DOCTEST_REGISTER_(suite, func, .WithDomains(__VA_ARGS__))

#else  // !BUILD_FUZZTEST

#include <doctest/doctest.h>

// Without the engine linked there is nothing to register. The case still
// exists, so the suite's shape does not change with the modifier, and `drive`
// reports why it did nothing.
#define FUZZ_TEST_DOCTEST(suite, func)                                         \
    TEST_CASE("fuzz: " #suite "." #func) {                                     \
        (void)&func;                                                           \
        ::fuzztest_doctest::drive(#suite "." #func);                           \
    }

#define FUZZ_TEST_DOCTEST_WITH_DOMAINS(suite, func, ...)                       \
    FUZZ_TEST_DOCTEST(suite, func)

// Nothing to register, but the property must still count as used or it trips
// -Wunused-function: it sits in an anonymous namespace and the registration was
// its only caller. Taking its address is enough, and is valid at namespace
// scope (unlike a bare expression), so the trailing semicolon still lands.
#define FUZZ_TEST_DOCTEST_REGISTER(suite, func)                                \
    [[maybe_unused]] static constexpr auto fuzztest_doctest_unused_##suite##_##func = &func

#define FUZZ_TEST_DOCTEST_REGISTER_WITH_DOMAINS(suite, func, ...)              \
    FUZZ_TEST_DOCTEST_REGISTER(suite, func)

#endif  // BUILD_FUZZTEST
