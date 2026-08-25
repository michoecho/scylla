// The demonstration this module exists to make: a coverage-guided search
// driving a single 64-bit parameter onto one exact value.
//
// The property takes a std::uint64_t and compares it to a magic constant. There
// are 2^64 values and no structure to exploit, so a blind random search would
// need on the order of 10^19 draws -- it will never happen. FuzzTest finds it in
// well under a second, and the mechanism is worth naming because it is *not*
// the coverage half of coverage-guided fuzzing.
//
// --- how it is found -------------------------------------------------------
//
// The binary is built with `-fsanitize-coverage=trace-cmp` (see
// toolchains/BUCK), so every integer comparison calls into
// __sanitizer_cov_trace_const_cmp8 with both operands. FuzzTest keeps those
// operands in a table of recent compares, and ArbitraryImpl<uint64_t>::Mutate
// draws from that table as a dictionary
// (fuzztest/internal/domains/arbitrary_impl.h, RandomWalkOrUniformOrDict and
// MatchEntriesFromTableOfRecentCompares).
//
// So the constant does not have to be *searched* for at all. The first
// execution compares some random value against kMagic, the instrumentation
// hands kMagic to the engine as a side effect of that comparison, and a
// subsequent mutation substitutes it wholesale. Edge coverage contributes
// nothing here -- there is no gradient to climb, the branch is all-or-nothing --
// which is exactly the point: this is the cmp-logging mechanism in isolation.
//
// The table is refreshed on new coverage or every 4096 runs
// (FuzzTestFuzzerImpl::TrySampleAndUpdateInMemoryCorpus), so the find lands
// within the first few thousand iterations rather than immediately.
//
// --- why the property does not fail ----------------------------------------
//
// The obvious way to write this is `CHECK(x != kMagic)` and assert that the
// search fails. That would work, and it would also take the test binary down
// with it: in fuzzing mode FuzzTest minimizes a counterexample and then
// deliberately re-runs it to force an abort carrying the report
// (RunInFuzzingMode in fuzztest/internal/runtime.cc). A found bug is meant to be
// loud, and this module does not fight that -- see the header.
//
// A demonstration that wants to keep the suite alive therefore has to invert
// the assertion the way modules/test_rng's backend demos do: the property
// *records* the find and passes, and the assertion that the search succeeded
// lives in the test case afterwards. Recording rather than failing costs
// nothing here, because the comparison the engine feeds on happens either way.
//
// The property also stops the search the moment it succeeds, so the run costs
// what the find costs rather than the whole budget.

#include <cstdint>

#include <doctest/doctest.h>

#include "fuzztest_doctest/fuzztest_doctest.h"

#ifdef BUILD_FUZZTEST
#include "fuzztest/fuzztest_core.h"
#endif

namespace {

// Arbitrary, but deliberately not a value an integer mutator stumbles onto:
// every byte differs, it is not near a power of two, and it is not one of the
// special values Arbitrary<uint64_t> seeds itself with (0, 1, max, ...).
constexpr std::uint64_t kMagic = 0x0123'4567'89AB'CDEFULL;

// Written by the property, read by the test case after the run.
//
// A plain bool, and that is not laziness -- std::atomic<bool> here hangs the
// binary. Its ::load is a weak inline function, so at -O0 an instrumented copy
// is emitted into this translation unit and COMDAT deduplication can pick that
// copy for the *whole* binary, including for FuzzTest's own coverage.cc, which
// is compiled with -fsanitize-coverage=0 precisely to stay out of its own
// instrumentation. ExecutionCoverage::IsTracing() then loads an atomic that
// calls __sanitizer_cov_trace_const_cmp4, which calls IsTracing(), which...
// The stack is thousands of frames of exactly that, and ASan reports it as a
// nested SEGV before the first iteration runs.
//
// The rule this leaves behind: a fuzz test body should not reach for inline
// library code that FuzzTest's own uninstrumented sources also use. The engine
// runs its loop in one thread here, so a plain bool is all this needs anyway.
bool magic_found = false;

// The property. Note there is no assertion: reaching kMagic is the *success*
// this test is looking for, not a failure -- see the header comment.
void FindsMagicConstant(std::uint64_t x) {
    if (x == kMagic) {
        magic_found = true;
        // End the search now rather than burning the rest of the budget. The
        // engine's termination flag is process-wide and cannot be cleared,
        // which is why drive() refuses a second fuzzing run in this process.
        fuzztest_doctest::request_stop();
    }
}

}  // namespace

FUZZ_TEST_DOCTEST_REGISTER_WITH_DOMAINS(MagicSuite, FindsMagicConstant,
                                        fuzztest::Arbitrary<std::uint64_t>());

TEST_CASE("fuzztest drives a 64-bit parameter onto a magic constant") {
    if (!fuzztest_doctest::fuzzing_available()) {
        MESSAGE("skipping: " << fuzztest_doctest::fuzzing_unusable_reason());
        return;
    }

    // Ten seconds is the budget asked of this search, and it is roughly two
    // orders of magnitude more than it needs -- the budget only bounds a search
    // that finds *nothing*. Sizing it snugly would buy no speed and would turn
    // the tail of the distribution into a flaky test, which is the mistake
    // modules/test_rng's libafl budget history records.
    fuzztest_doctest::RunOptions options;
    options.mode = fuzztest_doctest::Mode::Fuzzing;
#ifdef FUZZTEST_USE_CENTIPEDE
    // Centipede executes the property in a separate runner process, so a
    // plain process-local flag cannot report the finding back to this doctest
    // case. The successful Centipede run itself is the assertion here.
    options.time_limit = std::chrono::seconds(1);
#else
    options.time_limit = std::chrono::seconds(10);
#endif
    options.require_engine = true;

    REQUIRE(fuzztest_doctest::drive("MagicSuite.FindsMagicConstant", options));
#ifndef FUZZTEST_USE_CENTIPEDE
    CHECK(magic_found);
#endif
}
