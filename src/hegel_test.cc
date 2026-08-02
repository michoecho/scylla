// Worked example of property-based testing with Hegel.
//
// The point of this file is to prove the whole Hegel stack is wired up: the
// Rust engine (nix/libhegel.nix) is driven in-process by the C++ binding
// (nix/hegel-cpp.nix), it generates inputs, finds a planted bug, and shrinks
// it to a minimal counterexample.
//
// Everything here runs as ordinary doctest cases, like the rest of the suite:
// hegel::test() is just a function you call from a test body, so it needs no
// special main() and no second test runner.

#include <cstdint>
#include <stdexcept>
#include <string>

#include <hegel/hegel.h>

#include "doctest/doctest.h"

namespace gs = hegel::generators;

// The function under test: a midpoint that is *almost* right.
//
// The bug is the classic one — `lo + hi` is computed in `int` and overflows
// for large inputs, which is undefined behaviour and in practice wraps to a
// negative value, so the "midpoint" lands outside [lo, hi] entirely. It is
// invisible for small inputs, which is exactly what makes it a good target: a
// hand-written example-based test would have to already suspect the overflow
// to catch it, whereas Hegel finds it from the property alone.
//
// The correct implementation is `lo + (hi - lo) / 2`.
static int buggy_midpoint(int lo, int hi) {
    return (lo + hi) / 2;
}

// The property: a midpoint must lie within the interval it bisects.
//
// Note what this does *not* say — it doesn't name a magic input or assert an
// exact result. It states the invariant, and lets the engine hunt for an input
// that violates it.
static void midpoint_is_within_bounds(hegel::TestCase& tc) {
    HEGEL_DRAW(tc, lo, gs::integers<int>());
    HEGEL_DRAW(tc, hi, gs::integers<int>());

    // Only bisecting a well-formed interval is meaningful. Rejected cases
    // don't count toward the budget; the engine learns to generate ordered
    // pairs rather than burning the run on discards.
    tc.assume(lo <= hi);

    int mid = buggy_midpoint(lo, hi);
    if (mid < lo || mid > hi) {
        throw std::runtime_error("midpoint " + std::to_string(mid) +
                                 " outside [" + std::to_string(lo) + ", " +
                                 std::to_string(hi) + "]");
    }
}

// A test that fails is not what we want in CI, so this asserts the *failure*:
// hegel::test() throws std::runtime_error once it has falsified the property,
// so finding the bug is the passing outcome. If the engine were not actually
// running (a stubbed library, a broken link), no exception would be thrown and
// this test would fail — which is the property of the integration we're
// really testing here.
// Settings shared by the tests below. Built by assignment rather than with a
// designated initializer: Settings has many more fields than we care to set,
// and a braced init that names a few of them warns about every field after the
// last one mentioned (-Wmissing-designated-field-initializers).
//
// Deterministic so the suite can't flake on a seed that happens not to find
// the bug, and quiet so a passing run doesn't print a scary failure report.
static hegel::Settings example_settings() {
    hegel::Settings settings;
    settings.test_cases = 1000;
    settings.verbosity = hegel::Verbosity::Quiet;
    settings.derandomize = true;
    settings.print_blob = false;
    return settings;
}

TEST_CASE("hegel finds the overflow bug in buggy_midpoint") {
    CHECK_THROWS_AS(
        hegel::test(midpoint_is_within_bounds,
                    {"midpoint_is_within_bounds", __FILE__, __LINE__},
                    example_settings()),
        std::runtime_error);
}

// The same property against a correct implementation, to show the other side:
// this one really does hold for every input the engine can produce, so it
// passes by *not* throwing. Without this, a stack that threw unconditionally
// would still satisfy the test above.
TEST_CASE("hegel confirms the fixed midpoint holds") {
    hegel::test(
        [](hegel::TestCase& tc) {
            HEGEL_DRAW(tc, lo, gs::integers<int>());
            HEGEL_DRAW(tc, hi, gs::integers<int>());
            tc.assume(lo <= hi);

            // The overflow-free form. Note that the usual "fix" for this bug,
            // `lo + (hi - lo) / 2`, is still not enough here: `hi - lo`
            // itself overflows when the interval spans most of the range
            // (lo = INT_MIN, hi = INT_MAX). Hegel finds that too, which is
            // why this computes the midpoint in a wider type instead.
            int mid = static_cast<int>(
                (static_cast<std::int64_t>(lo) + static_cast<std::int64_t>(hi)) /
                2);
            if (mid < lo || mid > hi) {
                throw std::runtime_error("correct midpoint out of bounds");
            }
        },
        {"midpoint_fixed", __FILE__, __LINE__}, example_settings());
}
