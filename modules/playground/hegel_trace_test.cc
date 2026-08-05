// A Hegel property run inside a pt::Trace scope, so an Intel PT trace covers
// hegel::test itself: the engine drawing values, the property body, the
// exhaustigen enumeration underneath it, and the shrinker if a case fails.
//
// The property is copied from modules/exhaustigen/hegel_splits_test.cc -- the
// one asserting no pass repeats another. It earns its keep there as a test of
// gen_splits; here it is a workload, picked because a whole enumeration runs to
// exhaustion inside every case and so the call tree under hegel::test is deep
// enough to be worth looking at.
//
// Capture with:
//
//   tools/pt-trace --ftf -- ./out/build/Debug/cpp_template test \
//       --test-case='hegel under pt'
//
// Untraced -- a normal ctest run -- pt::Trace no-ops and this is an ordinary,
// somewhat small property test.

#include <cstddef>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

#include <hegel/hegel.h>

#include "doctest/doctest.h"
#include "exhaustigen/exhaustigen.h"
#include "pt/pt_control.h"

namespace {

namespace gs = hegel::generators;

using exhaustigen::Gen;

// As in the original: the enumeration is run to exhaustion inside every case
// and its size is C(n+k-1, k-1), so small bounds are what keep a single case
// cheap.
constexpr size_t kMaxN = 8;
constexpr size_t kMaxK = 5;
constexpr size_t kMaxBound = 6;

// Run one enumeration to exhaustion.
std::vector<std::vector<size_t>> enumerate(size_t n, size_t k, size_t min,
                                           size_t max) {
    std::vector<std::vector<size_t>> all;
    Gen g;
    do {
        all.emplace_back(g.gen_splits(n, k, min, max));
    } while (!g.is_done());
    return all;
}

std::string describe(size_t n, size_t k, size_t min, size_t max) {
    return "n=" + std::to_string(n) + " k=" + std::to_string(k) +
           " min=" + std::to_string(min) + " max=" + std::to_string(max);
}

// 20 cases rather than the 500 the original runs. Enough to exercise the
// machinery under trace, and few enough that the decoded trace is a readable
// size -- a full 500-case run buries the shape of one case in repetition.
//
// Built by assignment for the reason given in modules/main/hegel_test.cc: a
// braced init naming a few fields warns about every field after the last one
// mentioned.
hegel::Settings trace_settings() {
    hegel::Settings settings;
    settings.test_cases = 20;
    settings.verbosity = hegel::Verbosity::Quiet;
    settings.derandomize = true;
    settings.print_blob = false;
    return settings;
}

} // namespace

TEST_SUITE("playground") {

TEST_CASE("hegel under pt") {
    // The scope is around hegel::test, not around the property body: the
    // engine's own work -- drawing, bookkeeping, shrinking -- is as much the
    // subject here as the property is. Bounding it per-case would trace 20
    // disjoint fragments and miss everything between them.
    pt::Trace _;

    hegel::test(
        [](hegel::TestCase& tc) {
            HEGEL_DRAW(tc, n, gs::integers<size_t>({.min_value = 0, .max_value = kMaxN}));
            HEGEL_DRAW(tc, k, gs::integers<size_t>({.min_value = 0, .max_value = kMaxK}));
            HEGEL_DRAW(tc, min, gs::integers<size_t>({.min_value = 0, .max_value = kMaxBound}));
            HEGEL_DRAW(tc, max, gs::integers<size_t>({.min_value = 0, .max_value = kMaxBound}));

            const auto all = enumerate(n, k, min, max);

            const std::set<std::vector<size_t>> uniq(all.begin(), all.end());
            if (uniq.size() != all.size()) {
                throw std::runtime_error(
                    describe(n, k, min, max) + ": " +
                    std::to_string(all.size()) + " passes but only " +
                    std::to_string(uniq.size()) + " distinct");
            }
        },
        {"hegel_under_pt", __FILE__, __LINE__}, trace_settings());
}

} // TEST_SUITE
