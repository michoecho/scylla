// Property-based tests for Gen::gen_splits, alongside the snapshot tests in
// exhaustigen_test.cc.
//
// The two styles answer different questions and neither replaces the other. A
// snapshot pins the exact sequence for one set of arguments -- its order, its
// length, where the repeats fall -- which is what you want when the sequence
// itself is the interesting artefact. But it says nothing about arguments
// nobody thought to write down. These properties say what must hold for *every*
// (n, k, min, max), and let Hegel hunt for the combination that breaks it.
//
// The properties are checked over a whole enumeration at a time: each Hegel
// case draws the arguments, runs the do-while loop to exhaustion, and asserts
// against the collected result. So a failure names a bad (n, k, min, max)
// rather than a bad individual split.

#include <cstddef>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

#include <hegel/hegel.h>

#include "doctest/doctest.h"
#include "exhaustigen/exhaustigen.h"

namespace {

namespace gs = hegel::generators;

using exhaustigen::Gen;

// Bounds kept small deliberately. The enumeration is run to exhaustion inside
// every Hegel case, and its size is C(n+k-1, k-1) -- so letting `n` and `k`
// reach even the low tens would make a single case take longer than the whole
// suite. The interesting behaviour (empty slots, infeasible bounds, a `max`
// that binds before `n` does) all shows up at these sizes.
constexpr size_t kMaxN = 8;
constexpr size_t kMaxK = 5;
constexpr size_t kMaxBound = 6;

// Run one enumeration to exhaustion. This is the do-while from the header's
// worked example; every property below is a statement about what it collects.
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

// Whether any vector of exactly `k` elements in [min, max] can sum to `n`.
// Computed the obvious way, from the least and greatest reachable sums, rather
// than by reusing the header's overflow-dodging form -- a property test that
// borrows the implementation's own reasoning cannot falsify it. The small
// bounds above keep these products from overflowing.
bool feasible(size_t n, size_t k, size_t min, size_t max) {
    return min <= max && k * min <= n && n <= k * max;
}

// Deterministic so the suite can't flake on a seed, and quiet so a passing run
// prints nothing. Built by assignment for the reason given in
// modules/main/hegel_test.cc: a braced init naming a few fields warns about
// every field after the last one mentioned.
hegel::Settings splits_settings() {
    hegel::Settings settings;
    settings.test_cases = 500;
    settings.verbosity = hegel::Verbosity::Quiet;
    settings.derandomize = true;
    settings.print_blob = false;
    return settings;
}

// The four arguments, drawn once per case and shared by the properties below.
struct Args {
    size_t n;
    size_t k;
    size_t min;
    size_t max;
};

Args draw_args(hegel::TestCase& tc) {
    HEGEL_DRAW(tc, n, gs::integers<size_t>({.min_value = 0, .max_value = kMaxN}));
    HEGEL_DRAW(tc, k, gs::integers<size_t>({.min_value = 0, .max_value = kMaxK}));
    HEGEL_DRAW(tc, min, gs::integers<size_t>({.min_value = 0, .max_value = kMaxBound}));
    HEGEL_DRAW(tc, max, gs::integers<size_t>({.min_value = 0, .max_value = kMaxBound}));
    return Args{n, k, min, max};
}

}  // namespace

TEST_SUITE("hegel") {

// No pass repeats another. This is the property that makes the generator worth
// using: a driver that enumerated the same split twice would silently double
// the cost of every test written on top of it, and nothing else here would
// notice -- the duplicates would each be individually valid.
TEST_CASE("gen_splits enumerates distinct splits") {
    hegel::test(
        [](hegel::TestCase& tc) {
            const Args a = draw_args(tc);
            const auto all = enumerate(a.n, a.k, a.min, a.max);

            const std::set<std::vector<size_t>> uniq(all.begin(), all.end());
            if (uniq.size() != all.size()) {
                throw std::runtime_error(
                    describe(a.n, a.k, a.min, a.max) + ": " +
                    std::to_string(all.size()) + " passes but only " +
                    std::to_string(uniq.size()) + " distinct");
            }
        },
        {"gen_splits_distinct", __FILE__, __LINE__}, splits_settings());
}

// Every pass is a split meeting the criteria: exactly `k` elements, each in
// range, summing to `n`. The bounds are chosen so that no pass has to be
// filtered out, and this is what says so.
//
// The exception is the infeasible case, where nothing meets the criteria and
// gen_splits yields the empty vector -- documented in the header, and asserted
// here rather than skipped, so that "empty" cannot leak into a feasible case
// unnoticed.
TEST_CASE("gen_splits yields only splits meeting the criteria") {
    hegel::test(
        [](hegel::TestCase& tc) {
            const Args a = draw_args(tc);
            const auto all = enumerate(a.n, a.k, a.min, a.max);
            const std::string what = describe(a.n, a.k, a.min, a.max);

            if (!feasible(a.n, a.k, a.min, a.max)) {
                if (all.size() != 1 || !all.front().empty()) {
                    throw std::runtime_error(
                        what + ": infeasible, expected one empty pass, got " +
                        std::to_string(all.size()));
                }
                return;
            }

            for (const auto& split : all) {
                if (split.size() != a.k) {
                    throw std::runtime_error(what + ": length " +
                                             std::to_string(split.size()));
                }
                size_t sum = 0;
                for (const size_t e : split) {
                    if (e < a.min || e > a.max) {
                        throw std::runtime_error(what + ": element " +
                                                 std::to_string(e) +
                                                 " out of range");
                    }
                    sum += e;
                }
                if (sum != a.n) {
                    throw std::runtime_error(what + ": sums to " +
                                             std::to_string(sum));
                }
            }
        },
        {"gen_splits_valid", __FILE__, __LINE__}, splits_settings());
}

// With the default bounds every split is reachable, so the enumeration has
// exactly the stars-and-bars count: the ways to write `n` as `k` ordered
// addends is C(n+k-1, k-1).
//
// The two properties above are both satisfied by a generator that stops early
// -- distinct valid splits, just not all of them. This is the one that pins the
// count, and so the one that catches an omission.
TEST_CASE("gen_splits with default bounds enumerates C(n+k-1, k-1) splits") {
    hegel::test(
        [](hegel::TestCase& tc) {
            HEGEL_DRAW(tc, n, gs::integers<size_t>({.min_value = 0, .max_value = kMaxN}));
            HEGEL_DRAW(tc, k, gs::integers<size_t>({.min_value = 1, .max_value = kMaxK}));

            std::vector<std::vector<size_t>> all;
            Gen g;
            do {
                all.emplace_back(g.gen_splits(n, k));
            } while (!g.is_done());

            // C(n+k-1, k-1), multiplying and dividing in step so the
            // intermediate stays exact and small.
            size_t want = 1;
            for (size_t i = 1; i <= k - 1; ++i) {
                want = want * (n + i) / i;
            }

            if (all.size() != want) {
                throw std::runtime_error(
                    "n=" + std::to_string(n) + " k=" + std::to_string(k) +
                    ": " + std::to_string(all.size()) + " splits, expected " +
                    std::to_string(want));
            }
        },
        {"gen_splits_count", __FILE__, __LINE__}, splits_settings());
}

}  // TEST_SUITE
