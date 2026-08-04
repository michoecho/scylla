// Upstream's test is a main() that prints four enumerations to stdout and
// leaves you to eyeball them. That is exactly the shape a snapshot test is for:
// the interesting property of a generator is the *whole* sequence it produces
// -- its length, its order, and where the repeats fall -- and no hand-written
// scalar assertion describes that. So each enumeration is collected into one
// string and asserted whole, and a change in any of those properties shows up
// as a line diff.
//
// To accept a deliberate change, do not edit the expected values. Run:
//
//     SNAPSHOT_UPDATE=1 ninja -C out/build/Debug && \
//         ./out/build/Debug/modules/exhaustigen/exhaustigen_test
//
// and read the diff. See modules/snapshot/example_test.cc.

#include <doctest/doctest.h>

#include <cstddef>
#include <format>
#include <string>
#include <vector>

#include "exhaustigen/exhaustigen.h"
#include "snapshot/check.h"

namespace {

using exhaustigen::Gen;
using snapshot_testing::check_snapshot;
using snapshot_testing::snapshot;
// The multi-line spelling these snapshots are written in. A literal operator is
// found by ordinary lookup, not ADL, so it has to be named here.

// One pass per line, elements space-separated -- so an empty result is a blank
// line, and the number of lines is the size of the enumeration.
std::string render(const std::vector<size_t>& v) {
    std::string out;
    for (const size_t e : v) {
        out += std::format("{}{}", out.empty() ? "" : " ", e);
    }
    out += '\n';
    return out;
}

const std::vector<size_t> kIn{33, 55, 77};

}  // namespace

TEST_CASE("gen_vec enumerates every vector up to the length and element bound") {
    std::string out;
    Gen g;
    do {
        out += render(g.gen_vec(2, 1));
    } while (!g.is_done());

    check_snapshot(out, snapshot(R"snap(
        |
        |0
        |1
        |0 0
        |0 1
        |1 0
        |1 1
        )snap"_snap));
}

TEST_CASE("gen_comb enumerates combinations with repeats") {
    std::string out;
    Gen g;
    do {
        out += render(g.gen_comb(kIn));
    } while (!g.is_done());

    check_snapshot(out, snapshot(R"snap(
        |
        |33
        |55
        |77
        |33 33
        |33 55
        |33 77
        |55 33
        |55 55
        |55 77
        |77 33
        |77 55
        |77 77
        |33 33 33
        |33 33 55
        |33 33 77
        |33 55 33
        |33 55 55
        |33 55 77
        |33 77 33
        |33 77 55
        |33 77 77
        |55 33 33
        |55 33 55
        |55 33 77
        |55 55 33
        |55 55 55
        |55 55 77
        |55 77 33
        |55 77 55
        |55 77 77
        |77 33 33
        |77 33 55
        |77 33 77
        |77 55 33
        |77 55 55
        |77 55 77
        |77 77 33
        |77 77 55
        |77 77 77
        )snap"_snap));
}

TEST_CASE("gen_perm enumerates each ordering exactly once") {
    std::string out;
    Gen g;
    do {
        out += render(g.gen_perm(kIn));
    } while (!g.is_done());

    check_snapshot(out, snapshot(R"snap(
        |33 55 77
        |33 77 55
        |55 33 77
        |55 77 33
        |77 33 55
        |77 55 33
        )snap"_snap));
}

TEST_CASE("gen_subset enumerates the power set") {
    std::string out;
    Gen g;
    do {
        out += render(g.gen_subset(kIn));
    } while (!g.is_done());

    check_snapshot(out, snapshot(R"snap(
        |
        |77
        |55
        |55 77
        |33
        |33 77
        |33 55
        |33 55 77
        )snap"_snap));
}

TEST_CASE("gen_splits enumerates the ordered splits of a sum") {
    std::string out;
    Gen g;
    do {
        out += render(g.gen_splits(4, 3, 1));
    } while (!g.is_done());

    check_snapshot(out, snapshot(R"snap(
        |1 1 2
        |1 2 1
        |2 1 1
        )snap"_snap));
}

// Every pass has exactly `k` elements, so with a zero minimum the slots that
// contribute nothing are still present, as zeros.
TEST_CASE("gen_splits with a zero minimum pads with empty slots") {
    std::string out;
    Gen g;
    do {
        out += render(g.gen_splits(2, 2));
    } while (!g.is_done());

    check_snapshot(out, snapshot(R"snap(
        |0 2
        |1 1
        |2 0
        )snap"_snap));
}

// Nothing sums to 5 with exactly two elements of at most 2, so there is no
// choice point at all and the loop runs its single pass.
TEST_CASE("gen_splits yields nothing when the bounds cannot reach the sum") {
    std::string out;
    Gen g;
    do {
        out += render(g.gen_splits(5, 2, 0, 2));
    } while (!g.is_done());

    check_snapshot(out, snapshot(R"snap(
        |
        )snap"_snap));
}

// The bounds passed to gen() may depend on earlier choices, which is what a
// nest of for loops cannot express. Here the second choice's range is decided
// by the first.
TEST_CASE("a later bound may depend on an earlier choice") {
    std::string out;
    Gen g;
    do {
        const size_t n = g.gen(3);
        out += render({n, g.gen(n)});
    } while (!g.is_done());

    check_snapshot(out, snapshot(R"snap(
        |0 0
        |1 0
        |1 1
        |2 0
        |2 1
        |2 2
        |3 0
        |3 1
        |3 2
        |3 3
        )snap"_snap));
}

// At the head of a while loop the generator has made no choices yet, so
// is_done() reports done before the body has ever run. Asserted so the
// requirement is checked rather than only documented.
TEST_CASE("is_done on a fresh generator reports done") {
    Gen g;
    CHECK(g.is_done());
}
