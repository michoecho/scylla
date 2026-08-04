// A worked example of an expect test.
//
// The point of the form is visible in `render_table` below: the expected value
// is the function's actual output, laid out so a human can read it, rather than
// a set of scalar assertions picked because they were cheap to write by hand.
// A change in column alignment, in ordering, in the separator -- anything a
// row-by-row assertion would have missed -- shows up here as a diff.
//
// Note that nothing here is a macro. check_snapshot() and snapshot() are
// ordinary functions, which is what lets the updater find these literals again;
// see snapshot.h. Write your own helpers over them as functions too.
//
// To change what this asserts, do not edit the expected values. Change the code
// and run:
//
//     SNAPSHOT_UPDATE=1 ninja -C out/build/Debug && \
//         ./out/build/Debug/modules/snapshot/snapshot_test
//
// which rewrites every snapshot in this file at once, then read the diff. The
// run still fails, deliberately: accepting the new behaviour is the diff you
// then commit, not something a green run has already done for you.

#include <doctest/doctest.h>

#include <format>
#include <string>
#include <vector>

#include "snapshot/check.h"

namespace {

using snapshot_testing::check_snapshot;
using snapshot_testing::snapshot;

// The multi-line spelling below. A literal operator is found by ordinary
// lookup, not by ADL, so it has to be named here for the updater's block
// literals to compile -- unlike the two functions above, which would be
// reachable anyway.
using snapshot_testing::operator""_snap;

struct Item {
    std::string name;
    int count = 0;
    double price = 0.0;
};

// The kind of function snapshot tests are for: its output is a formatting
// decision, and formatting decisions are what hand-written assertions describe
// worst.
std::string render_table(const std::vector<Item>& items) {
    std::string out;
    int total = 0;
    for (const Item& item : items) {
        out += std::format("{:<12}{:>4}{:>9.2f}\n", item.name, item.count, item.price);
        total += item.count;
    }
    out += std::format("{:<12}{:>4}\n", "total", total);
    return out;
}

}  // namespace

TEST_CASE("render_table lays out rows in aligned columns") {
    const std::vector<Item> items = {
        {.name = "apple", .count = 3, .price = 1.5},
        {.name = "banana", .count = 12, .price = 0.25},
        {.name = "cherry", .count = 100, .price = 0.05},
    };

    check_snapshot(render_table(items), snapshot(R"snap(
                                                 |apple          3     1.50
                                                 |banana        12     0.25
                                                 |cherry       100     0.05
                                                 |total        115
                                                 )snap"_snap));
}

TEST_CASE("render_table on no items still prints a total row") {
    check_snapshot(render_table({}), snapshot("total          0\n"));
}

// A Snapshot is an ordinary value, so it can be passed to a helper -- and the
// helper is a plain function, because a macro would destroy the location the
// updater anchors on. That is the concrete payoff of the macroless design: a
// test can build its own abstractions over snapshots without any layer having
// to be a macro.
namespace {

void check_single_item(const Item& item, const snapshot_testing::Snapshot& expected) {
    check_snapshot(render_table({item}), expected);
}

}  // namespace

TEST_CASE("a snapshot can be passed to a helper") {
    check_single_item({.name = "kiwi", .count = 7, .price = 2.0},
                      snapshot(R"snap(
                               |kiwi           7     2.00
                               |total          7
                               )snap"_snap));
}
