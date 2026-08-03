// Tests for compare() -- the decision of what gets recorded for rewriting, and
// what merely fails.
//
// Not snapshot tests, for the same reason updater_test.cc is not: this is the
// code that decides whether a snapshot is rewritten, so expressing its
// assertions as snapshots would let a bug in it rewrite them.
//
// These deliberately provoke recordings that must never be applied -- the
// snapshots below hold values the code is *meant* to disagree with, and letting
// the flush reporter act on them would rewrite this file into nonsense. Every
// test therefore discards what it recorded, via the RAII guard, so cleanup
// survives a failing assertion too.

#include <doctest/doctest.h>

#include <cstddef>
#include <fstream>
#include <string>
#include <string_view>

#include "snapshot/snapshot.h"

namespace snapshot_testing {

// So a failed CHECK names the outcome rather than printing an integer -- the
// whole point of the enum is that these states are distinguishable by name.
doctest::String toString(const Comparison& result) {
    switch (result) {
        case Comparison::Matched: return "Matched";
        case Comparison::Mismatched: return "Mismatched";
        case Comparison::MismatchedAndRecorded: return "MismatchedAndRecorded";
        case Comparison::StaleUpdateMarker: return "StaleUpdateMarker";
    }
    return "Comparison(?)";
}

}  // namespace snapshot_testing

namespace {

using snapshot_testing::Comparison;
using snapshot_testing::snapshot;

// Discards anything compare() recorded during the test, whatever the outcome.
struct DiscardRecordings {
    ~DiscardRecordings() { snapshot_testing::discard_updates(); }
};

std::size_t recorded() { return snapshot_testing::pending_updates().size(); }

}  // namespace

TEST_CASE("a matching snapshot passes and records nothing") {
    const DiscardRecordings guard;
    const Comparison result = snapshot_testing::compare("same", snapshot("same"));
    CHECK(result == Comparison::Matched);
    CHECK_FALSE(snapshot_testing::failed(result));
    CHECK(recorded() == 0);
}

TEST_CASE("a mismatch fails and records nothing without update mode") {
    // The file system is touched only when asked. A plain failing snapshot
    // reports and stops there.
    const DiscardRecordings guard;
    const Comparison result = snapshot_testing::compare("actual", snapshot("expected"));
    CHECK(result == Comparison::Mismatched);
    CHECK(recorded() == 0);
}

TEST_CASE("a mismatch with .update() records a rewrite") {
    const DiscardRecordings guard;
    // Not a stale marker: this .update() still had work to do.
    const Comparison result =
        snapshot_testing::compare("actual", snapshot("expected").update());
    CHECK(result == Comparison::MismatchedAndRecorded);
    REQUIRE(recorded() == 1);

    const snapshot_testing::PendingUpdate& pending =
        snapshot_testing::pending_updates().back();
    CHECK(pending.old_value == "expected");
    CHECK(pending.new_value == "actual");
}

TEST_CASE("a matching snapshot with .update() left on it fails") {
    // The marker is a mistake once committed: it did nothing this run only
    // because the value happened to be right, and it would silently accept the
    // next change. Fails despite the values agreeing, and records nothing --
    // there is nothing to rewrite.
    const DiscardRecordings guard;
    // The state is StaleUpdateMarker and *not* either mismatched state: the
    // values agree, so nothing here may call this a mismatch. That distinction
    // is the reason these outcomes are an enum rather than a pass/fail bool.
    const Comparison result = snapshot_testing::compare("same", snapshot("same").update());
    CHECK(result == Comparison::StaleUpdateMarker);
    CHECK(snapshot_testing::failed(result));
    CHECK(recorded() == 0);
}

TEST_CASE("render_mismatch lays the two values out against each other") {
    // A pure function of the two values, which is what lets check_snapshot put
    // this inside a single assertion message rather than printing it on the
    // side. Multi-line values are the case it exists for.
    const snapshot_testing::Snapshot expected = snapshot("a\nb\n");
    const std::string rendered = snapshot_testing::render_mismatch("a\nc\n", expected);

    CHECK(rendered.find("--- expected (in source) ---\na\nb\n") != std::string::npos);
    CHECK(rendered.find("--- actual ---\na\nc\n") != std::string::npos);
    // Located, so a reader can go straight to the snapshot that failed.
    CHECK(rendered.find(expected.location.file_name()) != std::string::npos);
}

TEST_CASE("update() yields a copy, leaving the original alone") {
    const snapshot_testing::Snapshot plain = snapshot("value");
    const snapshot_testing::Snapshot forced = plain.update();

    CHECK(forced.value == plain.value);
    CHECK(forced.location.line() == plain.location.line());
    CHECK(forced.forced);
    CHECK_FALSE(plain.forced);
}

// --- the anchor ---------------------------------------------------------------
//
// The updater's entire design rests on a property of the compiler rather than
// of any code here: the location reported for a snapshot() call names the
// `snapshot` identifier, either at its first character (clang) or at the '('
// just past its end (gcc).
//
// That is worth asserting directly. If a future compiler picks some third
// point, every snapshot update would start failing with "no `snapshot`
// identifier at ..." and the cause would be far from obvious; this test names
// it. It reads its own source file to do so, which is the one honest way to
// check a claim about where in a file a location points.

TEST_CASE("the reported location names the snapshot identifier") {
    const snapshot_testing::Snapshot value = snapshot("anchor");

    std::ifstream in(value.location.file_name(), std::ios::binary);
    REQUIRE_MESSAGE(in, "cannot read this test's own source file");
    std::string line;
    for (unsigned n = 0; n < value.location.line(); ++n) REQUIRE(std::getline(in, line));

    // Columns are 1-based, so the reported point is at index column - 1.
    const std::size_t point = value.location.column() - 1;
    REQUIRE(point < line.size());

    static constexpr std::string_view kName = "snapshot";
    const bool at_start = line.compare(point, kName.size(), kName) == 0;
    const bool past_end =
        point >= kName.size() && line.compare(point - kName.size(), kName.size(), kName) == 0;

    // Bound to a name because doctest refuses to decompose a `||` expression.
    const bool anchored = at_start || past_end;
    CHECK_MESSAGE(anchored,
                  "std::source_location reported column ", value.location.column(),
                  " for a snapshot() call, which is neither end of the `snapshot` "
                  "identifier; the updater's anchoring rule no longer holds for "
                  "this compiler");
}
