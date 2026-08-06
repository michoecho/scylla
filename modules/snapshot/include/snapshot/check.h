// The snapshot assertion, for doctest test files.
//
// Separate from snapshot.h so that a translation unit which merely *holds* or
// passes around a Snapshot does not have to pull in doctest. Include this one
// from test files; include snapshot.h from anything else.
//
//     TEST_CASE("render") {
//         check_snapshot(render(3), "[1, 2, 3]"_snap);
//     }

#ifndef SNAPSHOT_CHECK_H
#define SNAPSHOT_CHECK_H

#include <string>
#include <string_view>

#include <doctest/doctest.h>

#include "snapshot/snapshot.h"

namespace snapshot_testing {

// Compare, and fail the enclosing test case on a mismatch.
//
// An ordinary function, not a macro -- which is the whole point of this design.
// A macro here would wreck the updater's anchor: std::source_location inside a
// macro expansion no longer names the literal, and the updater refuses a
// location it cannot pin to that expression. See snapshot.h.
//
// It can be a function because doctest can be *told* where a failure happened
// rather than inferring it from __LINE__ at the assertion site. So the failure
// is blamed on the _snap literal itself, even though this frame is elsewhere
// and even when the Snapshot was built in a different function entirely.
//
// Each outcome produces exactly one doctest failure carrying everything the
// reader needs -- including the two values, for a mismatch. Nothing is written
// to stderr on the side: a diagnostic split between an assertion message and a
// stray printf arrives interleaved with every other test's output, and under a
// parallel ctest run the two halves need not even stay adjacent.
//
// One message per outcome, and each says only what is true of that outcome.
// Routing both failures through a single assertion is how output ends up
// claiming "this snapshot matches" and "snapshot mismatch" about one snapshot.
inline void check_snapshot(std::string_view got, const Snapshot& expected) {
    const Comparison result = compare(got, expected);
    if (result == Comparison::Matched) return;

    const char* const file = expected.location.file_name();
    const auto line = static_cast<int>(expected.location.line());

    switch (result) {
        case Comparison::Matched:
            return;  // handled above; here so the switch stays exhaustive

        // The trailing hint differs only in whether acting on it is the next
        // step: once the rewrite is recorded -- by update mode or by this
        // snapshot's own .update() -- telling the author how to ask for one
        // would be noise on a run that just did it.
        //
        // The message is built into a string first because ADD_FAIL_CHECK_AT
        // streams its arguments into a MessageBuilder, so a `+` written inline
        // would bind to that rather than to the text.
        case Comparison::Mismatched: {
            const std::string message =
                render_mismatch(got, expected) +
                "\n\nRe-run with SNAPSHOT_UPDATE=1, or add .update() to this "
                "snapshot, to rewrite it.";
            ADD_FAIL_CHECK_AT(file, line, message);
            return;
        }

        case Comparison::MismatchedAndRecorded: {
            const std::string message = render_mismatch(got, expected) +
                                        "\n\nRewritten in place -- rebuild and "
                                        "review the diff.";
            ADD_FAIL_CHECK_AT(file, line, message);
            return;
        }

        case Comparison::StaleUpdateMarker:
            ADD_FAIL_CHECK_AT(file, line,
                              "this snapshot matches but still has .update() on it. "
                              "Remove it -- a committed .update() stops the snapshot "
                              "from ever failing.");
            return;
    }
}

}  // namespace snapshot_testing

#endif  // SNAPSHOT_CHECK_H
