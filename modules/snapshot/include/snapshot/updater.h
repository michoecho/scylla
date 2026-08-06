// Rewriting _snap literal expressions in a source file.
//
// This is the half of snapshot testing that edits your code, so it is written
// to be boring and suspicious. It does not parse C++, and it does not search
// for anything. It is handed a set of (line, column) locations that the
// implicit Snapshot conversion reported, see snapshot.h -- and each one *is*
// the answer. The updater only verifies that what is there matches the tiny
// shape it can rewrite.
//
// --- the anchor ---------------------------------------------------------------
//
// SnapshotLiteral converts implicitly to Snapshot through a constructor whose
// defaulted std::source_location argument reports the conversion site exactly:
// the opening quote of an ordinary literal, or the R of a block literal.
// Nothing else is accepted -- not a nearby literal and not a best guess. If a
// supported literal does not begin at that byte, the file is not what the
// compiler saw and the update is refused.
//
// --- verification, not matching ------------------------------------------------
//
// Having found the anchor by position alone, the updater checks that it begins
// exactly one of:
//
//     "..." "..."_snap
//     R"snap(...)snap"_snap
//
// The quoted form may contain several adjacent literals, with whitespace,
// comments and newlines between them; its last literal carries the suffix. The
// block form has margins stripped exactly as the suffix does at compile time.
// The updater then checks that the expression decodes to the value the test
// reported seeing. Any deviation aborts the whole update.
//
// The old-value check is a staleness guard, not a search key: it proves the
// file has not been edited since the test ran. Nothing is ever *located* by its
// text.
//
// --- multiple updates ---------------------------------------------------------
//
// A run typically records many updates, and rewriting one shifts everything
// below it: a snapshot that grew from one line to four moves every later
// location down by three. The updater therefore resolves every location against
// the *original* text first, before touching anything, and only then rewrites
// -- from the bottom of the file upwards, so each edit disturbs only text that
// has already been rewritten.
//
// That two-phase order is also what makes the reported locations valid despite
// "possible offset due to previous edits": no edit has happened yet when they
// are resolved. An update set is therefore order-independent and applied in one
// pass, and a set that cannot be fully resolved changes nothing at all.

#ifndef SNAPSHOT_UPDATER_H
#define SNAPSHOT_UPDATER_H

#include <string>
#include <string_view>
#include <vector>

namespace snapshot_testing {

// One rewrite: at (line, column), replace a _snap literal expression currently
// holding `old_value` with `new_value`.
//
// Lines and columns are 1-based, and columns count bytes. The column is an
// anchor, not a hint: it must name the literal's opening quote or R.
struct Update {
    unsigned line = 0;
    unsigned column = 0;
    // What the source is expected to hold right now. Checked against the
    // literals actually found there, and a mismatch aborts the update: it
    // means the file has changed since the test ran, and the recorded
    // locations can no longer be trusted to point at what we think they do.
    std::string old_value;
    std::string new_value;
};

// The outcome of an update attempt. Either `text` is the rewritten source, or
// `error` explains why nothing should be written.
struct UpdateResult {
    bool ok = false;
    std::string text;   // valid only when ok
    std::string error;  // valid only when !ok
};

// Apply `updates` to `source`, returning the rewritten text.
//
// Fails, changing nothing, if: `source` is not valid UTF-8; any new value is
// not valid UTF-8; a location does not name an existing line and column; there
// there is no supported _snap literal expression at the anchor; its literals do
// not decode to the update's `old_value`; or two updates resolve to the same
// expression.
//
// The result is all-or-nothing by construction: the rewrite is computed into a
// new string and only a wholly successful pass produces one.
UpdateResult apply_updates(std::string_view source, std::vector<Update> updates);

// Render `value` as the source text of a snapshot's literals, with any
// continuation lines indented by `indent` columns.
//
// `indent` is the final width, already including the continuation step: the
// caller computes it from the indentation of the line the literal sits on, not
// from its column, so layout does not depend on expression nesting.
//
// Exposed for the updater's own tests. A value occupying a single line becomes
// one literal on the same line as the call. A value spanning lines becomes a
// block literal -- R"snap(...)snap"_snap, one value line per source line behind
// a `|` margin -- so that a diff of the snapshot is a line diff of the value,
// and the file holds the text rather than a run of escapes.
//
// The escaped one-literal-per-line form is used instead for a value a raw
// string cannot carry legibly: one containing the closing delimiter, a tab, or
// a carriage return. Both decode identically and the parser reads either.
std::string render_literals(std::string_view value, unsigned indent);

}  // namespace snapshot_testing

#endif  // SNAPSHOT_UPDATER_H
