// Rewriting snapshot(...) literals in a source file.
//
// This is the half of snapshot testing that edits your code, so it is written
// to be boring and suspicious. It does not parse C++, and it does not search
// for anything. It is handed a set of (line, column) locations that the
// compiler itself reported -- via the defaulted std::source_location argument
// of snapshot(), see snapshot.h -- and each one *is* the answer. The updater
// only verifies that what is there matches the tiny shape it can rewrite.
//
// --- the anchor ---------------------------------------------------------------
//
// For a call to an ordinary function, std::source_location::current() as a
// default argument reports the call site exactly. The two compilers this
// project builds with pick opposite ends of the callee's name:
//
//     clang   the first character of the identifier      snapshot("a")
//                                                        ^
//     gcc     the opening parenthesis just past it       snapshot("a")
//                                                                ^
//
// Both are pinned to the `snapshot` token, and neither wanders with the value's
// length, the number of literals, the enclosing expression, or anything else.
// So the rule is: the identifier `snapshot` begins either *at* the reported
// column, or exactly `strlen("snapshot")` characters *before* it. Nothing else
// is accepted -- not a nearby snapshot, not a best guess. If the identifier is
// not at one of those two places, the file is not what the compiler saw, and
// the update is refused.
//
// This is why snapshot() must be a plain function called directly at the site
// of the literal. Wrapped in a macro, the reported location is wherever the
// expansion is anchored -- not the macro's name, and not stable -- and no rule
// of this kind could exist.
//
// --- verification, not matching ------------------------------------------------
//
// Having found the identifier by position alone, the updater checks that what
// follows it is exactly:
//
//     snapshot ( <literals> )
//
// where <literals> is either a run of zero or more single-line string literals,
// with whitespace, comments and newlines allowed between any two tokens, or a
// single block literal R"snap(...)snap"_snap whose margins are stripped exactly
// as the _snap suffix strips them at compile time (see snapshot.h). One or the
// other: a call mixing the two is refused. It then
// checks that those literals decode to the value the test reported seeing. Any
// deviation aborts the whole update rather than being handled cleverly.
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

// One rewrite: at (line, column), replace the literals of a snapshot() call
// currently holding `old_value` with `new_value`.
//
// Lines and columns are 1-based, and columns count bytes. The column is an
// anchor, not a hint: it must name either end of the `snapshot` identifier, by
// one of the two conventions above.
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
// is no `snapshot` identifier at either anchor position; it is not followed by
// `(`, a run of single-line string literals *or* one block literal, and `)`;
// the call mixes the two spellings; the literals there do not decode to the
// update's `old_value`; or two updates resolve to the same call.
//
// The result is all-or-nothing by construction: the rewrite is computed into a
// new string and only a wholly successful pass produces one.
UpdateResult apply_updates(std::string_view source, std::vector<Update> updates);

// Render `value` as the source text of a run of single-line string literals,
// indented to sit under a snapshot( at `indent` columns.
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
