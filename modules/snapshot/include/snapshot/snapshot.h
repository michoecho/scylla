// Snapshot ("expect") tests: assertions whose expected value is written back
// into the source file by the test runner itself.
//
// An ordinary assertion makes you author the expected value by hand, which is
// why nobody asserts on anything larger than a scalar. A snapshot test inverts
// that: you write the assertion with an empty expectation, run the suite once
// with SNAPSHOT_UPDATE=1, and the runner rewrites the literal in place. The
// value in the file is then a real, reviewable artifact -- a diff in it is a
// diff in behaviour, which is the whole point.
//
//     TEST_CASE("render") {
//         check_snapshot(render(3), snapshot("[1, 2, 3]"));
//     }
//
// --- no macros ---------------------------------------------------------------
//
// snapshot() is an ordinary function with a defaulted
// std::source_location::current() argument, and check_snapshot() is an ordinary
// function too. That is a load-bearing decision, not a style preference.
//
// The updater has to find the literal again in the source, and what it has to
// go on is the location the compiler reported. For a plain function call that
// location is exact and lands on the call itself: clang reports the first
// character of the callee's name, gcc reports the opening parenthesis just past
// it. Two conventions, both pinned to the `snapshot` token -- which is why the
// updater can insist on finding that identifier right there and refuse to guess
// otherwise. See updater.h.
//
// Inside a macro expansion none of that holds: the reported location is
// wherever the expansion happens to be anchored, which is not the macro's own
// name and moves with the surrounding expression. So an assertion *macro*
// wrapping snapshot() would break the updater's only reliable handle. Write
// helpers over snapshots as functions, and they keep working.
//
// --- the value ---------------------------------------------------------------
//
// Two spellings, and the updater writes whichever suits the value.
//
// A value with no line break is one ordinary literal, on the line of the call:
//
//     check_snapshot(render(3), snapshot("[1, 2, 3]\n"));
//
// Anything spanning lines is a *block literal*: a raw string with a `snap`
// delimiter, passed through the _snap suffix, whose lines carry a `|` margin.
//
//     check_snapshot(render(3), snapshot(R"snap(
//         |1
//         |2
//         )snap"_snap));
//
// which is exactly the value "1\n2\n". Escaped newlines are what expect tests
// are worst at reading, and this form has none: the text in the file is the
// text the value holds, laid out as the program actually printed it.
//
// The updater indents the block one step in from the line the call sits on --
// relative to the line, not to the `snapshot` token, so a call nested deep in
// an expression does not drag its value out to the right margin.
//
// _snap strips, at compile time (see below), the newline that follows the
// opening delimiter and, from every line, the leading spaces and the `|`. The
// margin is what makes the two independent: everything after the `|` is
// content, so the block may be indented freely without the indentation
// becoming part of the value, and a value with its own leading whitespace
// survives intact.
//
// A literal operator is found by ordinary unqualified lookup rather than by
// ADL, so a test file that may be rewritten into this form needs
//
//     using snapshot_testing::operator""_snap;
//
// alongside its using-declarations for snapshot() and check_snapshot(). Without
// it the block the updater writes will not compile -- which is a build error,
// not a corrupted file, but an avoidable surprise. See example_test.cc.
//
// Never a raw string without _snap, and no other escape form. The updater has
// to rewrite this text, and the set of things it must understand to do that
// safely is exactly these two shapes -- a deliberately tiny grammar it can
// verify completely and bail on when surprised. See updater.h.

#ifndef SNAPSHOT_SNAPSHOT_H
#define SNAPSHOT_SNAPSHOT_H

#include <algorithm>
#include <array>
#include <cstddef>
#include <source_location>
#include <string>
#include <string_view>
#include <vector>

namespace snapshot_testing {

// --- the block literal -------------------------------------------------------
//
// R"snap(...)snap"_snap, the multi-line spelling described above. Everything
// here runs at compile time and allocates nothing: the stripped text lives in a
// static constexpr array, so the resulting string_view has static storage
// duration exactly as a plain literal's does, and a Snapshot built from one can
// outlive the full-expression that made it.
//
// A template on a class-type non-type parameter rather than the GNU
// `template <char...>` string-literal extension, which is not standard C++.

namespace detail {

// The stripping rule, in one place, used both to size the result and to fill
// it -- so the two cannot disagree.
//
// Skips one leading newline (the one that follows the opening delimiter, which
// exists only so the first content line can start in column 1), then for each
// line drops leading spaces up to and including a `|`. A line with no `|` keeps
// its content but loses that indentation; a line whose content begins with
// spaces keeps them, because they sit after the margin.
//
// `emit` receives each retained character in order. Threading a callback
// through is what lets the size pass and the copy pass be the same code.
template <typename Emit>
constexpr void strip_margins(std::string_view raw, Emit emit) {
    std::size_t i = (!raw.empty() && raw[0] == '\n') ? 1 : 0;
    while (i < raw.size()) {
        while (i < raw.size() && raw[i] == ' ') ++i;
        if (i < raw.size() && raw[i] == '|') ++i;
        while (i < raw.size()) {
            const char c = raw[i++];
            emit(c);
            if (c == '\n') break;
        }
    }
}

constexpr std::size_t stripped_size(std::string_view raw) {
    std::size_t n = 0;
    strip_margins(raw, [&n](char) { ++n; });
    return n;
}

// A string literal usable as a template argument: a structural type holding the
// characters by value, which is how C++20 lets a literal parameterise a
// template at all.
template <std::size_t N>
struct RawLiteral {
    std::array<char, N> data{};

    consteval RawLiteral(const char (&literal)[N]) {  // NOLINT(google-explicit-constructor)
        std::copy_n(literal, N, data.begin());
    }

    // N counts the terminating null, which is not part of the text.
    constexpr std::string_view view() const { return {data.data(), N - 1}; }
};

// The stripped text, as static storage. Instantiated once per distinct literal,
// so identical blocks in different tests share one array.
template <RawLiteral L>
struct StrippedLiteral {
    static constexpr std::size_t size = stripped_size(L.view());

    static constexpr std::array<char, size + 1> text = [] {
        std::array<char, size + 1> out{};  // the extra element is the null
        std::size_t n = 0;
        strip_margins(L.view(), [&out, &n](char c) { out[n++] = c; });
        return out;
    }();
};

}  // namespace detail

// The suffix itself. See the block-literal section above for what it strips.
template <detail::RawLiteral L>
constexpr std::string_view operator""_snap() {
    return {detail::StrippedLiteral<L>::text.data(), detail::StrippedLiteral<L>::size};
}

// An expected value plus the source location of the call that wrote it.
//
// The location is an anchor, not a hint: it points at the `snapshot` token of
// the call below, by one of the two conventions described above, and the
// updater rejects anything that does not sit exactly there.
struct Snapshot {
    std::string_view value;
    std::source_location location;

    // Rewrite this one snapshot, without SNAPSHOT_UPDATE in the environment:
    //
    //     check_snapshot(render(x), snapshot("stale").update());
    //
    // For the inner loop where you are iterating on a single expected value.
    // Marking the one snapshot beats the env var there, because every *other*
    // snapshot in the suite keeps asserting normally and can still catch a
    // change you did not mean to make.
    //
    // Returns a copy rather than mutating, so a Snapshot stays a value and
    // `snapshot(...).update()` is a single expression. The location is carried
    // through unchanged -- .update() captures nothing of its own, so the anchor
    // still names the `snapshot` token.
    //
    // Leaving one of these in a committed test is a mistake the runner refuses
    // to let pass; see `forced` below and the check in compare().
    [[nodiscard]] Snapshot update() const {
        Snapshot copy = *this;
        copy.forced = true;
        return copy;
    }

    // Set by update(). Not part of the expected value.
    bool forced = false;
};

// snapshot("expected"): the expected value, tagged with where it is written.
//
// The defaulted location argument is what the updater anchors on, so this
// function must be called *directly* at the site of the literal. Forwarding it
// through another function would report that forwarder's call site instead, and
// the updater would refuse the location rather than rewrite the wrong text.
//
// The value is a string_view of the literal itself, which has static storage
// duration -- a Snapshot can therefore outlive the full-expression it was
// built in, and be stored or passed on freely.
//
// Defaulted so that snapshot() with nothing in it -- the way a new snapshot is
// written before its first update run -- is well-formed.
inline Snapshot snapshot(std::string_view value = {},
                         std::source_location location = std::source_location::current()) {
    return Snapshot{.value = value, .location = location};
}

// What compare() found. Exactly one of these holds.
//
// An enum rather than a set of flags because the states are mutually exclusive
// and the combinations are not meaningful: a leftover .update() is diagnosed
// precisely when the values *agree*, so "mismatched and stale marker" is not a
// state that exists. Spelling it as booleans invites a caller to test the wrong
// one and describe a matching snapshot as a mismatch -- which is how output ends
// up saying "this snapshot matches" and "snapshot mismatch" about the same
// snapshot.
enum class Comparison {
    // The values agree and nothing is left over. The only passing state.
    Matched,

    // The values disagree, and nothing was recorded: the run is not in update
    // mode and this snapshot carries no .update().
    Mismatched,

    // The values disagree and the rewrite has been recorded, because the run is
    // in update mode or this snapshot carries .update(). Distinct from
    // Mismatched only so the caller can say what happens next instead of
    // telling the author to ask for a rewrite that already happened.
    MismatchedAndRecorded,

    // The values agree, but .update() is still on this snapshot. A marker
    // someone left behind: it did nothing this run only because the value
    // happened to be right, and it would silently accept the next change, so
    // the suite would quietly stop asserting here.
    //
    // Only reachable when the values match -- a .update() that still has work to
    // do is the feature working as intended, not a mistake.
    StaleUpdateMarker,
};

// Whether `result` should fail the test.
[[nodiscard]] constexpr bool failed(Comparison result) {
    return result != Comparison::Matched;
}

// Compare `got` against `expected`, and in update mode record the difference
// instead of failing.
//
// Pure apart from that recording: it decides and reports, and prints nothing.
// Rendering the difference belongs to the caller, which is the layer that knows
// how failures are meant to surface -- see check_snapshot in check.h.
//
// A match records nothing and touches no file: the file system is written to
// only when the test would otherwise have failed, so a passing suite is
// read-only and remains runnable where the sources aren't present.
//
// On a mismatch in update mode (SNAPSHOT_UPDATE=1 in the environment, or
// .update() on this one snapshot) the pending rewrite is recorded, and the
// result is *still* a mismatch. Two reasons, and the second is the important
// one: every snapshot in the suite gets a chance to record, so one update run
// fixes the whole file rather than the first mismatch in it; and a run that
// rewrote sources must never come back green, or a behaviour change could be
// accepted by CI without anyone seeing it. Update mode is a tool for the author,
// and the failing run is the notification that it did something.
Comparison compare(std::string_view got, const Snapshot& expected);

// Render a mismatch: the two values whole, one after the other.
//
// Exposed because check_snapshot needs it and the two live in different
// headers. A snapshot value is usually multi-line, and laying the values out
// against each other is the only way that reads -- an assertion framework would
// show it as one escaped line.
std::string render_mismatch(std::string_view got, const Snapshot& expected);

// Whether update mode is on. Read once from the environment, on first use.
bool update_mode();

// A recorded difference: what the file says now, what it should say, and
// where. The updater consumes these.
struct PendingUpdate {
    std::string file;
    unsigned line = 0;
    unsigned column = 0;
    std::string old_value;
    std::string new_value;
};

// Everything `compare` has recorded this run.
const std::vector<PendingUpdate>& pending_updates();

// Drop the recorded updates without applying them.
//
// For tests of compare() itself, which deliberately provoke rewrites they must
// not let happen -- an unclaimed recording would have the flush reporter edit
// the test file at the end of the run. Not part of the normal workflow.
void discard_updates();

// Apply the recorded updates to their source files, and clear them. Returns an
// empty string on success, or a human-readable error. Called by the runner at
// exit in update mode; exposed here for tests.
std::string flush_updates();

}  // namespace snapshot_testing

using snapshot_testing::operator""_snap;

#endif  // SNAPSHOT_SNAPSHOT_H
