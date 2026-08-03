// Tests for the updater -- deliberately *not* snapshot tests.
//
// This is the one place in the project where an expect test would be the wrong
// tool. The updater is the code that rewrites expected values, so a snapshot
// test of it is circular: a change that breaks the updater would rewrite the
// very assertions meant to catch the break, and a run in update mode would
// "fix" the tests to match whatever the broken code now produces. Expected
// values here are therefore written by hand, and a failure means someone has to
// look at it.
//
// The subject is a pure function: given source text and a set of diffs, it
// produces either new source text or an error. Both are asserted with plain
// CHECK, and the error cases matter as much as the happy path -- refusing to
// touch a file it cannot understand is most of what this code is for.
//
// Columns below are anchors, not hints, so they are spelled the way a compiler
// would report them: at_name(n) is clang's convention (the first character of
// the identifier, whose 0-based offset in the line is n) and past_name(n) is
// gcc's (the '(' just past it). Going through these rather than bare numbers is
// what keeps the tests readable -- and what makes a test that deliberately uses
// a *wrong* column obviously deliberate.

#include <doctest/doctest.h>

#include <string>
#include <string_view>
#include <vector>

#include "snapshot/updater.h"

namespace {

using snapshot_testing::Update;

// The 1-based column of the 's' of `snapshot`. Clang's anchor.
constexpr unsigned at_name(unsigned offset_in_line) { return offset_in_line + 1; }

// The 1-based column of the '(' just past `snapshot`. Gcc's anchor.
constexpr unsigned past_name(unsigned offset_in_line) {
    return at_name(offset_in_line) + 8;  // strlen("snapshot")
}

// Rewrite `source` and return the resulting text, failing the test if the
// updater refused. For the cases that are *supposed* to succeed.
std::string rewrite(std::string_view source, std::vector<Update> updates) {
    const snapshot_testing::UpdateResult result =
        snapshot_testing::apply_updates(source, std::move(updates));
    REQUIRE_MESSAGE(result.ok, result.error);
    return result.text;
}

// The error from an update that is supposed to fail, and an assertion that it
// did fail. Returned as a string so the caller can assert on the message: a
// bail is only useful if it says which of the many bail conditions fired.
std::string error_from(std::string_view source, std::vector<Update> updates) {
    const snapshot_testing::UpdateResult result =
        snapshot_testing::apply_updates(source, std::move(updates));
    REQUIRE_FALSE_MESSAGE(result.ok, "expected the update to be refused");
    return result.error;
}

}  // namespace

TEST_CASE("rewrites a single-line snapshot in place") {
    //             0         1
    //             01234567890
    //             check(f(), snapshot("old"));
    CHECK(rewrite("check(f(), snapshot(\"old\"));\n",
                  {{.line = 1, .column = at_name(11), .old_value = "old", .new_value = "new"}}) ==
          "check(f(), snapshot(\"new\"));\n");
}

TEST_CASE("keeps a value that occupies a single line inline") {
    // A lone trailing newline is not a line break for layout purposes: nearly
    // every line-oriented value ends with one, and splitting those across two
    // source lines would be noise.
    CHECK(rewrite("x(snapshot(\"\"));\n", {{.line = 1,
                                           .column = at_name(2),
                                           .old_value = "",
                                           .new_value = "one line\n"}}) ==
          "x(snapshot(\"one line\\n\"));\n");
}

TEST_CASE("expands a value spanning lines to one literal per line") {
    // One literal per line of the value, so that a later change shows up as a
    // line-granular diff rather than one enormous changed line. Continuation
    // literals align under the call's opening parenthesis.
    CHECK(rewrite("x(snapshot(\"\"));\n",
                  {{.line = 1, .column = at_name(2), .old_value = "", .new_value = "a\nb\n"}}) ==
          "x(snapshot(\n"
          "           \"a\\n\"\n"
          "           \"b\\n\"));\n");
}

TEST_CASE("applies several updates whose line numbers shift") {
    // The first rewrite grows by three lines, so the second update's recorded
    // line would be wrong if the rewrites were applied top-down. This is the
    // case the bottom-up ordering exists for.
    CHECK(rewrite("a(snapshot(\"one\"));\n"
                  "b(snapshot(\"two\"));\n",
                  {
                      {.line = 1,
                       .column = at_name(2),
                       .old_value = "one",
                       .new_value = "1\n2\n3\n"},
                      {.line = 2, .column = at_name(2), .old_value = "two", .new_value = "2"},
                  }) ==
          "a(snapshot(\n"
          "           \"1\\n\"\n"
          "           \"2\\n\"\n"
          "           \"3\\n\"));\n"
          "b(snapshot(\"2\"));\n");
}

TEST_CASE("distinguishes two snapshots on one line") {
    //             0         1         2
    //             012345678901234567890
    //             p(snapshot("a")); q(snapshot("b"));
    CHECK(rewrite("p(snapshot(\"a\")); q(snapshot(\"b\"));\n",
                  {
                      {.line = 1, .column = at_name(2), .old_value = "a", .new_value = "A"},
                      {.line = 1, .column = at_name(20), .old_value = "b", .new_value = "B"},
                  }) == "p(snapshot(\"A\")); q(snapshot(\"B\"));\n");
}

TEST_CASE("distinguishes two snapshots on one line holding the same value") {
    // Nothing tells these apart but their position -- which is all the updater
    // has ever used, and exactly why it does not search by value.
    CHECK(rewrite("p(snapshot(\"x\")); q(snapshot(\"x\"));\n",
                  {
                      {.line = 1, .column = at_name(2), .old_value = "x", .new_value = "first"},
                      {.line = 1, .column = at_name(20), .old_value = "x", .new_value = "second"},
                  }) == "p(snapshot(\"first\")); q(snapshot(\"second\"));\n");
}

TEST_CASE("joins an existing multi-literal value before comparing") {
    // The old value is the concatenation of the literals, so a snapshot that
    // was previously written across several lines still matches.
    CHECK(rewrite("x(snapshot(\n"
                  "    \"a\\n\"\n"
                  "    \"b\\n\"));\n",
                  {{.line = 1, .column = at_name(2), .old_value = "a\nb\n", .new_value = "c\n"}}) ==
          "x(snapshot(\"c\\n\"));\n");
}

TEST_CASE("reads whitespace and comments between literals") {
    CHECK(rewrite("x(snapshot(\"a\"  // why\n"
                  "           /* and */ \"b\"));\n",
                  {{.line = 1, .column = at_name(2), .old_value = "ab", .new_value = "c"}}) ==
          "x(snapshot(\"c\"));\n");
}

TEST_CASE("allows whitespace between the name and its parenthesis") {
    // Not idiomatic, but legal C++, and the identifier is still where the
    // anchor says it is.
    CHECK(rewrite("x(snapshot (\"a\"));\n",
                  {{.line = 1, .column = at_name(2), .old_value = "a", .new_value = "b"}}) ==
          "x(snapshot (\"b\"));\n");
}

TEST_CASE("preserves text around the snapshot on both sides") {
    CHECK(rewrite("before x(snapshot(\"a\")) after\n"
                  "next line\n",
                  {{.line = 1, .column = at_name(9), .old_value = "a", .new_value = "z"}}) ==
          "before x(snapshot(\"z\")) after\n"
          "next line\n");
}

TEST_CASE("escapes characters that would otherwise break the literal") {
    CHECK(rewrite("x(snapshot(\"\"));\n", {{.line = 1,
                                           .column = at_name(2),
                                           .old_value = "",
                                           .new_value = "quote \" back \\ tab \t"}}) ==
          "x(snapshot(\"quote \\\" back \\\\ tab \\t\"));\n");
}

TEST_CASE("keeps a value that is valid multi-byte UTF-8") {
    CHECK(rewrite("x(snapshot(\"\"));\n", {{.line = 1,
                                           .column = at_name(2),
                                           .old_value = "",
                                           .new_value = "héllo → 日本"}}) ==
          "x(snapshot(\"héllo → 日本\"));\n");
}

TEST_CASE("an empty update set leaves the source untouched") {
    CHECK(rewrite("x(snapshot(\"a\"));\n", {}) == "x(snapshot(\"a\"));\n");
}

// --- the two location conventions --------------------------------------------
//
// std::source_location for a call to an ordinary function reports the call
// site, and the compilers this project builds with pick opposite ends of the
// callee's name: clang the first character, gcc the '(' just past it. Both are
// exact, so the updater accepts either anchor and nothing in between.

TEST_CASE("accepts clang's anchor: the first character of the name") {
    CHECK(rewrite("x(snapshot(\"a\"));\n",
                  {{.line = 1, .column = at_name(2), .old_value = "a", .new_value = "b"}}) ==
          "x(snapshot(\"b\"));\n");
}

TEST_CASE("accepts gcc's anchor: the parenthesis just past the name") {
    CHECK(rewrite("x(snapshot(\"a\"));\n",
                  {{.line = 1, .column = past_name(2), .old_value = "a", .new_value = "b"}}) ==
          "x(snapshot(\"b\"));\n");
}

TEST_CASE("accepts either anchor for a value spanning lines") {
    // Both conventions report line 1 here: the call *begins* there, and a value
    // spilling onto later lines moves neither anchor. That the anchor does not
    // move with the value is the property the whole design rests on.
    const std::string source = "x(snapshot(\n"
                               "    \"a\\n\"\n"
                               "    \"b\\n\"));\n";
    const std::string expected = "x(snapshot(\"c\\n\"));\n";

    CHECK(rewrite(source,
                  {{.line = 1, .column = at_name(2), .old_value = "a\nb\n", .new_value = "c\n"}}) ==
          expected);
    CHECK(rewrite(source, {{.line = 1,
                            .column = past_name(2),
                            .old_value = "a\nb\n",
                            .new_value = "c\n"}}) == expected);
}

// --- the bails ---------------------------------------------------------------
//
// A tool that edits source files in place gets one chance to be wrong before it
// destroys work, so every ambiguity resolves to "refuse and explain". Nothing
// below is a case the updater tries to recover from.

TEST_CASE("refuses a location that is merely near a snapshot") {
    // The identifier occupies columns 3..10; column 9 is inside it but is
    // neither anchor. Close is not good enough: a location no compiler would
    // produce means something is wrong, and guessing which snapshot was meant is
    // how the wrong one gets rewritten.
    CHECK(error_from("x(snapshot(\"a\"));\n",
                     {{.line = 1, .column = 9, .old_value = "a", .new_value = "b"}}) ==
          "no `snapshot` identifier at line 1 column 9; the file has changed "
          "since the test ran");
}

TEST_CASE("refuses a location naming a longer identifier that ends in the name") {
    // `my_snapshot` ends in `snapshot`, so a rule that only looked forward from
    // the reported point would happily accept its tail. The neighbouring
    // characters are checked on both sides for exactly this.
    CHECK(error_from("x(my_snapshot(\"a\"));\n",
                     {{.line = 1, .column = at_name(5), .old_value = "a", .new_value = "b"}}) ==
          "no `snapshot` identifier at line 1 column 6; the file has changed "
          "since the test ran");
}

TEST_CASE("refuses a name that is not followed by a parenthesis") {
    // Something *called* snapshot that is not a call to it. Position said this
    // is the place, the shape says it is not a snapshot, and the two
    // disagreeing is a refusal rather than a search for a better candidate.
    CHECK(error_from("int snapshot = 1;\n",
                     {{.line = 1, .column = at_name(4), .old_value = "", .new_value = "b"}}) ==
          "`snapshot` at line 1 column 5 is not followed by '('");
}

TEST_CASE("refuses when the source no longer holds the recorded value") {
    // The staleness guard: the location is well-formed, but the file has been
    // edited since the test ran, so rewriting here could clobber someone's work.
    CHECK(error_from(
              "x(snapshot(\"actual\"));\n",
              {{.line = 1, .column = at_name(2), .old_value = "stale", .new_value = "new"}}) ==
          "the snapshot at line 1 column 3 no longer holds the recorded value; "
          "the file has changed since the test ran");
}

TEST_CASE("refuses two updates that resolve to the same snapshot") {
    // Only reachable from a corrupt update set, but the alternative is applying
    // both rewrites to one span and silently keeping whichever landed last.
    CHECK(error_from("x(snapshot(\"a\"));\n",
                     {
                         {.line = 1, .column = at_name(2), .old_value = "a", .new_value = "b"},
                         {.line = 1, .column = past_name(2), .old_value = "a", .new_value = "c"},
                     }) == "two updates resolve to the same snapshot at line 1 column 11");
}

TEST_CASE("refuses a line past the end of the file") {
    CHECK(error_from("x(snapshot(\"a\"));\n",
                     {{.line = 9, .column = at_name(2), .old_value = "a", .new_value = "b"}}) ==
          "no such line in source file: line 9");
}

TEST_CASE("refuses a column past the end of its line") {
    CHECK(error_from("x(snapshot(\"a\"));\n",
                     {{.line = 1, .column = 500, .old_value = "a", .new_value = "b"}}) ==
          "no such column in source file: line 1 column 500");
}

TEST_CASE("refuses a raw string literal") {
    // Legal C++ that this deliberately will not read, rather than risk decoding
    // it wrongly and rewriting the wrong span.
    CHECK(error_from("x(snapshot(R\"(a)\"));\n",
                     {{.line = 1, .column = at_name(2), .old_value = "a", .new_value = "b"}}) ==
          "expected a string literal or ')' inside snapshot(...), found 'R' "
          "(at line 1 column 3)");
}

TEST_CASE("refuses an escape it does not decode") {
    // \x is variable-length, so decoding it requires knowing where it stops --
    // exactly the kind of judgement this parser refuses to make.
    CHECK(error_from("x(snapshot(\"a\\x41\"));\n",
                     {{.line = 1, .column = at_name(2), .old_value = "aA", .new_value = "b"}}) ==
          "unsupported escape '\\x' inside snapshot(...); snapshot literals "
          "support only \\n \\t \\r \\\" and \\\\ (at line 1 column 3)");
}

TEST_CASE("refuses a string literal running past its line") {
    CHECK(error_from("x(snapshot(\"a\n"
                     "b\"));\n",
                     {{.line = 1, .column = at_name(2), .old_value = "a", .new_value = "b"}}) ==
          "unterminated string literal inside snapshot(...) (at line 1 column 3)");
}

TEST_CASE("refuses an unterminated comment inside the call") {
    CHECK(error_from("x(snapshot(\"a\" /* forever\n",
                     {{.line = 1, .column = at_name(2), .old_value = "a", .new_value = "b"}}) ==
          "unterminated comment inside snapshot(...) (at line 1 column 3)");
}

TEST_CASE("refuses a source file that is not valid UTF-8") {
    // Offsets into a file that is not what we think it is have no meaning.
    CHECK(error_from("x(snapshot(\"\xff\"));\n",
                     {{.line = 1, .column = at_name(2), .old_value = "?", .new_value = "b"}}) ==
          "source file is not valid UTF-8");
}

TEST_CASE("refuses a new value that is not valid UTF-8") {
    // Writing invalid UTF-8 into a source file produces something the compiler
    // may reject and the user's editor may mangle.
    CHECK(error_from("x(snapshot(\"a\"));\n",
                     {{.line = 1, .column = at_name(2), .old_value = "a", .new_value = "\xc3"}}) ==
          "new snapshot value at line 1 is not valid UTF-8");
}
