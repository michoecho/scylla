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
// Columns below are anchors, not hints. at_literal(n) converts the literal's
// 0-based offset in its line into the 1-based column source_location reports.

#include <doctest/doctest.h>

#include <string>
#include <string_view>
#include <vector>

#include "snapshot/snapshot.h"  // for _snap, asserted against at the bottom
#include "snapshot/updater.h"

namespace {

using snapshot_testing::Update;

// The 1-based column of the opening quote (or R) of a _snap literal.
constexpr unsigned at_literal(unsigned offset_in_line) { return offset_in_line + 1; }

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
    //             check(f(), "old"_snap);
    CHECK(rewrite("check(f(), \"old\"_snap);\n",
                  {{.line = 1, .column = at_literal(11), .old_value = "old", .new_value = "new"}}) ==
          "check(f(), \"new\"_snap);\n");
}

TEST_CASE("keeps a value that occupies a single line inline") {
    // A lone trailing newline is not a line break for layout purposes: nearly
    // every line-oriented value ends with one, and splitting those across two
    // source lines would be noise.
    CHECK(rewrite("x(\"\"_snap);\n", {{.line = 1,
                                           .column = at_literal(2),
                                           .old_value = "",
                                           .new_value = "one line\n"}}) ==
          "x(\"one line\\n\"_snap);\n");
}

TEST_CASE("writes a value spanning lines as a block literal") {
    // One value line per source line, behind a `|` margin, one indentation step
    // in from the line holding the call. The point of the form is that the
    // escapes are gone: what is in the file is what the value holds.
    CHECK(rewrite("x(\"\"_snap);\n",
                  {{.line = 1, .column = at_literal(2), .old_value = "", .new_value = "a\nb\n"}}) ==
          "x(R\"snap(\n"
          "    |a\n"
          "    |b\n"
          "    )snap\"_snap);\n");
}

TEST_CASE("indents a value one step in from the line holding the call") {
    // The layout is relative to the *line*, not to the identifier: the call's
    // line is indented eight, so the value sits at twelve.
    CHECK(rewrite("        x(\"\"_snap);\n",
                  {{.line = 1, .column = at_literal(10), .old_value = "", .new_value = "a\nb\n"}}) ==
          "        x(R\"snap(\n"
          "            |a\n"
          "            |b\n"
          "            )snap\"_snap);\n");
}

TEST_CASE("does not indent a value by how deep in an expression the call sits") {
    // The same line indentation as above, but the call is buried far to the
    // right. Aligning under the identifier would fling the value out with it --
    // and a long enough prefix would push every line past the column limit with
    // nothing the author could do about it. The value lands in the same place.
    CHECK(rewrite("        f(g(h(1), \"\"_snap));\n",
                  {{.line = 1, .column = at_literal(18), .old_value = "", .new_value = "a\nb\n"}}) ==
          "        f(g(h(1), R\"snap(\n"
          "            |a\n"
          "            |b\n"
          "            )snap\"_snap));\n");
}

TEST_CASE("indents the escaped fallback form the same way") {
    // The two spellings are one layout rule, not two.
    CHECK(rewrite("        x(\"\"_snap);\n", {{.line = 1,
                                                   .column = at_literal(10),
                                                   .old_value = "",
                                                   .new_value = "a\tb\nc\n"}}) ==
          "        x(\n"
          "            \"a\\tb\\n\"\n"
          "            \"c\\n\"_snap);\n");
}

TEST_CASE("keeps the closing delimiter inline when the value has no final newline") {
    // A line break before the delimiter would be inside the raw string, so it
    // would come back as a trailing newline the value never had.
    CHECK(rewrite("x(\"\"_snap);\n",
                  {{.line = 1, .column = at_literal(2), .old_value = "", .new_value = "a\nb"}}) ==
          "x(R\"snap(\n"
          "    |a\n"
          "    |b)snap\"_snap);\n");
}

TEST_CASE("preserves leading whitespace in a block literal's lines") {
    // Everything after the `|` is content, which is the whole reason for the
    // margin: the block's own indentation cannot leak into the value, and the
    // value's own indentation cannot be mistaken for it.
    CHECK(rewrite("x(\"\"_snap);\n", {{.line = 1,
                                           .column = at_literal(2),
                                           .old_value = "",
                                           .new_value = "root\n    leaf\n"}}) ==
          "x(R\"snap(\n"
          "    |root\n"
          "    |    leaf\n"
          "    )snap\"_snap);\n");
}

TEST_CASE("reads a block literal back as the value it was written from") {
    // The round trip that the two independent implementations of the strip rule
    // -- here and in snapshot.h -- have to agree on. The old value is what the
    // previous case wrote, and it has to decode to what was written.
    CHECK(rewrite("x(R\"snap(\n"
                  "    |root\n"
                  "    |    leaf\n"
                  "    )snap\"_snap);\n",
                  {{.line = 1,
                    .column = at_literal(2),
                    .old_value = "root\n    leaf\n",
                    .new_value = "z\n"}}) == "x(\"z\\n\"_snap);\n");
}

TEST_CASE("strips only one margin pipe, so a value may begin with one") {
    // `|x` as content is written as `||x` and read back to `|x`: the strip stops
    // eating at the first pipe, so a second one is content.
    const std::string written =
        rewrite("x(\"\"_snap);\n", {{.line = 1,
                                         .column = at_literal(2),
                                         .old_value = "",
                                         .new_value = "|a\n  |b\n"}});
    CHECK(written ==
          "x(R\"snap(\n"
          "    ||a\n"
          "    |  |b\n"
          "    )snap\"_snap);\n");

    // And back again, unchanged.
    CHECK(rewrite(written, {{.line = 1,
                             .column = at_literal(2),
                             .old_value = "|a\n  |b\n",
                             .new_value = "z"}}) == "x(\"z\"_snap);\n");
}

TEST_CASE("falls back to escaped literals for a value a block cannot carry") {
    // A tab or carriage return written raw would be invisible in the file, and
    // an invisible character in an expected value is one nobody can review.
    CHECK(rewrite("x(\"\"_snap);\n", {{.line = 1,
                                           .column = at_literal(2),
                                           .old_value = "",
                                           .new_value = "a\tb\nc\n"}}) ==
          "x(\n"
          "    \"a\\tb\\n\"\n"
          "    \"c\\n\"_snap);\n");
}

TEST_CASE("falls back to escaped literals for a value holding the closing delimiter") {
    // The one structural case: a raw string ends at its delimiter, and the
    // delimiter is fixed, so no choice of margin could rescue this.
    CHECK(rewrite("x(\"\"_snap);\n",
                  {{.line = 1,
                    .column = at_literal(2),
                    .old_value = "",
                    .new_value = "a\nsee )snap\"_snap here\n"}}) ==
          "x(\n"
          "    \"a\\n\"\n"
          "    \"see )snap\\\"_snap here\\n\"_snap);\n");
}

TEST_CASE("refuses an unterminated block literal") {
    CHECK(error_from("x(R\"snap(\n"
                     "    |a\n",
                     {{.line = 1, .column = at_literal(2), .old_value = "a\n", .new_value = "b"}}) ==
          "unterminated block snapshot literal: no )snap\"_snap "
          "(at line 1 column 3)");
}

TEST_CASE("refuses a longer user-defined suffix beginning with _snap") {
    CHECK(error_from("x(R\"snap(\n"
                     "    |a\n"
                     "    )snap\"_snapshot);\n",
                     {{.line = 1, .column = at_literal(2), .old_value = "a\n", .new_value = "b"}}) ==
          "block snapshot literal has an unsupported suffix (at line 1 column 3)");
}

TEST_CASE("applies several updates whose line numbers shift") {
    // The first rewrite grows by three lines, so the second update's recorded
    // line would be wrong if the rewrites were applied top-down. This is the
    // case the bottom-up ordering exists for.
    CHECK(rewrite("a(\"one\"_snap);\n"
                  "b(\"two\"_snap);\n",
                  {
                      {.line = 1,
                       .column = at_literal(2),
                       .old_value = "one",
                       .new_value = "1\n2\n3\n"},
                      {.line = 2, .column = at_literal(2), .old_value = "two", .new_value = "2"},
                  }) ==
          "a(R\"snap(\n"
          "    |1\n"
          "    |2\n"
          "    |3\n"
          "    )snap\"_snap);\n"
          "b(\"2\"_snap);\n");
}

TEST_CASE("distinguishes two snapshots on one line") {
    //             0         1         2
    //             012345678901234567890
    //             p("a"_snap); q("b"_snap);
    CHECK(rewrite("p(\"a\"_snap); q(\"b\"_snap);\n",
                  {
                      {.line = 1, .column = at_literal(2), .old_value = "a", .new_value = "A"},
                      {.line = 1, .column = at_literal(15), .old_value = "b", .new_value = "B"},
                  }) == "p(\"A\"_snap); q(\"B\"_snap);\n");
}

TEST_CASE("distinguishes two snapshots on one line holding the same value") {
    // Nothing tells these apart but their position -- which is all the updater
    // has ever used, and exactly why it does not search by value.
    CHECK(rewrite("p(\"x\"_snap); q(\"x\"_snap);\n",
                  {
                      {.line = 1, .column = at_literal(2), .old_value = "x", .new_value = "first"},
                      {.line = 1, .column = at_literal(15), .old_value = "x", .new_value = "second"},
                  }) == "p(\"first\"_snap); q(\"second\"_snap);\n");
}

TEST_CASE("joins an existing multi-literal value before comparing") {
    // The old value is the concatenation of the literals, so a snapshot that
    // was previously written across several lines still matches.
    CHECK(rewrite("x(\n"
                  "    \"a\\n\"\n"
                  "    \"b\\n\"_snap);\n",
                  {{.line = 2, .column = at_literal(4), .old_value = "a\nb\n", .new_value = "c\n"}}) ==
          "x(\n"
          "    \"c\\n\"_snap);\n");
}

TEST_CASE("reads whitespace and comments between literals") {
    CHECK(rewrite("x(\"a\"  // why\n"
                  "           /* and */ \"b\"_snap);\n",
                  {{.line = 1, .column = at_literal(2), .old_value = "ab", .new_value = "c"}}) ==
          "x(\"c\"_snap);\n");
}

TEST_CASE("preserves text around the snapshot on both sides") {
    CHECK(rewrite("before x(\"a\"_snap) after\n"
                  "next line\n",
                  {{.line = 1, .column = at_literal(9), .old_value = "a", .new_value = "z"}}) ==
          "before x(\"z\"_snap) after\n"
          "next line\n");
}

TEST_CASE("escapes characters that would otherwise break the literal") {
    CHECK(rewrite("x(\"\"_snap);\n", {{.line = 1,
                                           .column = at_literal(2),
                                           .old_value = "",
                                           .new_value = "quote \" back \\ tab \t"}}) ==
          "x(\"quote \\\" back \\\\ tab \\t\"_snap);\n");
}

TEST_CASE("keeps a value that is valid multi-byte UTF-8") {
    CHECK(rewrite("x(\"\"_snap);\n", {{.line = 1,
                                           .column = at_literal(2),
                                           .old_value = "",
                                           .new_value = "héllo → 日本"}}) ==
          "x(\"héllo → 日本\"_snap);\n");
}

TEST_CASE("an empty update set leaves the source untouched") {
    CHECK(rewrite("x(\"a\"_snap);\n", {}) == "x(\"a\"_snap);\n");
}

// --- the location convention -------------------------------------------------

TEST_CASE("accepts the opening character of the literal as its anchor") {
    CHECK(rewrite("x(\"a\"_snap);\n",
                  {{.line = 1, .column = at_literal(2), .old_value = "a", .new_value = "b"}}) ==
          "x(\"b\"_snap);\n");
}

// --- the bails ---------------------------------------------------------------
//
// A tool that edits source files in place gets one chance to be wrong before it
// destroys work, so every ambiguity resolves to "refuse and explain". Nothing
// below is a case the updater tries to recover from.

TEST_CASE("refuses a location that is merely near a snapshot") {
    // Close is not good enough: the location must name the opening character.
    CHECK(error_from("x(\"a\"_snap);\n",
                     {{.line = 1, .column = 9, .old_value = "a", .new_value = "b"}}) ==
          "no snapshot literal at line 1 column 9; the file has changed "
          "since the test ran");
}

TEST_CASE("refuses when the source no longer holds the recorded value") {
    // The staleness guard: the location is well-formed, but the file has been
    // edited since the test ran, so rewriting here could clobber someone's work.
    CHECK(error_from(
              "x(\"actual\"_snap);\n",
              {{.line = 1, .column = at_literal(2), .old_value = "stale", .new_value = "new"}}) ==
          "the snapshot at line 1 column 3 no longer holds the recorded value; "
          "the file has changed since the test ran");
}

TEST_CASE("refuses two updates that resolve to the same snapshot") {
    // Only reachable from a corrupt update set, but the alternative is applying
    // both rewrites to one span and silently keeping whichever landed last.
    CHECK(error_from("x(\"a\"_snap);\n",
                     {
                         {.line = 1, .column = at_literal(2), .old_value = "a", .new_value = "b"},
                         {.line = 1, .column = at_literal(2), .old_value = "a", .new_value = "c"},
                     }) == "two updates resolve to the same snapshot at line 1 column 3");
}

TEST_CASE("refuses a line past the end of the file") {
    CHECK(error_from("x(\"a\"_snap);\n",
                     {{.line = 9, .column = at_literal(2), .old_value = "a", .new_value = "b"}}) ==
          "no such line in source file: line 9");
}

TEST_CASE("refuses a column past the end of its line") {
    CHECK(error_from("x(\"a\"_snap);\n",
                     {{.line = 1, .column = 500, .old_value = "a", .new_value = "b"}}) ==
          "no such column in source file: line 1 column 500");
}

TEST_CASE("refuses a raw string literal") {
    // Legal C++ that this deliberately will not read, rather than risk decoding
    // it wrongly and rewriting the wrong span.
    CHECK(error_from("x(R\"(a)\"_snap);\n",
                     {{.line = 1, .column = at_literal(2), .old_value = "a", .new_value = "b"}}) ==
          "no snapshot literal at line 1 column 3; the file has changed since the test ran");
}

TEST_CASE("refuses an escape it does not decode") {
    // \x is variable-length, so decoding it requires knowing where it stops --
    // exactly the kind of judgement this parser refuses to make.
    CHECK(error_from("x(\"a\\x41\"_snap);\n",
                     {{.line = 1, .column = at_literal(2), .old_value = "aA", .new_value = "b"}}) ==
          "unsupported escape '\\x' inside snapshot literal; snapshot literals "
          "support only \\n \\t \\r \\\" and \\\\ (at line 1 column 3)");
}

TEST_CASE("refuses a string literal running past its line") {
    CHECK(error_from("x(\"a\n"
                     "b\"_snap);\n",
                     {{.line = 1, .column = at_literal(2), .old_value = "a", .new_value = "b"}}) ==
          "unterminated string snapshot literal (at line 1 column 3)");
}

TEST_CASE("refuses an unterminated comment inside the call") {
    CHECK(error_from("x(\"a\" /* forever\n",
                     {{.line = 1, .column = at_literal(2), .old_value = "a", .new_value = "b"}}) ==
          "unterminated comment inside snapshot literal (at line 1 column 3)");
}

TEST_CASE("refuses a source file that is not valid UTF-8") {
    // Offsets into a file that is not what we think it is have no meaning.
    CHECK(error_from("x(\"\xff\"_snap);\n",
                     {{.line = 1, .column = at_literal(2), .old_value = "?", .new_value = "b"}}) ==
          "source file is not valid UTF-8");
}

TEST_CASE("refuses a new value that is not valid UTF-8") {
    // Writing invalid UTF-8 into a source file produces something the compiler
    // may reject and the user's editor may mangle.
    CHECK(error_from("x(\"a\"_snap);\n",
                     {{.line = 1, .column = at_literal(2), .old_value = "a", .new_value = "\xc3"}}) ==
          "new snapshot value at line 1 is not valid UTF-8");
}

// --- the two strip rules -----------------------------------------------------
//
// The margin rule exists twice: once in snapshot.h, evaluated by the compiler
// when a block literal is read, and once in updater.cc, evaluated when the
// updater reads that same text back out of the file. They describe the same
// transformation and neither can be expressed in terms of the other -- one runs
// at compile time on a literal, the other at run time on a string_view -- so
// the risk is that they drift apart and the updater starts believing a value
// the program never held.
//
// These assert the compile-time half directly. The run-time half is asserted by
// the round-trip cases above, and both are pinned to the same expected values.

using snapshot_testing::operator""_snap;

static_assert("  leading space"_snap.value == "  leading space",
              "ordinary literals do not use the block margin convention");
static_assert("a\nb\n"_snap.value == "a\nb\n",
              "ordinary multiline literals retain their decoded value");

static_assert(R"snap(
              |a
              |b
              )snap"_snap.value == "a\nb\n",
              "the ordinary case: one value line per source line");

static_assert(R"snap(
              |a
              |b)snap"_snap.value == "a\nb",
              "a closing delimiter on the last content line means no final newline");

static_assert(R"snap(
              |root
              |    leaf
              )snap"_snap.value == "root\n    leaf\n",
              "everything after the margin is content, including whitespace");

static_assert(R"snap(
              ||a
              |  |b
              )snap"_snap.value == "|a\n  |b\n",
              "only one pipe is stripped, so a value may itself start with one");

TEST_CASE("updates multiple file snapshot values on one line") {
    const std::string source = "x(\"\"_filesnap, \"\"_filesnap);\n";
    const auto result = snapshot_testing::apply_filesnap_updates(
        source,
        {{.line = 1,
          .column = 3,
          .old_value = "",
          .new_value = "11111111-1111-4111-8111-111111111111|"
                       "f32b67c7e26342af42efabc674d441dca0a281c5"},
         {.line = 1,
          .column = 16,
          .old_value = "",
          .new_value = "22222222-2222-4222-8222-222222222222|"
                       "744b7e20b33c2020eb47b0d542b4b556779ce78d"}});
    REQUIRE(result.ok);
    CHECK(result.text ==
          "x(\"11111111-1111-4111-8111-111111111111|"
          "f32b67c7e26342af42efabc674d441dca0a281c5\"_filesnap, "
          "\"22222222-2222-4222-8222-222222222222|"
          "744b7e20b33c2020eb47b0d542b4b556779ce78d\"_filesnap);\n");
}

static_assert(R"snap(
              |
              |b
              )snap"_snap.value == "\nb\n", "an empty value line is a bare margin");
