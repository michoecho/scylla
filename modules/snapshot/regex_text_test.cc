// Tests for RegexText and the comparison built on it.
//
// Plain assertions rather than snapshots, for the reason compare_test.cc gives:
// this is the code that decides whether a snapshot is rewritten, so writing its
// expectations as snapshots would let a bug in it rewrite them. The tests that
// deliberately provoke a recording discard it again through the same guard.

#include <doctest/doctest.h>

#include <cstddef>
#include <string>

#include "snapshot/regex_text.h"
#include "snapshot/snapshot.h"

namespace {

using snapshot_testing::Comparison;
using snapshot_testing::RegexText;
using snapshot_testing::escape_regex;
using snapshot_testing::operator""_snap;

struct DiscardRecordings {
    ~DiscardRecordings() { snapshot_testing::discard_updates(); }
};

std::size_t recorded() { return snapshot_testing::pending_updates().size(); }

// A record whose middle field is different every run: the shape this whole
// mechanism exists for.
RegexText serialize_reading(const std::string& sensor, const std::string& address) {
    RegexText out;
    out.literal(sensor + " at ");
    out.variable(address, "0x[0-9a-f]+");
    out.literal("\n");
    return out;
}

}  // namespace

TEST_CASE("a literal piece contributes itself to both halves") {
    RegexText value;
    value.literal("total 3\n");
    CHECK(value.text() == "total 3\n");
    CHECK(value.pattern() == escape_regex("total 3\n"));
}

TEST_CASE("escape_regex escapes anything that is not a word character") {
    CHECK(escape_regex("a.b") == R"(a\.b)");
    CHECK(escape_regex("word_1") == "word_1");
}

TEST_CASE("a literal piece is matched literally, metacharacters and all") {
    const DiscardRecordings guard;
    RegexText value;
    value.literal("f(x) = 1.5 [a|b]\n");
    CHECK(snapshot_testing::compare(value, "f(x) = 1.5 [a|b]\n"_snap) == Comparison::Matched);
    // Each of these would have matched had the escape missed one character.
    CHECK(snapshot_testing::compare(value, "f(x) = 1X5 [a|b]\n"_snap) == Comparison::Mismatched);
    CHECK(snapshot_testing::compare(value, "f(x) = 1.5 a\n"_snap) == Comparison::Mismatched);
}

TEST_CASE("a variable piece is grouped, so an alternation stays local") {
    RegexText value;
    value.literal("state ");
    value.variable("on", "on|off");
    value.literal("!\n");
    CHECK(value.pattern() == escape_regex("state ") + "(?:on|off)" + escape_regex("!\n"));

    const DiscardRecordings guard;
    CHECK(snapshot_testing::compare(value, "state off!\n"_snap) == Comparison::Matched);
    // Ungrouped, "state on" alone would have satisfied the left alternative.
    CHECK(snapshot_testing::compare(value, "state on"_snap) == Comparison::Mismatched);
}

TEST_CASE("a recorded sample keeps matching when only its variable field moves") {
    const DiscardRecordings guard;
    const RegexText value = serialize_reading("thermometer", "0x7ffd1a2b");

    // What a previous run wrote into the source, from a different address.
    CHECK(snapshot_testing::compare(value, "thermometer at 0xdeadbeef\n"_snap) ==
          Comparison::Matched);
    CHECK(recorded() == 0);
}

TEST_CASE("a change outside the variable field still fails") {
    const DiscardRecordings guard;
    const RegexText value = serialize_reading("barometer", "0x7ffd1a2b");
    CHECK(snapshot_testing::compare(value, "thermometer at 0xdeadbeef\n"_snap) ==
          Comparison::Mismatched);
}

TEST_CASE("the comparison is a full match, not a search") {
    const DiscardRecordings guard;
    RegexText value;
    value.literal("ab");
    CHECK(snapshot_testing::compare(value, "xxabxx"_snap) == Comparison::Mismatched);
}

TEST_CASE("a mismatch records the full serialization, not the pattern") {
    const DiscardRecordings guard;
    const RegexText value = serialize_reading("hygrometer", "0x1234");
    const Comparison result =
        snapshot_testing::compare(value, "hygrometer at UNSET\n"_snap.update());
    CHECK(result == Comparison::MismatchedAndRecorded);
    REQUIRE(recorded() == 1);
    CHECK(snapshot_testing::pending_updates().front().new_value == "hygrometer at 0x1234\n");
    CHECK(snapshot_testing::pending_updates().front().old_value == "hygrometer at UNSET\n");
}

TEST_CASE("append concatenates both halves at once") {
    RegexText value;
    value.append(serialize_reading("a", "0x1"));
    value.append(serialize_reading("b", "0x2"));
    CHECK(value.text() == "a at 0x1\nb at 0x2\n");
    CHECK(value.pattern() == escape_regex("a at ") + "(?:0x[0-9a-f]+)" + escape_regex("\nb at ") +
                                 "(?:0x[0-9a-f]+)" + escape_regex("\n"));
}

TEST_CASE("a leftover .update() on a matching value is still caught") {
    const DiscardRecordings guard;
    const RegexText value = serialize_reading("thermometer", "0x1");
    CHECK(snapshot_testing::compare(value, "thermometer at 0xabc\n"_snap.update()) ==
          Comparison::StaleUpdateMarker);
    CHECK(recorded() == 0);
}

TEST_CASE("a malformed pattern fails the assertion instead of aborting the case") {
    const DiscardRecordings guard;
    RegexText value;
    value.variable("whatever", "[unterminated");
    CHECK(snapshot_testing::compare(value, "whatever"_snap) == Comparison::Mismatched);
    CHECK(snapshot_testing::render_mismatch(value, "whatever"_snap)
              .starts_with("malformed snapshot pattern at "));
}
