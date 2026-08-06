#include <string>

#include <doctest/doctest.h>

#include "snapshot/snapshot.h"

using snapshot_testing::Comparison;
using snapshot_testing::operator""_filesnap;
using namespace std::string_view_literals;

TEST_CASE("file snapshot matches bytes from the repository store") {
  const snapshot_testing::FileSnapshot expected =
      "7d40a849-31d5-4f45-9c09-aa43493fdcd8"_filesnap;
  CHECK(snapshot_testing::compare("file-backed fixture\n", expected) ==
        Comparison::Matched);
}

TEST_CASE(
    "missing and malformed file snapshots fail without recording normally") {
  snapshot_testing::discard_updates();
  const snapshot_testing::FileSnapshot missing(
      snapshot_testing::FileSnapshotLiteral{
          .id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"});
  CHECK(snapshot_testing::compare("value", missing) == Comparison::Mismatched);
  CHECK(snapshot_testing::pending_updates().empty());

  const snapshot_testing::FileSnapshot malformed(
      snapshot_testing::FileSnapshotLiteral{.id = "NOT-A-UUID"});
  CHECK(snapshot_testing::compare("value", malformed) ==
        Comparison::Mismatched);
  CHECK(snapshot_testing::pending_updates().empty());
  snapshot_testing::discard_updates();
}

static snapshot_testing::FileSnapshot
through_helper(const snapshot_testing::FileSnapshot &expected) {
  return expected;
}

TEST_CASE("file snapshot conversion captures direct and helper use sites") {
  const snapshot_testing::FileSnapshot direct =
      "cccccccc-cccc-4ccc-8ccc-cccccccccccc"_filesnap;
  const unsigned direct_line = direct.location.line();
  CHECK(direct_line + 2 == __LINE__);
  const auto helper =
      through_helper("dddddddd-dddd-4ddd-8ddd-dddddddddddd"_filesnap);
  CHECK(helper.location.line() + 1 == __LINE__);
  snapshot_testing::discard_updates();
}

TEST_CASE("a missing file snapshot update records creation") {
  snapshot_testing::discard_updates();
  const snapshot_testing::FileSnapshot expected(
      snapshot_testing::FileSnapshotLiteral{
          .id = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee", .forced = true});
  CHECK(snapshot_testing::compare("bytes\0kept"sv, expected) ==
        Comparison::MismatchedAndRecorded);
  REQUIRE(snapshot_testing::pending_updates().size() == 1);
  const auto &pending = snapshot_testing::pending_updates().front();
  CHECK(pending.kind == snapshot_testing::PendingUpdate::Kind::File);
  CHECK(pending.new_value == std::string("bytes\0kept", 10));
  CHECK_FALSE(pending.existed);
  snapshot_testing::discard_updates();
}

TEST_CASE("duplicate file snapshot ownership reports a mismatch") {
  snapshot_testing::discard_updates();
  constexpr auto literal = snapshot_testing::FileSnapshotLiteral{
      .id = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"};
  const snapshot_testing::FileSnapshot first(literal);
  const snapshot_testing::FileSnapshot second(literal);
  CHECK(snapshot_testing::compare("value", first) == Comparison::Mismatched);
  CHECK(snapshot_testing::compare("value", second) == Comparison::Mismatched);
  CHECK(snapshot_testing::render_mismatch("value", second).find("duplicate") !=
        std::string::npos);
  snapshot_testing::discard_updates();
}
