#include <string>

#include <doctest/doctest.h>

#include "snapshot/snapshot.h"

using snapshot_testing::Comparison;
using snapshot_testing::operator""_filesnap;
using namespace std::string_view_literals;

constexpr auto file_fixture =
    "7d40a849-31d5-4f45-9c09-aa43493fdcd8|9e47920253497e3a098d9d1ad26e0e57b7424fb9"_filesnap;
static_assert(file_fixture.id == "7d40a849-31d5-4f45-9c09-aa43493fdcd8");
static_assert(file_fixture.hash == "9e47920253497e3a098d9d1ad26e0e57b7424fb9");

TEST_CASE("file snapshot matches bytes from the repository store") {
  const snapshot_testing::FileSnapshot expected = file_fixture;
  CHECK(snapshot_testing::compare("file-backed fixture\n", expected) ==
        Comparison::Matched);
}

TEST_CASE(
    "missing and malformed file snapshots fail without recording normally") {
  snapshot_testing::discard_updates();
  auto missing = snapshot_testing::FileSnapshot(file_fixture);
  missing.id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
  CHECK(snapshot_testing::compare("value", missing) == Comparison::Mismatched);
  CHECK(snapshot_testing::pending_updates().empty());

  auto malformed = snapshot_testing::FileSnapshot(file_fixture);
  malformed.id = "NOT-A-UUID";
  CHECK(snapshot_testing::compare("value", malformed) ==
        Comparison::Mismatched);
  CHECK(snapshot_testing::pending_updates().empty());
  snapshot_testing::discard_updates();
}

TEST_CASE("a matching hash does not need the snapshot store") {
  snapshot_testing::discard_updates();
  auto expected = snapshot_testing::FileSnapshot(file_fixture);
  expected.id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
  expected.hash = "f32b67c7e26342af42efabc674d441dca0a281c5";
  CHECK(snapshot_testing::compare("value", expected) == Comparison::Matched);
  CHECK(snapshot_testing::pending_updates().empty());
  snapshot_testing::discard_updates();
}

TEST_CASE("an update refreshes a stale file snapshot hash") {
  snapshot_testing::discard_updates();
  auto expected = snapshot_testing::FileSnapshot(file_fixture.update());
  expected.hash = "0000000000000000000000000000000000000000";
  CHECK(snapshot_testing::compare("file-backed fixture\n", expected) ==
        Comparison::MismatchedAndRecorded);
  REQUIRE(snapshot_testing::pending_updates().size() == 1);
  const auto &pending = snapshot_testing::pending_updates().front();
  CHECK(pending.new_literal ==
        "7d40a849-31d5-4f45-9c09-aa43493fdcd8|"
        "9e47920253497e3a098d9d1ad26e0e57b7424fb9");
  CHECK(pending.new_value == "file-backed fixture\n");
  snapshot_testing::discard_updates();
}

static snapshot_testing::FileSnapshot
through_helper(const snapshot_testing::FileSnapshot &expected) {
  return expected;
}

TEST_CASE("file snapshot conversion captures direct and helper use sites") {
  const snapshot_testing::FileSnapshot direct =
      "cccccccc-cccc-4ccc-8ccc-cccccccccccc|45f00739b4a152f5971d108752254b1e6e08ca95"_filesnap;
  const unsigned direct_line = direct.location.line();
  CHECK(direct_line + 2 == __LINE__);
  const auto helper =
      through_helper("dddddddd-dddd-4ddd-8ddd-dddddddddddd|45f00739b4a152f5971d108752254b1e6e08ca95"_filesnap);
  CHECK(helper.location.line() + 1 == __LINE__);
  snapshot_testing::discard_updates();
}

TEST_CASE("a missing file snapshot update records creation") {
  snapshot_testing::discard_updates();
  auto expected = snapshot_testing::FileSnapshot(file_fixture.update());
  expected.id = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee";
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
  constexpr auto literal = file_fixture;
  const snapshot_testing::FileSnapshot first(literal);
  const snapshot_testing::FileSnapshot second(literal);
  CHECK(snapshot_testing::compare("value", first) == Comparison::Mismatched);
  CHECK(snapshot_testing::compare("value", second) == Comparison::Mismatched);
  CHECK(snapshot_testing::render_mismatch("value", second).find("duplicate") !=
        std::string::npos);
  snapshot_testing::discard_updates();
}
