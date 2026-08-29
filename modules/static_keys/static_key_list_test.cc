// What keys the process holds, as a snapshot.
//
// This file has a test executable to itself -- see the `static_key_list` module
// in BUCK. list_static_keys() answers a question about the *process*, so in a
// binary shared with the branch tests the snapshot below would describe their
// keys too, and adding a key to one of those would break an expectation with
// nothing to do with it. Here the listing describes this file and
// plugin/list_plugin.cc, and nothing else can get into it.
//
// The listing is mostly made of fields that cannot be reproduced: the path of a
// loaded object is a buck-out path with a configuration hash in it, its load
// address is whatever ASLR chose, and a line number moves whenever anyone edits
// the file above it. A verbatim snapshot of that fails on the second run.
//
// So this is a *regex* snapshot: the serializers below hand back a RegexText,
// which carries the text and a pattern describing its general form side by
// side. The snapshot in this file was recorded from the text -- real paths,
// real offsets, and a reader can see what a listing actually looks like -- but
// it is compared against the pattern. What that asserts is the part worth
// asserting: which keys exist, what they are called, and the shape of the
// record each one produces. A key appearing, disappearing, or being renamed
// fails this test; a rebuild does not. See snapshot/regex_text.h.

#include <doctest/doctest.h>

#include <cstdint>
#include <format>
#include <set>
#include <string>
#include <vector>

#include "snapshot/check.h"
#include "snapshot/regex_text.h"
#include "static_keys/static_keys.h"

namespace {

using snapshot_testing::RegexText;
using snapshot_testing::check_snapshot;
using snapshot_testing::operator""_snap;

// The keys in the executable: one of each naming form. The first is named after
// its identifier; the second overrides that, which is the case for a key whose
// C++ name and its name to the outside world are legitimately different.
//
// plugin/list_plugin.cc defines the same pair in a shared library, so the
// listing has two objects in it. That is the half a single-object test cannot
// show: the section is per-DSO, and a process-wide answer has to be assembled
// from all of them.
DEFINE_STATIC_KEY_FALSE(list_test_default_name);
DEFINE_STATIC_KEY_TRUE(list_test_renamed, "static_keys.explicitly.named");

extern "C" {
int sk_list_plugin_probe_default_name();
int sk_list_plugin_probe_renamed();
}

// A branch on each, so these are keys in the ordinary sense and not just
// descriptors: the listing has to report a key whether or not anything branches
// on it, but a test that only ever defined unbranched keys would not be showing
// that.
[[gnu::noinline]] int probe_default_name() {
    if (static_branch_unlikely(&list_test_default_name)) return 42;
    return 7;
}

[[gnu::noinline]] int probe_renamed() {
    if (static_branch_likely(&list_test_renamed)) return 42;
    return 7;
}

// --- the serializers ---------------------------------------------------------
//
// Each returns both halves at once: the field as it is in this run, and the
// pattern matching every value the field can take. Written as functions
// returning RegexText so the record serializer can compose them, which is the
// same reason a snapshot helper is a function rather than a macro.

// A loaded object's path. Under Buck2 this is somewhere in buck-out, with a
// configuration hash in the middle of it; installed elsewhere it is anything.
// `[^\n]*` and not `\S*`, so a path with a space in it still matches -- and
// still cannot run past the end of its line into the next field.
RegexText serialize_dso(const std::string& path) {
    RegexText out;
    out.variable(path, R"([^\n]*)");
    return out;
}

// Where the key sits in its object: the address it was linked at, which is the
// value nm prints for it. Not its runtime address, which is wherever ASLR put
// the object today -- but still a link-order artifact, so still a variable
// field. See static_key_info in static_keys.h for what the number is exactly.
RegexText serialize_dso_offset(std::uintptr_t offset) {
    RegexText out;
    out.variable(std::format("{:#x}", offset), "0x[0-9a-f]+");
    return out;
}

// Where the key was defined. The path is stable, the line is not: every key
// below this comment would have to be re-recorded each time a line is added
// above it, which is a snapshot that trains its reader to stop looking.
RegexText serialize_source_location(const std::string& file, int line) {
    RegexText out;
    out.variable(std::format("{}:{}", file, line), R"([^\n:]+:\d+)");
    return out;
}

// One key, four lines. Deliberately one field per line rather than a table:
// aligning columns would pad the literal text by the width of the variable
// field beside it, so the padding -- which is literal, and therefore asserted
// -- would depend on how long today's path happened to be.
RegexText serialize_key(const static_keys::static_key_info& key) {
    RegexText out;
    out.literal("key    ").literal(key.name).literal("\n");
    out.literal("  dso    ").append(serialize_dso(key.dso)).literal("\n");
    out.literal("  offset ").append(serialize_dso_offset(key.offset)).literal("\n");
    out.literal("  at     ")
        .append(serialize_source_location(key.file, key.line))
        .literal("\n");
    return out;
}

RegexText serialize_keys(const std::vector<static_keys::static_key_info>& keys) {
    RegexText out;
    for (const static_keys::static_key_info& key : keys) {
        out.append(serialize_key(key));
    }
    return out;
}

}  // namespace

TEST_CASE("static_keys: a key is named after its identifier by default") {
    const std::vector<static_keys::static_key_info> keys = static_keys::list_static_keys();
    int found = 0;
    for (const static_keys::static_key_info& key : keys) {
        if (key.name == "list_test_default_name") found++;
    }
    CHECK(found == 1);
}

TEST_CASE("static_keys: an explicit name replaces the identifier entirely") {
    const std::vector<static_keys::static_key_info> keys = static_keys::list_static_keys();
    int named = 0;
    int by_identifier = 0;
    for (const static_keys::static_key_info& key : keys) {
        if (key.name == "static_keys.explicitly.named") named++;
        if (key.name == "list_test_renamed") by_identifier++;
    }
    CHECK(named == 1);
    CHECK(by_identifier == 0);
}

TEST_CASE("static_keys: a listed key is a real key, not just a descriptor") {
    // The two this file defines, reached the way anything reaches a key.
    CHECK(probe_default_name() == 7);
    CHECK(probe_renamed() == 42);

    static_keys::static_key_enable(&list_test_default_name.key);
    CHECK(probe_default_name() == 42);
    static_keys::static_key_disable(&list_test_default_name.key);
    CHECK(probe_default_name() == 7);

    // And the two in the library, whose branch sites are in a different object
    // from the loop that patches them.
    CHECK(sk_list_plugin_probe_default_name() == 7);
    CHECK(sk_list_plugin_probe_renamed() == 42);
}

TEST_CASE("static_keys: the listing spans every object that defines a key") {
    std::set<std::string> objects;
    for (const static_keys::static_key_info& key : static_keys::list_static_keys()) {
        objects.insert(key.dso);
    }
    // The executable and list_plugin.so. Each `__static_keys` section is
    // bracketed per object, so one entry here would mean the registry is only
    // ever seeing the caller's own.
    CHECK(objects.size() == 2);
}

// The listing itself: this file's keys and the library's, in one answer.
TEST_CASE("static_keys: the process lists every key it holds") {
    check_snapshot(serialize_keys(static_keys::list_static_keys()), R"snap(
        |key    list_plugin_default_name
        |  dso    /home/michal/projects/cpp_template/buck-out/v2/art/root/5bb6e0e3d998e4f4/modules/static_keys/__static_key_list_test__/./__static_key_list_test__shared_libs_symlink_tree/libmodules_static_keys_list_plugin.so
        |  offset 0x60d0
        |  at     modules/static_keys/plugin/list_plugin.cc:18
        |key    list_test_default_name
        |  dso    /home/michal/projects/cpp_template/buck-out/v2/art/root/5bb6e0e3d998e4f4/modules/static_keys/__static_key_list_test__/static_key_list_test
        |  offset 0x156b90
        |  at     modules/static_keys/static_key_list_test.cc:50
        |key    static_keys.explicitly.named
        |  dso    /home/michal/projects/cpp_template/buck-out/v2/art/root/5bb6e0e3d998e4f4/modules/static_keys/__static_key_list_test__/static_key_list_test
        |  offset 0x156098
        |  at     modules/static_keys/static_key_list_test.cc:51
        |key    static_keys.plugin.named
        |  dso    /home/michal/projects/cpp_template/buck-out/v2/art/root/5bb6e0e3d998e4f4/modules/static_keys/__static_key_list_test__/./__static_key_list_test__shared_libs_symlink_tree/libmodules_static_keys_list_plugin.so
        |  offset 0x6008
        |  at     modules/static_keys/plugin/list_plugin.cc:19
        )snap"_snap);
}
