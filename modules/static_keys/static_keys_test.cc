#include <doctest/doctest.h>

#include <cstdint>

#include "static_keys/static_keys.h"

namespace {

// Four keys, one per cell of the type/branch table in static_keys.h.
DEFINE_STATIC_KEY_FALSE(false_key_unlikely);
DEFINE_STATIC_KEY_FALSE(false_key_likely);
DEFINE_STATIC_KEY_TRUE(true_key_likely);
DEFINE_STATIC_KEY_TRUE(true_key_unlikely);

// noinline so that each branch site stays a single, patchable instruction that
// every call actually re-executes.

[[gnu::noinline]] int probe_false_unlikely() {
    if (static_branch_unlikely(&false_key_unlikely)) {
        return 42;
    }
    return 7;
}

[[gnu::noinline]] int probe_false_likely() {
    if (static_branch_likely(&false_key_likely)) {
        return 42;
    }
    return 7;
}

[[gnu::noinline]] int probe_true_likely() {
    if (static_branch_likely(&true_key_likely)) {
        return 42;
    }
    return 7;
}

[[gnu::noinline]] int probe_true_unlikely() {
    if (static_branch_unlikely(&true_key_unlikely)) {
        return 42;
    }
    return 7;
}

// The instruction currently sitting at `key`'s (single) branch site.
enum class site_insn { nop, jmp };

site_insn insn_at_branch_site(const static_keys::static_key* key) {
    static_keys::jump_label_init();

    const static_keys::jump_entry* entry = static_keys::static_key_entries(key);
    REQUIRE(entry != nullptr);
    REQUIRE(static_keys::jump_entry_key(entry) == key);

    const auto* code = reinterpret_cast<const unsigned char*>(static_keys::jump_entry_code(entry));
    switch (code[0]) {
        case 0x66:
        case 0x0f:
            return site_insn::nop;
        case static_keys::JMP8_INSN_OPCODE:
        case static_keys::JMP32_INSN_OPCODE:
            return site_insn::jmp;
        default:
            FAIL("unrecognised instruction at branch site");
            return site_insn::nop;
    }
}

}  // namespace

TEST_CASE("static_keys: an unlikely branch on a false key toggles") {
    // Initially false, and the branch site is the fall-through nop.
    CHECK(probe_false_unlikely() == 7);
    CHECK(insn_at_branch_site(&false_key_unlikely.key) == site_insn::nop);

    // Enabling rewrites that nop into a jmp to the branch body.
    static_keys::static_key_enable(&false_key_unlikely.key);
    CHECK(insn_at_branch_site(&false_key_unlikely.key) == site_insn::jmp);
    CHECK(probe_false_unlikely() == 42);

    static_keys::static_key_disable(&false_key_unlikely.key);
    CHECK(insn_at_branch_site(&false_key_unlikely.key) == site_insn::nop);
    CHECK(probe_false_unlikely() == 7);
}

TEST_CASE("static_keys: a likely branch on a false key toggles") {
    // A false key under static_branch_likely() starts as a jmp *over* the body.
    CHECK(probe_false_likely() == 7);
    CHECK(insn_at_branch_site(&false_key_likely.key) == site_insn::jmp);

    static_keys::static_key_enable(&false_key_likely.key);
    CHECK(insn_at_branch_site(&false_key_likely.key) == site_insn::nop);
    CHECK(probe_false_likely() == 42);

    static_keys::static_key_disable(&false_key_likely.key);
    CHECK(insn_at_branch_site(&false_key_likely.key) == site_insn::jmp);
    CHECK(probe_false_likely() == 7);
}

TEST_CASE("static_keys: a likely branch on a true key toggles") {
    CHECK(probe_true_likely() == 42);
    CHECK(insn_at_branch_site(&true_key_likely.key) == site_insn::nop);

    static_keys::static_key_disable(&true_key_likely.key);
    CHECK(insn_at_branch_site(&true_key_likely.key) == site_insn::jmp);
    CHECK(probe_true_likely() == 7);

    static_keys::static_key_enable(&true_key_likely.key);
    CHECK(insn_at_branch_site(&true_key_likely.key) == site_insn::nop);
    CHECK(probe_true_likely() == 42);
}

TEST_CASE("static_keys: an unlikely branch on a true key toggles") {
    CHECK(probe_true_unlikely() == 42);
    CHECK(insn_at_branch_site(&true_key_unlikely.key) == site_insn::jmp);

    static_keys::static_key_disable(&true_key_unlikely.key);
    CHECK(insn_at_branch_site(&true_key_unlikely.key) == site_insn::nop);
    CHECK(probe_true_unlikely() == 7);

    static_keys::static_key_enable(&true_key_unlikely.key);
    CHECK(insn_at_branch_site(&true_key_unlikely.key) == site_insn::jmp);
    CHECK(probe_true_unlikely() == 42);
}

TEST_CASE("static_keys: static_key_enabled reflects the key state") {
    CHECK(static_key_enabled(&false_key_unlikely) == false);
    CHECK(static_key_enabled(&true_key_likely) == true);
}

TEST_CASE("static_keys: refcounting only patches on the 0<->1 transitions") {
    CHECK(probe_false_unlikely() == 7);

    static_keys::static_key_slow_inc(&false_key_unlikely.key);
    CHECK(probe_false_unlikely() == 42);

    // Already enabled: this must not try to patch a jmp into a jmp.
    static_keys::static_key_slow_inc(&false_key_unlikely.key);
    CHECK(probe_false_unlikely() == 42);

    static_keys::static_key_slow_dec(&false_key_unlikely.key);
    CHECK(probe_false_unlikely() == 42);

    static_keys::static_key_slow_dec(&false_key_unlikely.key);
    CHECK(probe_false_unlikely() == 7);
}

TEST_CASE("static_keys: the jump table is sorted by key") {
    // Grouping every branch of a key into one contiguous run is what lets a
    // key store a single jump_entry pointer and stop at the first foreign key.
    static_keys::jump_label_init();

    std::uintptr_t last_key = 0;
    for (const auto* entry = static_keys::__start___jump_table;
         entry < static_keys::__stop___jump_table; entry++) {
        const auto key = reinterpret_cast<std::uintptr_t>(static_keys::jump_entry_key(entry));
        CHECK(last_key <= key);
        last_key = key;
    }
}
