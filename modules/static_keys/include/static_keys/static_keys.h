#pragma once

// Static keys ("jump labels") for userspace x86-64.
//
// This is a port of the Linux kernel's jump label machinery
// (include/linux/jump_label.h, kernel/jump_label.c, arch/x86/{include/asm,
// kernel}/jump_label.{h,c}) to a single-threaded userspace program. It
// optimizes branches whose outcome changes extremely rarely: the branch is
// compiled to an unconditional `jmp` or a 5-byte `nop`, and flipping the key
// rewrites that instruction in place instead of testing a variable.
//
// Usage:
//
//     DEFINE_STATIC_KEY_FALSE(feature);
//
//     int f() {
//         if (static_branch_unlikely(&feature)) return 1;  // compiled as nop
//         return 0;
//     }
//
//     static_keys::static_key_enable(&feature.key);        // nop -> jmp
//
// The branch site emits, besides the nop/jmp, a `struct jump_entry` into a
// `__jump_table` section. The linker brackets that section with
// `__start___jump_table` / `__stop___jump_table`, which is how we find every
// branch belonging to a key at runtime.
//
// Differences from the kernel, all consequences of being a single-threaded
// userspace program:
//
//   - Patching uses mprotect() + memcpy() rather than the kernel's int3-based
//     text_poke_bp() dance, which exists to make the update safe against other
//     CPUs concurrently executing the patched instruction. Nothing here is
//     safe against concurrent execution of a branch being flipped.
//   - The mprotect() syscalls double as the architecturally required
//     serializing event between writing and executing the new instruction.
//   - There is no module support and no __init text, so `struct static_key`
//     has no JUMP_TYPE_LINKED state and jump entries have no "init" bit.
//   - `arch_jump_entry_size()` recognises the four encodings we can emit
//     instead of running a general instruction decoder.

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <type_traits>

#include <sys/mman.h>
#include <unistd.h>

namespace static_keys {

// -------------------------------------------------------------------------
// struct static_key
// -------------------------------------------------------------------------

// `type` holds a pointer to this key's first jump_entry, with the initial
// branch direction stashed in the low bits:
//
//   bit 0 => 1 if the key is initially true, 0 if initially false
//
// The kernel also uses bit 1 to mark a pointer to a static_key_mod list; we
// have no modules, so JUMP_TYPE_LINKED does not exist here. The mask stays
// two bits wide so the encoding matches the kernel's.
struct static_key {
    int enabled;
    unsigned long type;
};

inline constexpr unsigned long JUMP_TYPE_FALSE = 0UL;
inline constexpr unsigned long JUMP_TYPE_TRUE = 1UL;
inline constexpr unsigned long JUMP_TYPE_MASK = 3UL;

// Two type wrappers around static_key, so that the branch macros can tell the
// initial direction at compile time and emit the right instruction.
struct static_key_true {
    static_key key;
};
struct static_key_false {
    static_key key;
};

#define STATIC_KEY_TRUE_INIT \
    { .enabled = 1, .type = ::static_keys::JUMP_TYPE_TRUE }
#define STATIC_KEY_FALSE_INIT \
    { .enabled = 0, .type = ::static_keys::JUMP_TYPE_FALSE }

#define DEFINE_STATIC_KEY_TRUE(name) \
    ::static_keys::static_key_true name = {.key = STATIC_KEY_TRUE_INIT}
#define DEFINE_STATIC_KEY_FALSE(name) \
    ::static_keys::static_key_false name = {.key = STATIC_KEY_FALSE_INIT}

#define DECLARE_STATIC_KEY_TRUE(name) extern ::static_keys::static_key_true name
#define DECLARE_STATIC_KEY_FALSE(name) extern ::static_keys::static_key_false name

enum class jump_label_type {
    nop = 0,
    jmp = 1,
};

[[noreturn]] inline void static_key_bug(const char* what, const void* where) {
    std::fprintf(stderr, "static_keys: fatal: %s at %p\n", what, where);
    std::abort();
}

// -------------------------------------------------------------------------
// struct jump_entry
// -------------------------------------------------------------------------

// Every field is stored relative to its own address, so the table needs no
// relocations at load time and survives being sorted in place. This mirrors
// CONFIG_HAVE_ARCH_JUMP_LABEL_RELATIVE, which x86-64 selects.
struct jump_entry {
    std::int32_t code;    // the nop/jmp instruction being patched
    std::int32_t target;  // where the jmp goes when the branch is taken
    long key;             // owning static_key, with the branch type in bit 0
};

inline std::uintptr_t jump_entry_code(const jump_entry* entry) {
    return reinterpret_cast<std::uintptr_t>(&entry->code) + entry->code;
}

inline std::uintptr_t jump_entry_target(const jump_entry* entry) {
    return reinterpret_cast<std::uintptr_t>(&entry->target) + entry->target;
}

inline static_key* jump_entry_key(const jump_entry* entry) {
    const long offset = entry->key & ~static_cast<long>(JUMP_TYPE_MASK);
    return reinterpret_cast<static_key*>(reinterpret_cast<std::uintptr_t>(&entry->key) + offset);
}

// 1 for static_branch_likely(), 0 for static_branch_unlikely().
inline bool jump_entry_is_branch(const jump_entry* entry) {
    return (entry->key & 1L) != 0;
}

// Bracketing symbols the linker synthesises for a section whose name is a
// valid C identifier. Declared weak so a translation unit that includes this
// header without defining any branch still links.
extern "C" {
extern jump_entry __start___jump_table[] __attribute__((weak));
extern jump_entry __stop___jump_table[] __attribute__((weak));
}

// -------------------------------------------------------------------------
// Architecture layer: x86-64
// -------------------------------------------------------------------------
//
// Everything between here and the next banner is arch-specific and would be
// the contents of an <arch>/jump_label.h in the kernel. A port supplies:
// ARCH_STATIC_BRANCH_ASM / ARCH_STATIC_BRANCH_JUMP_ASM, arch_jump_entry_size()
// and arch_jump_label_transform().

#if !defined(__x86_64__)
#error "static_keys: only x86-64 is implemented"
#endif

// The jump table entry emitted alongside the branch instruction. `1:` is the
// label on that instruction, placed by the ARCH_STATIC_BRANCH_*_ASM macros.
// Operand 0 is the key, operand 1 the branch type, which is folded into the
// key's low bit exactly as the kernel does.
#define JUMP_TABLE_ENTRY                       \
    ".pushsection __jump_table, \"aw\" \n\t"   \
    ".balign 8 \n\t"                           \
    ".long 1b - . \n\t"                        \
    ".long %l[l_yes] - . \n\t"                 \
    ".quad %c0 + %c1 - . \n\t"                 \
    ".popsection \n\t"

// The default-off form: a 5-byte nop that static_key_enable() turns into a
// jmp. BYTES_NOP5 from arch/x86/include/asm/nops.h.
#define ARCH_STATIC_BRANCH_ASM "1: .byte 0x0f,0x1f,0x44,0x00,0x00 \n\t" JUMP_TABLE_ENTRY

// The default-on form: a real jmp that gets nop'ed out. The assembler picks
// jmp rel8 or jmp rel32 depending on the distance; both are handled.
#define ARCH_STATIC_BRANCH_JUMP_ASM "1: jmp %l[l_yes] \n\t" JUMP_TABLE_ENTRY

// `Key` and `Branch` are template parameters rather than function arguments
// because the "i" (immediate) operands must be constants even at -O0, where
// the kernel's __always_inline-plus-constant-argument trick does not hold.
template <static_key* Key, bool Branch>
[[gnu::always_inline]] inline bool arch_static_branch() {
    asm goto(ARCH_STATIC_BRANCH_ASM : : "i"(Key), "i"(Branch) : : l_yes);
    return false;
l_yes:
    return true;
}

template <static_key* Key, bool Branch>
[[gnu::always_inline]] inline bool arch_static_branch_jump() {
    asm goto(ARCH_STATIC_BRANCH_JUMP_ASM : : "i"(Key), "i"(Branch) : : l_yes);
    return false;
l_yes:
    return true;
}

inline constexpr int JMP8_INSN_SIZE = 2;
inline constexpr int JMP8_INSN_OPCODE = 0xeb;
inline constexpr int JMP32_INSN_SIZE = 5;
inline constexpr int JMP32_INSN_OPCODE = 0xe9;

// BYTES_NOP2 / BYTES_NOP5, i.e. x86_nops[2] and x86_nops[5].
inline constexpr unsigned char x86_nop2[JMP8_INSN_SIZE] = {0x66, 0x90};
inline constexpr unsigned char x86_nop5[JMP32_INSN_SIZE] = {0x0f, 0x1f, 0x44, 0x00, 0x00};

// A branch site holds one of exactly four encodings, and their first bytes are
// pairwise distinct, so the size is recoverable without a decoder. The kernel
// runs insn_decode_kernel() here because its branch sites can also be patched
// by alternatives and objtool.
inline int arch_jump_entry_size(const jump_entry* entry) {
    const auto* code = reinterpret_cast<const unsigned char*>(jump_entry_code(entry));
    switch (code[0]) {
        case JMP8_INSN_OPCODE:
        case 0x66:  // x86_nop2
            return JMP8_INSN_SIZE;
        case JMP32_INSN_OPCODE:
        case 0x0f:  // x86_nop5
            return JMP32_INSN_SIZE;
        default:
            static_key_bug("branch site holds an unrecognised instruction", code);
    }
}

// text_gen_insn(): a relative jump from `addr` to `dest`, `size` bytes wide.
inline void arch_gen_jmp(unsigned char* buf, std::uintptr_t addr, std::uintptr_t dest, int size) {
    const long rel = static_cast<long>(dest) - static_cast<long>(addr + size);
    if (size == JMP8_INSN_SIZE) {
        if (rel < -128 || rel > 127) {
            static_key_bug("jmp rel8 target out of range", reinterpret_cast<const void*>(addr));
        }
        buf[0] = JMP8_INSN_OPCODE;
        const auto rel8 = static_cast<std::int8_t>(rel);
        std::memcpy(buf + 1, &rel8, sizeof(rel8));
    } else {
        buf[0] = JMP32_INSN_OPCODE;
        const auto rel32 = static_cast<std::int32_t>(rel);
        std::memcpy(buf + 1, &rel32, sizeof(rel32));
    }
}

// text_poke(): make the page(s) holding `addr` writable, overwrite, restore.
//
// The window is PROT_WRITE *on top of* PROT_EXEC rather than instead of it:
// the page holding a branch site also holds unrelated code, up to and
// including the PLT stub for the memcpy() below, so dropping PROT_EXEC would
// make the poke fault on its own next instruction fetch. The kernel likewise
// keeps its text executable throughout (text_poke_early()).
//
// The mprotect() calls double as the serializing events x86 requires between
// storing an instruction and executing it.
inline void arch_text_poke(void* addr, const void* opcode, std::size_t len) {
    const auto page_size = static_cast<std::uintptr_t>(::sysconf(_SC_PAGESIZE));
    const auto page_mask = ~(page_size - 1);
    const auto first = reinterpret_cast<std::uintptr_t>(addr) & page_mask;
    const auto last = (reinterpret_cast<std::uintptr_t>(addr) + len - 1) & page_mask;
    auto* pages = reinterpret_cast<void*>(first);
    const std::size_t span = last - first + page_size;

    if (::mprotect(pages, span, PROT_READ | PROT_WRITE | PROT_EXEC) != 0) {
        static_key_bug("mprotect(PROT_READ|PROT_WRITE|PROT_EXEC) failed", addr);
    }
    std::memcpy(addr, opcode, len);
    if (::mprotect(pages, span, PROT_READ | PROT_EXEC) != 0) {
        static_key_bug("mprotect(PROT_READ|PROT_EXEC) failed", addr);
    }
}

// __jump_label_patch() + __jump_label_transform(). Refuses to patch a site
// that does not currently hold the instruction the requested transition
// starts from, which catches a mis-sorted table or a corrupted entry.
inline void arch_jump_label_transform(const jump_entry* entry, jump_label_type type) {
    const auto addr = jump_entry_code(entry);
    const auto dest = jump_entry_target(entry);
    const int size = arch_jump_entry_size(entry);

    unsigned char jmp[JMP32_INSN_SIZE];
    arch_gen_jmp(jmp, addr, dest, size);
    const unsigned char* nop = (size == JMP8_INSN_SIZE) ? x86_nop2 : x86_nop5;

    const unsigned char* code = (type == jump_label_type::jmp) ? jmp : nop;
    const unsigned char* expect = (type == jump_label_type::jmp) ? nop : jmp;

    auto* site = reinterpret_cast<void*>(addr);
    if (std::memcmp(site, expect, static_cast<std::size_t>(size)) != 0) {
        static_key_bug("branch site does not hold the expected instruction", site);
    }
    arch_text_poke(site, code, static_cast<std::size_t>(size));
}

// -------------------------------------------------------------------------
// Core
// -------------------------------------------------------------------------

inline int static_key_count(const static_key* key) {
    return key->enabled;
}

inline bool static_key_is_enabled(const static_key* key) {
    return static_key_count(key) > 0;
}

inline jump_entry* static_key_entries(const static_key* key) {
    return reinterpret_cast<jump_entry*>(key->type & ~JUMP_TYPE_MASK);
}

inline bool static_key_type(const static_key* key) {
    return (key->type & JUMP_TYPE_TRUE) != 0;
}

inline void static_key_set_entries(static_key* key, jump_entry* entries) {
    const unsigned long type = key->type & JUMP_TYPE_MASK;
    key->type = reinterpret_cast<unsigned long>(entries) | type;
}

// Which instruction a branch site should hold right now. See the truth table
// in include/linux/jump_label.h: instruction = enabled ^ branch.
inline jump_label_type jump_label_type_of(const jump_entry* entry) {
    const bool enabled = static_key_is_enabled(jump_entry_key(entry));
    const bool branch = jump_entry_is_branch(entry);
    return static_cast<jump_label_type>(enabled ^ branch);
}

// Which instruction the compiler emitted: instruction = type ^ branch.
inline jump_label_type jump_label_init_type(const jump_entry* entry) {
    const bool type = static_key_type(jump_entry_key(entry));
    const bool branch = jump_entry_is_branch(entry);
    return static_cast<jump_label_type>(type ^ branch);
}

// jump_label_sort_entries(). Entries land in __jump_table in whatever order
// the linker produced; sorting by key groups every branch of a key into one
// contiguous run, which is what lets a key store a single pointer.
//
// The kernel sorts in place with a swap function that fixes up the relative
// offsets. We instead decode to absolute addresses, sort, and re-encode into
// the destination slots, which is equivalent and easier to follow.
inline void jump_label_sort_entries(jump_entry* start, jump_entry* stop) {
    struct absolute_entry {
        std::uintptr_t code;
        std::uintptr_t target;
        std::uintptr_t key;  // low bits still carry the branch type
    };

    const auto count = static_cast<std::size_t>(stop - start);
    if (count < 2) {
        return;
    }

    auto* decoded = static_cast<absolute_entry*>(std::malloc(count * sizeof(absolute_entry)));
    if (decoded == nullptr) {
        static_key_bug("out of memory sorting the jump table", start);
    }
    for (std::size_t i = 0; i < count; i++) {
        decoded[i] = {
            jump_entry_code(&start[i]),
            jump_entry_target(&start[i]),
            reinterpret_cast<std::uintptr_t>(&start[i].key) + static_cast<std::uintptr_t>(start[i].key),
        };
    }

    // jump_label_cmp(): by key, then by code. Sorting by code within a key
    // keeps a key's entries in address order, which the kernel's batched
    // patching relies on and which makes the table easier to inspect.
    const auto less = [](const absolute_entry& a, const absolute_entry& b) {
        const std::uintptr_t ka = a.key & ~JUMP_TYPE_MASK;
        const std::uintptr_t kb = b.key & ~JUMP_TYPE_MASK;
        if (ka != kb) {
            return ka < kb;
        }
        return a.code < b.code;
    };
    for (std::size_t i = 1; i < count; i++) {
        const absolute_entry pending = decoded[i];
        std::size_t j = i;
        while (j > 0 && less(pending, decoded[j - 1])) {
            decoded[j] = decoded[j - 1];
            j--;
        }
        decoded[j] = pending;
    }

    for (std::size_t i = 0; i < count; i++) {
        start[i].code =
            static_cast<std::int32_t>(decoded[i].code - reinterpret_cast<std::uintptr_t>(&start[i].code));
        start[i].target =
            static_cast<std::int32_t>(decoded[i].target - reinterpret_cast<std::uintptr_t>(&start[i].target));
        start[i].key =
            static_cast<long>(decoded[i].key - reinterpret_cast<std::uintptr_t>(&start[i].key));
    }
    std::free(decoded);
}

// __jump_label_update(): walk this key's run of entries and patch each one.
inline void __jump_label_update(const static_key* key, jump_entry* entry, const jump_entry* stop) {
    for (; entry < stop && jump_entry_key(entry) == key; entry++) {
        arch_jump_label_transform(entry, jump_label_type_of(entry));
    }
}

inline bool& static_key_initialized() {
    static bool initialized = false;
    return initialized;
}

// jump_label_init(). Sorts the table, points every key at its run of entries,
// and checks that each site holds the instruction its initial state implies.
//
// The kernel additionally rewrites sites to nop here, because with
// CONFIG_HAVE_JUMP_LABEL_HACK every site is assembled as a jmp and objtool
// nops it out later. We emit the nop directly, so nothing needs rewriting and
// the loop only has to verify.
inline void jump_label_init() {
    if (static_key_initialized()) {
        return;
    }
    static_key_initialized() = true;

    jump_entry* const iter_start = __start___jump_table;
    jump_entry* const iter_stop = __stop___jump_table;
    if (iter_start == nullptr || iter_stop == nullptr) {
        return;
    }

    jump_label_sort_entries(iter_start, iter_stop);

    static_key* key = nullptr;
    for (jump_entry* iter = iter_start; iter < iter_stop; iter++) {
        if (jump_label_type_of(iter) != jump_label_init_type(iter)) {
            static_key_bug("branch site disagrees with its key's initial state",
                           reinterpret_cast<const void*>(jump_entry_code(iter)));
        }

        static_key* const iterk = jump_entry_key(iter);
        if (iterk == key) {
            continue;
        }
        key = iterk;
        static_key_set_entries(key, iter);
    }
}

inline void jump_label_update(const static_key* key) {
    jump_entry* const entry = static_key_entries(key);
    if (entry != nullptr) {  // a key with no branches
        __jump_label_update(key, entry, __stop___jump_table);
    }
}

inline void static_key_enable(static_key* key) {
    jump_label_init();
    if (key->enabled > 0) {
        return;
    }
    key->enabled = 1;
    jump_label_update(key);
}

inline void static_key_disable(static_key* key) {
    jump_label_init();
    if (key->enabled != 1) {
        return;
    }
    key->enabled = 0;
    jump_label_update(key);
}

// static_key_slow_inc()/dec(): "make more true" / "make more false". Only the
// 0 -> 1 and 1 -> 0 transitions touch the text.
inline void static_key_slow_inc(static_key* key) {
    jump_label_init();
    if (key->enabled++ == 0) {
        jump_label_update(key);
    }
}

inline void static_key_slow_dec(static_key* key) {
    jump_label_init();
    if (key->enabled == 0) {
        static_key_bug("static_key_slow_dec() underflow", key);
    }
    if (--key->enabled == 0) {
        jump_label_update(key);
    }
}

}  // namespace static_keys

// -------------------------------------------------------------------------
// Branch macros
// -------------------------------------------------------------------------
//
// Combine the initial value (the key's type) with the branch order to pick the
// instruction the compiler should emit, so that the common case falls through
// and the rare case is the one that costs a jump:
//
//   type\branch |  likely (1)   |  unlikely (0)
//   ------------+---------------+---------------
//    true  (1)  |     NOP       |     JMP
//    false (0)  |     JMP       |     NOP
//
// See the full table in include/linux/jump_label.h.

#define STATIC_KEY_IS_TYPE(x, wrapper) \
    std::is_same_v<std::remove_cv_t<std::remove_reference_t<decltype(*(x))>>, \
                   ::static_keys::wrapper>

// Declared but never defined: naming a key that is neither wrapper type picks
// the last branch and fails to link, the same trick the kernel uses.
bool ____wrong_branch_error();

// Wrappers so the comma between the two template arguments stays inside
// parentheses when these are handed to STATIC_KEY_SELECT_BRANCH below.
#define STATIC_KEY_ARCH_BRANCH(x, branch) \
    ::static_keys::arch_static_branch<&(x)->key, branch>()
#define STATIC_KEY_ARCH_BRANCH_JUMP(x, branch) \
    ::static_keys::arch_static_branch_jump<&(x)->key, branch>()

// A statement expression rather than a ternary, because `if constexpr` is what
// guarantees the unused arm is discarded. Both arms contain an `asm goto` that
// would otherwise emit a second, bogus jump table entry for this branch site.
#define STATIC_KEY_SELECT_BRANCH(x, true_expr, false_expr)                  \
    ({                                                                      \
        bool branch_;                                                       \
        if constexpr (STATIC_KEY_IS_TYPE(x, static_key_true)) {             \
            branch_ = (true_expr);                                          \
        } else if constexpr (STATIC_KEY_IS_TYPE(x, static_key_false)) {     \
            branch_ = (false_expr);                                         \
        } else {                                                            \
            branch_ = ____wrong_branch_error();                             \
        }                                                                   \
        branch_;                                                            \
    })

#define static_branch_likely(x)                                             \
    __builtin_expect(STATIC_KEY_SELECT_BRANCH(x,                            \
                                              !STATIC_KEY_ARCH_BRANCH(x, true), \
                                              !STATIC_KEY_ARCH_BRANCH_JUMP(x, true)), \
                     1)

#define static_branch_unlikely(x)                                           \
    __builtin_expect(STATIC_KEY_SELECT_BRANCH(x,                            \
                                              STATIC_KEY_ARCH_BRANCH_JUMP(x, false), \
                                              STATIC_KEY_ARCH_BRANCH(x, false)), \
                     0)

#define static_key_enabled(x) (::static_keys::static_key_is_enabled(&(x)->key))
