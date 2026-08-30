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
// Keys work the same way inside a shared library, including one loaded with
// dlopen(). A key that other objects branch on is declared with the _EXPORTED
// forms of the macros below. Executables must be linked with
// -Wl,--export-dynamic; the "Modules" section explains both.
//
// The branch site emits, besides the nop/jmp, a `struct jump_entry` into a
// `__jump_table` section. The linker brackets that section with
// `__start___jump_table` / `__stop___jump_table`, which is how we find every
// branch belonging to a key at runtime.
//
// A key definition likewise emits a `struct static_key_desc` into a
// `__static_keys` section, holding the key's name and the source location that
// defined it. The name defaults to the identifier and can be given explicitly:
//
//     DEFINE_STATIC_KEY_FALSE(tracing);                 // named "tracing"
//     DEFINE_STATIC_KEY_FALSE(tracing, "net.tracing");  // named "net.tracing"
//
// A key defined inside a function -- one per expansion of some macro, rather
// than one per identifier -- uses the DEFINE_STATIC_KEY_*_LOCAL forms beside
// those. modules/tracer's TRACEPOINT() is what they exist for.
//
// static_keys::list_static_keys() walks those sections -- every loaded object's
// -- and answers what keys the process holds, each with its defining object,
// its offset within it, its name, and its source location. See "Key metadata"
// and "The key registry" below.
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
//   - Shared libraries stand in for kernel modules: each DSO carries its own
//     __jump_table and registers it as it loads (see "Modules" below). There is
//     no __init text, so jump entries have no "init" bit, and a branch site
//     reaches its key through a per-DSO slot rather than naming it, which is
//     what lets a key cross an object boundary at all.
//   - `arch_jump_entry_size()` recognises the four encodings we can emit
//     instead of running a general instruction decoder.

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <type_traits>
#include <vector>

#include <dlfcn.h>
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
// The kernel also uses bit 1 to mark a pointer to a static_key_mod list, which
// it needs because a module can branch on a key owned by the core kernel. A key
// here can likewise be branched on from another DSO, so JUMP_TYPE_LINKED means
// the same thing: these bits hold a static_key_mod list rather than a pointer
// to the key's single run of entries. See "Modules". The mask stays two
// bits wide so the encoding matches the kernel's.
struct static_key {
    int enabled;
    unsigned long type;
};

inline constexpr unsigned long JUMP_TYPE_FALSE = 0UL;
inline constexpr unsigned long JUMP_TYPE_TRUE = 1UL;
inline constexpr unsigned long JUMP_TYPE_LINKED = 2UL;
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

// -------------------------------------------------------------------------
// Key metadata
// -------------------------------------------------------------------------

// What a key is called and where it was defined.
//
// Every DEFINE_STATIC_KEY_* below emits one of these into a `__static_keys`
// section beside the key itself. Nothing on any code path reads a descriptor;
// it exists so that a process can be *asked* what keys it holds, by
// list_static_keys() further down, which is otherwise unanswerable -- a key is
// a two-word object with no name in it, and a branch site is an instruction.
//
// The section name is a valid C identifier, so the linker brackets it with
// __start___static_keys/__stop___static_keys exactly as it does __jump_table.
// Unlike __jump_table this one is an ordinary array of ordinary variables:
// nothing here is an `asm` operand, so the pointers are plain pointers filled
// in by the loader with the relocations it already implements, and no
// self-relative encoding is needed.
struct static_key_desc {
    const char* name;  // the explicit name, or the defining identifier
    const char* file;  // __FILE__ where the key was defined
    int line;          // __LINE__ where the key was defined
    static_key* key;
};

// `used` because nothing ever references a descriptor by name -- it is reached
// only by walking the section, which the compiler cannot see -- and `retain`,
// where the compiler has it, so that --gc-sections does not drop it either.
//
// Deliberately not `const`. A const object lands in a read-only section, and
// this one's initialiser holds pointers, so it would need dynamic relocations
// applied to read-only memory -- which is what `-z relro` exists to refuse.
#if defined(__has_attribute)
#  if __has_attribute(retain)
#    define STATIC_KEY_DESC_SECTION_ATTRS __attribute__((used, retain, section("__static_keys")))
#  else
#    define STATIC_KEY_DESC_SECTION_ATTRS __attribute__((used, section("__static_keys")))
#  endif
#else
#  define STATIC_KEY_DESC_SECTION_ATTRS __attribute__((used, section("__static_keys")))
#endif

// Hidden, so each object's descriptors are its own even for a key that is
// exported: what the section describes is what this object defined.
//
// Only namespace-scope descriptors need saying so. A descriptor declared inside
// a function has no linkage at all, and the attribute on one is not merely
// redundant but diagnosed ("visibility attribute ignored"), which is why
// STATIC_KEY_DESC_SECTION_ATTRS above exists separately for the local forms.
#define STATIC_KEY_DESC_ATTRS STATIC_KEY_DESC_SECTION_ATTRS __attribute__((visibility("hidden")))

extern "C" {
extern static_key_desc __start___static_keys[] __attribute__((weak));
extern static_key_desc __stop___static_keys[] __attribute__((weak));
}

// Keys are hidden, which is a hard requirement rather than hygiene.
//
// A branch site stores its key as a link-time constant (see JUMP_TABLE_ENTRY),
// and in position-independent code a symbol's address is only a link-time
// constant if the symbol cannot be preempted by another object. Defining a key
// with default visibility in a shared library does not merely risk the wrong
// key being patched -- it fails to compile, with "impossible constraint in
// 'asm'". Hidden visibility is what makes the address foldable again.
//
// Hidden also means private to the defining DSO. A key other objects branch on
// wants DEFINE_STATIC_KEY_*_EXPORTED below instead, which is default-visibility
// and reaches its branch sites through key_ref.
#define STATIC_KEY_VISIBILITY __attribute__((visibility("hidden")))

// Defining a key: the wrapper object, plus the descriptor that names it.
//
// The two are one macro because they must stay together. A descriptor separated
// from its key is a name for something that may no longer exist, and a key
// defined without one is invisible to list_static_keys() -- which would make
// that listing "the keys someone remembered to register" rather than the keys
// the process has.
//
// Both spellings are the same macro:
//
//     DEFINE_STATIC_KEY_FALSE(tracing);                  // named "tracing"
//     DEFINE_STATIC_KEY_FALSE(tracing, "net.tracing");   // named "net.tracing"
//
// The identifier is the default because it is the name the code already uses,
// and a name that has to be repeated is a name that will eventually disagree
// with the key it labels. An explicit one is for the case where the two are
// legitimately different: a key whose C++ identifier is scoped or abbreviated
// but which is toggled by a name from a config file, a flag, or an RPC.
#define STATIC_KEY_CAT_(a, b) a##b
#define STATIC_KEY_CAT(a, b) STATIC_KEY_CAT_(a, b)

#define STATIC_KEY_IDENT_OF_(first, ...) first
#define STATIC_KEY_IDENT_OF(...) STATIC_KEY_IDENT_OF_(__VA_ARGS__, )

// Argument-count dispatch: with one argument the third token of the expanded
// list is _DEFAULT, with two it is _EXPLICIT.
#define STATIC_KEY_NAME_DEFAULT(ident) #ident
#define STATIC_KEY_NAME_EXPLICIT(ident, name) name
#define STATIC_KEY_PICK_NAME(_1, _2, macro, ...) macro
#define STATIC_KEY_NAME_OF(...)                                                                   \
    STATIC_KEY_PICK_NAME(__VA_ARGS__, STATIC_KEY_NAME_EXPLICIT,                                   \
                         STATIC_KEY_NAME_DEFAULT, )                                               \
    (__VA_ARGS__)

#define STATIC_KEY_DEFINE(vis, wrapper, init, ...)                                                \
    vis ::static_keys::wrapper STATIC_KEY_IDENT_OF(__VA_ARGS__) = {.key = init};                  \
    STATIC_KEY_DESC_ATTRS ::static_keys::static_key_desc                                          \
        STATIC_KEY_CAT(STATIC_KEY_IDENT_OF(__VA_ARGS__), _static_key_desc) = {                    \
            .name = STATIC_KEY_NAME_OF(__VA_ARGS__),                                              \
            .file = __FILE__,                                                                     \
            .line = __LINE__,                                                                     \
            .key = &STATIC_KEY_IDENT_OF(__VA_ARGS__).key}

#define DEFINE_STATIC_KEY_TRUE(...) \
    STATIC_KEY_DEFINE(STATIC_KEY_VISIBILITY, static_key_true, STATIC_KEY_TRUE_INIT, __VA_ARGS__)
#define DEFINE_STATIC_KEY_FALSE(...) \
    STATIC_KEY_DEFINE(STATIC_KEY_VISIBILITY, static_key_false, STATIC_KEY_FALSE_INIT, __VA_ARGS__)

#define DECLARE_STATIC_KEY_TRUE(name) \
    STATIC_KEY_VISIBILITY extern ::static_keys::static_key_true name
#define DECLARE_STATIC_KEY_FALSE(name) \
    STATIC_KEY_VISIBILITY extern ::static_keys::static_key_false name

// Defining a key inside a function.
//
// The key is a block-scope static, so it still has static storage duration and
// a link-time-constant address -- which is what the branch site needs, and the
// only thing it needs. What it does not have is linkage, so it takes no
// visibility attribute (see STATIC_KEY_DESC_SECTION_ATTRS) and cannot be
// declared elsewhere: a local key is reachable only through its descriptor, or
// through whatever table the surrounding macro records it in.
//
// This is for keys a macro defines at its expansion site -- one key per call
// site, named after the call site rather than after any identifier. The
// tracer's TRACEPOINT() is the case it was added for: see
// modules/tracer/include/tracer/tracer.h.
#define STATIC_KEY_DEFINE_LOCAL(wrapper, init, ...)                                               \
    static ::static_keys::wrapper STATIC_KEY_IDENT_OF(__VA_ARGS__) = {.key = init};               \
    static STATIC_KEY_DESC_SECTION_ATTRS ::static_keys::static_key_desc                           \
        STATIC_KEY_CAT(STATIC_KEY_IDENT_OF(__VA_ARGS__), _static_key_desc) = {                    \
            .name = STATIC_KEY_NAME_OF(__VA_ARGS__),                                              \
            .file = __FILE__,                                                                     \
            .line = __LINE__,                                                                     \
            .key = &STATIC_KEY_IDENT_OF(__VA_ARGS__).key}

#define DEFINE_STATIC_KEY_TRUE_LOCAL(...) \
    STATIC_KEY_DEFINE_LOCAL(static_key_true, STATIC_KEY_TRUE_INIT, __VA_ARGS__)
#define DEFINE_STATIC_KEY_FALSE_LOCAL(...) \
    STATIC_KEY_DEFINE_LOCAL(static_key_false, STATIC_KEY_FALSE_INIT, __VA_ARGS__)

// A key other objects may branch on, which is to say one whose address is not
// settled until load time. key_ref absorbs that; nothing else here changes.
// This is the counterpart of EXPORT_SYMBOL_GPL on a kernel key, and the
// executable defining one needs -Wl,--export-dynamic for a dlopen()ed library
// to find it.
#define STATIC_KEY_EXPORTED __attribute__((visibility("default")))

#define DEFINE_STATIC_KEY_TRUE_EXPORTED(...) \
    STATIC_KEY_DEFINE(STATIC_KEY_EXPORTED, static_key_true, STATIC_KEY_TRUE_INIT, __VA_ARGS__)
#define DEFINE_STATIC_KEY_FALSE_EXPORTED(...) \
    STATIC_KEY_DEFINE(STATIC_KEY_EXPORTED, static_key_false, STATIC_KEY_FALSE_INIT, __VA_ARGS__)

#define DECLARE_STATIC_KEY_TRUE_EXPORTED(name) \
    STATIC_KEY_EXPORTED extern ::static_keys::static_key_true name
#define DECLARE_STATIC_KEY_FALSE_EXPORTED(name) \
    STATIC_KEY_EXPORTED extern ::static_keys::static_key_false name

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
//
// `key` is one step removed from the kernel's: it locates a pointer to the
// static_key rather than the static_key itself. See key_ref below for why the
// indirection is what lets a branch site name a key in another DSO.
struct jump_entry {
    std::int32_t code;    // the nop/jmp instruction being patched
    std::int32_t target;  // where the jmp goes when the branch is taken
    long key;             // pointer to the owning key, branch type in bit 0
};

inline std::uintptr_t jump_entry_code(const jump_entry* entry) {
    return reinterpret_cast<std::uintptr_t>(&entry->code) + entry->code;
}

inline std::uintptr_t jump_entry_target(const jump_entry* entry) {
    return reinterpret_cast<std::uintptr_t>(&entry->target) + entry->target;
}

// The slot this entry's key is reached through. Its address is what the entry
// stores; the key itself is one dereference away.
struct jump_table;

// One run of entries belonging to one table, for a key that has runs in more
// than one. The kernel's struct static_key_mod, with the module replaced by the
// table -- `mod == NULL` there means vmlinux, but here the executable has an
// ordinary table like everything else, so the pointer is never null.
struct static_key_mod {
    static_key_mod* next;
    jump_entry* entries;
    jump_table* table;
};

// What a branch site actually points at: one object's view of one key.
//
// `key` comes first and is the pointer an entry dereferences to reach its key.
// `node` is this object's list node for that key, used only once the key turns
// out to have runs in more than one table.
//
// Putting the node here rather than allocating it is what keeps load time free
// of allocation. There is exactly one slot per key per object -- see key_ref --
// which is exactly the number of nodes a key can ever need, and the linker
// places them all. Reaching one is a fixed offset from the entry that named it.
struct key_ref_slot {
    static_key* key;
    static_key_mod node;
};

// A hidden, per-object slot for `Key`. This is what a jump entry stores the
// address of, and it is the whole reason a branch site can name a key that
// lives in a different shared library.
//
// An entry field is a link-time constant, which in PIC means the symbol it
// names must not be preemptible. A key exported for other objects to use is
// preemptible by definition, so naming it in the entry does not compile --
// with either the kernel's self-relative encoding or an absolute one, because
// the "i" constraint rejects the symbol before the encoding matters.
//
// What is not preemptible is this slot: hidden, so every object gets its own,
// and so the entry's offset to it is fixed at link time. The `key` field's
// initialiser is a plain pointer, which the dynamic loader resolves with an
// ordinary data relocation -- R_X86_64_64 for a key in another object,
// R_X86_64_RELATIVE for one at home. Those the loader implements; the
// R_X86_64_PC64 an entry would otherwise need, it does not.
//
// One slot per key per object, not per branch site: every site naming a key
// instantiates the same template, and the linker folds them. __jump_table
// itself stays free of relocations, and nothing on the fast path reads any of
// this -- a branch is a nop or a jmp, and only patching walks the table.
template <static_key* Key>
[[gnu::visibility("hidden")]] inline key_ref_slot key_ref = {Key, {}};

inline key_ref_slot* jump_entry_key_ref(const jump_entry* entry) {
    const long offset = entry->key & ~static_cast<long>(JUMP_TYPE_MASK);
    return reinterpret_cast<key_ref_slot*>(reinterpret_cast<std::uintptr_t>(&entry->key) + offset);
}

inline static_key* jump_entry_key(const jump_entry* entry) {
    return jump_entry_key_ref(entry)->key;
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
// Operand 0 is the key's slot (see key_ref), operand 1 the branch type, which
// is folded into the low bit exactly as the kernel folds it into the key's.
#define JUMP_TABLE_ENTRY                              \
    ".pushsection __jump_table, \"aw?\",@progbits \n\t" \
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
    asm goto(ARCH_STATIC_BRANCH_ASM : : "i"(&key_ref<Key>), "i"(Branch) : : l_yes);
    return false;
l_yes:
    return true;
}

template <static_key* Key, bool Branch>
[[gnu::always_inline]] inline bool arch_static_branch_jump() {
    asm goto(ARCH_STATIC_BRANCH_JUMP_ASM : : "i"(&key_ref<Key>), "i"(Branch) : : l_yes);
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

// Only meaningful while the key is unlinked; once JUMP_TYPE_LINKED is set the
// same bits hold a static_key_mod list instead. See "Modules".
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
        std::uintptr_t key_slot;  // low bits still carry the branch type
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
    //
    // The comparison is on the key each slot points at, not on the slot. One
    // key has one slot per DSO and a table belongs to one DSO, so the two orders
    // agree here -- but grouping a key's entries is the entire point of the
    // sort, and it is the key that has to do the grouping.
    const auto resolve = [](const absolute_entry& e) {
        return reinterpret_cast<const key_ref_slot*>(e.key_slot & ~JUMP_TYPE_MASK)->key;
    };
    const auto less = [&resolve](const absolute_entry& a, const absolute_entry& b) {
        const static_key* const ka = resolve(a);
        const static_key* const kb = resolve(b);
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
            static_cast<long>(decoded[i].key_slot - reinterpret_cast<std::uintptr_t>(&start[i].key));
    }
    std::free(decoded);
}

// -------------------------------------------------------------------------
// Modules
// -------------------------------------------------------------------------
//
// A shared library is this port's kernel module. The linker synthesises a
// __jump_table section and a __start/__stop bracket pair per output object, so
// a process holds one table per DSO plus one for the executable, and each DSO's
// brackets describe only its own entries. The core therefore keeps a registry
// of tables rather than reading the brackets directly, exactly as the kernel
// walks a module's jump_entries rather than the vmlinux table.
//
// A key may be branched on from an object that does not own it, as a module
// may branch on a key in vmlinux. That takes the same static_key_mod list and
// JUMP_TYPE_LINKED state the kernel uses, ported below, because a key's entries
// are then several runs in several tables rather than one run in one.
//
// Getting the reference across the boundary at all is where this diverges from
// the kernel, and the difference is who resolves it. A module reaches the
// kernel as an ET_REL object with its relocations intact and the kernel applies
// them itself -- apply_relocate_add() in arch/x86/kernel/module.c handles
// R_X86_64_PC64 -- so a module's entry can name a key in vmlinux directly and
// have the self-relative offset filled in at load.
//
// A shared library is ET_DYN, and the linker that resolves its references is
// ld.so, which implements a far smaller set of relocation types. R_X86_64_PC64
// is not among them: ld emits the dynamic relocation without complaint and the
// loader then refuses the library outright, with "unexpected reloc type 0x18".
// An entry naming a foreign key directly cannot work, in this encoding or an
// absolute one -- and it does not get that far anyway, since a preemptible
// symbol is not an "i" operand. key_ref is the way around both: the entry names
// a hidden local slot, and the loader fills the slot with an ordinary data
// relocation it does implement. The table itself stays relocation-free.
//
// What is not supported is unloading a library that owns a key another loaded
// object still branches on. The kernel's module reference counting refuses such
// an unload; nothing here can refuse one, so jump_label_del_table() forgets the
// key's runs and leaves the other object's entries pointing at a key that is
// gone.
//
// Two things the registry needs from the build:
//
//   - An executable must be linked with -Wl,--export-dynamic, which is the
//     moral equivalent of the kernel's EXPORT_SYMBOL on this machinery. The
//     registry lives in an inline function's local static, so every DSO carries
//     a weak copy of it, and which one wins is decided per symbol at load time.
//     An executable exports nothing to a dlopen()ed library by default, so
//     without the flag every DSO keeps its own registry holding only its own
//     table. Keys still toggle correctly that way -- each DSO consistently uses
//     its own copy -- but the process no longer has one view of its tables, and
//     jump_label_table_count() below counts only the caller's. The flag is what
//     makes the registry actually process-wide.
//   - Nothing may build this header with -fvisibility=hidden and expect
//     cross-DSO keys to share a registry; jump_label_tables() is pinned to
//     default visibility below so the common case survives it.

// One registered table: a DSO's __jump_table, as the registry sees it.
struct jump_table {
    jump_entry* start;
    jump_entry* stop;
    const void* dso_base;  // which loaded object this table came from
    int refs;              // translation units in this DSO that registered it
    bool initialized;      // sorted, validated, and linked to its keys
    jump_table* next;
};

// Storage for this object's entry in the registry.
//
// Hidden and inline: vague linkage folds the copies within one object, hidden
// stops objects folding onto each other, so there is exactly one of these per
// loaded object -- which is exactly how many tables exist. The registration
// constructor at the end of this header hands its own to
// jump_label_add_table(), which is how a shared core routine gets at per-object
// storage without allocating.
[[gnu::visibility("hidden")]] inline jump_table dso_jump_table = {};

// __module_address(): the loaded object an address belongs to. dladdr() is a
// slow-path call, and this is only ever on the slow path -- registration and
// patching -- never on a branch.
inline const void* dso_base_of(const void* addr) {
    Dl_info info;
    if (::dladdr(addr, &info) == 0) {
        return nullptr;
    }
    return info.dli_fbase;
}

inline bool static_key_linked(const static_key* key) {
    return (key->type & JUMP_TYPE_LINKED) != 0;
}

inline static_key_mod* static_key_mod_of(const static_key* key) {
    return reinterpret_cast<static_key_mod*>(key->type & ~JUMP_TYPE_MASK);
}

inline void static_key_set_mod(static_key* key, static_key_mod* mod) {
    const unsigned long type = key->type & JUMP_TYPE_MASK;
    key->type = reinterpret_cast<unsigned long>(mod) | type;
}

inline void static_key_set_linked(static_key* key) {
    key->type |= JUMP_TYPE_LINKED;
}

inline void static_key_clear_linked(static_key* key) {
    key->type &= ~JUMP_TYPE_LINKED;
}

// The registry head. Process-wide: every DSO's copy of this inline function
// collapses onto one definition at load time, which is what --export-dynamic
// above is for.
[[gnu::visibility("default")]] inline jump_table*& jump_label_tables() {
    static jump_table* head = nullptr;
    return head;
}

inline int jump_label_table_count() {
    int count = 0;
    for (const jump_table* table = jump_label_tables(); table != nullptr; table = table->next) {
        count++;
    }
    return count;
}

// The table an entry belongs to, and so the bound a walk from it must stop at.
inline jump_table* jump_label_table_of(const jump_entry* entry) {
    for (jump_table* table = jump_label_tables(); table != nullptr; table = table->next) {
        if (entry >= table->start && entry < table->stop) {
            return table;
        }
    }
    return nullptr;
}

inline void jump_label_init_table(jump_table* table);

// jump_label_add_module(): record the table in `storage`, then sort it, link
// its keys and patch anything that arrived out of date.
//
// Called once per translation unit, so a DSO built from several files registers
// the same bracket pair repeatedly. Those are counted rather than rejected: the
// matching destructors run one per translation unit too, and the table has to
// outlive all of them.
// Prepend a run to a key's list, converting the key to the linked form first if
// it is not already in it.
inline void static_key_add_mod(static_key* key, jump_entry* entries, jump_table* table) {
    // The node for this run is the one in the key_ref slot its entries already
    // point at: one slot per key per object is exactly one node per run.
    static_key_mod* const node = &jump_entry_key_ref(entries)->node;

    if (!static_key_linked(key)) {
        // Fold the key's existing run, if it has been found yet, into a node of
        // its own so the list is uniform -- the home object's node, reached the
        // same way. A key whose home table has not been initialised has no run
        // to fold; that table adds its own node when its turn comes, which is
        // what makes this independent of load order.
        jump_entry* const existing = static_key_entries(key);
        static_key_set_mod(key, nullptr);
        static_key_set_linked(key);
        if (existing != nullptr) {
            static_key_mod* const home = &jump_entry_key_ref(existing)->node;
            *home = {nullptr, existing, jump_label_table_of(existing)};
            static_key_set_mod(key, home);
        }
    }

    *node = {static_key_mod_of(key), entries, table};
    static_key_set_mod(key, node);
}

inline void jump_label_add_table(jump_entry* start, jump_entry* stop, jump_table* storage) {
    if (start == nullptr || start == stop) {
        return;  // a DSO with no branch sites has no section and no brackets
    }
    for (jump_table* table = jump_label_tables(); table != nullptr; table = table->next) {
        if (table->start == start) {
            table->refs++;
            return;
        }
    }

    *storage = {start, stop, dso_base_of(start), 1, false, jump_label_tables()};
    jump_label_tables() = storage;

    // Initialise straight away rather than at first use. A library can be
    // loaded while one of the keys it branches on is already enabled, and its
    // sites then hold the wrong instruction from the moment it is mapped --
    // before anything calls into static_keys to trigger a lazy pass. The
    // kernel sorts, links and pokes inside jump_label_add_module() for the
    // same reason.
    jump_label_init_table(storage);
}

// jump_label_del_module(). The keys and the branch sites are unmapped with the
// library, so there is nothing to unpatch -- the point is that no later update
// walks a table whose text is gone.
inline void jump_label_del_table(const jump_entry* start) {
    jump_table** prev = &jump_label_tables();
    jump_table* table = *prev;
    for (; table != nullptr; prev = &table->next, table = table->next) {
        if (table->start == start) {
            break;
        }
    }
    if (table == nullptr) {
        return;  // never registered: a DSO with no branch sites
    }
    if (--table->refs > 0) {
        return;
    }

    // Drop this table's run from every key that has one elsewhere, so that a
    // later update does not walk entries whose text has been unmapped.
    static_key* key = nullptr;
    for (jump_entry* iter = table->start; table->initialized && iter < table->stop; iter++) {
        static_key* const iterk = jump_entry_key(iter);
        if (iterk == key) {
            continue;
        }
        key = iterk;

        if (dso_base_of(key) == table->dso_base) {
            // The key goes away with the table, and so do the nodes in this
            // object's slots. Clearing the linked bit matters: the other tables
            // holding runs for this key are torn down after this one -- that is
            // the order at process exit, where the executable's destructors run
            // before its libraries' -- and each would otherwise walk a list
            // threaded through memory that has been unmapped.
            //
            // The kernel does not have to handle this at all: module reference
            // counting refuses to unload a module whose keys are still in use.
            // Nothing here can refuse, so this forgets instead.
            if (static_key_linked(key)) {
                static_key_set_mod(key, nullptr);
                static_key_clear_linked(key);
            }
            continue;
        }
        if (!static_key_linked(key)) {
            continue;
        }

        static_key_mod* node = static_key_mod_of(key);
        static_key_mod* before = nullptr;
        while (node != nullptr && node->table != table) {
            before = node;
            node = node->next;
        }
        if (node == nullptr) {
            continue;
        }
        if (before == nullptr) {
            static_key_set_mod(key, node->next);
        } else {
            before->next = node->next;
        }

        // With a single run left there is nothing to keep a list for: fold it
        // back into the key, exactly as jump_label_del_module() does.
        static_key_mod* const rest = static_key_mod_of(key);
        if (rest != nullptr && rest->next == nullptr) {
            jump_entry* const entries = rest->entries;
            static_key_clear_linked(key);
            static_key_set_entries(key, entries);
        }
    }

    *prev = table->next;
}

// -------------------------------------------------------------------------
// The key registry
// -------------------------------------------------------------------------
//
// The same shape as the jump table registry above, and for the same reason: a
// DSO's `__static_keys` brackets describe that DSO's section and nothing else,
// so a process-wide view has to be a list of ranges, each registered by the
// object it belongs to as that object loads.
//
// A second registry rather than two more fields on jump_table, because the two
// sections do not line up. An object can define a key nothing branches on, and
// jump_label_add_table() declines to register a table with no entries at all --
// so a DSO holding only keys would have no jump_table to hang its descriptors
// off. It can equally branch on keys it does not define, so a jump table is no
// guide to what is described. The ranges are independent; the registries are
// too.

struct static_key_desc_table {
    static_key_desc* start;
    static_key_desc* stop;
    int refs;  // translation units in this DSO that registered it
    static_key_desc_table* next;
};

// Per-object storage, exactly as dso_jump_table is: hidden and inline, so
// vague linkage folds the copies within an object and hidden keeps objects from
// folding onto each other.
[[gnu::visibility("hidden")]] inline static_key_desc_table dso_desc_table = {};

// The registry head, process-wide for the same reason -- and subject to the
// same requirement that an executable be linked with -Wl,--export-dynamic. See
// "Modules" above.
[[gnu::visibility("default")]] inline static_key_desc_table*& static_key_desc_tables() {
    static static_key_desc_table* head = nullptr;
    return head;
}

inline void static_key_add_desc_table(static_key_desc* start, static_key_desc* stop,
                                      static_key_desc_table* storage) {
    if (start == nullptr || start == stop) {
        return;  // a DSO defining no keys has no section and no brackets
    }
    for (static_key_desc_table* table = static_key_desc_tables(); table != nullptr;
         table = table->next) {
        if (table->start == start) {
            table->refs++;  // another translation unit in the same DSO
            return;
        }
    }
    *storage = {start, stop, 1, static_key_desc_tables()};
    static_key_desc_tables() = storage;
}

inline void static_key_del_desc_table(const static_key_desc* start) {
    static_key_desc_table** prev = &static_key_desc_tables();
    for (static_key_desc_table* table = *prev; table != nullptr;
         prev = &table->next, table = table->next) {
        if (table->start != start) {
            continue;
        }
        if (--table->refs > 0) {
            return;
        }
        *prev = table->next;
        return;
    }
}

// One key, described from outside the object that owns it.
//
// `dso` and `offset` locate the key in the process as it is loaded now; `name`
// and `file`/`line` are what the definition said. The two halves answer
// different questions -- "which key is this address" and "which key is this" --
// and a listing that had only one of them would not be much use for either.
struct static_key_info {
    // Path of the loaded object that defines the key, as dladdr reports it.
    std::string dso;

    // The key's address minus that object's load bias (dladdr's dli_fbase) --
    // which is to say the address the object was *linked* at. That is exactly
    // the value `nm` and `readelf -s` print for the symbol, and it lands inside
    // the section `objdump -h` gives the enclosing VMA for, so the number is
    // directly comparable with what the standard tools say about the file on
    // disk.
    //
    // It is a virtual address and not a file offset. The two differ in general
    // -- in a shared object here, `__static_keys` has VMA 0x6080 and file
    // offset 0x5080 -- and a key in .bss has no file offset at all.
    //
    // "Offset" is the right word only because everything this is used with is
    // position-independent, so the first PT_LOAD sits at vaddr 0, the load bias
    // is the mapping base, and "linked at" and "offset into the mapping"
    // coincide. A non-PIE executable linked at 0x400000 has a zero bias, and
    // this would report that absolute address -- still the value nm prints,
    // no longer an offset from anything.
    std::uintptr_t offset;

    std::string name;
    std::string file;
    int line;
};

// Every key defined by every loaded object, ordered by name, then by object,
// then by offset within it.
//
// Sorted rather than left in registry order, which is load order: that puts the
// answer at the mercy of when a library happened to be dlopen()ed, and makes
// the listing of a process that loaded the same objects in a different sequence
// a different listing. Offset rather than address for the same reason -- an
// address is ASLR's answer, an offset is the object's.
//
// By name first, and not by object, because the name is the only field of the
// three that the build cannot move. Ordering on the object path would order the
// listing by where the linker put its outputs, and ordering on the offset would
// reshuffle it whenever the link order within an object changed -- neither of
// which is a change in what keys the process has. Object and offset stay in the
// comparison only to break ties between two objects defining the same name.
//
// dladdr() is what resolves both, and it is the reason this is not something to
// call on a hot path; it is also why a key whose object dladdr() cannot place
// (which should not happen -- the key is in a section of a loaded object)
// reports an empty path and a zero offset rather than a wrong one.
//
// The pair is deliberately the one a reader can act on: `dso` names a file and
// `offset` is the address inside it that nm, readelf and addr2line agree on, so
// a key in a listing can be looked up in the object it came from without
// knowing where this process happened to map it.
inline std::vector<static_key_info> list_static_keys() {
    std::vector<static_key_info> keys;
    for (const static_key_desc_table* table = static_key_desc_tables(); table != nullptr;
         table = table->next) {
        for (const static_key_desc* desc = table->start; desc < table->stop; desc++) {
            static_key_info info = {
                .dso = {},
                .offset = 0,
                .name = desc->name != nullptr ? desc->name : "",
                .file = desc->file != nullptr ? desc->file : "",
                .line = desc->line,
            };
            Dl_info found;
            if (::dladdr(desc->key, &found) != 0) {
                if (found.dli_fname != nullptr) {
                    info.dso = found.dli_fname;
                }
                info.offset = reinterpret_cast<std::uintptr_t>(desc->key) -
                              reinterpret_cast<std::uintptr_t>(found.dli_fbase);
            }
            keys.push_back(std::move(info));
        }
    }

    std::sort(keys.begin(), keys.end(),
              [](const static_key_info& a, const static_key_info& b) {
                  if (a.name != b.name) {
                      return a.name < b.name;
                  }
                  if (a.dso != b.dso) {
                      return a.dso < b.dso;
                  }
                  return a.offset < b.offset;
              });
    return keys;
}

// -------------------------------------------------------------------------
// Core
// -------------------------------------------------------------------------

// __jump_label_update(): walk this key's run of entries and patch each one.
inline void __jump_label_update(const static_key* key, jump_entry* entry, const jump_entry* stop) {
    for (; entry < stop && jump_entry_key(entry) == key; entry++) {
        arch_jump_label_transform(entry, jump_label_type_of(entry));
    }
}

// The second half of jump_label_add_module(): sort the table, point every key
// at its run of entries, and check that each site holds the instruction its
// key's initial state implies.
//
// The kernel additionally rewrites sites to nop here, because with
// CONFIG_HAVE_JUMP_LABEL_HACK every site is assembled as a jmp and objtool nops
// it out later. We emit the nop directly, so nothing needs rewriting and the
// loop only has to verify.
inline void jump_label_init_table(jump_table* table) {
    if (table->initialized) {
        return;
    }
    table->initialized = true;

    jump_label_sort_entries(table->start, table->stop);

    static_key* key = nullptr;
    for (jump_entry* iter = table->start; iter < table->stop; iter++) {
        static_key* const iterk = jump_entry_key(iter);
        if (iterk == key) {
            continue;
        }
        key = iterk;

        // within_module(): a key this object owns takes the cheap path, a
        // single pointer and no allocation. It is also the only case where the
        // site's instruction can be trusted to match the key's initial state,
        // since the key was mapped moments ago and nobody can have touched it.
        const bool owned = dso_base_of(key) == table->dso_base;
        if (owned && !static_key_linked(key)) {
            if (jump_label_type_of(iter) != jump_label_init_type(iter)) {
                static_key_bug("branch site disagrees with its key's initial state",
                               reinterpret_cast<const void*>(jump_entry_code(iter)));
            }
            static_key_set_entries(key, iter);
            continue;
        }

        // Otherwise the key already has, or is about to have, runs in more than
        // one table: either it belongs to another object, or it belongs to this
        // one but a library that loaded earlier already linked it.
        static_key_add_mod(key, iter, table);

        // The key may have been toggled long before this object was loaded, in
        // which case the site arrives holding the wrong instruction. This is
        // the kernel's `do_poke`.
        if (jump_label_type_of(iter) != jump_label_init_type(iter)) {
            __jump_label_update(key, iter, table->stop);
        }
    }
}

// jump_label_init(). Every table is initialised as it registers, so in a normal
// run this finds nothing to do; it stays because it is the kernel's boot-time
// entry point, because it is idempotent, and because it puts a program that
// wants a deterministic point of initialisation in control of when that is.
inline void jump_label_init() {
    for (jump_table* table = jump_label_tables(); table != nullptr; table = table->next) {
        jump_label_init_table(table);
    }
}

// __jump_label_mod_update(): one walk per table the key has entries in.
inline void __jump_label_mod_update(const static_key* key) {
    for (const static_key_mod* mod = static_key_mod_of(key); mod != nullptr; mod = mod->next) {
        __jump_label_update(key, mod->entries, mod->table->stop);
    }
}

inline void jump_label_update(const static_key* key) {
    if (static_key_linked(key)) {
        __jump_label_mod_update(key);
        return;
    }

    jump_entry* const entry = static_key_entries(key);
    if (entry == nullptr) {
        return;  // a key with no branches
    }

    // The walk stops at the end of the *owning* table. Running to some other
    // table's stop would read across a malloc'd gap between two mappings.
    const jump_table* const table = jump_label_table_of(entry);
    if (table == nullptr) {
        static_key_bug("key's entries are in no registered jump table", entry);
    }
    __jump_label_update(key, entry, table->stop);
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

// The registration itself, one object per translation unit.
//
// Anonymous namespace, and that is the whole trick. Every other function here
// is inline, so all DSOs share one copy at runtime -- which is what the
// registry wants, and exactly what the bracket symbols must not do. A copy of
// jump_label_init() in a library binds to whichever definition wins globally,
// and reads that DSO's table forever after. Internal linkage gives each DSO a
// constructor that is genuinely its own, so `__start___jump_table` here is
// resolved against the object being loaded.
//
// init_priority(101) is the earliest a user constructor may ask for, so a
// table is registered before any static initialiser that might flip one of its
// keys. Destructors run in reverse, hence after those same initialisers.
namespace {

struct jump_label_module {
    jump_label_module() {
        jump_label_add_table(__start___jump_table, __stop___jump_table, &dso_jump_table);
        static_key_add_desc_table(__start___static_keys, __stop___static_keys, &dso_desc_table);
    }
    ~jump_label_module() {
        static_key_del_desc_table(__start___static_keys);
        jump_label_del_table(__start___jump_table);
    }
};

[[maybe_unused]] __attribute__((init_priority(101))) const jump_label_module jump_label_module_registration;

}  // namespace

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
