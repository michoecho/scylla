#pragma once

// A caller's source location as one word, and a way to read that word back
// outside the process that wrote it.
//
// std::source_location is already one pointer to a compiler-generated constant,
// which is exactly the right size to put in a tracepoint -- and the constant
// behind it is finished data in .rodata, laid down by the compiler with no help
// from the optimiser and no cost at the call site beyond a `lea`. So that
// pointer is what this stores, verbatim: `location` is `__builtin_source_location()`
// and nothing else.
//
//     void traced(int x, srcloc::location loc = {});
//
//     void somewhere() { traced(1); }   // loc names somewhere(), line and column
//
// That keeps the property the whole thing exists for -- a default argument names
// the *caller* -- at every optimisation level, which is the reason for storing
// the address rather than something cleverer. See "why not an index" below.
//
// What the address costs is that it is a fact about one run: turning it back
// into a file and a line needs the object it points into and the offset it sits
// at. modules/tracer carries both -- its metadata stream says which object was
// mapped where -- and its generated decoders read the location out of the object
// file itself. See "resolving a location" in tracer/codegen.h.

#include <bit>
#include <cstdint>
#include <source_location>
#include <string_view>
#include <vector>

namespace srcloc {

// One location as the compiler lays it out: the four fields of a
// std::source_location, which is what std::source_location::__impl is in both
// libstdc++ and libc++ and what __builtin_source_location() points at.
//
// Nothing in the standard promises this layout. What guards it is
// entry::matches_std_source_location(), which compares a location taken here
// against the std::source_location taken beside it, and is asserted by this
// module's tests: a standard library that laid __impl out differently would fail
// them rather than decode to nonsense.
//
// `file` and `function` point at string literals, so an entry is only
// self-describing to code that can dereference them -- this process. A decoder
// elsewhere reads them out of the object file, at the offsets these addresses
// become once the object's load address is subtracted.
struct entry {
    const char* file;
    const char* function;
    std::uint32_t line;
    std::uint32_t column;
};

// --- why not an index --------------------------------------------------------
//
// The obvious alternative -- and what an earlier version of this did -- is to
// put each entry in an ELF section of its own, so that a location becomes an
// index into a per-object table rather than an address in a mapping, and a
// decoder needs no object at all.
//
// It cannot be done without the optimiser. The entry has to be one object per
// call site holding a location only the caller knows, and C++ has no way to
// write that down: a default function argument names the caller but arrives as a
// parameter, and a parameter is never a constant expression, not even in a
// consteval function. The way round it is inline asm -- by the time asm operands
// are resolved the call has been inlined and the location constant-folded, so an
// "i" constraint can name what the constant-expression rules would not -- and
// that needs at least -O1. This repository compiles at -O0, so an index would be
// a mechanism that works in the build nobody debugs and not in the build
// everybody does.
//
// The address needs none of that, because the compiler has already done the
// work. So the address is what is stored, and the section below survives only
// for what it is genuinely good at.

// --- the location table -------------------------------------------------------
//
// A `source_locations` section holding, for each call site, a pointer to its
// location. The linker collects it into an array bracketed by
// `__start_source_locations` / `__stop_source_locations`, so a program can list
// every location captured anywhere in an object -- including call sites this run
// never reached, which is the thing no run-time collection can offer.
//
// It is the inline-asm mechanism above, and so exists only at -O1 and up; at -O0
// SRCLOC_LOCATION_TABLE is 0, no section is emitted, and the table is empty. It
// is a listing, not a decoding mechanism: nothing here needs it, and a location
// captured with it missing is exactly as good.
//
// Two consequences of assembling data from inside a function body. The compiler
// may duplicate an asm block, and it discards the block along with a function it
// discards, so the table is neither exactly one row per call site nor free of
// repeats -- it is the set of locations the object still mentions, which is what
// it claims to be.

#if defined(__OPTIMIZE__) && defined(__x86_64__) && defined(__ELF__)
#define SRCLOC_LOCATION_TABLE 1
#else
#define SRCLOC_LOCATION_TABLE 0
#endif

// The handle the rest of the program passes around: one pointer, nothing to
// copy, and an implicit constructor so that a function can ask for it as a
// default argument and get its caller.
class location {
   public:
    // Capture. Written as `= {}` in a parameter list, or as a bare
    // `srcloc::location here;` to name the line it appears on. The default
    // argument is the mechanism and may not be passed explicitly.
    //
    // always_inline is a correctness requirement rather than a tuning hint: the
    // asm below can only name a constant if this call has been inlined into the
    // caller, and a build where it is not fails rather than quietly recording an
    // entry for the wrong place. (The *location* would still be right -- it is a
    // default argument -- so only the listing is at stake.)
    [[gnu::always_inline]]
    location(std::source_location captured = std::source_location::current())  // NOLINT(*explicit*)
        : entry_(std::bit_cast<const entry*>(captured)) {
#if SRCLOC_LOCATION_TABLE
        // Emits the pointer as *data*, into the section, at compile time.
        // Nothing here executes; the section is writable because the pointer is
        // a relocation, and a read-only section holding one is a text
        // relocation the loader would rather not have.
        asm volatile(
            ".pushsection source_locations,\"aw\",@progbits\n\t"
            ".balign 8\n\t"
            ".quad %c0\n\t"
            ".popsection" ::"i"(entry_));
#endif
    }

    // An explicitly empty location, for a caller that has none to give. Not the
    // constructor above with a null argument: that would put a null row in the
    // table.
    [[nodiscard]] static constexpr location none() noexcept { return location(static_cast<const entry*>(nullptr)); }

    // An entry that is already in hand -- one read out of the table below, or an
    // address a decoder has placed -- wrapped without capturing anything. The
    // constructor above cannot do this: it is a capture, and it would both name
    // the wrong place and try to assemble a table row out of a value that is not
    // a compile-time constant.
    [[nodiscard]] static constexpr location at(const entry* e) noexcept { return location(e); }

    [[nodiscard]] constexpr bool has_value() const noexcept { return entry_ != nullptr; }
    [[nodiscard]] constexpr const entry* get() const noexcept { return entry_; }

    // The address, which is what a trace records. Meaningless on its own -- see
    // the header comment -- and the whole of what is recorded.
    [[nodiscard]] std::uintptr_t address() const noexcept {
        return reinterpret_cast<std::uintptr_t>(entry_);
    }

    [[nodiscard]] std::string_view file() const noexcept {
        return entry_ != nullptr && entry_->file != nullptr ? entry_->file : "";
    }
    [[nodiscard]] std::string_view function() const noexcept {
        return entry_ != nullptr && entry_->function != nullptr ? entry_->function : "";
    }
    [[nodiscard]] std::uint32_t line() const noexcept {
        return entry_ != nullptr ? entry_->line : 0;
    }
    [[nodiscard]] std::uint32_t column() const noexcept {
        return entry_ != nullptr ? entry_->column : 0;
    }

    friend constexpr bool operator==(location, location) noexcept = default;

   private:
    explicit constexpr location(const entry* e) noexcept : entry_(e) {}

    const entry* entry_;
};

static_assert(sizeof(location) == sizeof(void*), "a location is meant to be one word");

// The capture above goes through std::source_location::current() and bit_casts
// what comes out, rather than calling __builtin_source_location() directly.
//
// The builtin is the more honest spelling -- it is a compiler detail, and so is
// everything else here -- and it is what this did first. What it cannot survive
// is a precompiled header. The object it yields is a std::source_location::__impl,
// which libstdc++ declares *private*, and clang checks that access at the call
// site; when <source_location> arrives through a PCH the check fails outright
// ("'__impl' is a private member of 'std::source_location'") in a translation
// unit where the same code compiles fine without one. Every module in this
// repository is compiled with -include-pch, so that is not an edge case here but
// the normal build.
//
// current() is the accessor the standard provides for exactly this object, and
// it is one pointer wide by the same implementation detail that makes entry's
// layout right -- so the bit_cast rests on nothing the field-for-field check
// below does not already rest on.
static_assert(sizeof(std::source_location) == sizeof(void*),
              "a std::source_location is meant to be the one pointer entry describes");

// Whether entry's fields line up with std::source_location's, which is the
// assumption the whole module rests on and the one thing about it the standard
// does not promise.
//
// Both are taken at the same call site, so a mismatch is a difference in layout
// rather than in position. Not consteval: reading through the result of a cast
// from the builtin's void* is not a constant expression.
[[nodiscard]] [[gnu::always_inline]] inline bool matches_std_source_location(
    location taken = {}, std::source_location expected = std::source_location::current()) {
    const entry* e = taken.get();
    return e != nullptr && e->line == expected.line() && e->column == expected.column() &&
           std::string_view(e->file) == expected.file_name() &&
           std::string_view(e->function) == expected.function_name();
}

// Synthesised by the linker around *this object's* `source_locations` section,
// and weak because an object that captures no location -- or that was compiled
// without the optimiser -- has no such section.
extern "C" {
extern const entry* const __start_source_locations[] __attribute__((weak));
extern const entry* const __stop_source_locations[] __attribute__((weak));
}

// --- the location registry ----------------------------------------------------
//
// One table per loaded object, registered as the object is mapped: the shape
// modules/tracer uses for tracepoints, and for the same reason -- the brackets
// above describe the object they were compiled into, so a process-wide view has
// to be a list of ranges. The comments there explain why the head is defined out
// of line and why the per-object storage is hidden; both apply verbatim.

struct location_table {
    const entry* const* start;
    const entry* const* stop;
    int refs;  // translation units in this object that registered it
    location_table* next;
};

[[gnu::visibility("default")]] location_table*& location_tables();

[[gnu::visibility("hidden")]] inline location_table dso_location_table = {};

inline void add_location_table(const entry* const* start, const entry* const* stop,
                               location_table* storage) {
    if (start == nullptr || start == stop) {
        return;  // an object with no table, which at -O0 is every object
    }
    for (location_table* table = location_tables(); table != nullptr; table = table->next) {
        if (table->start == start) {
            table->refs++;  // another translation unit of the same object
            return;
        }
    }
    *storage = {start, stop, 1, location_tables()};
    location_tables() = storage;
}

inline void del_location_table(const entry* const* start) {
    location_table** prev = &location_tables();
    for (location_table* table = *prev; table != nullptr; prev = &table->next, table = table->next) {
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

namespace {

struct location_module {
    location_module() {
        add_location_table(__start_source_locations, __stop_source_locations, &dso_location_table);
    }
    ~location_module() { del_location_table(__start_source_locations); }
};

[[maybe_unused]] __attribute__((init_priority(101)))
const location_module location_module_registration;

}  // namespace

// Every location mentioned by every loaded object, in registry order, with
// repeats -- see "the location table" above. Empty in an unoptimised build,
// where there is no table to walk.
[[nodiscard]] std::vector<location> locations();

}  // namespace srcloc
