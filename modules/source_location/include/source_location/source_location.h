#pragma once

// A caller's source location: one word at the call site, decodable without the
// binary.
//
// std::source_location is already one pointer to a compiler-generated constant,
// which is exactly the right size to put in a tracepoint. What it is not is
// *decodable*: the pointer names an anonymous piece of .rodata, and turning it
// back into a file and a line needs the executable, its libraries, and the same
// load addresses -- none of which a trace has.
//
// So this does to source locations what modules/tracer does to tracepoints: the
// description goes in an ELF section of its own, `source_locations`, which the
// linker collects into an array bracketed by `__start_source_locations` /
// `__stop_source_locations`. A location is still one pointer, but it points
// *into a table*, and `pointer - table start` is an index that means the same
// thing in every run of the same object -- which is what a decoder generated
// once can act on. See "the location registry" below.
//
// The point of the exercise is that this keeps std::source_location's defining
// property: a default argument captures the *caller*.
//
//     void traced(int x, srcloc::location loc = {});
//
//     void somewhere() { traced(1); }   // loc names somewhere(), line and column
//
// In an optimised build that costs one `lea` and nothing else, and the entry is
// finished data in the object file. See "getting the entry into the section",
// which is where all the difficulty is.

#include <cstdint>
#include <source_location>
#include <string_view>
#include <vector>

namespace srcloc {

// One location, as it sits in the section: the four fields of a
// std::source_location, flattened.
//
// `file` and `function` point at string literals, so an entry is only
// self-describing to code that can dereference them -- this process, or a
// generator running inside it, exactly as tracer's code generator does. A
// decoder built elsewhere reads the *index*, not the pointers.
struct entry {
    const char* file;
    const char* function;
    std::uint32_t line;
    std::uint32_t column;
};

// --- getting the entry into the section ---------------------------------------
//
// The entry has to be one object per call site, in a named section, holding a
// location only the *caller* knows. C++ has no way to write that down:
//
//   - A default *function* argument -- std::source_location::current() -- is
//     evaluated at the call site and does name the caller, but it arrives as a
//     value in a parameter, and a function parameter is never a constant
//     expression. Not even in a consteval function: "function parameter 'l' with
//     unknown value cannot be used in a constant expression".
//   - A default *template* argument is instantiated per use, so it could name
//     per-call-site storage, but current() there reports the location where it
//     is *written*, not the caller -- the standard only promises the caller for
//     default function arguments and default member initialisers.
//   - std::source_location cannot be a template argument at all (its pointer
//     member is private, so the type is not structural), and neither can the
//     compiler's underlying constant (it is an unnamed label, so there is no
//     declaration for a pointer template argument to refer to).
//
// What does work is going under the language. By the time inline asm operands
// are resolved, the call has been inlined and the location constant-folded, so
// an "i" constraint can name what the constant-expression rules would not: the
// asm below emits the entry as *data*, into the section, at compile time. This
// is how the Linux kernel builds its `__jump_table` and its BUG table.
//
// Two consequences worth knowing:
//
//   - It needs the optimiser. At -O0 -- which is how this repository builds --
//     nothing is inlined or folded and the constraint cannot be satisfied
//     ("impossible constraint in 'asm'"), so there is a second implementation
//     below for that case. It fills its entries at run time and is therefore the
//     slower one, which is the right way round: the cost lands in the build that
//     has already given up on cost.
//   - The compiler is free to duplicate an asm block, so one call site
//     occasionally yields two identical entries. Nothing decodes wrongly as a
//     result -- each copy is complete, and every pointer handed out points at a
//     complete one -- but the table is not exactly one row per call site.

#if defined(__OPTIMIZE__) && defined(__x86_64__) && defined(__ELF__)
#define SRCLOC_COMPILE_TIME_CAPTURE 1
#else
#define SRCLOC_COMPILE_TIME_CAPTURE 0
#endif

namespace detail {
namespace {

// Storage for the run-time path, one entry per call site, named by `Tag` -- the
// closure type of the lambda in the constructor's default template argument,
// which is distinct for each call site and for each instantiation of a caller
// that is itself a template. (That the *identity* of a call site can be carried
// this way, even though its *location* cannot, is what makes the fallback
// possible at all.)
//
// The anonymous namespace is load-bearing, and modules/tracer needs the same
// fix: an explicit section attribute defeats the section groups that would fold
// vague-linkage duplicates, so two translation units instantiating the same
// holder -- which is what a call in an inline function or a template is -- would
// otherwise be a multiple-definition error at link time. Internal linkage makes
// them two entries instead, one per translation unit, which is exactly what
// happens to a tracepoint written in a shared header.
//
// The explicit alignment matters as much as the section does: gcc aligns statics
// of 16 bytes or more to 16 by default, which would leave an 8-byte hole after
// each 24-byte entry and stop the section from being an array at all.
template <auto Tag>
struct holder {
    static entry value;
};
template <auto Tag>
entry holder<Tag>::value __attribute__((section("source_locations"), used,
                                        aligned(alignof(entry)))){};

}  // namespace
}  // namespace detail

// The handle the rest of the program passes around: one pointer, nothing to
// copy, and an implicit constructor so that a function can ask for it as a
// default argument and get its caller.
class location {
   public:
    // Capture. Written as `= {}` in a parameter list, or as a bare
    // `srcloc::location here;` to name the line it appears on.
    //
    // Both defaults are part of the mechanism; neither may be passed explicitly.
    // always_inline is not an optimisation here but a correctness requirement:
    // the asm below can only see a constant if this call has been inlined into
    // the caller, and if it somehow is not, the build fails rather than quietly
    // recording the wrong place.
    template <auto Tag = [] {}>
    [[gnu::always_inline]]
    location(std::source_location loc = std::source_location::current()) {  // NOLINT(*explicit*)
#if SRCLOC_COMPILE_TIME_CAPTURE
        // The entry is assembled as data, at compile time, and `entry_` is the
        // address of the label in front of it. Nothing here executes.
        const entry* captured = nullptr;
        asm(".pushsection source_locations,\"aw\",@progbits\n\t"
            ".balign %c5\n"
            "771:\t.quad %c1\n\t"
            "\t.quad %c2\n\t"
            "\t.long %c3\n\t"
            "\t.long %c4\n\t"
            ".popsection\n\t"
            "leaq 771b(%%rip), %0"
            : "=r"(captured)
            : "i"(loc.file_name()), "i"(loc.function_name()), "i"(loc.line()),
              "i"(loc.column()), "i"(alignof(entry)));
        entry_ = captured;
#else
        entry* slot = &detail::holder<Tag>::value;
        // The entry cannot be initialised at compile time here -- `loc` is a
        // parameter -- so it is filled the first time this call site runs. A
        // local static rather than a flag of our own: initialisation of one is
        // exactly "at most once, and safely if several threads arrive at once",
        // which is the whole of what is wanted.
        [[maybe_unused]] static const bool filled = [slot, loc] {
            *slot = {loc.file_name(), loc.function_name(), loc.line(), loc.column()};
            return true;
        }();
        entry_ = slot;
#endif
    }

    // An explicitly empty location, for a caller that has none to give.
    [[nodiscard]] static constexpr location none() noexcept { return location(nullptr); }

    [[nodiscard]] constexpr bool has_value() const noexcept { return entry_ != nullptr; }
    [[nodiscard]] constexpr const entry* get() const noexcept { return entry_; }

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

// Synthesised by the linker around *this object's* `source_locations` section,
// and weak because an object that captures no location has no such section. See
// the same pair, for the same reason, in modules/tracer.
extern "C" {
extern entry __start_source_locations[] __attribute__((weak));
extern entry __stop_source_locations[] __attribute__((weak));
}

// --- the location registry ----------------------------------------------------
//
// One table per loaded object, registered as the object is mapped: the shape
// modules/tracer uses for tracepoints, and for the same reason -- the brackets
// above describe the object they were compiled into, so a process-wide view has
// to be a list of ranges. The comments there explain why the head is defined out
// of line and why the per-object storage is hidden; both apply verbatim.

struct location_table {
    const entry* start;
    const entry* stop;
    int refs;  // translation units in this object that registered it
    location_table* next;
};

[[gnu::visibility("default")]] location_table*& location_tables();

[[gnu::visibility("hidden")]] inline location_table dso_location_table = {};

inline void add_location_table(const entry* start, const entry* stop, location_table* storage) {
    if (start == nullptr || start == stop) {
        return;  // an object that captures no location has no section
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

inline void del_location_table(const entry* start) {
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

// Every location of every loaded object, in registry order. In an optimised
// build every entry is complete, including the call sites this run never
// reached; in an unoptimised one an unreached call site is still zeroed. Either
// way this is what a generator walks to emit a decoder.
[[nodiscard]] std::vector<const entry*> locations();

// The table `loc` belongs to, and its index in it, or {nullptr, 0} if the
// pointer is in no registered table -- which is what a location from an object
// that has since been unloaded looks like.
struct location_index {
    const location_table* table;
    std::size_t index;
};
[[nodiscard]] location_index index_of(location loc);

}  // namespace srcloc
