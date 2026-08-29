#pragma once

// Binary tracepoints: a logging hot path that formats nothing.
//
// A TRACEPOINT() records a *pointer to its own static description* plus a
// timestamp plus the raw bytes of its arguments, into a thread-local ring of
// buffers. No formatting, no allocation, no string copying -- neither the
// tracepoint's name nor its parameters' names ever reach the buffer. What lands
// there is roughly a store, a timestamp read, and a memcpy per argument.
//
// Arguments are *named*: a tracepoint is written as
//
//     TRACEPOINT(event_level::debug, "accepted_connection", "conn", i, "peer", addr);
//
// and the names live in the description, alongside the wire types, as a single
// `conn:u32,peer:str` signature. That is what makes a decoded trace a struct
// with fields rather than a line of text.
//
// A tracepoint that is switched off costs less than that: each one carries a
// static key of its own, named after the tracepoint and disabled at startup, so
// an untraced call site is a five-byte nop with the recording code laid out
// elsewhere. tracer::set_tracepoint_enabled() turns one on by name. See the
// TRACEPOINT() macro at the bottom of this header, and modules/static_keys.
//
// The description lives in a dedicated ELF section, `tracepoints`, so the
// linker collects every tracepoint in the binary into one array bracketed by
// `__start_tracepoints` / `__stop_tracepoints`. A record identifies its
// tracepoint by *index* into that array, which is what makes a trace decodable
// by something other than the process that wrote it.
//
// Decoding is the other half, and it is not in this header: tracer/codegen.h
// walks that same section and emits the C++ source of a decoder specialised to
// this binary's tracepoints -- one struct per tracepoint, with the parameter
// names as members. See modules/tracer/BUCK for how the two halves are wired
// into a build.
//
// Derived from the Seastar tracer patch in references/tracer.patch, with the
// bugs noted there fixed and the argument-list macro machinery replaced.

#include <array>
#include <cassert>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <list>
#include <span>
#include <string_view>
#include <type_traits>
#include <vector>

#include "static_keys/static_keys.h"

namespace tracer {

inline std::uint64_t rdtsc() noexcept {
    std::uint64_t lo = 0;
    std::uint64_t hi = 0;
    asm volatile("rdtsc" : "=a"(lo), "=d"(hi));
    return (hi << 32) | lo;
}

// The timestamp source, as a macro so that overriding it costs nothing at
// runtime. Define it before including this header to substitute another clock;
// modules/tracer/trace_producer.cc uses a counter so that the traces it emits
// -- and therefore the snapshot taken of the decoded output -- are reproducible.
#ifndef TRACER_TIMESTAMP
#define TRACER_TIMESTAMP() ::tracer::rdtsc()
#endif

// Which ring a tracepoint is recorded into. Separate rings so that a flood of
// debug events cannot evict the sparse, important ones.
enum class event_level : std::size_t {
    info,
    debug,
    count,
};

// A ring of fixed-size buffers with a byte budget.
//
// Writing is a bounds check and a cursor bump. When a record does not fit, the
// live buffer is retired and the oldest one is recycled in its place, so a
// steady-state trace never allocates.
class buffer_group {
public:
    static constexpr std::size_t default_buffer_size = 128 * 1024;

    explicit buffer_group(std::size_t capacity,
                          std::size_t buffer_size = default_buffer_size);

    // Reserve n bytes and return where to put them. n must fit in one buffer:
    // records are never split, because a split record cannot be decoded from
    // the middle of the stream.
    [[gnu::always_inline]] std::byte* write(std::size_t n) {
        assert(n <= buffer_size_ && "record larger than one trace buffer");
        if (current_.size() - cur_pos_ < n) [[unlikely]] {
            rotate();
        }
        std::byte* result = current_.data() + cur_pos_;
        cur_pos_ += n;
        return result;
    }

    // Every live byte, oldest record first.
    //
    // Retired buffers are trimmed to their used length on retirement, but the
    // buffer still being written is not -- it carries buffer_size_ bytes of
    // which only cur_pos_ are real. Nothing downstream can tell the difference
    // (a record is not self-delimiting and a run of zeroes decodes as garbage),
    // so collecting the tail is this function's job and not the caller's.
    [[nodiscard]] std::vector<std::byte> collect() const;

    [[nodiscard]] std::size_t buffer_size() const noexcept { return buffer_size_; }

private:
    [[gnu::noinline]] void rotate();

    using buffer = std::vector<std::byte>;

    buffer current_;
    std::size_t cur_pos_ = 0;
    std::size_t used_ = 0;
    std::size_t capacity_;
    std::size_t buffer_size_;
    std::list<buffer> old_;  // oldest at the front
};

// One ring per event level.
class trace_buffers {
public:
    static constexpr std::size_t level_count = static_cast<std::size_t>(event_level::count);

    explicit trace_buffers(std::size_t info_capacity = 4 * 1024 * 1024,
                           std::size_t debug_capacity = 64 * 1024 * 1024,
                           std::size_t buffer_size = buffer_group::default_buffer_size)
        : groups_{buffer_group(info_capacity, buffer_size),
                  buffer_group(debug_capacity, buffer_size)} {}

    [[gnu::always_inline]] std::byte* write(event_level level, std::size_t n) {
        return groups_[static_cast<std::size_t>(level)].write(n);
    }

    [[nodiscard]] const buffer_group& group(event_level level) const {
        return groups_[static_cast<std::size_t>(level)];
    }

private:
    std::array<buffer_group, level_count> groups_;
};

// The tracer TRACEPOINT() writes to. Every thread that traces must have one
// installed; there is deliberately no null check on the hot path.
extern thread_local trace_buffers* local_tracer;

// --- argument signatures -----------------------------------------------------
//
// Each tracepoint carries one signature string naming both the parameter names
// and their wire types: "conn:u32,keepalive:bool,peer:bytes". That string is
// the whole of what the code generator has to work from -- it is what lets it
// emit a struct with correctly typed, correctly named members without seeing
// the call site.
//
// Building it needs two things that live in different worlds. The wire types
// come from the argument *types*, which decltype can take from the call site
// without evaluating anything. The parameter names come from the argument
// *values*, which it cannot: a name is a string literal, and a consteval
// function cannot be handed the runtime values sitting between the literals.
//
// So the names are taken from the preprocessor instead. TRACEPOINT() passes
// `#__VA_ARGS__` -- the argument list as written -- as a template parameter,
// and the two halves are interleaved at compile time below.

template <typename...>
inline constexpr bool always_false = false;

// The wire type of one parameter, or a compile error naming the parameter's
// type. A tracepoint's arguments are the fields of a struct someone will read
// by name, so a type this does not recognise is a call site to fix, not bytes
// to copy blindly.
template <typename T>
consteval std::string_view type_to_sig() {
    if constexpr (std::is_same_v<T, bool>) {
        return "bool";
    } else if constexpr (std::is_same_v<T, std::span<const std::byte>>) {
        return "bytes";
    } else if constexpr (std::is_same_v<T, std::string_view> ||
                         std::is_same_v<T, const char*> || std::is_same_v<T, char*> ||
                         (std::is_array_v<T> &&
                          std::is_same_v<std::remove_cv_t<std::remove_extent_t<T>>, char>)) {
        // Checked before the pointer case below, which `const char*` would
        // otherwise match: a string is worth decoding as text, not as an
        // address.
        return "str";
    } else if constexpr (std::is_pointer_v<T>) {
        return "ptr";
    } else if constexpr (std::integral<T>) {
        // Selected by width and signedness rather than by exact type, so that
        // `long` and `long long` -- distinct types of the same width -- do not
        // need separate specialisations and cannot silently fall through.
        if constexpr (std::is_signed_v<T>) {
            if constexpr (sizeof(T) == 8) return "i64";
            else if constexpr (sizeof(T) == 4) return "i32";
            else if constexpr (sizeof(T) == 2) return "i16";
            else return "i8";
        } else {
            if constexpr (sizeof(T) == 8) return "u64";
            else if constexpr (sizeof(T) == 4) return "u32";
            else if constexpr (sizeof(T) == 2) return "u16";
            else return "u8";
        }
    } else {
        static_assert(always_false<T>,
                      "a TRACEPOINT parameter has no wire type. Convert it at the call site: "
                      "an integer, a bool, a pointer, a string, or std::as_bytes() over its "
                      "representation. There is deliberately no catch-all -- a trace of bytes "
                      "nothing can name is not structured tracing.");
    }
}

// A string literal usable as a template argument, which is how the stringified
// argument list reaches the code below.
template <std::size_t N>
struct fixed_string {
    char chars[N]{};

    consteval fixed_string(const char (&s)[N]) {  // NOLINT(google-explicit-constructor)
        for (std::size_t i = 0; i < N; ++i) {
            chars[i] = s[i];
        }
    }

    [[nodiscard]] constexpr std::string_view view() const { return {chars, N - 1}; }
};

// Never defined, never called; named only inside decltype. The arguments supply
// their types without being evaluated, and without needing to be constant
// expressions -- which is the only reason a tracepoint can carry the type of a
// local variable at all.
template <typename... Args>
struct arg_types {};

template <typename... Args>
arg_types<Args...> sig_probe(const Args&...);

namespace detail {

// The argument list as written, split on top-level commas.
//
// Capacity is generous rather than exact, and the count is kept even when the
// items overflow it: a value containing a comma the preprocessor did not see as
// an argument separator (a braced initialiser, say) yields more fields than
// there are arguments, and signature_builder diagnoses that by the count. A
// tight array would instead fail as a subscript out of range, which says
// nothing about the tracepoint.
template <std::size_t Capacity>
struct arg_text {
    std::array<std::string_view, Capacity> items{};
    std::size_t count = 0;
};

constexpr std::string_view trim(std::string_view s) {
    while (!s.empty() && (s.front() == ' ' || s.front() == '\t')) s.remove_prefix(1);
    while (!s.empty() && (s.back() == ' ' || s.back() == '\t')) s.remove_suffix(1);
    return s;
}

template <std::size_t Capacity>
consteval arg_text<Capacity> split_args(std::string_view s) {
    arg_text<Capacity> out{};
    if (trim(s).empty()) {
        return out;  // TRACEPOINT() with no parameters
    }

    std::size_t depth = 0;
    std::size_t start = 0;
    bool in_string = false;
    bool in_char = false;

    for (std::size_t i = 0; i <= s.size(); ++i) {
        if (i == s.size() || (depth == 0 && !in_string && !in_char && s[i] == ',')) {
            if (out.count < Capacity) {
                out.items[out.count] = trim(s.substr(start, i - start));
            }
            ++out.count;
            start = i + 1;
            continue;
        }
        const char c = s[i];
        if (in_string || in_char) {
            if (c == '\\') ++i;
            else if (c == (in_string ? '"' : '\'')) in_string = in_char = false;
        } else if (c == '"') {
            in_string = true;
        } else if (c == '\'') {
            in_char = true;
        } else if (c == '(' || c == '[' || c == '{') {
            ++depth;
        } else if ((c == ')' || c == ']' || c == '}') && depth > 0) {
            --depth;
        }
    }
    return out;
}

// The text of a parameter name argument, with its quotes taken off. Whether it
// is a *well-formed* name -- an identifier, and distinct from its siblings --
// is not decided here but by the code generator, which is also the only place
// that can see the names of other translation units' tracepoints.
constexpr bool is_quoted(std::string_view field) {
    return field.size() >= 2 && field.front() == '"' && field.back() == '"';
}

constexpr std::string_view unquote(std::string_view field) {
    return is_quoted(field) ? field.substr(1, field.size() - 2) : std::string_view{};
}

}  // namespace detail

template <fixed_string Raw, typename Types>
struct signature_builder;

template <fixed_string Raw, typename... Args>
struct signature_builder<Raw, arg_types<Args...>> {
    static_assert(sizeof...(Args) % 2 == 0,
                  "TRACEPOINT parameters must come in pairs: a name literal then a value");

    static constexpr std::size_t arg_count = sizeof...(Args);
    static constexpr std::size_t pair_count = arg_count / 2;

    // Room for a few stray commas, so that a miscount is diagnosed by the
    // static_assert below rather than by a subscript out of range.
    static constexpr auto text = detail::split_args<arg_count + 8>(Raw.view());
    static_assert(text.count == arg_count,
                  "a TRACEPOINT parameter value containing a top-level comma must be "
                  "parenthesised");

    static constexpr std::array<std::string_view, arg_count> sigs{
        type_to_sig<std::remove_cvref_t<Args>>()...};

    static consteval std::size_t length() {
        std::size_t n = 0;
        for (std::size_t i = 0; i < pair_count; ++i) {
            n += (i == 0 ? 0U : 1U) + detail::unquote(text.items[2 * i]).size() + 1 +
                 sigs[2 * i + 1].size();
        }
        return n;
    }

    static consteval auto build() {
        std::array<char, length() + 1> out{};
        std::size_t at = 0;
        auto put = [&](std::string_view s) {
            for (char c : s) out[at++] = c;
        };
        for (std::size_t i = 0; i < pair_count; ++i) {
            if (i != 0) out[at++] = ',';
            put(detail::unquote(text.items[2 * i]));
            out[at++] = ':';
            put(sigs[2 * i + 1]);
        }
        return out;
    }

    // Every name has to be a literal, because that is the only way its text
    // survives into the signature at all. A non-literal is caught here rather
    // than by silently producing a nameless field.
    static consteval bool names_are_literals() {
        for (std::size_t i = 0; i < pair_count; ++i) {
            if (!detail::is_quoted(text.items[2 * i])) return false;
        }
        return true;
    }
    static_assert(names_are_literals(),
                  "each TRACEPOINT parameter name must be a string literal");

    static constexpr auto value = build();
};

// --- serialisation -----------------------------------------------------------
//
// Only the values are written; a parameter's name is in the signature and never
// on the wire.
//
// There is an overload per wire type and no fallback, so the set of types that
// can be traced is exactly the set type_to_sig() can name.

template <typename T>
concept string_like =
    std::is_same_v<T, std::string_view> || std::is_same_v<T, const char*> ||
    std::is_same_v<T, char*> ||
    (std::is_array_v<T> && std::is_same_v<std::remove_cv_t<std::remove_extent_t<T>>, char>);

// A string argument as bytes, without its terminator. An array carries its
// length in its type -- and is assumed to be a literal, so the last byte is
// dropped as the terminator -- while a pointer has to be walked. A null pointer
// is an empty string rather than a crash: a tracepoint is not the place to
// discover one.
template <string_like T>
constexpr std::string_view as_view(const T& x) {
    if constexpr (std::is_array_v<T>) {
        return {x, std::extent_v<T> - 1};
    } else if constexpr (std::is_pointer_v<T>) {
        return x == nullptr ? std::string_view{} : std::string_view{x};
    } else {
        return x;
    }
}

template <std::integral T>
constexpr std::size_t arg_size(const T& x) {
    return sizeof(x);
}

template <string_like T>
constexpr std::size_t arg_size(const T& x) {
    return as_view(x).size() + sizeof(std::uint16_t);
}

constexpr std::size_t arg_size(const void* const&) {
    return sizeof(std::uint64_t);
}

constexpr std::size_t arg_size(const std::span<const std::byte>& x) {
    return x.size() + sizeof(std::uint16_t);
}

template <typename T>
    requires std::is_trivially_copyable_v<T>
inline void write_raw(std::byte*& out, const T& x) {
    // Native byte order throughout: a trace is decoded by a program built from
    // the same binary's tracepoint table, on the same machine.
    std::memcpy(out, &x, sizeof(x));
    out += sizeof(x);
}

// Length-prefixed with a uint16_t, so a run of bytes has to fit in one.
inline void write_bytes(std::byte*& out, const void* data, std::size_t size) {
    assert(size <= UINT16_MAX && "byte run too long for a tracepoint");
    write_raw(out, static_cast<std::uint16_t>(size));
    std::memcpy(out, data, size);
    out += size;
}

template <std::integral T>
inline void serialize_arg(std::byte*& out, const T& x) {
    write_raw(out, x);
}

template <string_like T>
inline void serialize_arg(std::byte*& out, const T& x) {
    const std::string_view s = as_view(x);
    write_bytes(out, s.data(), s.size());
}

inline void serialize_arg(std::byte*& out, const void* const& x) {
    write_raw(out, reinterpret_cast<std::uintptr_t>(x));
}

inline void serialize_arg(std::byte*& out, const std::span<const std::byte>& x) {
    write_bytes(out, x.data(), x.size());
}

// The two below walk the argument list in (name, value) pairs, dropping the
// names: they are compile-time data, already folded into the signature.

constexpr std::size_t args_size() { return 0; }

template <std::size_t N, typename T, typename... Rest>
constexpr std::size_t args_size(const char (&)[N], const T& value, const Rest&... rest) {
    return arg_size(value) + args_size(rest...);
}

inline void serialize_args(std::byte*&) {}

template <std::size_t N, typename T, typename... Rest>
inline void serialize_args(std::byte*& out, const char (&)[N], const T& value,
                           const Rest&... rest) {
    serialize_arg(out, value);
    serialize_args(out, rest...);
}

// --- the tracepoint table ----------------------------------------------------

struct tracepoint_entry {
    // An identifier, not a sentence: the code generator turns it into the name
    // of a struct type, and rejects it if it cannot.
    const char* name;
    const char* file;
    int line;
    const char* function;
    // "conn:u32,keepalive:bool", or "" for a tracepoint with no parameters.
    const char* signature;

    // The static key gating this tracepoint, named after `name`. Not something
    // the hot path reads -- the branch is patched into the instruction stream,
    // not tested -- but what lets a tracepoint be found and flipped by name;
    // see set_tracepoint_enabled() below.
    ::static_keys::static_key_false* key;
};

// Synthesised by the linker around the `tracepoints` section.
extern "C" const tracepoint_entry __start_tracepoints[];
extern "C" const tracepoint_entry __stop_tracepoints[];

[[nodiscard]] std::span<const tracepoint_entry> tracepoints() noexcept;

// --- turning tracepoints on ---------------------------------------------------
//
// Tracepoints are off by default, so something has to switch them on, and the
// handle it switches them on by is the name. That is the one thing about a
// tracepoint that a config file, a flag or an RPC can carry: its key has no
// linkage and its index is a fact about this build's link order.
//
// Nothing in this header makes a name unique -- two call sites may share one,
// and both are meant when it is named, so these speak of however many
// tracepoints matched rather than of "the" tracepoint. The code generator is
// stricter: a decoder needs one struct per name, so it rejects a table in which
// a name repeats.

[[nodiscard]] bool is_enabled(const tracepoint_entry& entry) noexcept;

// Enable or disable every tracepoint whose name is exactly `name`, and return
// how many that was. Zero means no such tracepoint, which is the caller's to
// report: a misspelt name is otherwise indistinguishable from a quiet one.
std::size_t set_tracepoint_enabled(std::string_view name, bool enabled);

// Every tracepoint in the binary at once. Patching is a syscall per page
// touched, not per branch, so this is cheap enough to do at startup and far too
// expensive to do in a loop.
void set_all_tracepoints_enabled(bool enabled);

// A record is: uint32 tracepoint index, uint64 timestamp, then packed arguments.
//
// The index rather than `&entry` -- which is what the original patch stored --
// so that a trace does not depend on where the process happened to be mapped.
// Under PIE the two runs that a build needs (one to emit the decoder, one to
// emit a trace) land at different addresses, and absolute pointers would make
// them disagree. The subtraction is also link-time constant, so this is if
// anything cheaper.
inline constexpr std::size_t record_header_size = sizeof(std::uint32_t) + sizeof(std::uint64_t);

// TRACEPOINT(level, name, "param", value, "param", value, ...)
//
// `name` and each parameter name must be string literals. `name` names the
// tracepoint, the static key gating it, and the struct the generated decoder
// deserialises this tracepoint's records into; the parameter names become that
// struct's members. Both are checked for being usable as identifiers -- and for
// being unique -- by the code generator, in tracer/codegen.h.
//
// Every tracepoint is compiled behind a static key of its own, named after the
// tracepoint and disabled at startup. A tracepoint that nobody has turned on is
// therefore not a load-and-test but a five-byte nop, and the recording code is
// laid out off the fallthrough path -- so the cost of a tracepoint in a hot
// function that is not being traced is the nop, and the instruction cache lines
// it does not touch. Enabling one rewrites that nop into a jmp; see
// set_tracepoint_enabled() above, and modules/static_keys for how the patching
// works.
//
// The key is block-scope, so the only thing that can name it is this expansion.
// It reaches the outside world twice over: through the `tracepoints` entry
// below, which is how the tracer finds it by name, and through the descriptor
// DEFINE_STATIC_KEY_FALSE_LOCAL() emits, which is how static_keys' own listing
// does.
//
// `#__VA_ARGS__` is the parameter list as written. It is the only way the
// parameter *names* -- as opposed to their types -- reach the signature; see
// signature_builder above.
#define TRACEPOINT(level_, name_, ...)                                                    \
    do {                                                                                  \
        DEFINE_STATIC_KEY_FALSE_LOCAL(tracer_key_, name_);                                \
        static constexpr auto tracer_sig_ __attribute__((                                 \
            section("tracepoint_signatures"), used)) =                                    \
            ::tracer::signature_builder<                                                  \
                ::tracer::fixed_string{#__VA_ARGS__},                                     \
                decltype(::tracer::sig_probe(__VA_ARGS__))>::value;                       \
        static constexpr char tracer_name_[] __attribute__((                              \
            section("tracepoint_names"), used)) = name_;                                  \
        static constexpr char tracer_file_[] __attribute__((                              \
            section("tracepoint_files"), used)) = __FILE__;                               \
        static constexpr ::tracer::tracepoint_entry tracer_tp_ __attribute__((            \
            section("tracepoints"), used)) = {tracer_name_,                               \
                                              tracer_file_,                               \
                                              __LINE__,                                   \
                                              __PRETTY_FUNCTION__,                        \
                                              tracer_sig_.data(),                         \
                                              &tracer_key_};                              \
        if (static_branch_unlikely(&tracer_key_)) {                                       \
            const std::size_t tracer_size_ = ::tracer::args_size(__VA_ARGS__);            \
            std::byte* tracer_out_ = ::tracer::local_tracer->write(                       \
                (level_), tracer_size_ + ::tracer::record_header_size);                   \
            ::tracer::write_raw(tracer_out_,                                              \
                                static_cast<std::uint32_t>(                               \
                                    &tracer_tp_ - ::tracer::__start_tracepoints));        \
            ::tracer::write_raw(tracer_out_,                                              \
                                static_cast<std::uint64_t>(TRACER_TIMESTAMP()));          \
            ::tracer::serialize_args(tracer_out_ __VA_OPT__(, ) __VA_ARGS__);             \
        }                                                                                 \
    } while (0)

}  // namespace tracer
