#pragma once

// Binary tracepoints: a logging hot path that formats nothing.
//
// A TRACEPOINT() records a *pointer to its own static description* plus a
// timestamp plus the raw bytes of its arguments, into a thread-local ring of
// buffers. No formatting, no allocation, no string copying -- the format string
// never even reaches the buffer. What lands there is roughly a store, a
// timestamp read, and a memcpy per argument.
//
// The description lives in a dedicated ELF section, `tracepoints`, so the
// linker collects every tracepoint in the binary into one array bracketed by
// `__start_tracepoints` / `__stop_tracepoints`. A record identifies its
// tracepoint by *index* into that array, which is what makes a trace decodable
// by something other than the process that wrote it.
//
// Decoding is the other half, and it is not in this header: tracer/codegen.h
// walks that same section and emits the C++ source of a decoder specialised to
// this binary's tracepoints. See modules/tracer/BUCK for how the two halves are
// wired into a build.
//
// Derived from the Seastar tracer patch in references/tracer.patch, with the
// bugs noted there fixed and the argument-list macro machinery replaced.

#include <algorithm>
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

// Severity carried in the tracepoint description and printed by the decoder. It
// never reaches the trace itself.
enum class log_level : int {
    error,
    warn,
    info,
    debug,
    trace,
};

std::string_view to_string(log_level level) noexcept;

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

// --- argument type signatures ------------------------------------------------
//
// Each tracepoint carries a comma-separated signature ("u32,bool,bytes") naming
// the wire type of every argument. That string is what lets the code generator
// emit a correctly typed reader without seeing the call site.

template <typename T>
consteval std::string_view type_to_sig() {
    if constexpr (std::is_same_v<T, bool>) {
        return "bool";
    } else if constexpr (std::is_same_v<T, std::span<const std::byte>>) {
        return "bytes";
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
    } else if constexpr (sizeof(T) <= 8) {
        return "unknown64";
    } else {
        return "unknown128";
    }
}

template <typename... Ts>
consteval std::size_t signature_length() {
    std::size_t n = 0;
    bool first = true;
    ((n += (first ? 0U : 1U) + type_to_sig<Ts>().size(), first = false), ...);
    return n;
}

template <typename... Ts>
consteval auto make_signature() {
    std::array<char, signature_length<Ts...>() + 1> out{};
    std::size_t i = 0;
    bool first = true;
    auto append = [&](std::string_view s) {
        if (!first) {
            out[i++] = ',';
        }
        first = false;
        for (char c : s) {
            out[i++] = c;
        }
    };
    (append(type_to_sig<Ts>()), ...);
    return out;
}

template <typename... Args>
struct signature_of {
    static constexpr auto value = make_signature<std::remove_cvref_t<Args>...>();
};

// Never defined, never called. TRACEPOINT() only ever names it inside decltype,
// which is an unevaluated context -- so the arguments supply their types
// without being evaluated, and without needing to be constant expressions.
//
// This replaces the NARGS/SIG_1..SIG_13 macro ladder in the original patch, and
// with it the thirteen-argument ceiling.
template <typename... Args>
signature_of<Args...> signature_probe(const Args&...);

// --- serialisation -----------------------------------------------------------

template <typename T>
consteval std::size_t unknown_size() {
    return sizeof(T) <= 8 ? 8 : 16;
}

template <typename T>
    requires(!std::integral<T>)
constexpr std::size_t arg_size(const T&) {
    return unknown_size<T>();
}

template <std::integral T>
constexpr std::size_t arg_size(const T& x) {
    return sizeof(x);
}

constexpr std::size_t arg_size(const void* const&) {
    return sizeof(std::uint64_t);
}

constexpr std::size_t arg_size(const std::span<const std::byte>& x) {
    return x.size() + sizeof(std::uint16_t);
}

template <typename... Args>
constexpr std::size_t args_size(const Args&... args) {
    return (arg_size(args) + ... + 0);
}

template <typename T>
    requires std::is_trivially_copyable_v<T>
inline void write_raw(std::byte*& out, const T& x) {
    // Native byte order throughout: a trace is decoded by a program built from
    // the same binary's tracepoint table, on the same machine.
    std::memcpy(out, &x, sizeof(x));
    out += sizeof(x);
}

template <typename T>
    requires(!std::integral<T>)
inline void serialize_arg(std::byte*& out, const T& x) {
    constexpr std::size_t sz = unknown_size<T>();
    std::memcpy(out, &x, std::min(sizeof(x), sz));
    if constexpr (sizeof(T) < sz) {
        // The slot is a fixed 8 or 16 bytes wide. Zero the remainder rather
        // than leaving it whatever the buffer held last time round the ring:
        // the decoder prints the whole slot as hex.
        std::memset(out + sizeof(T), 0, sz - sizeof(T));
    }
    out += sz;
}

template <std::integral T>
inline void serialize_arg(std::byte*& out, const T& x) {
    write_raw(out, x);
}

inline void serialize_arg(std::byte*& out, const void* const& x) {
    write_raw(out, reinterpret_cast<std::uintptr_t>(x));
}

inline void serialize_arg(std::byte*& out, const std::span<const std::byte>& x) {
    // Length-prefixed with a uint16_t, so a span has to fit in one.
    assert(x.size() <= UINT16_MAX && "byte span too long for a tracepoint");
    write_raw(out, static_cast<std::uint16_t>(x.size()));
    std::memcpy(out, x.data(), x.size());
    out += x.size();
}

template <typename... Args>
inline void serialize_args(std::byte*& out, const Args&... args) {
    (serialize_arg(out, args), ...);
}

// --- the tracepoint table ----------------------------------------------------

struct tracepoint_entry {
    const char* name;  // also the format string the decoder fills in
    const char* file;
    int line;
    int level;
    const char* function;
    const char* signature;
};

// Synthesised by the linker around the `tracepoints` section.
extern "C" const tracepoint_entry __start_tracepoints[];
extern "C" const tracepoint_entry __stop_tracepoints[];

[[nodiscard]] std::span<const tracepoint_entry> tracepoints() noexcept;

// A record is: uint32 tracepoint index, uint64 timestamp, then packed arguments.
//
// The index rather than `&entry` -- which is what the original patch stored --
// so that a trace does not depend on where the process happened to be mapped.
// Under PIE the two runs that a build needs (one to emit the decoder, one to
// emit a trace) land at different addresses, and absolute pointers would make
// them disagree. The subtraction is also link-time constant, so this is if
// anything cheaper.
inline constexpr std::size_t record_header_size = sizeof(std::uint32_t) + sizeof(std::uint64_t);

// TRACEPOINT(level, format, severity, args...)
//
// `format` is both the tracepoint's name and the format string the decoder
// applies to the arguments; it must be a literal, and its placeholders must
// match the arguments, which the *generated decoder* checks at compile time.
#define TRACEPOINT(level_, format_, severity_, ...)                                       \
    do {                                                                                  \
        static constexpr auto tracer_sig_ __attribute__((                                 \
            section("tracepoint_signatures"), used)) =                                    \
            decltype(::tracer::signature_probe(__VA_ARGS__))::value;                       \
        static constexpr char tracer_name_[] __attribute__((                              \
            section("tracepoint_names"), used)) = format_;                                \
        static constexpr char tracer_file_[] __attribute__((                              \
            section("tracepoint_files"), used)) = __FILE__;                               \
        static constexpr ::tracer::tracepoint_entry tracer_tp_ __attribute__((            \
            section("tracepoints"), used)) = {tracer_name_,                               \
                                              tracer_file_,                               \
                                              __LINE__,                                   \
                                              static_cast<int>(severity_),                \
                                              __PRETTY_FUNCTION__,                        \
                                              tracer_sig_.data()};                        \
        const std::size_t tracer_size_ = ::tracer::args_size(__VA_ARGS__);                \
        std::byte* tracer_out_ = ::tracer::local_tracer->write(                           \
            (level_), tracer_size_ + ::tracer::record_header_size);                       \
        ::tracer::write_raw(tracer_out_,                                                  \
                            static_cast<std::uint32_t>(&tracer_tp_ -                      \
                                                       ::tracer::__start_tracepoints));   \
        ::tracer::write_raw(tracer_out_, static_cast<std::uint64_t>(TRACER_TIMESTAMP())); \
        ::tracer::serialize_args(tracer_out_ __VA_OPT__(, ) __VA_ARGS__);                 \
    } while (0)

}  // namespace tracer
