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
// static key of its own, named after the tracepoint, so a call site that has
// been switched off is a five-byte nop with the recording code laid out
// elsewhere. tracer::set_tracepoint_enabled() flips one by name. Tracepoints
// start *disabled*, so a program that says nothing about tracing pays nothing
// for the tracepoints compiled into it -- see the TRACEPOINT() macro at the
// bottom of this header, and modules/static_keys.
//
// The description lives in a dedicated ELF section, `tracepoints`, so the
// linker collects every tracepoint of one loaded object into an array bracketed
// by `__start_tracepoints` / `__stop_tracepoints`. There is one such array per
// object -- the executable and each shared library have their own -- and each
// registers itself as it loads, so the process has a list of tables rather than
// a single one. See "the tracepoint registry" below.
//
// A record identifies its tracepoint by the *address* of its entry -- or, for a
// tracepoint that fires often enough to want a shorter name for itself, by a
// static id given at the call site; see "static ids" below. A trace also
// carries a second stream of records -- the metadata level -- saying which
// object was mapped where, and when. That pair is what makes a trace decodable
// by something other than the process that wrote it: a record is read against
// the objects that were loaded at its timestamp, so subtracting the table's
// address turns it into an offset within an object named by its build ID rather
// than by where it happened to land. See "the metadata stream" below.
//
// A tracepoint may also carry a srcloc::location -- where its *caller* was --
// which is the one parameter type whose meaning is not in the tables at all: it
// is an address inside a loaded object, so a decoder needs the object files
// themselves. See "handing the objects to a decoder" below, and "resolving a
// location" in modules/trace-viewer/trace_wire.h.
//
// Decoding is the other half, and it is nowhere in this module: what a record
// means is in the `tracepoints` section of the object that wrote it, and what
// reads that section back -- and the records -- is the viewer, in
// modules/trace-viewer/{tracepoint_table,trace_wire}.h. modules/tracer's own
// tests read a trace back with those; see trace_reader.h beside them.
//
// Derived from the Seastar tracer patch in references/tracer.patch, with the
// bugs noted there fixed and the argument-list macro machinery replaced.

#include <algorithm>
#include <array>
#include <cassert>
#include <bit>
#include <chrono>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <list>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "source_location/source_location.h"
#include "static_keys/static_keys.h"

namespace tracer {

inline std::uint64_t rdtsc() noexcept {
    std::uint64_t lo = 0;
    std::uint64_t hi = 0;
    asm volatile("rdtsc" : "=a"(lo), "=d"(hi));
    return (hi << 32) | lo;
}

// The largest value a vint holds: 56 bits, the most that fits above an
// eight-bit tag in the word write_int() stores. Its callers keep to it -- see
// buffer_group::write_record() -- and everything below takes that as given.
inline constexpr std::uint64_t max_vint_value = (std::uint64_t{1} << 56) - 1;

// An unsigned vint. The value is stored little-endian above a length tag: the
// low `size` bits of the first byte are a run of `size - 1` one bits, so a
// reader counts the trailing ones of the byte it is looking at and knows how
// many bytes to take. A value of 0x1234 -- fourteen significant bits, so two
// bytes -- goes down as (0x1234 << 2) | 0b01.
//
// The tag is at the bottom and the value is little-endian, rather than
// Scylla's leading run of ones over a big-endian value, because that puts the
// whole encoding one shift away from a word: write_int() computes it with a
// shift and an or, and stores it with a single eight-byte store.
//
// How many bytes that is: each byte gives one bit to the tag, so a value takes
// as many bytes as it has groups of seven bits -- eight bytes at 56 bits, which
// is where this stops. A value above that would need a tag byte of its own and
// a ninth byte behind it, and nothing writes one: what goes through here is a
// timestamp measured from the head of its buffer, and buffers do not last 56
// bits of ticks. See write_record() and "reading a sync record back" below.
//
// The divide by seven is unsigned, and done with a multiply. Both follow from
// the contract above: a value that keeps to it has a magnitude of at least
// eight, so `magnitude - 1` cannot go negative, and nothing is owed the
// round-toward-zero correction a signed divide carries -- which clang emitted
// as eight instructions over byte registers, sitting on the dependency chain a
// record's timestamp is already waiting on. (x * 37) >> 8 is exactly x / 7 for
// every x a 64-bit magnitude can reach, which is well inside where the two
// part company.
inline constexpr std::size_t vint_size(std::uint64_t value) noexcept {
    assert(value <= max_vint_value && "a vint of more than 56 bits");
    const auto magnitude = static_cast<std::size_t>(std::countl_zero(value | 1));
    return std::size_t{9} - (((magnitude - 1) * 37) >> 8);
}

// Write one, advancing `out` by `size`, which must be vint_size(value).
//
// Taking the length rather than asking for it again is what keeps a record to
// one clz and one multiply: buffer_group::write_record() has already asked, to
// know how much of its buffer the record takes.
//
// The caller must leave *eight* bytes writable at `out` however few the value
// needs, because the store is eight bytes whatever the length is and only the
// pointer knows the difference. Writing over bytes the arguments are about to
// be written into is the point -- it is what makes a one-byte timestamp cost
// one store rather than one store per byte.
//
// Always inlined, because of where it is called from: a tracepoint records
// inside the *unlikely* arm of its static key, so everything there is costed
// against the inliner's cold threshold, and two stores and an assertion lose
// against it. A call would cost more than the encoding does -- `out` spilled to
// a stack slot, and the caller's vint_size() unable to fold with anything.
//
// The tag and the value go down under one shift rather than two. Written out,
// the encoding is `(value << size) | ((1 << (size - 1)) - 1)`: a value shifted
// by a variable, over a mask built from the same variable. Shifting a
// *constant* eight instead and taking the whole word down by `8 - size` puts
// the value at the same bit and leaves `0x7f >> (8 - size)`, which is that same
// run of `size - 1` ones. The word cannot overflow on the way -- 56 bits shifted
// by eight is exactly 64 -- and `8 - size` is the quotient vint_size() has
// already divided out, so inlined beside it the two cancel and only one shift
// count ever reaches `cl`.
[[gnu::always_inline]] inline void write_int_sized(std::byte*& out, std::uint64_t value,
                                                   std::size_t size) noexcept {
    assert(value <= max_vint_value && "a vint of more than 56 bits");
    const std::uint64_t encoded = ((value << 8) | 0x7f) >> (8 - size);
    std::memcpy(out, &encoded, sizeof(encoded));
    out += size;
}

// The same, for a caller with no use for the length.
inline void write_int(std::byte*& out, std::uint64_t value) noexcept {
    write_int_sized(out, value, vint_size(value));
}

// The most a record's header can take: the entry address and the longest vint.
// What a ring checks before it writes, since the length of the timestamp is not
// known until it has been sampled -- and what write_int_sized() needs writable
// at the timestamp whatever it ends up storing there. See record_header_size
// below for the other end of the range.
inline constexpr std::size_t max_record_header_size = 2 * sizeof(std::uint64_t);

// The timestamp source, as a macro so that overriding it costs nothing at
// runtime. Define it before including this header to substitute another clock;
// modules/tracer/trace_producer.cc uses a counter so that the traces it emits
// -- and therefore the snapshot taken of the decoded output -- are reproducible.
#ifndef TRACER_TIMESTAMP
#define TRACER_TIMESTAMP() ::tracer::rdtsc()
#endif

// --- turning ticks into times -------------------------------------------------
//
// rdtsc counts ticks of an invariant clock nobody has calibrated: it says how
// far apart two records are only once something says how many of its ticks go
// in a second, and it says *when* a record happened only once something pairs a
// tick count with a wall clock reading. That something is the clock_sync
// tracepoint, written to every level (bar the metadata one) whenever a ring
// rotates, and once right after a tracer is built -- so a trace that has been
// running long enough to evict its oldest buffer still begins with one.
//
// A sync record carries both halves: the wall clock at the moment it was
// written, and the ticks-per-second the process believes in. See
// "reading a sync record back" below for what a decoder is expected to do with
// a pair of them.

// Nanoseconds since the epoch, from the clock a sync record pairs with the
// timestamp in its own header.
[[nodiscard]] std::uint64_t realtime_nanoseconds() noexcept;

// The wall clock source, overridable exactly as TRACER_TIMESTAMP is and for the
// same reason: a sync record holds a real time, and a real time is different on
// every run. modules/tracer/plugin/demo_clock.h pins it to a constant so that
// the demo's trace -- and the snapshot of its decoded output -- stays stable.
#ifndef TRACER_REALTIME_NS
#define TRACER_REALTIME_NS() ::tracer::realtime_nanoseconds()
#endif

// What ticks_per_second is until somebody measures it: the rate of the machine
// this was written on, which is the right order of magnitude everywhere and
// exact nowhere.
//
// A default rather than a required calibration because calibrating costs a
// sleep, and a sleep is not something every program -- or every test -- should
// have to pay to be traceable. The cost of the default is that a trace with one
// sync record in it converts ticks to times with a rate that may be a percent
// or two out; a trace with two, which is the usual case, does not use the rate
// at all. See below.
inline constexpr std::uint64_t default_tsc_ticks_per_second = 3'187'000'000;

// The rate the next sync record will carry. Process-wide -- it is a property of
// the machine, not of a thread's ring -- and readable and writable from any
// thread.
[[nodiscard]] std::uint64_t tsc_ticks_per_second() noexcept;
void set_tsc_ticks_per_second(std::uint64_t ticks) noexcept;

// Measure the rate and install it, returning what was measured.
//
// Reads the pair of clocks, sleeps, reads them again, and divides. **This
// sleeps**, for `interval` -- 1ms by default, which is enough for the two
// readings to be far enough apart to divide and short enough not to be felt at
// startup. Nothing requires it to be called: a program that never does traces
// with default_tsc_ticks_per_second above.
std::uint64_t calibrate_tsc(std::chrono::nanoseconds interval = std::chrono::milliseconds(1));

// Whether clock sync records are written at all; true unless something says
// otherwise. The one reason to turn them off is a program whose records are
// decoded by a decoder generated from *another* object's tracepoint table --
// which cannot read the sync records this object writes any more than it could
// read any other of its records. modules/tracer/tracer_test.cc is that case.
[[nodiscard]] bool clock_sync_enabled() noexcept;
void set_clock_sync_enabled(bool enabled) noexcept;

// Which stream a record is written to. Separate rings so that a flood of debug
// events cannot evict the sparse, important ones.
//
// `metadata` is the tracer's own: which object was mapped where, and when. It
// is a ring like the others, written by the same macro -- see "the metadata
// stream" below.
enum class event_level : std::size_t {
    info,
    debug,
    metadata,
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
        if (current_.bytes.size() - cur_pos_ < n) [[unlikely]] {
            rotate();
        }
        return write_unchecked(n);
    }

    // The caller has already established that n fits in the current buffer.
    // Used by trace_buffers::write() so its hot path does not repeat the
    // bounds check after handling rotation in its slow path.
    [[gnu::always_inline]] std::byte* write_unchecked(std::size_t n) {
        std::byte* result = current_.bytes.data() + cur_pos_;
        cur_pos_ += n;
        return result;
    }

    [[nodiscard]] std::uint64_t last_timestamp() const noexcept {
        return last_timestamp_;
    }

    // Start this buffer's deltas from `timestamp` rather than from zero.
    //
    // Called on a buffer nothing has been written to yet -- at rotation, and
    // when a tracer is built -- because a delta from zero is an absolute tick
    // count, and an absolute tick count on a machine that has been up for
    // months does not fit a vint. What tells a decoder where the chain starts
    // is the clock_sync record written straight after this; see
    // trace_buffers::write_clock_sync().
    void rebase(std::uint64_t timestamp) noexcept { last_timestamp_ = timestamp; }

    // Reserve and write a complete record header. The timestamp is sampled by
    // trace_buffers::write(), after any rotation and its clock-sync record.
    //
    // `id_word` is the record's first field as it goes on the wire and
    // `id_size` is how much of it is meant: eight bytes of entry address, or
    // the one to eight bytes of an encoded static id. The store is eight bytes
    // either way -- the bytes past `id_size` are the timestamp's, and are
    // written over a moment later.
    [[gnu::always_inline]] std::byte* write_record(std::size_t args_size,
                                                    std::uint64_t id_word,
                                                    std::size_t id_size,
                                                    std::uint64_t timestamp) {
        // Measured from the last record, and so from the head of this buffer,
        // which rebase() put a moment before the first record of it. That is
        // what keeps a delta inside a vint: it is the age of a record within
        // its buffer, not the age of the machine.
        //
        // Clamped for the one case that is neither: a ring so quiet that two of
        // its records are 56 bits of ticks -- most of a year -- apart. Stamping
        // the second one a year after the first is wrong by however long the
        // silence ran over that, and is the wrongness that costs nothing to
        // carry; a branch here would be paid for by every record that is not
        // the one in a decade.
        const std::uint64_t delta =
            std::min(timestamp - last_timestamp_, max_vint_value);
        // Asked once, and used for both cursors: how far cur_pos_ moves, and
        // how far `out` does. A vint's length is a clz and a divide, and a
        // record should pay for it once.
        const std::size_t delta_size = vint_size(delta);

        // What has to fit is the largest header, not this record's: both stores
        // below put down eight bytes whatever they advance by, and the bytes
        // past the header are the arguments' -- written over a moment later, or
        // left outside cur_pos_ and so outside the record.
        assert(fits(args_size + max_record_header_size));
        std::byte* out = write_unchecked(args_size + id_size + delta_size);
        std::memcpy(out, &id_word, sizeof(id_word));
        out += id_size;
        write_int_sized(out, delta, delta_size);
        last_timestamp_ = timestamp;
        return out;
    }

    // The same, for a tracepoint whose records carry no timestamp: the id, and
    // then straight into the arguments.
    //
    // last_timestamp_ is left alone, which is the whole of what such a record
    // means -- it happened when the record before it did, and the record after
    // it counts from that same place. Nothing here samples the clock, which is
    // the instruction this saves beside the byte it saves in the buffer.
    [[gnu::always_inline]] std::byte* write_record_untimed(std::size_t args_size,
                                                           std::uint64_t id_word,
                                                           std::size_t id_size) {
        // Still the largest header, for the same reason as above: the store
        // below puts down eight bytes whatever `id_size` is, and the bytes past
        // the id are the arguments'.
        assert(fits(args_size + max_record_header_size));
        std::byte* out = write_unchecked(args_size + id_size);
        std::memcpy(out, &id_word, sizeof(id_word));
        return out + id_size;
    }

    // Every live byte, oldest record first.
    //
    // Retired buffers are trimmed to their used length on retirement, but the
    // buffer still being written is not -- it carries buffer_size_ bytes of
    // which only cur_pos_ are real. Nothing downstream can tell the difference
    // (a record is not self-delimiting and a run of zeroes decodes as garbage),
    // so collecting the tail is this function's job and not the caller's.
    [[nodiscard]] std::vector<std::byte> collect() const;

    // Whether n more bytes would fit without retiring the live buffer. What
    // trace_buffers::write() asks so that it can write a clock sync record into
    // the fresh buffer *before* the record that forced the rotation, rather
    // than after it; see below.
    [[nodiscard, gnu::always_inline]] bool fits(std::size_t n) const {
        return current_.bytes.size() - cur_pos_ >= n;
    }

    // Retire the live buffer and recycle the oldest in its place. write() does
    // this itself when it has to; it is public for the one caller that wants to
    // know that it is about to happen.
    [[gnu::noinline]] void rotate();

    [[nodiscard]] std::size_t buffer_size() const noexcept { return buffer_size_; }

    // When the records this ring still holds were written, on the wall clock:
    // the moment the oldest buffer that still has anything in it became the live
    // one, and the moment of this call -- which is when the buffer being written
    // stops being written, from the point of view of whoever is collecting.
    //
    // Not derivable from the records themselves. Their timestamps are rdtsc
    // ticks, and turning ticks into times is the two-pass job described under
    // "reading a sync record back"; a snapshot wants to say *when* it is from
    // without its reader having to do that first, so each buffer notes the wall
    // clock as it is activated and retired. Rotation is rare enough for a
    // clock_gettime to be free there.
    [[nodiscard]] std::pair<std::uint64_t, std::uint64_t> time_range() const;

private:

    // A buffer and the two moments that bracket it. The times are a property of
    // the buffer rather than of the group because a ring outlives its contents:
    // the oldest buffer is recycled as the newest, and what a snapshot covers is
    // whatever survived.
    struct buffer {
        std::vector<std::byte> bytes;
        std::uint64_t activated_ns = 0;  // wall clock when it became the live one
        std::uint64_t retired_ns = 0;    // ... and when it stopped being it
    };

    buffer current_;
    // Kept immediately beside current_.bytes' pointer, so the hot write path
    // finds the buffer pointer and its timestamp base in the same cacheline.
    std::uint64_t last_timestamp_ = 0;
    std::size_t cur_pos_ = 0;
    std::size_t used_ = 0;
    std::size_t capacity_;
    std::size_t buffer_size_;
    std::list<buffer> old_;  // oldest at the front
};

// One ring per event level.
//
// Constructing one writes the metadata prologue into its own metadata ring --
// the objects loaded now, as load events -- so that a tracer is decodable from
// the moment it exists. See "the metadata stream" below.
class trace_buffers {
public:
    static constexpr std::size_t level_count = static_cast<std::size_t>(event_level::count);

    // Defined at the bottom of this header, with the other members that record:
    // they expand TRACEPOINT_UNGATED(), which is declared there.
    explicit trace_buffers(std::size_t info_capacity = 4 * 1024 * 1024,
                           std::size_t debug_capacity = 64 * 1024 * 1024,
                           std::size_t metadata_capacity = 1024 * 1024,
                           std::size_t buffer_size = buffer_group::default_buffer_size);

    [[gnu::always_inline]] std::byte* write(event_level level, std::uint64_t id_word,
                                            std::size_t id_size, std::size_t args_size) {
        buffer_group& group = groups_[static_cast<std::size_t>(level)];
        // Check against the largest header before sampling. If rotation is
        // needed, the first sample must belong to the emitted record rather
        // than being discarded while the cold path writes its sync record. The
        // id counts as a full eight bytes here however short it encodes to: a
        // rotation one record early costs nothing, and a bounds check that
        // depends on the id's length is one the compiler has to keep.
        if (!group.fits(args_size + max_record_header_size)) [[unlikely]] {
            return write_slow(level, id_word, id_size, args_size);
        }
        const std::uint64_t timestamp = TRACER_TIMESTAMP();
        return group.write_record(args_size, id_word, id_size, timestamp);
    }

    // The same, for a record with no timestamp in it. Still checked against the
    // largest header rather than against what this record takes: a rotation one
    // record early costs nothing, and this way the two paths rotate alike.
    [[gnu::always_inline]] std::byte* write_untimed(event_level level, std::uint64_t id_word,
                                                    std::size_t id_size, std::size_t args_size) {
        buffer_group& group = groups_[static_cast<std::size_t>(level)];
        if (!group.fits(args_size + max_record_header_size)) [[unlikely]] {
            return write_slow_untimed(level, id_word, id_size, args_size);
        }
        return group.write_record_untimed(args_size, id_word, id_size);
    }

    [[nodiscard]] const buffer_group& group(event_level level) const {
        return groups_[static_cast<std::size_t>(level)];
    }

    [[nodiscard]] buffer_group& group(event_level level) {
        return groups_[static_cast<std::size_t>(level)];
    }

    // Record what has been loaded or unloaded since this tracer last looked, as
    // load and unload events on the metadata ring.
    //
    // The *loader's* to call -- the thread that dlopen()s and dlclose()s objects
    // with tracepoints in them -- and its whole obligation to the tracer. It
    // costs a walk of every loaded object and a parse of each one's build note,
    // which is why no registration constructor does it: it belongs where the
    // program already knows that an object came or went.
    //
    // The cost of forgetting is a range of addresses this ring still attributes
    // to the object that used to be there -- silently, because a plausible
    // address decodes to a plausible tracepoint. Call it after every load and
    // every unload, before the threads that trace are let back in.
    void note_objects_changed();

    // A clock_sync record on one level: where this buffer's timestamps start,
    // the wall clock now, and the rate that turns this trace's ticks into
    // seconds. Defined at the bottom of this header, with the other members
    // that record.
    //
    // `always` writes one even where clock_sync_enabled() says not to, which
    // the metadata ring's opening record needs: a decoder reads the head of
    // that stream by position, so the record has to be there whatever anyone
    // asked for. Nothing has to *recognise* it, which is what turning syncs off
    // is about.
    [[gnu::noinline]] void write_clock_sync(event_level level, bool always = false);

    // Cold path for a record that does not fit in the current buffer. Rotation
    // and clock-sync emission stay out of trace_buffers::write()'s hot path.
    [[gnu::noinline]] std::byte* write_slow(event_level level, std::uint64_t id_word,
                                           std::size_t id_size, std::size_t args_size);

    // And for an untimed record. A rotation is where a buffer's chain of
    // timestamps starts over, so the sync record written here is also what the
    // untimed record about to be written inherits its moment from.
    [[gnu::noinline]] std::byte* write_slow_untimed(event_level level, std::uint64_t id_word,
                                                   std::size_t id_size, std::size_t args_size);

private:
    // What both cold paths do before they write: retire the full buffer, rebase
    // the fresh one, and open it with a sync record. Defined at the bottom of
    // this header, beside them, because it expands TRACEPOINT_UNGATED().
    [[gnu::noinline]] buffer_group& rotate_for(event_level level,
                                               [[maybe_unused]] std::size_t args_size);

    // The objects this ring has already described, so that what has gone can be
    // named after it is gone.
    struct known_object {
        std::string build_id;
        std::uintptr_t base_address;
    };

    std::array<buffer_group, level_count> groups_;
    std::vector<known_object> known_;
    bool described_ = false;  // whether the count has been written
    bool syncing_ = false;    // guards write_clock_sync() against itself
};

// The tracer TRACEPOINT() writes to. Every thread that traces must have one
// installed; there is deliberately no null check on the hot path.
constinit extern thread_local trace_buffers* local_tracer;

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
    if constexpr (std::is_same_v<T, ::srcloc::location>) {
        // A caller's source location, recorded as the one word it is: the
        // address of the compiler's own constant. Only a decoder with the
        // object in hand can read it back -- see "resolving a location" in
        // modules/trace-viewer/trace_wire.h -- which is why it is a wire type
        // of its own rather than a "ptr".
        return "srcloc";
    } else if constexpr (std::is_same_v<T, bool>) {
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

constexpr std::size_t arg_size(const ::srcloc::location&) {
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

// The address and nothing else: the file, the function and the line are already
// in the object the address points into, and copying them into every record is
// exactly the formatting a tracepoint exists not to do.
inline void serialize_arg(std::byte*& out, const ::srcloc::location& x) {
    write_raw(out, static_cast<std::uint64_t>(x.address()));
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

// --- static ids ---------------------------------------------------------------
//
// A record names its tracepoint by the address of its entry, which costs eight
// bytes and buys a name nobody has to allocate: the linker gives every
// tracepoint in the program a distinct address, so a tracepoint can be added
// anywhere without anyone deciding what to call it.
//
// Eight bytes is a lot for a record whose arguments are four. A tracepoint that
// fires often enough for that to matter can be given a *static id* instead --
// a small number, chosen by hand, written on the wire in one to eight bytes --
// at the cost of being the one thing about a tracepoint that has to be kept
// unique by hand. See TRACEPOINT_STATIC_ID() at the bottom of this header.
//
// The two kinds of id share the field, so a decoder has to tell them apart from
// the bytes alone. An entry address has three zero bits at the bottom, being the
// address of an aligned struct; a static id is doubled and made odd before it is
// encoded, which leaves at least one of those three bits set whichever length
// the vint comes out as -- bit 1 when the doubled id fits in a byte and the
// length tag is empty, bit 0 when it does not and the tag is a run of ones. So:
// three zero bits at the bottom of the first byte means an address, anything
// else means a static id.
enum class tracepoint_id : std::uint64_t {
    // Not an id. What an entry carries when its tracepoint has none, and the
    // one value TRACEPOINT_STATIC_ID() refuses -- a zero id would encode to a
    // single byte with the value 1, which is a fine encoding of a tracepoint
    // that does not exist.
    none = 0,
};

// The largest id there is. Doubled it has to fit above its own length tag
// within one eight-byte word, because that word is what the write path stores;
// a program with more tracepoints than this has outgrown static ids rather
// than found a bug.
inline constexpr std::uint64_t max_tracepoint_id = (std::uint64_t{1} << 55) - 1;

[[nodiscard]] inline constexpr bool valid_tracepoint_id(tracepoint_id id) noexcept {
    return id != tracepoint_id::none &&
           static_cast<std::uint64_t>(id) <= max_tracepoint_id;
}

// The doubled, oddened value that goes on the wire.
[[nodiscard]] inline constexpr std::uint64_t static_id_value(tracepoint_id id) noexcept {
    return 2 * static_cast<std::uint64_t>(id) + 1;
}

// The word a record's id field is stored from, and how many of its bytes are
// meant. Both are constant expressions at every call site: what a static-id
// tracepoint emits is one immediate store and a pointer bumped by a constant.
[[nodiscard]] inline constexpr std::uint64_t static_id_word(tracepoint_id id) noexcept {
    const std::uint64_t value = static_id_value(id);
    const std::size_t size = vint_size(value);
    return (value << size) | ((std::uint64_t{1} << (size - 1)) - 1);
}

[[nodiscard]] inline constexpr std::size_t static_id_size(tracepoint_id id) noexcept {
    return vint_size(static_id_value(id));
}

// --- when a record says it happened ------------------------------------------
//
// How a record carries its timestamp is a fact about its *tracepoint*, not
// about records in general: a decoder learns which of these it is looking at
// from the id, and reads the timestamp as the first part of the body, ahead of
// the arguments. See "the wire format" below.
//
// The point of having more than one is that a timestamp is not free. It is a
// counter read on the hot path and one to eight bytes in the ring, and for a
// record that always follows another one closely -- the second half of a pair,
// an entry in a snapshot that is being dumped in a loop -- it is a cost paid
// for a number the record before it already gave.
enum class timestamp_encoding : std::uint8_t {
    // A vint delta from the record before it in the same buffer, which is the
    // usual thing and what an unqualified TRACEPOINT() writes.
    delta,

    // A vint delta from the tick count in the record's *own first parameter*,
    // which must be a u64. What opens a buffer, whose first record has nothing
    // before it to count from; see "reading a sync record back" below.
    sync,

    // Nothing on the wire at all. The record happened when the record before it
    // in the same buffer did, as far as anything reading the trace can tell.
    none,
};

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
    //
    // A *false* key: tracepoints start disabled. See TRACEPOINT() at the bottom.
    ::static_keys::static_key_false* key;

    // The id this tracepoint's records carry instead of this entry's address,
    // or tracepoint_id::none for the usual case of a tracepoint identified by
    // where it landed. Nothing in this process reads it -- it is written at the
    // call site, where it is a constant -- but the code generator does: it is
    // how a decoder knows which struct a static id means, and the one thing it
    // checks for collisions. See "static ids" above.
    tracepoint_id static_id;

    // How this tracepoint's records carry the moment they were taken. Nothing
    // in this process reads it either -- the write path is chosen at the call
    // site, where this is a constant -- but the code generator does: it is what
    // decides how many bytes of a record's body are the timestamp, and what
    // they mean. See timestamp_encoding above.
    timestamp_encoding timestamps;
};

// The bottom three bits of an entry's address are what tells a decoder it is
// looking at one rather than at a static id.
static_assert(alignof(tracepoint_entry) >= 8,
              "a tracepoint entry must be eight-byte aligned for its address to be "
              "distinguishable from a static id on the wire");

// An entry's layout is read back out of the object by whoever decodes a trace
// it wrote -- see modules/trace-viewer/tracepoint_table.h -- so these offsets
// are as much a wire format as the records are, and a change to them makes
// every object built before it unreadable by a reader built after it.
//
// Asserted rather than commented, so that the change is a compile error here
// and a paired edit there, rather than a decoder quietly reading names out of
// the middle of somebody's pointers.
static_assert(sizeof(tracepoint_entry) == 64);
static_assert(offsetof(tracepoint_entry, name) == 0);
static_assert(offsetof(tracepoint_entry, file) == 8);
static_assert(offsetof(tracepoint_entry, line) == 16);
static_assert(offsetof(tracepoint_entry, function) == 24);
static_assert(offsetof(tracepoint_entry, signature) == 32);
static_assert(offsetof(tracepoint_entry, static_id) == 48);
static_assert(offsetof(tracepoint_entry, timestamps) == 56);

// Synthesised by the linker around *this object's* `tracepoints` section.
//
// Weak, because a translation unit -- or a whole shared library -- may include
// this header and write no tracepoint at all, in which case the section does
// not exist and the brackets are null rather than undefined. The registration
// below declines to register such a table.
extern "C" {
extern const tracepoint_entry __start_tracepoints[] __attribute__((weak));
extern const tracepoint_entry __stop_tracepoints[] __attribute__((weak));
}

// --- the tracepoint registry -------------------------------------------------
//
// The brackets above describe one loaded object, so a process-wide view of the
// tracepoints has to be a list of ranges, each registered by the object it
// belongs to as that object is mapped. This is modules/static_keys' jump table
// registry in miniature -- the same problem, the same shape of answer -- minus
// everything that registry needs for patching, since nothing here has to reach
// across an object boundary.
//
// Two details carry the whole thing, and both are borrowed from there:
//
//   - The registration object lives in an anonymous namespace, so each object
//     gets a constructor that is genuinely its own and `__start_tracepoints`
//     inside it resolves against the object being loaded. An inline function
//     would collapse onto one definition and register one table many times.
//   - The registry head is an inline function with default visibility, so all
//     those copies *do* collapse onto one. That requires an executable to be
//     linked with -Wl,--export-dynamic, which //modules/static_keys already
//     exports as a linker flag to everything that uses a static key.

struct tracepoint_table {
    const tracepoint_entry* start;
    const tracepoint_entry* stop;
    int refs;  // translation units in this object that registered it
    tracepoint_table* next;
};

// The registry head. Process-wide, and defined out of line in tracer.cc rather
// than as an inline function whose copies collapse at load time: collapsing
// only happens if the executable exports the symbol, which needs
// -Wl,--export-dynamic on the link. Putting the definition in one translation
// unit makes the shared object that holds it the single owner instead, so a
// program linking tracer.cc into a library -- which is how Scylla consumes this
// -- gets one registry without any link flag at all.
[[gnu::visibility("default")]] tracepoint_table*& tracepoint_tables();

// Per-object storage for the entry above. Hidden and inline, so vague linkage
// folds the copies within one object and hidden keeps objects from folding onto
// each other: exactly one of these per loaded object, which is exactly how many
// tables there are.
[[gnu::visibility("hidden")]] inline tracepoint_table dso_tracepoint_table = {};

inline void add_tracepoint_table(const tracepoint_entry* start, const tracepoint_entry* stop,
                                 tracepoint_table* storage) {
    if (start == nullptr || start == stop) {
        return;  // an object with no tracepoints has no section and no brackets
    }
    for (tracepoint_table* table = tracepoint_tables(); table != nullptr; table = table->next) {
        if (table->start == start) {
            table->refs++;  // another translation unit of the same object
            return;
        }
    }
    *storage = {start, stop, 1, tracepoint_tables()};
    tracepoint_tables() = storage;
}

inline void del_tracepoint_table(const tracepoint_entry* start) {
    tracepoint_table** prev = &tracepoint_tables();
    for (tracepoint_table* table = *prev; table != nullptr;
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

namespace {

struct tracepoint_module {
    tracepoint_module() {
        add_tracepoint_table(__start_tracepoints, __stop_tracepoints, &dso_tracepoint_table);
    }
    ~tracepoint_module() { del_tracepoint_table(__start_tracepoints); }
};

// Earliest a user constructor may ask for, so a table is registered before any
// static initialiser that might enable one of its tracepoints. Destructors run
// in reverse, and so after those same initialisers.
[[maybe_unused]] __attribute__((init_priority(101)))
const tracepoint_module tracepoint_module_registration;

}  // namespace

// Every tracepoint of every loaded object, in registry order.
//
// Pointers rather than a span: the tables are separate ranges in separate
// mappings, and there is no array of all of them to hand out a view of.
[[nodiscard]] std::vector<const tracepoint_entry*> tracepoints();

// --- naming a loaded object ---------------------------------------------------

// One object's table, as a trace has to describe it.
//
// `build_id` is the object's GNU build ID as lowercase hex. It is the identity a
// trace carries, because it is the only one that is both stable across runs and
// independent of where the object was mapped or what path it was loaded from --
// a path names a file that may since have been rebuilt, and an address names
// nothing at all once the process is gone.
struct trace_object {
    std::string build_id;
    std::uintptr_t table_address;  // where this run mapped the object's table

    // Where this run mapped the object itself, and how far it reaches: the
    // load bias and the end of its last PT_LOAD. Together they are the range of
    // addresses that belong to this object, which is what turns an address
    // recorded inside it -- a srcloc::location -- back into an offset in a file
    // that a decoder can open. The table address cannot do that job: it says
    // where one section landed, not where the object begins.
    std::uintptr_t base_address;
    std::uint64_t mapping_size;

    // Where this run loaded the object from, for a program collecting the
    // objects a trace will need to be decoded against; see
    // write_dso_directory() below. The loader reports the main executable as
    // the empty string, so that case is resolved to /proc/self/exe here rather
    // than by every caller.
    std::string path;

    std::span<const tracepoint_entry> table;
};

// Every loaded object, ordered by build ID so that two runs of the same program
// describe themselves the same way whatever order their libraries happened to
// load in. An object holding a registered table carries it, and its address; one
// that holds none carries an empty table and a table address of zero.
//
// Not just the objects that trace, because not everything a trace records is a
// tracepoint address. A source location is an address anywhere in whichever
// object captured it -- in a program whose tracepoints live in one shared
// library, usually a different one -- and an object nothing describes is an
// address a decoder cannot even name, let alone read. An object with no build ID
// is left out unless it holds tracepoints, which is how the vdso stays out of
// the way of the refusal below.
//
// Throws std::runtime_error if an object holding tracepoints has no build ID:
// its tracepoints could be recorded but never attributed, so that is a link to
// fix (-Wl,--build-id) rather than a trace to write half of.
[[nodiscard]] std::vector<trace_object> trace_objects();

// The build ID of the main executable, as lowercase hex, or empty if it has
// none. What a snapshot writes down to say which build it came out of -- the
// name a reader hands to a build-ID server, or looks up under `dsos/`.
[[nodiscard]] std::string executable_build_id();

// --- handing the objects to a decoder ------------------------------------------
//
// A trace names its objects by build ID, and a source location in one is an
// offset into the object's file. So a decoder needs the files, and it finds them
// by build ID in a directory laid out the way a debuginfo directory is:
//
//     <root>/.build-id/<first two hex digits>/<the rest>.debug
//
// which is what llvm-cov's and gdb's --debug-file-directory expect, and what
// tools/vscode-buck2 already builds for coverage. Copies rather than symlinks,
// so that the directory keeps working once the build outputs it came from have
// been rewritten.
//
// This writes the objects loaded *now*, which is every object a tracer built now
// would describe -- all of them, since a location may be in any. That is a copy
// of the program and of every library it has open, so a directory is as big as
// the process's text; it is what reading a location back costs. A program that dlopen()s something and traces through it has
// to call this again, for the same reason it has to call note_objects_changed().
//
// Throws std::runtime_error if an object cannot be read or copied.
void write_dso_directory(const std::string& root);

// --- turning tracepoints on ---------------------------------------------------
//
// Tracepoints are off by default, so switching one on is the interesting
// direction, and the handle it switches by is the name. That is the one thing about a
// tracepoint that a config file, a flag or an RPC can carry: its key has no
// linkage and its address is a fact about where this run mapped it.
//
// Nothing here makes a name unique -- two call sites may share one, and a
// tracepoint in a header shared by two libraries is *compiled twice*, once into
// each -- and all of them are meant when the name is given, so these speak of
// however many tracepoints matched rather than of "the" tracepoint. The code
// generator agrees: it merges same-named tracepoints into one struct, and
// objects only if two of them disagree about their parameters.
//
// The tracer's own metadata tracepoints have no key and are passed over here:
// they are the frame a trace is read in, not something to switch off. See "the
// metadata stream" below.

[[nodiscard]] bool is_enabled(const tracepoint_entry& entry) noexcept;

// Enable or disable every tracepoint whose name is exactly `name`, and return
// how many that was. Zero means no such tracepoint, which is the caller's to
// report: a misspelt name is otherwise indistinguishable from a quiet one.
std::size_t set_tracepoint_enabled(std::string_view name, bool enabled);

// Every tracepoint in the process at once. Patching is a syscall per page
// touched, not per branch, so this is cheap enough to do at startup and far too
// expensive to do in a loop.
void set_all_tracepoints_enabled(bool enabled);

// --- the wire format ----------------------------------------------------------
//
// A trace is a magic number followed by chunks:
//
//     uint32 magic
//     per chunk: uint8 level, uint64 length, that many bytes of records
//
// A chunk is one level's records, oldest first. There may be several chunks of
// a level -- one ring per thread, say -- and there is exactly one chunk of the
// metadata level, which is the process's stream rather than any thread's.
//
// A record is: an id, then its body. The body is the timestamp -- if the
// tracepoint carries one -- followed by the packed arguments.
//
// Which of the three timestamp encodings the body opens with is a fact about
// the tracepoint, not about the record, so it is only known once the id has
// been read: an unsigned vint delta for the usual tracepoint, that same delta
// measured from the tick count in the record's own first parameter for a
// clock_sync, and nothing at all for a tracepoint declared with
// TRACEPOINT_UNTIMED(). See timestamp_encoding above, and "reading a sync
// record back" below.
//
// That is why the timestamp is described here as part of the body rather than
// as a second header field. A decoder reads it in the code the id selects,
// beside the arguments, which is what lets a tracepoint leave it out or write
// it in a form of its own without every other record paying for the
// possibility.
//
// The id is one of two things, told apart by the bottom three bits of its first
// byte. All zero: eight bytes of tracepoint entry address, which is what a
// tracepoint carries unless it was given something shorter. Anything else: a
// vint of the tracepoint's static id, doubled and made odd -- one byte for the
// first sixty-three of them. See "static ids" above.
//
// The address rather than an index -- which is what an earlier
// version of this stored -- because an index is only meaningful against a table,
// and with shared libraries in the picture there is no single table to index:
// every object has one of its own, and an index into "the" table is a number
// that two objects both claim.
//
// An address, being a fact about this run, is not decodable on its own -- and
// not against a fixed table of objects either, because a library can be
// unloaded and another mapped over the range it had, so one address is two
// tracepoints at two different moments. What makes it decodable is the metadata
// stream below, read alongside the timestamp.
// The smallest a record identified by its entry can be, arguments aside: the
// address, and the shortest timestamp vint. A record with a static id is
// shorter -- two bytes, for a small id and a small delta -- and an untimed one
// with a static id is a single byte.
inline constexpr std::size_t record_header_size = sizeof(std::uint64_t) + 1;

// "TRC2", little-endian. A trace that does not start with it is not one, which
// is worth establishing before a stream of bytes is read as addresses.
inline constexpr std::uint32_t trace_magic = 0x32435254;

// --- the metadata stream ------------------------------------------------------
//
// Which object is mapped where is not a property of a trace but of a *moment*
// in it, so it is recorded the way everything else here is: as tracepoints, at
// a level of their own.
//
//     clock_sync{tsc, realtime_ns, ticks_per_second}
//     trace_objects_loaded{count}
//     trace_object_loaded{build_id, table_address, base_address, mapping_size}
//     trace_object_unloaded{build_id, base_address}
//
// There is one load event per *loaded object*, and not one per tracepoint table:
// an object with no tracepoints in it can still be where a source location
// points, and an object nothing described is an address a decoder cannot place.
// Such an event carries a table address of zero, which is below every real one
// and so never claims a record.
//
// A load event says where the object's tracepoint table landed *and* where the
// object itself did. Both are needed and neither implies the other: a record
// names its tracepoint by the address of an entry, which is an offset from the
// table, while a srcloc::location is an address anywhere in the object's
// .rodata, which is an offset from the base. The base could in principle be
// recovered from the table address by finding the `tracepoints` section in the
// object file, but only for an object whose section headers survived, so both
// are written down rather than one being derived.
//
// They are written by TRACEPOINT_UNGATED() like any other record, into the
// metadata ring of the tracer that is recording. There is no second writer, no
// second clock and no second buffer: a metadata event is a tracepoint, and the
// only thing unusual about it is that it has no key, because a trace missing
// these is not a trace but a heap of addresses.
//
// A decoder reads the metadata ring interleaved with the record rings, in
// timestamp order, keeping a table of the objects loaded *as of the record it
// is looking at*. A record whose object has since been unloaded still decodes,
// against the object that was there when it was written; a record from an
// object mapped over that range afterwards decodes as the new object's.
//
// What makes the ring decodable from its own first byte is where it starts. A
// tracer writes a clock_sync, then trace_objects_loaded{count = N}, then N load
// events, as the first thing its constructor does, so those N+2 records are
// always the first in the ring -- and a decoder reads them by that invariant
// rather than by their addresses, which is the only way round the circle: an
// address means nothing until some load event has said where an object is.
//
// The sync is there for the same reason it is in every other ring -- to say
// where this one's deltas start -- and it is written whatever
// clock_sync_enabled() says, because a stream read by position has no room for
// a record that is sometimes there. It is also the *only* one this ring gets:
// see write_slow() for why a rotation cannot add another.
//
// A ring per tracer, and so per thread, rather than one for the process: it is
// the same ring, written by the same macro, as everything else. The cost is
// that a thread learns about a dlopen() only if it is told -- see
// note_objects_changed() -- and that the objects are described once per thread
// rather than once. Both are cheap beside a second machine for one kind of
// record.

// --- reading a sync record back -----------------------------------------------
//
// A clock_sync record is a tick count -- `tsc`, its first parameter -- beside a
// wall clock reading, plus the rate the process believed in when it was written.
//
// `tsc` is a parameter and not the record's own timestamp because a timestamp
// is a *delta*: every record on the wire says how long after the record before
// it, and the first record of a buffer has nothing before it to count from. So
// a sync record carries the count its buffer's deltas are measured from, and a
// decoder reads a sync as `tsc` plus its own delta rather than as the delta
// alone. That is what keeps every one of them inside a vint, whatever the
// machine's counter has climbed to; see write_int() above.
//
// That is the whole of what timestamp_encoding::sync means, and the reason a
// decoder reads a record's timestamp only after its id has said which
// tracepoint it is: two records can carry the same bytes and mean different
// moments.
//
// Two of them bracket most of a trace, and that is the case worth writing code
// for:
// between two syncs the conversion is an *interpolation* -- the two (tick,
// time) pairs give a rate measured over exactly this trace, on exactly this
// machine, with no reliance on the rate field at all -- while outside them it
// is an extrapolation from the rate, which is an estimate and may be a default
// nobody measured. Extrapolate only where there is no sync on both sides.
//
// So a consumer that wants wall clock times is a two-pass one: the first pass
// walks the trace collecting the sync records, the second converts each record
// against the syncs either side of it. A single-pass converter can only ever
// extrapolate forwards from the last sync it saw, which is the one arrangement
// this format makes avoidable.
//
// Nothing here does that yet: the tracer writes the sync records and the
// generated decoder delivers them as clock_sync events like any other. The
// consumer that will need it is the trace-viewer export, which is not written.

// --- writing a trace ----------------------------------------------------------

// One chunk: a level, a length, and that level's records.
void append_chunk(std::vector<std::byte>& out, event_level level,
                  std::span<const std::byte> records);

// A whole trace of one tracer's rings: the magic, then a chunk per level.
[[nodiscard]] std::vector<std::byte> collect_trace(const trace_buffers& buffers);

// One level as a trace of its own: the magic, the metadata chunk, then that
// level's records. Self-contained, because the metadata chunk goes in whichever
// level was asked for -- a file holding the info stream alone still says where
// the objects its records name were mapped.
//
// Why anyone would want that: the debug ring is an order of magnitude larger
// than the info one, and a snapshot that keeps them apart is a snapshot whose
// expensive half can be thrown away without losing the cheap one.
[[nodiscard]] std::vector<std::byte> collect_trace_level(const trace_buffers& buffers,
                                                         event_level level);

// The static description of a tracepoint: everything about it that is known at
// compile time, in the sections the linker collects.
//
// `key_` is the address of the static key gating it, or nullptr for a
// tracepoint that is never gated, and `id_` is its static id or
// tracepoint_id::none. Only the entry itself goes in `tracepoints`, so that the
// section stays an array the code generator can index; the strings live in
// sections of their own.
#define TRACER_TRACEPOINT_ENTRY(name_, key_, id_, stamps_, ...)                           \
    static constexpr auto tracer_sig_ __attribute__((                                     \
        section("tracepoint_signatures"), used)) =                                        \
        ::tracer::signature_builder<                                                      \
            ::tracer::fixed_string{#__VA_ARGS__},                                         \
            decltype(::tracer::sig_probe(__VA_ARGS__))>::value;                           \
    static constexpr char tracer_name_[] __attribute__((                                  \
        section("tracepoint_names"), used)) = name_;                                      \
    static constexpr char tracer_file_[] __attribute__((                                  \
        section("tracepoint_files"), used)) = __FILE__;                                   \
    static constexpr ::tracer::tracepoint_entry tracer_tp_ __attribute__((                \
        section("tracepoints"), used)) = {tracer_name_,                                   \
                                          tracer_file_,                                   \
                                          __LINE__,                                       \
                                          __PRETTY_FUNCTION__,                            \
                                          tracer_sig_.data(),                             \
                                          key_,                                           \
                                          id_,                                            \
                                          stamps_}

// The record: its id, its timestamp if it has one, and the arguments.
// `id_word_` and `id_size_` are the id field as it goes on the wire; see
// write_record().
//
// `stamps_` is a constant expression, so the branch below is folded and a call
// site compiles to exactly one of the two write paths. A ternary rather than an
// `if constexpr` because what it chooses between is how one variable is
// initialised, and the two calls differ only in the timestamp.
#define TRACER_RECORD(level_, stamps_, id_word_, id_size_, ...)                           \
    const std::size_t tracer_size_ = ::tracer::args_size(__VA_ARGS__);                    \
    std::byte* tracer_out_ =                                                              \
        (stamps_) == ::tracer::timestamp_encoding::none                                   \
            ? ::tracer::local_tracer->write_untimed((level_), (id_word_), (id_size_),     \
                                                    tracer_size_)                         \
            : ::tracer::local_tracer->write((level_), (id_word_), (id_size_),             \
                                            tracer_size_);                                \
    ::tracer::serialize_args(tracer_out_ __VA_OPT__(, ) __VA_ARGS__)

// The id of a record identified by its entry: the entry's address, all eight
// bytes of it. Named after the entry TRACER_TRACEPOINT_ENTRY() just defined, so
// the two only ever appear together.
#define TRACER_ENTRY_ID_WORD                                                              \
    static_cast<std::uint64_t>(reinterpret_cast<std::uintptr_t>(&tracer_tp_))

// The gated form, which TRACEPOINT() and TRACEPOINT_UNTIMED() are the two
// spellings of: `stamps_` is the one thing they differ in, so it is the one
// parameter this takes over theirs.
#define TRACER_KEYED_TRACEPOINT(level_, name_, stamps_, ...)                              \
    do {                                                                                  \
        DEFINE_STATIC_KEY_FALSE_LOCAL(tracer_key_, name_);                                \
        TRACER_TRACEPOINT_ENTRY(name_, &tracer_key_, ::tracer::tracepoint_id::none,       \
                                (stamps_) __VA_OPT__(, ) __VA_ARGS__);                    \
        if (static_branch_unlikely(&tracer_key_)) {                                         \
            TRACER_RECORD(level_, (stamps_), TRACER_ENTRY_ID_WORD,                        \
                          sizeof(std::uint64_t) __VA_OPT__(, ) __VA_ARGS__);              \
        }                                                                                 \
    } while (0)

// TRACEPOINT(level, name, "param", value, "param", value, ...)
//
// `name` and each parameter name must be string literals. `name` names the
// tracepoint, the static key gating it, and the struct a decoder deserialises
// this tracepoint's records into; the parameter names become that struct's
// members. Both have to be usable as identifiers, and unique: a decoder
// generated from a table where they are not is refused, by name, in
// modules/trace-viewer/decoder_plugin.cc.
//
// Every tracepoint is compiled behind a static key of its own, named after the
// tracepoint -- and *disabled* at startup, so a program which never says
// anything about tracing carries a five-byte nop at each call site and nothing
// else. The branch is a patched instruction rather than a load and a test, so
// switching one on with set_tracepoint_enabled() rewrites that nop into a jump
// and costs the call site nothing thereafter.
//
// Disabled-by-default is the direction that costs the host program nothing
// until it asks, but it does mean somebody has to rewrite instructions under
// running threads. Whoever flips a key is responsible for doing it where that
// is safe: Seastar's reactor, for one, only ever does it from a rendezvous
// where every shard is parked in its poll loop.
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
    TRACER_KEYED_TRACEPOINT(level_, name_,                                                \
                            ::tracer::timestamp_encoding::delta __VA_OPT__(, ) __VA_ARGS__)

// TRACEPOINT_UNTIMED(level, name, "param", value, ...)
//
// The same, for a record that carries no timestamp: it happened when the record
// before it in the same buffer did, and a decoder hands it that one's.
//
// What it saves is the clock read and the one to eight bytes the delta would
// have taken. What it costs is that the record is only located as precisely as
// its neighbours are -- so this is for an event that follows another one
// closely and always: the second half of a pair, or one row of a snapshot being
// dumped in a loop. A consumer is told which it is looking at: the metadata a
// generated decoder hands the callback says whether the timestamp was the
// record's own. See timestamp_encoding above.
#define TRACEPOINT_UNTIMED(level_, name_, ...)                                            \
    TRACER_KEYED_TRACEPOINT(level_, name_,                                                \
                            ::tracer::timestamp_encoding::none __VA_OPT__(, ) __VA_ARGS__)

// The same for a tracepoint named by a static id, in the two spellings below.
#define TRACER_STATIC_ID_TRACEPOINT(id_, level_, name_, stamps_, ...)                     \
    do {                                                                                  \
        constexpr ::tracer::tracepoint_id tracer_id_ = (id_);                             \
        static_assert(::tracer::valid_tracepoint_id(tracer_id_),                          \
                      "a static tracepoint id must be neither tracepoint_id::none nor "   \
                      "above tracer::max_tracepoint_id");                                 \
        DEFINE_STATIC_KEY_FALSE_LOCAL(tracer_key_, name_);                                \
        TRACER_TRACEPOINT_ENTRY(name_, &tracer_key_, tracer_id_,                          \
                                (stamps_) __VA_OPT__(, ) __VA_ARGS__);                    \
        if (static_branch_unlikely(&tracer_key_)) {                                        \
            TRACER_RECORD(level_, (stamps_), ::tracer::static_id_word(tracer_id_),        \
                          ::tracer::static_id_size(tracer_id_) __VA_OPT__(, ) __VA_ARGS__); \
        }                                                                                 \
    } while (0)

// TRACEPOINT_STATIC_ID(id, level, name, "param", value, ...)
//
// The same, with the record naming its tracepoint by a number given here rather
// than by where this entry landed. `id` is any constant expression of type
// tracepoint_id other than tracepoint_id::none; a small one costs a single byte
// on the wire, against the eight an address costs, which is worth having for a
// tracepoint that fires in a loop and carries a word.
//
// What it costs is the uniqueness the linker was providing for free. Two
// tracepoints sharing an id are two events a decoder cannot tell apart, so the
// code generator refuses a table in which one id names two of them -- which
// catches it at build time, and which is the *only* thing about a tracepoint
// anyone has to keep unique by hand. Names are still free to repeat, and so are
// the copies of one tracepoint that a header compiled into two objects makes:
// they are one tracepoint, and they carry one id.
//
// The id is folded at compile time into the one word the write path stores and
// the constant it advances the cursor by, so a shorter id is genuinely shorter
// work and not a branch on a length.
#define TRACEPOINT_STATIC_ID(id_, level_, name_, ...)                                     \
    TRACER_STATIC_ID_TRACEPOINT(id_, level_, name_,                                       \
                                ::tracer::timestamp_encoding::delta __VA_OPT__(, ) __VA_ARGS__)

// TRACEPOINT_STATIC_ID_UNTIMED(id, level, name, "param", value, ...)
//
// Both shortenings at once, which is the smallest a record gets: one byte of
// id, no timestamp, and the arguments. For the tracepoint in the innermost
// loop, where the two things a record costs beyond its arguments are both
// things the record before it already paid for.
#define TRACEPOINT_STATIC_ID_UNTIMED(id_, level_, name_, ...)                             \
    TRACER_STATIC_ID_TRACEPOINT(id_, level_, name_,                                       \
                                ::tracer::timestamp_encoding::none __VA_OPT__(, ) __VA_ARGS__)

// The ungated form. Also what writes the clock sync, which is the one
// tracepoint timed from a parameter of its own -- and the one caller that
// passes anything but `delta` here.
#define TRACER_UNGATED_TRACEPOINT(level_, name_, stamps_, ...)                            \
    do {                                                                                  \
        TRACER_TRACEPOINT_ENTRY(name_, nullptr, ::tracer::tracepoint_id::none,            \
                                (stamps_) __VA_OPT__(, ) __VA_ARGS__);                    \
        TRACER_RECORD(level_, (stamps_), TRACER_ENTRY_ID_WORD,                            \
                      sizeof(std::uint64_t) __VA_OPT__(, ) __VA_ARGS__);                  \
    } while (0)

// The same, without a key: a tracepoint that is always recorded.
//
// For records a trace cannot be read without, which is to say the metadata
// events and nothing else. A tracepoint that can be switched off is one a trace
// can be missing, and there is nothing to be gained from being able to switch
// off the events that say what an address means.
//
// The entry's key is null, which is how is_enabled() and the two functions that
// flip keys by name know to pass it over.
#define TRACEPOINT_UNGATED(level_, name_, ...)                                            \
    TRACER_UNGATED_TRACEPOINT(level_, name_,                                              \
                              ::tracer::timestamp_encoding::delta __VA_OPT__(, ) __VA_ARGS__)

// --- a tracer's own records ---------------------------------------------------
//
// Down here because they expand the macros above, which need everything else in
// this header. They are ordinary tracepoints; what is particular about them is
// only when they are written.

inline trace_buffers::trace_buffers(std::size_t info_capacity, std::size_t debug_capacity,
                                    std::size_t metadata_capacity, std::size_t buffer_size)
    : groups_{buffer_group(info_capacity, buffer_size),
              buffer_group(debug_capacity, buffer_size),
              buffer_group(metadata_capacity, buffer_size)} {
    // Every ring's chain of deltas starts here, whether or not a sync record
    // goes down to say so: a delta from zero is an absolute tick count, which
    // is the one value that does not fit a vint.
    const std::uint64_t now = TRACER_TIMESTAMP();
    for (buffer_group& group : groups_) {
        group.rebase(now);
    }

    // A sync record at the head of every ring, so that a trace collected from a
    // tracer that never filled a buffer still says what its ticks mean -- and so
    // that every ring has a record saying where its chain of deltas starts.
    //
    // The metadata ring's goes down first, and goes down whatever
    // clock_sync_enabled() says: a decoder reads that stream from its first
    // byte, so the head of it is a shape rather than a choice.
    write_clock_sync(event_level::metadata, /*always=*/true);
    for (std::size_t i = 0; i < level_count; ++i) {
        const auto level = static_cast<event_level>(i);
        if (level != event_level::metadata) {
            write_clock_sync(level);
        }
    }

    // The prologue is the first difference this ring sees: everything loaded,
    // against the nothing it knows. One path rather than two, so that a tracer
    // built while a library is open describes it exactly as it would describe
    // one opened a moment later.
    note_objects_changed();
}

inline buffer_group& trace_buffers::rotate_for(event_level level,
                                              [[maybe_unused]] std::size_t args_size) {
    buffer_group& group = groups_[static_cast<std::size_t>(level)];
    assert(args_size + max_record_header_size <= group.buffer_size() &&
           "record larger than one trace buffer");

    // The timestamp source is a macro and may be overridden by the translation
    // unit recording the event. Keep this cold path here, beside the hot path,
    // so a rotation does not accidentally fall back to tracer.cc's clock.
    group.rotate();
    // Rebased whether or not a sync record follows, because the delta of the
    // record below has to fit a vint either way. What a missing sync costs is a
    // reader's ability to say where the new buffer's chain starts, not the
    // ring's ability to write one.
    group.rebase(TRACER_TIMESTAMP());

    // And that is what the metadata ring pays. Its stream opens with a fixed
    // run of records -- a count and that many load events -- which a decoder
    // reads by position, because until it has read them it has no mappings and
    // so no way to tell one record from another. A sync record written into the
    // middle of that run, which is where a rotation puts it when the prologue
    // is longer than a buffer, is a record the decoder cannot recognise and
    // cannot skip. So the metadata ring gets its opening sync from the
    // constructor and none afterwards, and a metadata event past the first
    // rotation is placed relative to a base nothing wrote down. Making it
    // recognisable is what a static id on clock_sync would buy; see "static
    // ids" above.
    if (level != event_level::metadata) {
        write_clock_sync(level);
    }
    return group;
}

inline std::byte* trace_buffers::write_slow(event_level level, std::uint64_t id_word,
                                            std::size_t id_size, std::size_t args_size) {
    return rotate_for(level, args_size)
        .write_record(args_size, id_word, id_size, TRACER_TIMESTAMP());
}

// The untimed record inherits the moment of the record before it, and after a
// rotation that record is the sync above -- so it is stamped with the tick
// count the fresh buffer was rebased to, which is the closest thing to the
// truth a record carrying no time of its own can be given.
inline std::byte* trace_buffers::write_slow_untimed(event_level level, std::uint64_t id_word,
                                                    std::size_t id_size, std::size_t args_size) {
    return rotate_for(level, args_size).write_record_untimed(args_size, id_word, id_size);
}

inline void trace_buffers::write_clock_sync(event_level level, bool always) {
    // syncing_ because the record below goes through this tracer's own write(),
    // which is the function that calls this one. It cannot rotate a buffer it
    // has just been given -- a sync record is a few dozen bytes -- but a ring
    // whose buffers were sized smaller than one record would recurse forever,
    // and that is not a stack to overflow to find out about.
    if (syncing_ || (!always && !clock_sync_enabled())) {
        return;
    }
    syncing_ = true;
    trace_buffers* const previous = local_tracer;
    local_tracer = this;

    // The absolute tick count, as a parameter rather than as this record's own
    // header. Every header on the wire is a delta from the record before it,
    // and the first record of a buffer has nothing before it -- so the ring is
    // rebased here, and the value it was rebased to is written down where a
    // decoder can read it. `tsc` first, because that is where a decoder looks
    // for it before it has read the rest.
    const std::uint64_t tsc = TRACER_TIMESTAMP();
    groups_[static_cast<std::size_t>(level)].rebase(tsc);
    TRACER_UNGATED_TRACEPOINT(level, "clock_sync", ::tracer::timestamp_encoding::sync, "tsc",
                              tsc, "realtime_ns",
                              static_cast<std::uint64_t>(TRACER_REALTIME_NS()),
                              "ticks_per_second", tsc_ticks_per_second());
    local_tracer = previous;
    syncing_ = false;
}

inline void trace_buffers::note_objects_changed() {
    const std::vector<trace_object> objects = trace_objects();
    // Two objects are the same load if they are the same file at the same
    // address. The base rather than the table address, because an object may
    // have no tracepoint table at all -- and because the base is the thing that
    // is unique per load, a table being one section inside it.
    const auto same = [](const known_object& a, const trace_object& b) {
        return a.build_id == b.build_id && a.base_address == b.base_address;
    };

    // The records below go through TRACEPOINT_UNGATED(), which writes to
    // whichever tracer is installed -- so this one is, for as long as it takes
    // to describe itself. Restoring rather than clearing: a thread describing a
    // second tracer keeps the one it was recording into.
    trace_buffers* const previous = local_tracer;
    local_tracer = this;

    if (!described_) {
        // How many load events follow, which is what lets a decoder read them
        // before it can read anything by address. See "the metadata stream".
        TRACEPOINT_UNGATED(event_level::metadata, "trace_objects_loaded", "count",
                           static_cast<std::uint32_t>(objects.size()));
        described_ = true;
    }

    // Unloads first, so that an object mapped over the range of the one that
    // went is a load over a range this ring has already given up -- which is the
    // order it happened in, and the only order a decoder can read.
    for (auto it = known_.begin(); it != known_.end();) {
        if (std::any_of(objects.begin(), objects.end(),
                        [&](const trace_object& o) { return same(*it, o); })) {
            ++it;
            continue;
        }
        TRACEPOINT_UNGATED(event_level::metadata, "trace_object_unloaded", "build_id",
                           std::string_view(it->build_id), "base_address",
                           static_cast<std::uint64_t>(it->base_address));
        it = known_.erase(it);
    }

    for (const trace_object& object : objects) {
        if (std::any_of(known_.begin(), known_.end(),
                        [&](const known_object& o) { return same(o, object); })) {
            continue;
        }
        TRACEPOINT_UNGATED(event_level::metadata, "trace_object_loaded", "build_id",
                           std::string_view(object.build_id), "table_address",
                           static_cast<std::uint64_t>(object.table_address), "base_address",
                           static_cast<std::uint64_t>(object.base_address), "mapping_size",
                           static_cast<std::uint64_t>(object.mapping_size));
        known_.push_back({object.build_id, object.base_address});
    }

    local_tracer = previous;
}

}  // namespace tracer
