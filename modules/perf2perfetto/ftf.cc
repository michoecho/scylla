#include "perf2perfetto/ftf.h"

#include <algorithm>
#include <bit>
#include <cstring>

namespace perf2perfetto::ftf {
namespace {

// Fuchsia trace record types, from the low 4 bits of a record header.
constexpr uint64_t kRecordMetadata = 0;
constexpr uint64_t kRecordString = 2;
constexpr uint64_t kRecordThread = 3;
constexpr uint64_t kRecordEvent = 4;

// Event record subtypes.
constexpr uint64_t kEventDurationBegin = 2;
constexpr uint64_t kEventDurationEnd = 3;
constexpr uint64_t kEventDurationComplete = 4;

// Argument record types.
constexpr uint64_t kArgUint64 = 4;
constexpr uint64_t kArgString = 6;

// A string longer than this cannot be described by a string record's 15-bit
// length field, so it is truncated rather than corrupting the stream.
constexpr size_t kMaxStringLen = 32000;

static_assert(std::endian::native == std::endian::little,
              "the trace format is little-endian; a big-endian host would have to swap");

void write_u64(std::ostream& w, uint64_t x) {
    w.write(reinterpret_cast<const char*>(&x), sizeof(x));
}

// Every record is a whole number of 8-byte words, so string payloads are
// padded out with zeros.
void write_string(std::ostream& w, std::string_view s) {
    w.write(s.data(), static_cast<std::streamsize>(s.size()));
    if (s.size() % 8 != 0) {
        static constexpr char kZeros[8] = {};
        w.write(kZeros, static_cast<std::streamsize>(8 - s.size() % 8));
    }
}

uint64_t words_for_bytes(size_t x) {
    return (x + 7) / 8;
}

void write_string_record(std::ostream& w, uint64_t index, std::string_view s) {
    const size_t len = std::min(s.size(), kMaxStringLen);
    const uint64_t rsize = 1 + words_for_bytes(len);
    write_u64(w, kRecordString | rsize << 4 | index << 16 | static_cast<uint64_t>(len) << 32);
    write_string(w, s.substr(0, len));
}

void write_thread_record(std::ostream& w, uint64_t index, ThreadId thread) {
    const uint64_t rsize = 3;
    write_u64(w, kRecordThread | rsize << 4 | index << 16);
    write_u64(w, thread.pid);
    write_u64(w, thread.tid);
}

std::string_view internal_string(InternalString s) {
    switch (s) {
        case InternalString::Empty:
            return "";
        case InternalString::Instructions:
            return "Instructions";
        case InternalString::Cycles:
            return "Cycles";
        case InternalString::Footprint:
            return "Footprint";
        case InternalString::Symbol:
            return "Symbol";
        case InternalString::Timespan:
            return "Timespan";
        case InternalString::Count:
            break;
    }
    return "";
}

struct EventHeader {
    std::string_view name;
    std::string_view category;
    ThreadId thread;
    uint64_t timestamp = 0;
    uint8_t nargs = 0;
    uint8_t etype = 0;
    // Words the event's arguments occupy, beyond the two words written here.
    size_t extra_data_size = 0;
};

void write_event_header(std::ostream& w, Caches& c, const EventHeader& e) {
    const uint64_t rsize = 2 + e.extra_data_size;
    // The string records these may emit have to precede the event that
    // references them, so they are resolved before the header word is written.
    const uint64_t name_ref = c.strings.get_ref(w, e.name);
    const uint64_t category_ref = c.strings.get_ref(w, e.category);
    const uint64_t thread_ref = c.threads.get_ref(w, e.thread);
    write_u64(w, kRecordEvent | rsize << 4 | static_cast<uint64_t>(e.etype) << 16 |
                     static_cast<uint64_t>(e.nargs) << 20 | thread_ref << 24 |
                     category_ref << 32 | name_ref << 48);
    write_u64(w, e.timestamp);
}

// The counters attached to every closed frame: three integers plus the
// human-readable time span.
void write_info_args(std::ostream& w, uint64_t insns, uint64_t cycles, uint64_t footprint,
                     std::string_view timespan) {
    write_u64(w, kArgUint64 | 2 << 4 |
                     static_cast<uint64_t>(InternalString::Instructions) << 16);
    write_u64(w, insns);

    write_u64(w, kArgUint64 | 2 << 4 | static_cast<uint64_t>(InternalString::Cycles) << 16);
    write_u64(w, cycles);

    write_u64(w, kArgUint64 | 2 << 4 | static_cast<uint64_t>(InternalString::Footprint) << 16);
    write_u64(w, footprint);

    const uint64_t ts_size = 1 + words_for_bytes(timespan.size());
    // Bit 47 marks the string as inline rather than a table reference: the
    // span differs for every frame, so interning it would only churn the
    // string table.
    write_u64(w, kArgString | ts_size << 4 |
                     static_cast<uint64_t>(InternalString::Timespan) << 16 |
                     static_cast<uint64_t>(timespan.size()) << 32 | uint64_t{1} << 47);
    write_string(w, timespan);
}

// Number of arguments write_info_args() writes, and the words they occupy.
std::pair<uint8_t, size_t> info_nargs_size(std::string_view timespan) {
    return {4, 7 + words_for_bytes(timespan.size())};
}

// Nanoseconds as `1234.567890000`, right-aligned in buf[0..len). Returns the
// number of characters written, which end at buf[len - 1].
size_t print_timestamp(char* buf, size_t len, uint64_t nanos) {
    size_t i = 0;
    while (i < 9) {
        buf[len - i - 1] = static_cast<char>('0' + nanos % 10);
        nanos /= 10;
        ++i;
    }
    buf[len - i - 1] = '.';
    ++i;
    // At least one digit before the point, and then as many as the value needs.
    while (i < 11 || nanos > 0) {
        buf[len - i - 1] = static_cast<char>('0' + nanos % 10);
        nanos /= 10;
        ++i;
    }
    return i;
}

}  // namespace

uint64_t StringCache::get_ref(std::ostream& w, std::string_view s) {
    if (s.empty()) {
        return static_cast<uint64_t>(InternalString::Empty);
    }
    const std::string key(s);
    if (auto index = lru_.touch(key)) {
        return *index + kReserved;
    }
    const uint64_t index = lru_.insert(key) + kReserved;
    write_string_record(w, index, s);
    return index;
}

uint64_t ThreadCache::get_ref(std::ostream& w, ThreadId thread) {
    if (auto index = lru_.touch(thread)) {
        return *index + kReserved;
    }
    const uint64_t index = lru_.insert(thread) + kReserved;
    write_thread_record(w, index, thread);
    return index;
}

std::string_view print_timespan(char (&buf)[48], uint64_t start, uint64_t end) {
    const size_t len_end = print_timestamp(buf, sizeof(buf), end);
    buf[sizeof(buf) - len_end - 1] = ',';
    const size_t len_start = print_timestamp(buf, sizeof(buf) - len_end - 1, start);
    const size_t total = len_start + 1 + len_end;
    return {buf + sizeof(buf) - total, total};
}

void write_header(std::ostream& w) {
    // Magic number, which also pins the format's word order.
    write_u64(w, 0x0016547846040010ULL);

    // Provider info metadata. There is only ever one provider.
    constexpr uint64_t kProviderId = 0;
    {
        const uint64_t mtype = 1;
        const std::string_view name = "scylla";
        const uint64_t rsize = 1 + words_for_bytes(name.size());
        write_u64(w, kRecordMetadata | rsize << 4 | mtype << 16 | kProviderId << 20 |
                         static_cast<uint64_t>(name.size()) << 52);
        write_string(w, name);
    }

    // Provider section metadata.
    {
        const uint64_t mtype = 2;
        const uint64_t rsize = 1;
        write_u64(w, kRecordMetadata | rsize << 4 | mtype << 16 | kProviderId << 20);
    }

    // The reserved head of the string table. Index 0 is the empty string,
    // which needs no record.
    for (uint64_t i = 1; i < static_cast<uint64_t>(InternalString::Count); ++i) {
        write_string_record(w, i, internal_string(static_cast<InternalString>(i)));
    }
}

void write_frame_start(std::ostream& w, Caches& c, uint64_t timestamp, ThreadId thread,
                       std::string_view symbol) {
    write_event_header(w, c,
                       EventHeader{
                           .name = symbol,
                           .category = "Misc",
                           .thread = thread,
                           .timestamp = timestamp,
                           .nargs = 0,
                           .etype = kEventDurationBegin,
                           .extra_data_size = 0,
                       });
}

void write_frame_end(std::ostream& w, Caches& c, uint64_t timestamp, ThreadId thread,
                     uint64_t insns, uint64_t cycles, uint64_t footprint, uint64_t ts_start,
                     uint64_t ts_end) {
    char buf[48];
    const std::string_view ts = print_timespan(buf, ts_start, ts_end);
    const auto [nargs, args_size] = info_nargs_size(ts);
    write_event_header(w, c,
                       EventHeader{
                           .name = "",
                           .category = "Misc",
                           .thread = thread,
                           .timestamp = timestamp,
                           .nargs = nargs,
                           .etype = kEventDurationEnd,
                           .extra_data_size = args_size,
                       });
    write_info_args(w, insns, cycles, footprint, ts);
}

void write_frame_full(std::ostream& w, Caches& c, uint64_t timestamp, ThreadId thread,
                      uint64_t insns, uint64_t cycles, uint64_t footprint,
                      std::string_view symbol, uint64_t end_timestamp, uint64_t ts_start,
                      uint64_t ts_end) {
    char buf[48];
    const std::string_view ts = print_timespan(buf, ts_start, ts_end);
    const auto [nargs, args_size] = info_nargs_size(ts);
    write_event_header(w, c,
                       EventHeader{
                           .name = symbol,
                           .category = "Misc",
                           .thread = thread,
                           .timestamp = timestamp,
                           .nargs = nargs,
                           .etype = kEventDurationComplete,
                           // The trailing end timestamp, on top of the args.
                           .extra_data_size = 1 + args_size,
                       });
    write_info_args(w, insns, cycles, footprint, ts);
    write_u64(w, end_timestamp);
}

}  // namespace perf2perfetto::ftf
