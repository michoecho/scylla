#pragma once

// Writer for the Fuchsia trace format (.ftf), the binary format the Perfetto
// UI reads directly.
//
// A trace is a stream of 8-byte little-endian words. Every record starts with
// a header word whose low 4 bits are the record type and whose next 12 bits
// are the record's total size in words, so a reader can skip a record it does
// not understand. Strings and thread ids are not repeated inside events;
// they are interned once into small tables (see Caches) and referenced by
// index afterwards, which is what keeps a per-call trace of a whole program
// down to a manageable size.
//
// Only the handful of records a call trace needs are implemented: the header,
// string and thread records, and duration begin/end/complete events.

#include <cstddef>
#include <cstdint>
#include <list>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

namespace perf2perfetto::ftf {

// A process/thread pair, the unit a trace track is keyed by.
struct ThreadId {
    uint64_t pid = 0;
    uint64_t tid = 0;

    bool operator==(const ThreadId&) const = default;
};

struct ThreadIdHash {
    size_t operator()(const ThreadId& t) const {
        // The two halves are hashed together rather than xor'd, so that the
        // common (pid == tid) main-thread case does not collapse to zero.
        return std::hash<uint64_t>{}(t.pid) * 1099511628211u ^ std::hash<uint64_t>{}(t.tid);
    }
};

// An index-assigning table with a fixed number of slots.
//
// The trace format's tables are small (16-bit string indices, 8-bit thread
// indices), so a long trace inevitably runs out of slots. Reusing the least
// recently used one keeps the entries that are still being referenced: the
// record for a reused slot is simply written again, which redefines it from
// that point on.
template <class K, class V, class Hash = std::hash<K>>
class LruTable {
public:
    explicit LruTable(size_t capacity) : capacity_(capacity) {}

    // The index already assigned to `key`, if any, marking it most recently
    // used.
    std::optional<V> touch(const K& key) {
        auto it = map_.find(key);
        if (it == map_.end()) {
            return std::nullopt;
        }
        order_.splice(order_.end(), order_, it->second.second);
        return it->second.first;
    }

    // Assign an index to `key`, taking a fresh slot while any are left and
    // otherwise recycling the least recently used one.
    V insert(const K& key) {
        V index;
        if (map_.size() < capacity_) {
            index = static_cast<V>(map_.size());
        } else {
            const K& lru = order_.front();
            index = map_.at(lru).first;
            map_.erase(lru);
            order_.pop_front();
        }
        auto it = order_.insert(order_.end(), key);
        map_.emplace(key, std::pair{index, it});
        return index;
    }

private:
    size_t capacity_;
    // Least recently used at the front, most recently used at the back.
    std::list<K> order_;
    std::unordered_map<K, std::pair<V, typename std::list<K>::iterator>, Hash> map_;
};

// The strings the writer defines for itself, before any string from the trace.
// They occupy the first indices of the string table and are written once by
// write_header().
enum class InternalString : uint64_t {
    Empty = 0,
    Instructions = 1,
    Cycles = 2,
    Footprint = 3,
    Symbol = 4,
    Timespan = 5,
    Count = 6,
};

class StringCache {
public:
    StringCache() : lru_(kTableSize) {}

    // The table index to reference `s` by, writing its string record first if
    // this is the first time (or the first since eviction) that it is used.
    uint64_t get_ref(std::ostream& w, std::string_view s);

private:
    static constexpr uint64_t kReserved = static_cast<uint64_t>(InternalString::Count);
    static constexpr size_t kTableSize = 32 * 1024 - kReserved;

    LruTable<std::string, uint16_t, std::hash<std::string>> lru_;
};

class ThreadCache {
public:
    ThreadCache() : lru_(kTableSize) {}

    uint64_t get_ref(std::ostream& w, ThreadId thread);

private:
    // Index 0 is reserved for "no thread".
    static constexpr uint64_t kReserved = 1;
    static constexpr size_t kTableSize = 256 - kReserved;

    LruTable<ThreadId, uint8_t, ThreadIdHash> lru_;
};

struct Caches {
    StringCache strings;
    ThreadCache threads;
};

// Format a pair of nanosecond timestamps as `1234.567890000,2345.678912340`,
// into `buf`. The returned view points into `buf`.
//
// This is the human-readable span shown as an event argument; the trace's own
// timestamps are the raw numbers the timestamp mode selected.
std::string_view print_timespan(char (&buf)[48], uint64_t start, uint64_t end);

// The magic number and the provider/section metadata every trace opens with,
// followed by the internal string table above.
void write_header(std::ostream& w);

// A duration begin event: the frame is open from `timestamp` until a matching
// write_frame_end() on the same thread.
void write_frame_start(std::ostream& w, Caches& c, uint64_t timestamp, ThreadId thread,
                       std::string_view symbol);

// A duration end event, carrying the counters accumulated while the frame was
// open. The name comes from the matching begin event, so it is not repeated.
void write_frame_end(std::ostream& w, Caches& c, uint64_t timestamp, ThreadId thread,
                     uint64_t insns, uint64_t cycles, uint64_t footprint, uint64_t ts_start,
                     uint64_t ts_end);

// A duration complete event: one record carrying both ends of a frame. Used
// for a frame whose start was never seen, which therefore has no begin event
// to be matched with.
void write_frame_full(std::ostream& w, Caches& c, uint64_t timestamp, ThreadId thread,
                      uint64_t insns, uint64_t cycles, uint64_t footprint,
                      std::string_view symbol, uint64_t end_timestamp, uint64_t ts_start,
                      uint64_t ts_end);

}  // namespace perf2perfetto::ftf
