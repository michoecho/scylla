#include <implot.h>
#include <imgui/imgui.h>
#include <imgui/backends/imgui_impl_sdl3.h>
#include <imgui/backends/imgui_impl_opengl3.h>
#include <SDL3/SDL.h>
#include <SDL3/SDL_opengl.h>
#include <algorithm>
#include <stdio.h>
#include <math.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <span>
#include <functional>
#include <chrono>
#include <thread>
#include <vector>
#include <cerrno>
#include <system_error>
#include <fmt/core.h>
#include <fmt/ranges.h>
#include <cinttypes>
#include <cstdint>
#include <ctime>
#include <optional>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <map>
#include <set>
#include <array>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <limits>
#include "address_decoder/address_decoder.h"
#include "decoder.h"

// Nanoseconds per rdtsc tick.  A fallback: the rate of the machine the original
// experiment ran on, used only for a trace that carries no clock_sync records
// at all.  A trace that does carries a better one -- see wall_clock below --
// measured over the trace itself, and main() installs that over this.
double MULTIPLIER = 0.2941171840072451;

// --- turning ticks into wall clock times --------------------------------------
//
// A record's timestamp is an rdtsc reading: a tick count, which is a duration
// away from another tick count and nothing at all on its own.  What dates it is
// the clock_sync record -- a tick count (its own header timestamp) beside a
// wall clock reading, plus the ticks-per-second the process believed in -- which
// the tracer writes into the head of every ring and again at every rotation.
//
// This is the two-pass consumer that "reading a sync record back" in
// modules/tracer/include/tracer/tracer.h asks for.  Pass one is load_trace(),
// which collects every sync record as it decodes; pass two is realtime_ns()
// below, which converts a record against the syncs on *either side* of it.
// Between two syncs that is an interpolation, and its rate is one measured over
// exactly this trace on exactly this machine -- the rate field is not consulted
// at all.  Only outside the outermost pair does it extrapolate with that field,
// which may well be a default nobody ever calibrated.
struct clock_sync_point {
    int64_t ticks;
    uint64_t realtime_ns;
    uint64_t ticks_per_second;
};

// Shards in one process share a clock; nodes do not. Keep the syncs per input
// snapshot so multiple machines can be aligned through CLOCK_REALTIME.
static std::unordered_map<uint32_t, std::vector<clock_sync_point>> clock_syncs;

class wall_clock {
public:
    // Sort and dedupe the syncs collected during the decode.  Ticks are the key:
    // two syncs written at the same tick (one per level, at construction) are one
    // point, and a sync is useless until it can be ordered against the records.
    void build(std::vector<clock_sync_point> points) {
        std::ranges::sort(points, {}, &clock_sync_point::ticks);
        const auto dup = std::ranges::unique(points, {}, &clock_sync_point::ticks);
        points.erase(dup.begin(), dup.end());
        points_ = std::move(points);
    }

    bool empty() const { return points_.empty(); }
    size_t size() const { return points_.size(); }

    // When a record whose timestamp is `ticks` was taken, in nanoseconds since
    // the epoch, or nothing at all if the trace said nothing about its clock.
    //
    // The arithmetic is 128-bit and relative to a sync rather than double: a
    // nanosecond count since 1970 is ~2^61, so a double holds it only to a few
    // hundred nanoseconds -- which is coarser than the column this ends up in.
    std::optional<uint64_t> realtime_ns(int64_t ticks) const {
        if (points_.empty()) {
            return std::nullopt;
        }
        const auto after = std::ranges::lower_bound(points_, ticks, {}, &clock_sync_point::ticks);
        if (after == points_.begin()) {
            // Before the first sync, or exactly on it: extrapolate backwards.
            return extrapolate(points_.front(), ticks);
        }
        const clock_sync_point& before = *(after - 1);
        if (after == points_.end()) {
            return extrapolate(before, ticks);
        }
        // Bracketed, which is the case worth having written this for.
        const __int128 span_ticks = __int128(after->ticks) - before.ticks;
        const __int128 span_ns = __int128(after->realtime_ns) - before.realtime_ns;
        const __int128 offset = (__int128(ticks - before.ticks) * span_ns) / span_ticks;
        return uint64_t(__int128(before.realtime_ns) + offset);
    }

    // The other direction: which tick count a wall clock reading corresponds to.
    //
    // For the one thing in the trace that is dated rather than stamped -- a perf
    // stack sample, whose time comes from the kernel in CLOCK_REALTIME and whose
    // record header is an rdtsc from whenever the poll loop got round to
    // draining it. Converting it back puts the sample among the records it
    // interrupted instead of among the ones that were being written a poll
    // later.
    std::optional<int64_t> ticks_from_realtime(uint64_t ns) const {
        if (points_.empty()) {
            return std::nullopt;
        }
        const auto after =
            std::ranges::lower_bound(points_, ns, {}, &clock_sync_point::realtime_ns);
        if (after == points_.begin()) {
            return extrapolate_ticks(points_.front(), ns);
        }
        const clock_sync_point& before = *(after - 1);
        if (after == points_.end()) {
            return extrapolate_ticks(before, ns);
        }
        const __int128 span_ticks = __int128(after->ticks) - before.ticks;
        const __int128 span_ns = __int128(after->realtime_ns) - before.realtime_ns;
        if (span_ns == 0) {
            return before.ticks;
        }
        const __int128 offset = (__int128(ns - before.realtime_ns) * span_ticks) / span_ns;
        return int64_t(__int128(before.ticks) + offset);
    }

    // What a tick is worth in nanoseconds, for the durations everything else in
    // here measures.  The outermost pair of syncs, because that is the longest
    // baseline the trace offers; the recorded rate if there is only one sync;
    // and nothing if there are none, leaving MULTIPLIER as it was.
    std::optional<double> ns_per_tick() const {
        if (points_.size() >= 2) {
            const auto& first = points_.front();
            const auto& last = points_.back();
            return double(last.realtime_ns - first.realtime_ns) / double(last.ticks - first.ticks);
        }
        if (points_.size() == 1 && points_.front().ticks_per_second != 0) {
            return 1e9 / double(points_.front().ticks_per_second);
        }
        return std::nullopt;
    }

private:
    // Outside the syncs: the recorded rate is all there is, and it is an
    // estimate.  A trace normally has a sync at each end, so this is the path
    // taken only by the handful of records before the opening one.
    static std::optional<int64_t> extrapolate_ticks(const clock_sync_point& sync, uint64_t ns) {
        if (sync.ticks_per_second == 0) {
            return std::nullopt;
        }
        const __int128 offset = ((__int128(ns) - sync.realtime_ns) *
                                 __int128(sync.ticks_per_second)) / 1'000'000'000;
        return int64_t(__int128(sync.ticks) + offset);
    }

    static std::optional<uint64_t> extrapolate(const clock_sync_point& sync, int64_t ticks) {
        if (sync.ticks_per_second == 0) {
            return std::nullopt;
        }
        const __int128 offset =
            (__int128(ticks - sync.ticks) * 1'000'000'000) / __int128(sync.ticks_per_second);
        const __int128 ns = __int128(sync.realtime_ns) + offset;
        if (ns < 0) {
            return std::nullopt;
        }
        return uint64_t(ns);
    }

    std::vector<clock_sync_point> points_;
};

static wall_clock the_clock;

// The sample the "Stack samples" window is showing the backtrace of, and
// whether it was picked somewhere else -- from a SAMPLE line in one of the log
// windows -- and so needs scrolling to. The two windows point at each other, so
// the selection cannot live inside either.
static size_t selected_sample = size_t(-1);

// The state of every address in the backtrace on screen.
//
// Symbolising is asynchronous, so a frame's text is not available in the frame
// that asks for it and the window needs somewhere to remember that it asked.
// Three states, and the transitions run one way only:
//
//   fresh    nothing has been asked about this address -- request it.
//   sent     it is with a worker; look for it in what was reaped this frame.
//   decoded  the answer is in the decoder and frame_line() will render it.
//
// Selecting another sample puts every address back to `fresh`. That is not a
// cache flush -- the decoder still knows every address it has ever answered,
// and re-requesting one it knows is a no-op that resolves on the next frame
// through the `sent` check below. It is just this window's per-row bookkeeping,
// which belongs to the sample being shown and not to the addresses.
enum class frame_state { fresh, sent, decoded };
static std::vector<frame_state> frame_states;
// Which sample frame_states describes; size_t(-1) for none yet.
static size_t frame_states_for = size_t(-1);
static bool sample_needs_scroll = false;

// The width of what format_realtime() returns, so a trace without a clock can
// leave the column blank and keep the rest of the line where it was.
inline constexpr size_t realtime_width = 29;

// "YYYY-MM-DD HH:MM:SS.NNNNNNNNN", in UTC -- which is what Scylla's own logs
// are stamped in, and the only reading of a wall clock that means the same
// thing on the node and on the machine looking at its trace.
static std::string format_realtime(uint64_t ns) {
    const std::time_t seconds = std::time_t(ns / 1'000'000'000ull);
    const uint64_t fraction = ns % 1'000'000'000ull;
    std::tm tm{};
    gmtime_r(&seconds, &tm);
    return fmt::format("{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:09}", tm.tm_year + 1900,
                       tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec, fraction);
}

// The wall clock column for one record: the time, or blanks of the same width
// for a record no sync could date.
static std::string realtime_column(int64_t ticks) {
    if (const auto ns = the_clock.realtime_ns(ticks)) {
        return format_realtime(*ns);
    }
    return std::string(realtime_width, ' ');
}

inline int64_t rdtsc() {
    uint64_t rax, rdx;
    asm volatile ( "rdtsc" : "=a" (rax), "=d" (rdx) );
    return (int64_t)(( rdx << 32 ) + rax);
}

// The viewer's own flat record.
//
// Scylla emits *named* tracepoints now (decoder.h, generated from the very
// binary that produced the trace), not the four raw words this analysis was
// originally written against. The numbers below are what those tracepoints are
// flattened back into, because everything downstream -- the query grouping, the
// io/cpu/starve accounting, the log formatting -- keys off them:
//
//   0  run_task{prev, task}         the reactor started running a task
//   1  cql_request{prev, task}      a CQL frame opened a new request chain
//   4  io_begin{task, io}           a task submitted an I/O
//   5  io_end{task, io}             that I/O completed
//   0xa semaphore_execute{prev, task} the semaphore's loop ran a queued read
//   0xb execution_stage{prev, task} an execution stage ran a queued work item
//   0xc stacktrace_sample             the shard was interrupted for a stack sample
//   0xd prepared_query_run            a prepared query ran; metadata is attached below
//   0xe/f prepared_statement_{added,removed} cache-set deltas
//   0x10..12 prepared-statement snapshot begin, entry, end
//   RPC records are kept in rpc_event rather than this task-oriented list.
//
// (0x3, the admission decision of the original experiment, is not emitted by
// this build; the formatter for it is left in place.)
struct entry {
    uint64_t event;
    uint64_t id;
    uint64_t arg;
    int64_t ts;

    // Where the task this record is about was created, as an index into
    // locations() below. Zero is "none" -- most events carry no location at all,
    // and a task nobody gave a resume point to does not either.
    uint32_t loc = 0;

    // Which shard's file this came out of. Task *ids* carry a shard in their top
    // bits, but that is the shard that minted the id and not the one running it
    // -- a continuation inherits its id across a cross-shard hop -- so the only
    // honest answer to "which cpu was this on" is which file it was in. Stack
    // samples are matched to tasks per shard, and that is what needs it.
    uint32_t shard = 0;
    uint32_t node = 0;

    // Prepared-query metadata is copied out of the trace buffer while it is
    // still alive. Runs acquire the two text fields during the reverse info
    // pass below; delta and snapshot records carry them already.
    std::string prepared_id;
    std::string prepared_keyspace;
    std::string prepared_statement;

    // Which request this record belongs to. For a *switch* that is the task
    // being switched to; for everything else, the task it happened under.
    uint64_t query() const {
        if (event == 0 || event == 1 || event == 0xa || event == 0xb || event == 0x13) {
            return arg;
        } else {
            return id;
        }
    }
};

// Whether a record is a *switch*: one of the events whose `arg` is the task the
// shard is running from here on. The last one at or before a time says who was
// on that cpu then, and the next one says when it came off -- which is what the
// plot's tooltip and its highlight are built from.
static bool is_switch(const entry& e) {
    return e.event == 0 || e.event == 1 || e.event == 0xa || e.event == 0xb || e.event == 0x13;
}

// Scylla's task counter is process-local.  When several node snapshots are
// loaded, the same numeric task id can therefore occur independently on every
// node, so the viewer adds a namespace of its own.
//
// It goes in the top byte, which a task id leaves free: seastar seeds the
// counter at `(this_shard_id() << 48) | 1`, so bits 48-55 are the shard and
// everything above is spare while a process has fewer than 256 shards.  An id
// then still reads as what it is -- `0101000000000101` is node 1, shard 1,
// counter 0x101 -- which a hash of the same two numbers does not, and node 0
// keeps its ids byte for byte.
//
// Above 255 shards the shard would run into the byte, and there is nothing
// clever to do about it: fall back to mixing, and say so once, because ids that
// silently collided across nodes would merge two requests into one.
static uint64_t namespace_task(uint32_t node, uint64_t task) {
    if (node == 0 || task == 0) {
        return task;
    }
    if ((task >> 56) == 0 && node < 256) {
        return (uint64_t(node) << 56) | task;
    }
    static bool warned = false;
    if (!warned) {
        warned = true;
        fmt::print("task ids use the top byte, so node namespaces are hashed and"
                   " will not read as node/shard/counter\n");
    }
    uint64_t x = task ^ (uint64_t(node) * 0x9e3779b97f4a7c15ULL);
    x ^= x >> 30;
    x *= 0xbf58476d1ce4e5b9ULL;
    x ^= x >> 27;
    x *= 0x94d049bb133111ebULL;
    x ^= x >> 31;
    return x ? x : 1;
}

enum class rpc_event_kind { connection_open, connection_close, message_sent,
                            message_received, reply_sent, reply_received,
                            snapshot_entry, request_handled };

struct rpc_event {
    rpc_event_kind kind;
    uint64_t connection = 0;
    uint64_t sequence = 0;
    int64_t msg_id = 0;
    // The task this record is *about*, which for the two send records and for
    // request_handled is not the task that emitted it: see the comment on the
    // RPC tracepoints in seastar/include/seastar/core/scylla_tracer.hh. Zero on
    // the records that have no such task -- a receive is read by the connection's
    // receive loop and there is nothing better to say about it.
    uint64_t task = 0;
    int64_t ts = 0;
    uint32_t shard = 0;
    uint32_t node = 0;
    std::string local;
    std::string remote;
    // Who the far end said it was, in the RPC handshake: its boot id's two
    // words and the shard the connection landed on. Zero when the peer does not
    // speak PEER_IDENTITY, or on a record kind that does not carry it.
    uint64_t peer_boot_msb = 0;
    uint64_t peer_boot_lsb = 0;
    uint32_t peer_shard = 0;
};

// The decoded source locations, interned.
//
// run_task carries one per record and the same call site turns up thousands of
// times -- every continuation the reactor runs off one `then()` -- so the entries
// hold an index into this and not a string. Index 0 is the empty location, which
// is what an unlocated event and a task with no resume point both get.
static std::vector<std::string> location_strings{""};

// The same locations with the function kept, for the one reader that has room
// for it: the plot's tooltip. The log line deliberately drops it -- see below --
// but "which function was this continuation created in" is most of what somebody
// hovering a bar wants to know.
static std::vector<std::string> location_details{""};

// How the interning went, printed on the way in beside the other counts. A
// directory of objects that is missing, stripped of the wrong thing, or simply
// not the build the trace came from shows up here as every location unresolved,
// which is far easier to read than a window full of `<unresolved 0x...>`.
static size_t locations_resolved = 0;
static size_t locations_unresolved = 0;

static uint32_t intern_location(const trace::source_location& loc) {
    if (!loc.resolved && loc.address == 0) {
        return 0;
    }
    // By address, because that is the identity of a location -- two records of
    // the same call site are the same word -- and it is one integer compare
    // instead of a string one.
    static std::unordered_map<uint64_t, uint32_t> seen;
    const auto [it, fresh] = seen.emplace(loc.address, uint32_t(location_strings.size()));
    if (fresh) {
        // Just the tail of the path and the function: the log line this ends up
        // on is already wide, and "reactor.cc:1234" is what identifies a call
        // site to someone reading it.
        std::string file = loc.file;
        if (const auto slash = file.rfind('/'); slash != std::string::npos) {
            file = file.substr(slash + 1);
        }
        (loc.resolved ? locations_resolved : locations_unresolved) += 1;
        location_strings.push_back(loc.resolved
                                       ? fmt::format("{}:{}", file, loc.line)
                                       : loc.to_string());
        location_details.push_back(loc.resolved
                                       ? fmt::format("{}:{}  {}", file, loc.line,
                                                     loc.function.empty() ? "?" : loc.function)
                                       : loc.to_string());
    }
    return it->second;
}

static const std::string& location_string(uint32_t index) {
    return location_strings[index];
}

static const std::string& location_detail(uint32_t index) {
    return location_details[index];
}


// --- stack samples ------------------------------------------------------------
//
// Every shard interrupts itself 100 times a second and records where it was, as
// a run of return addresses walked off the frame pointers -- see
// seastar/include/seastar/core/scylla_stacktrace_sampler.hh. An address is not
// a function name and the trace does not carry one: what is at an address is in
// the object it points into, exactly as for a srcloc::location, and turning it
// into something readable is this program's job and llvm-addr2line's.
//
// That is done *lazily*, when a sample is clicked. A minute of a two-shard node
// is twelve thousand samples of a couple of dozen frames each, and symbolising
// all of them at startup would be a quarter of a million addr2line lookups for
// the handful anybody will ever look at.

struct stack_sample {
    // The kernel's timestamp, in CLOCK_REALTIME nanoseconds -- the domain the
    // clock_sync records pair rdtsc with, which is the whole reason the perf
    // event is opened with that clockid.
    uint64_t realtime_ns = 0;
    // The same moment as a tick count, so the sample sorts among the records it
    // interrupted. See wall_clock::ticks_from_realtime().
    int64_t ts = 0;
    uint32_t shard = 0;
    uint32_t node = 0;
    // The task that was on the cpu, filled in below by walking the merged
    // timeline; zero if the shard was between tasks or the trace does not reach
    // back far enough to say.
    uint64_t task = 0;
    // Innermost first, as perf gave them: the sampled pc, then one return
    // address per frame above it.
    std::vector<uint64_t> frames;
};

static std::vector<stack_sample> samples;

// The objects the trace said were mapped, and where their files are. Both are
// filled in by main() once the traces are read; a viewer with no dsos/ directory
// simply has no paths and every frame stays an address.
static std::vector<trace::object_mapping> object_mappings;
static trace::dso_directory* the_dsos = nullptr;

// Symbolising, which is done on worker threads and never on this one.
//
// Every address in the sample list goes through modules/address-decoder: one
// persistent llvm-symbolizer per object, on a thread of its own, answering into
// a queue this loop drains once a frame. The header there has the reasoning;
// the short version is that spawning addr2line per click is one to two seconds
// on Scylla's binary, all of it spent indexing the same debug info again, and a
// backtrace that appears a second after the click is a backtrace nobody waits
// for.
//
// The consequence for everything below is that a decoded frame is *not*
// available in the frame that asked for it. There is no blocking call to fall
// back on: the window draws what it has, marks what it has asked about, and
// picks the answers up whenever they land.
static std::unique_ptr<addrdec::address_decoder> the_decoder;

// The address to look an entry of `frames` up at.
//
// A frame above the innermost is a *return* address -- the instruction after
// the call -- and looking it up as it stands attributes the call to whatever
// follows it, which for a call in tail position is the next function
// altogether. One byte back is inside the call instruction, which is what every
// unwinder does and what makes the line numbers right.
static uint64_t frame_key(const stack_sample& sample, size_t i) {
    return i == 0 ? sample.frames[i] : sample.frames[i] - 1;
}

// Queue one address, mapping it to the object it falls in first.
//
// An address in no known object, or in one the dsos/ directory does not have,
// is still handed to the decoder with an empty path: it answers those itself,
// immediately and as unresolved. Dropping them here instead would leave the
// caller's state machine stuck in "sent" for exactly the frames that are most
// likely to be unresolvable.
static void request_frame(uint64_t key) {
    addrdec::decode_request request;
    request.address = key;
    if (const trace::object_mapping* const in = trace::mapping_of(object_mappings, key)) {
        if (the_dsos != nullptr) {
            request.object_path = the_dsos->path(in->build_id);
            // The offset within the object's own address space, which is what
            // its ELF file is written in -- the same subtraction the decoder
            // does to read a source location.
            request.file_offset = key - in->base;
        }
    }
    the_decoder->request(request);
}

// One rendered backtrace line: the address, and whatever is known about it.
static std::string frame_line(const stack_sample& sample, size_t i, bool decoded) {
    const uint64_t key = frame_key(sample, i);
    const addrdec::decoded_address* const answer = the_decoder->lookup(key);
    if (!decoded || answer == nullptr) {
        return fmt::format("#{:<3} {:#018x}  ...", i, sample.frames[i]);
    }
    const std::string text = answer->to_string();
    return text.empty() ? fmt::format("#{:<3} {:#018x}", i, sample.frames[i])
                        : fmt::format("#{:<3} {:#018x}  {}", i, sample.frames[i], text);
}

// Ask for every frame of a sample and wait for the answers.
//
// Only for the headless path: the window must never do this, which is the whole
// point of the module. Bounded, because a symbolizer that never answers would
// otherwise hang a command whose job is to diagnose exactly that.
static std::vector<std::string> sample_backtrace_blocking(size_t index) {
    const stack_sample& sample = samples[index];
    for (size_t i = 0; i < sample.frames.size(); ++i) {
        request_frame(frame_key(sample, i));
    }
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(120);
    while (the_decoder->outstanding() != 0 && std::chrono::steady_clock::now() < deadline) {
        the_decoder->reap();
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    the_decoder->reap();

    std::vector<std::string> out;
    out.reserve(sample.frames.size());
    for (size_t i = 0; i < sample.frames.size(); ++i) {
        out.push_back(frame_line(sample, i, true));
    }
    return out;
}

// The log line a sample gets, which must not symbolise anything: it is built for
// every sample in a task's log, and symbolising is what clicking one is for.
static std::string sample_message(uint64_t index) {
    const stack_sample& sample = samples[index];
    return fmt::format("{:10s} {} frames from {:#x}", "SAMPLE", sample.frames.size(),
                       sample.frames.empty() ? 0 : sample.frames.front());
}

static std::string prepared_id_string(const entry& e) {
    std::string out;
    out.reserve(e.prepared_id.size() * 2);
    for (const unsigned char c : e.prepared_id) {
        out += fmt::format("{:02x}", c);
    }
    return out;
}

static std::string entry_message(const entry& e) {
    switch (e.event) {
    case 0: return fmt::format("{:10s} {}", "SWITCH", location_string(e.loc));
    case 1: return "START";
    case 0xa: return "PERMIT";
    case 0xb: return "ES";
    case 0x3: {
        const char* rcs_status[] = {
            "admitted immediately",
            "queued because of non-empty ready",
            "queued because of used permits",
            "queued because of memory resources",
            "queued because of count resources",
        };
        return fmt::format("{:10s} {}", "RCS", rcs_status[e.arg]);
    }
    case 0xc: return sample_message(e.arg);
    case 0x4: return fmt::format("{:10s} {:16x}", "IO_BEGIN", e.arg);
    case 0x5: return fmt::format("{:10s} {:16x}", "IO_END", e.arg);
    case 0xd:
        return fmt::format("{:10s} {}{} [id={}]", "PREPARED",
                           e.prepared_keyspace.empty() ? "" : e.prepared_keyspace + ".",
                           e.prepared_statement.empty() ? "<unknown>" : e.prepared_statement,
                           prepared_id_string(e));
    case 0xe:
        return fmt::format("{:10s} {}{} [id={}]", "PREP_ADD",
                           e.prepared_keyspace.empty() ? "" : e.prepared_keyspace + ".",
                           e.prepared_statement, prepared_id_string(e));
    case 0xf:
        return fmt::format("{:10s} {}{} [id={}]", "PREP_REMOVE",
                           e.prepared_keyspace.empty() ? "" : e.prepared_keyspace + ".",
                           e.prepared_statement, prepared_id_string(e));
    case 0x10: return "PREP_SNAPSHOT_BEGIN";
    case 0x11: return fmt::format("{:10s} {}{} [id={}]", "PREP_ENTRY",
                                  e.prepared_keyspace.empty() ? "" : e.prepared_keyspace + ".",
                                  e.prepared_statement, prepared_id_string(e));
    case 0x12: return "PREP_SNAPSHOT_END";
    case 0x13: return fmt::format("{:10s} from {:16x}", "RPC_HANDLE", e.id);
    default: return fmt::format("UNKNOWN ({})", e.event);
    }
}

struct cached_log_line {
    size_t source_index;
    entry record;
    std::string text;
};

struct log_cache {
    uint64_t task_id = 0;
    int threshold = -1;
    size_t item_count = 0;
    size_t source_begin = 0;
    std::vector<cached_log_line> lines;
};

struct cached_plot_item {
    ImPlotPoint min;
    ImPlotPoint max;
    ImPlotPoint line_end;
    ImU32 color;
    bool draw_line;
};

struct full_log_cache {
    uint64_t task_id = 0;
    int threshold = -1;
    size_t task_count = 0;
    size_t source_begin = 0;
    int64_t start_ts = 0;
    int64_t end_ts = 0;
    std::vector<cached_log_line> lines;
};

static std::string log_line_text(const entry& e, int64_t start_ts, bool include_task_id) {
    auto dt_nano = std::chrono::duration<double, std::nano>(double(e.ts - start_ts) * MULTIPLIER);
    auto dt = std::chrono::duration<double, std::milli>(dt_nano);
    // The wall clock first and the offset from the start of the request second:
    // one says when this happened, the other how far into the request it is, and
    // a line reading a latency breakdown wants both.
    const std::string when = realtime_column(e.ts);
    if (include_task_id) {
        return fmt::format("{}  {:12.9f}: cpu{} {:16x}: {}", when, dt.count(), e.shard,
                           e.query(), entry_message(e));
    }
    return fmt::format("{}  {:12.9f}: {}", when, dt.count(), entry_message(e));
}

static void render_truncation_warning(size_t count, int threshold, bool spanned) {
    ImGui::PushStyleColor(ImGuiCol_Text, ImVec4(1.f, 0.f, 0.f, 1.f));
    if (spanned) {
        ImGui::Text("number of tasks spanned %zu is greater than configured threshold %d, not rendering the rest", count, threshold);
    } else {
        ImGui::Text("number of tasks %zu is greater than configured threshold %d, not rendering the rest", count, threshold);
    }
    ImGui::PopStyleColor();
}

static void update_log_cache(log_cache& cache, uint64_t task_id, int threshold,
                             const std::vector<entry>& sorted) {
    if (cache.task_id == task_id && cache.threshold == threshold) {
        return;
    }

    cache = {};
    cache.task_id = task_id;
    cache.threshold = threshold;
    auto range = std::ranges::equal_range(sorted, task_id, std::ranges::less(),
                                          [] (const auto& e) { return e.query(); });
    cache.item_count = range.size();
    if (range.empty()) {
        return;
    }
    cache.source_begin = range.begin() - sorted.begin();

    const size_t cached_count = std::min(cache.item_count, static_cast<size_t>(threshold));
    cache.lines.reserve(cached_count);
    const int64_t start_ts = range.front().ts;
    for (size_t source_index = cache.source_begin;
         source_index < cache.source_begin + cached_count; ++source_index) {
        const auto& record = sorted[source_index];
        cache.lines.push_back({source_index, record, log_line_text(record, start_ts, false)});
    }
}

// The log lines are every record in the request's window, over every node
// loaded; the plot below them is not. A blue rectangle means "this shard was
// running something else", which is a statement about one reactor: letting
// another node's records end an interval would cut the selected task's green
// bars short in proportion to how many snapshots happen to be open. So the plot
// walks the coordinator's records only, which is what a single-node trace --
// where they are all there is -- has always drawn.
static void update_full_log_cache(full_log_cache& cache, uint64_t task_id, int threshold,
                                  const std::vector<entry>& sorted,
                                  std::span<const entry> span) {
    if (cache.task_id == task_id && cache.threshold == threshold) {
        return;
    }

    cache = {};
    cache.task_id = task_id;
    cache.threshold = threshold;
    auto sorted_range = std::ranges::equal_range(sorted, task_id, std::ranges::less(),
                                                 [] (const auto& e) { return e.query(); });
    if (sorted_range.empty()) {
        return;
    }
    auto span_range = std::ranges::equal_range(
        span, 1, std::ranges::less(), [&sorted_range] (const auto& e) {
            return (e.ts >= sorted_range.front().ts) + (e.ts > sorted_range.back().ts);
    });
    cache.task_count = span_range.size();
    cache.source_begin = span_range.begin() - span.begin();

    cache.start_ts = sorted_range.front().ts;
    cache.end_ts = sorted_range.back().ts;
    const size_t cached_count = std::min(cache.task_count, static_cast<size_t>(threshold));
    cache.lines.reserve(cached_count);
    for (size_t source_index = cache.source_begin;
         source_index < cache.source_begin + cached_count; ++source_index) {
        const auto& record = span[source_index];
        cache.lines.push_back({source_index, record, log_line_text(record, cache.start_ts, true)});
    }
}

// Task ids are not namespaced by shard here: a request coordinated on one shard
// reaches a tablet on another, and inherited continuations must stay together.
// When snapshots from several processes are loaded, namespace_task() adds a
// viewer-only process namespace before this grouping happens.

static std::string copy_bytes(std::span<const std::byte> bytes) {
    return std::string(reinterpret_cast<const char*>(bytes.data()), bytes.size());
}

static void add_prepared_entry(std::vector<entry>& out, uint64_t event,
                               std::string_view keyspace, std::string_view statement,
                               std::span<const std::byte> id, int64_t timestamp,
                               uint32_t shard, uint32_t node) {
    entry result{event, 0, 0, timestamp, 0, shard, node};
    result.prepared_id = copy_bytes(id);
    result.prepared_keyspace = keyspace;
    result.prepared_statement = statement;
    out.push_back(std::move(result));
}

// The callback the generated decode() hands each record to: one overload per
// tracepoint the viewer has a use for, and a template that swallows the rest.
struct sink {
    std::vector<entry>& out;
    std::vector<rpc_event>& rpc;
    // Which file this is, so that every record knows which cpu wrote it. See
    // entry::shard.
    uint32_t shard;
    uint32_t node;

    void operator()(const trace::run_task& e, const trace::tracepoint_metadata& m) const {
        out.push_back({0, namespace_task(node, e.prev), namespace_task(node, e.task),
                       int64_t(m.timestamp), intern_location(e.at), shard, node});
    }
    void operator()(const trace::cql_request& e, const trace::tracepoint_metadata& m) const {
        out.push_back({1, namespace_task(node, e.prev), namespace_task(node, e.task),
                       int64_t(m.timestamp), 0, shard, node});
    }
    void operator()(const trace::execution_stage& e, const trace::tracepoint_metadata& m) const {
        out.push_back({0xb, namespace_task(node, e.prev), namespace_task(node, e.task),
                       int64_t(m.timestamp), 0, shard, node});
    }
    // The reader concurrency semaphore's hop, and a switch in exactly the sense
    // the ones above are: the read the loop is about to run belongs to the task
    // that asked for it, not to the loop. Dropping these -- which is what the
    // catch-all below did until this overload existed -- attributes every
    // queued read, and every stack sample taken inside one, to whichever
    // request happened to spin the loop up.
    void operator()(const trace::semaphore_execute& e, const trace::tracepoint_metadata& m) const {
        out.push_back({0xa, namespace_task(node, e.prev), namespace_task(node, e.task),
                       int64_t(m.timestamp), 0, shard, node});
    }
    void operator()(const trace::io_begin& e, const trace::tracepoint_metadata& m) const {
        out.push_back({0x4, namespace_task(node, e.task), e.io, int64_t(m.timestamp), 0, shard, node});
    }
    void operator()(const trace::io_end& e, const trace::tracepoint_metadata& m) const {
        out.push_back({0x5, namespace_task(node, e.task), e.io, int64_t(m.timestamp), 0, shard, node});
    }
    void operator()(const trace::prepared_statement_added& e,
                    const trace::tracepoint_metadata& m) const {
        add_prepared_entry(out, 0xe, e.keyspace, e.statement, e.id,
                           int64_t(m.timestamp), shard, node);
    }
    void operator()(const trace::prepared_statement_removed& e,
                    const trace::tracepoint_metadata& m) const {
        add_prepared_entry(out, 0xf, e.keyspace, e.statement, e.id,
                           int64_t(m.timestamp), shard, node);
    }
    void operator()(const trace::prepared_query_run& e,
                    const trace::tracepoint_metadata& m) const {
        add_prepared_entry(out, 0xd, {}, {}, e.id, int64_t(m.timestamp), shard, node);
    }
    void operator()(const trace::prepared_statements_snapshot_begin&,
                    const trace::tracepoint_metadata& m) const {
        out.push_back({0x10, 0, 0, int64_t(m.timestamp), 0, shard, node});
    }
    void operator()(const trace::prepared_statement_snapshot_entry& e,
                    const trace::tracepoint_metadata& m) const {
        add_prepared_entry(out, 0x11, e.keyspace, e.statement, e.id,
                           int64_t(m.timestamp), shard, node);
    }
    void operator()(const trace::prepared_statements_snapshot_end&,
                    const trace::tracepoint_metadata& m) const {
        out.push_back({0x12, 0, 0, int64_t(m.timestamp), 0, shard, node});
    }
    // A sample is put aside rather than turned into an entry here: its place in
    // the timeline is its *own* timestamp converted to ticks, and no sync record
    // has been read yet. main() makes the entries once the clock is built.
    void operator()(const trace::stacktrace_sample& e, const trace::tracepoint_metadata&) const {
        stack_sample sample;
        sample.realtime_ns = e.time_ns;
        sample.shard = e.shard;
        sample.node = node;
        const auto* const words = reinterpret_cast<const uint64_t*>(e.frames.data());
        auto n_frames = e.frames.size() / sizeof(uint64_t);
        sample.frames.assign(words, words + n_frames);
        samples.push_back(std::move(sample));
    }
    // Pass one of the wall clock conversion: a sync record is not an event of
    // the program's own, so it never becomes an entry -- it is put aside, and
    // the whole per-node collection is handed to a wall_clock once every file
    // is decoded.
    void operator()(const trace::clock_sync& e, const trace::tracepoint_metadata& m) const {
        clock_syncs[node].push_back({int64_t(m.timestamp), e.realtime_ns, e.ticks_per_second});
    }
    void operator()(const trace::rpc_connection_open& e, const trace::tracepoint_metadata& m) const {
        rpc.push_back({rpc_event_kind::connection_open, e.connection, 0, 0, 0,
                       int64_t(m.timestamp), shard, node, std::string(e.local), std::string(e.remote),
                       e.peer_boot_msb, e.peer_boot_lsb, e.peer_shard});
    }
    void operator()(const trace::rpc_connection_close& e, const trace::tracepoint_metadata& m) const {
        rpc.push_back({rpc_event_kind::connection_close, e.connection, 0, 0, 0,
                       int64_t(m.timestamp), shard, node, {}, {},
                       e.peer_boot_msb, e.peer_boot_lsb, e.peer_shard});
    }
    void operator()(const trace::rpc_message_sent& e, const trace::tracepoint_metadata& m) const {
        rpc.push_back({rpc_event_kind::message_sent, e.connection, e.sequence, 0,
                       namespace_task(node, e.task), int64_t(m.timestamp), shard, node});
    }
    void operator()(const trace::rpc_message_received& e, const trace::tracepoint_metadata& m) const {
        rpc.push_back({rpc_event_kind::message_received, e.connection, e.sequence, 0, 0,
                       int64_t(m.timestamp), shard, node});
    }
    void operator()(const trace::rpc_reply_sent& e, const trace::tracepoint_metadata& m) const {
        rpc.push_back({rpc_event_kind::reply_sent, e.connection, e.sequence, e.msg_id,
                       namespace_task(node, e.task), int64_t(m.timestamp), shard, node});
    }
    void operator()(const trace::rpc_reply_received& e, const trace::tracepoint_metadata& m) const {
        rpc.push_back({rpc_event_kind::reply_received, e.connection, e.sequence, e.msg_id, 0,
                       int64_t(m.timestamp), shard, node});
    }
    void operator()(const trace::rpc_request_handled& e, const trace::tracepoint_metadata& m) const {
        // The task chain the inbound message opened on this shard. It is also a
        // task switch, so the timeline needs it as an entry too -- without one,
        // the plot has nothing marking where the replica's work begins.
        rpc.push_back({rpc_event_kind::request_handled, e.connection, e.sequence, 0,
                       namespace_task(node, e.task), int64_t(m.timestamp), shard, node});
        out.push_back({0x13, namespace_task(node, e.prev), namespace_task(node, e.task),
                       int64_t(m.timestamp), 0, shard, node});
    }
    void operator()(const trace::rpc_connection_snapshot_entry& e,
                    const trace::tracepoint_metadata& m) const {
        rpc.push_back({rpc_event_kind::snapshot_entry, e.connection, 0, 0, 0,
                       int64_t(m.timestamp), shard, node, std::string(e.local), std::string(e.remote),
                       e.peer_boot_msb, e.peer_boot_lsb, e.peer_shard});
    }
    template <typename Event>
    void operator()(const Event&, const trace::tracepoint_metadata&) const {}
};

// One shard's trace file, decoded into the records above.
//
// `shard` is which file this is rather than anything the file says: a trace has
// no field for the cpu it came off, and it does not need one, because there is
// one file per shard.
static void load_trace(const std::filesystem::path& path, uint32_t shard, uint32_t node,
                       std::vector<entry>& out, std::vector<rpc_event>& rpc,
                       trace::dso_directory& dsos) {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        throw std::system_error(errno, std::generic_category(), path.string());
    }
    const std::vector<char> raw{std::istreambuf_iterator<char>(in),
                                std::istreambuf_iterator<char>()};
    const std::span<const std::byte> bytes{reinterpret_cast<const std::byte*>(raw.data()),
                                           raw.size()};
    trace::decode(bytes, sink{out, rpc, shard, node}, dsos);

    // The objects this thread had mapped, for the raw addresses in a stack
    // sample. Every shard of one process saw the same objects at the same
    // addresses, so the first file that has any is enough.
    if (object_mappings.empty()) {
        object_mappings = trace::trace_mappings(bytes);
    }
}

// --- what a snapshot file says about itself -----------------------------------
//
// A trace file used to be called `shard-N.trace`, and the viewer read the shard
// out of the name. That was the only thing the name could carry, and it carried
// it badly: two nodes' snapshots could not be copied into one directory without
// colliding, and nothing in the file said which process, which build or which
// stretch of time it was.
//
// Now a file is named after a fresh time-based UUID and a `<uuid>.metadata.json`
// beside it says the rest. See the trace_snapshot endpoint in Scylla's
// api/system.cc, which writes both.
struct snapshot_metadata {
    std::string build_id;   // the executable the addresses inside belong to
    std::string boot_id;    // the *process*: see boot_id in scylla_tracer.hh
    uint32_t shard = 0;
    std::string level;      // "info" or "debug" -- one file per level now
    uint64_t first_record_ns = 0;
    uint64_t last_record_ns = 0;
    bool present = false;   // false for a .trace with no metadata beside it
};

// A reader for exactly the object Scylla writes: a flat map of strings and
// integers, no nesting and no arrays. Hand-rolled rather than a JSON library
// because that is the whole of the grammar this has to accept, and a field it
// does not find keeps its default -- an old snapshot decodes as far as it can
// rather than not at all.
static std::optional<std::string> json_field(const std::string& text, const char* key) {
    const std::string quoted = std::string("\"") + key + "\"";
    const auto at = text.find(quoted);
    if (at == std::string::npos) {
        return std::nullopt;
    }
    auto p = text.find(':', at + quoted.size());
    if (p == std::string::npos) {
        return std::nullopt;
    }
    ++p;
    while (p < text.size() && (text[p] == ' ' || text[p] == '\t')) {
        ++p;
    }
    if (p < text.size() && text[p] == '"') {
        const auto end = text.find('"', p + 1);
        if (end == std::string::npos) {
            return std::nullopt;
        }
        return text.substr(p + 1, end - p - 1);
    }
    const auto end = text.find_first_of(",}\n", p);
    return text.substr(p, (end == std::string::npos ? text.size() : end) - p);
}

static snapshot_metadata read_metadata(const std::filesystem::path& trace_path) {
    std::filesystem::path meta_path = trace_path;
    meta_path.replace_extension();  // drop ".trace"
    meta_path += ".metadata.json";
    std::ifstream in(meta_path);
    if (!in) {
        return {};
    }
    const std::string text{std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};

    snapshot_metadata meta;
    meta.present = true;
    if (const auto v = json_field(text, "build_id")) meta.build_id = *v;
    if (const auto v = json_field(text, "boot_id")) meta.boot_id = *v;
    if (const auto v = json_field(text, "level")) meta.level = *v;
    const auto number = [&text](const char* key, uint64_t& into) {
        if (const auto v = json_field(text, key)) {
            try {
                into = std::stoull(*v);
            } catch (const std::exception&) {
            }
        }
    };
    uint64_t shard = 0;
    number("shard", shard);
    meta.shard = uint32_t(shard);
    number("first_record_ns", meta.first_record_ns);
    number("last_record_ns", meta.last_record_ns);
    return meta;
}

// A boot id's 32 hex digits as the two words the tracepoints carry. Zero for
// anything that is not a UUID, which is also what a peer that did not answer
// the handshake's PEER_IDENTITY leaves in a connection record -- so an unparsed
// id and an unknown peer compare equal, and both mean "no idea".
static std::pair<uint64_t, uint64_t> boot_id_halves(std::string_view text) {
    uint64_t halves[2] = {0, 0};
    unsigned digits = 0;
    for (const char c : text) {
        if (c == '-') {
            continue;
        }
        unsigned value;
        if (c >= '0' && c <= '9') value = unsigned(c - '0');
        else if (c >= 'a' && c <= 'f') value = unsigned(c - 'a') + 10;
        else if (c >= 'A' && c <= 'F') value = unsigned(c - 'A') + 10;
        else return {0, 0};
        if (digits >= 32) return {0, 0};
        halves[digits / 16] = (halves[digits / 16] << 4) | value;
        ++digits;
    }
    return digits == 32 ? std::pair{halves[0], halves[1]} : std::pair<uint64_t, uint64_t>{0, 0};
}

// The boot id of each node, by node index, as the tracepoints' two words. Filled
// in by main() as the files are gathered; used to check an endpoint pairing
// against what the two ends said about each other in the RPC handshake.
static std::vector<std::pair<uint64_t, uint64_t>> node_boot_ids;
static std::vector<std::string> node_boot_id_strings;

// A row's name in the plot: which process, which cpu. The boot id is cut to its
// first group -- a time-based UUID's time_low, which differs between two nodes
// booted a second apart -- because a full one is 36 characters of axis. The
// whole of it is in the tooltip and in the Nodes window.
static std::string lane_label(uint32_t node, uint32_t shard) {
    std::string head = fmt::format("node{}", node);
    if (node < node_boot_id_strings.size()) {
        const std::string& boot = node_boot_id_strings[node];
        if (const auto dash = boot.find('-'); dash != std::string::npos) {
            head = boot.substr(0, dash);
        }
    }
    return fmt::format("{}/shard{}", head, shard);
}

// The full boot id, for the tooltip, or a placeholder for a snapshot that
// carried no metadata to take one from.
static std::string node_boot_id_text(uint32_t node) {
    if (node < node_boot_id_strings.size() && !node_boot_id_strings[node].empty()) {
        return node_boot_id_strings[node];
    }
    return "(unknown)";
}

// The number in `shard-N.trace`, or nothing if the name is not that shape. Kept
// for a snapshot written before the metadata files existed.
static std::optional<uint32_t> shard_of(const std::filesystem::path& path) {
    const std::string stem = path.stem().string();
    const auto dash = stem.rfind('-');
    if (dash == std::string::npos) {
        return std::nullopt;
    }
    try {
        return uint32_t(std::stoul(stem.substr(dash + 1)));
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

using rpc_key = std::pair<uint32_t, uint64_t>; // node, local connection id

struct rpc_endpoint {
    rpc_key key;
    std::string local;
    std::string remote;
    uint32_t shard = 0;
    // What this end said about the other one, from the handshake.
    uint64_t peer_boot_msb = 0;
    uint64_t peer_boot_lsb = 0;
    uint32_t peer_shard = 0;
};

struct rpc_span {
    rpc_key source;
    rpc_key destination;
    uint32_t source_shard = 0;
    uint32_t destination_shard = 0;
    uint64_t source_task = 0;
    uint64_t destination_task = 0;
    int64_t sent = 0;
    int64_t received = 0;
    uint64_t sequence = 0;
    std::optional<int64_t> reply_id;
};

struct task_plot_cache {
    uint64_t task_id = 0;
    int threshold = -1;
    int64_t start_ts = 0;
    int64_t end_ts = 0;
    std::vector<cached_plot_item> plot_items;
};

struct distributed_plot_row {
    uint32_t node = 0;
    uint32_t shard = 0;
    uint64_t task_id = 0;
    task_plot_cache cache;
};

static void update_task_plot_cache(task_plot_cache& cache, uint64_t task_id, int threshold,
                                   const std::vector<const entry*>& lane_entries) {
    if (cache.task_id == task_id && cache.threshold == threshold) {
        return;
    }

    cache = {};
    cache.task_id = task_id;
    cache.threshold = threshold;

    std::vector<const entry*> task_entries;
    for (const entry* const e : lane_entries) {
        if (e->query() == task_id) {
            task_entries.push_back(e);
        }
    }
    if (task_entries.empty()) {
        return;
    }

    cache.start_ts = task_entries.front()->ts;
    cache.end_ts = task_entries.back()->ts;
    const auto first = std::ranges::lower_bound(
        lane_entries, cache.start_ts, std::ranges::less(), [](const entry* e) { return e->ts; });
    const auto last = std::upper_bound(
        lane_entries.begin(), lane_entries.end(), cache.end_ts,
        [](int64_t ts, const entry* e) { return ts < e->ts; });
    const size_t item_count = last - first;
    const size_t cached_count = std::min(item_count, static_cast<size_t>(threshold));
    cache.plot_items.reserve(cached_count);

    uint64_t iostack = 0;
    int64_t iostart = 0;
    int64_t prev_ts = cache.start_ts;
    bool cpu = true;
    for (size_t i = 0; i < cached_count; ++i) {
        const entry& record = *first[i];
        const double x_min = double(prev_ts - cache.start_ts) * MULTIPLIER / 1e6;
        const double x_max = double(record.ts - cache.start_ts) * MULTIPLIER / 1e6;
        cache.plot_items.push_back({
            {x_min, 1.0},
            {x_max, 0.0},
            {x_min, 0.0},
            cpu ? IM_COL32(0, 128, 0, 255) : IM_COL32(0, 0, 128, 32),
            cpu,
        });

        if (record.query() == task_id) {
            if (record.event != 0x5) {
                cpu = true;
            }
            if (record.event == 0x4) {
                // Submitting an I/O takes the task off the cpu until it
                // completes, so the stretch that follows is not green. It used
                // to be: io_begin fell under the "any record of ours means we
                // are running" rule above, and the white in-I/O wash is drawn
                // at alpha 32 over whatever is underneath -- legible over blue,
                // invisible over green. On a shard with other work the green was
                // broken up by that work and the I/O showed anyway; on an idle
                // one the whole wait came out as one solid green bar.
                cpu = false;
                if (iostack == 0) {
                    iostart = record.ts;
                }
                ++iostack;
            } else if (record.event == 0x5) {
                --iostack;
                if (iostack == 0) {
                    cache.plot_items.push_back({
                        {double(iostart - cache.start_ts) * MULTIPLIER / 1e6, 1.0},
                        {x_max, 0.0},
                        {},
                        IM_COL32(255, 255, 255, 32),
                        false,
                    });
                }
            }
        } else {
            cpu = false;
        }
        prev_ts = record.ts;
    }
}

// The RPC edges one request is responsible for, as a causal walk over task ids.
//
// Two joins do the work, and neither needs a tracing id on the wire:
//
//   sent -> received   by (connection, sequence). The connection ids are local
//                      to their processes, so the two ends are paired first
//                      through the local/remote endpoints in the connection
//                      snapshot; the sequence is then a per-direction counter
//                      that both ends keep over the same set of frames.
//   received -> task   by (connection, sequence) again, against the
//                      request_handled record the server emits when it opens a
//                      task chain for the message.
//
// The walk starts at the selected task -- the messages *it* enqueued, which is
// what rpc_message_sent's task field says -- and follows each message into the
// task that handled it. Nothing here looks at what happened to be running on a
// shard at a timestamp: a message is written by the connection's send loop and
// read by its receive loop, so the running task at either instant is the
// connection's, not the request's, and seeding off it pulls in every unrelated
// request the node handled while this one was in flight.
static std::vector<rpc_span> make_rpc_spans(const std::vector<rpc_event>& events,
                                             uint64_t task_id) {
    if (task_id == 0) {
        return {};
    }

    std::vector<rpc_event> ordered = events;
    std::ranges::sort(ordered, {}, &rpc_event::ts);
    std::map<rpc_key, rpc_endpoint> endpoints;
    for (const rpc_event& e : ordered) {
        const rpc_key key{e.node, e.connection};
        if (e.kind == rpc_event_kind::connection_open ||
            e.kind == rpc_event_kind::snapshot_entry) {
            endpoints[key] = {key, e.local, e.remote, e.shard,
                              e.peer_boot_msb, e.peer_boot_lsb, e.peer_shard};
        } else if (e.kind == rpc_event_kind::connection_close) {
            endpoints.erase(key);
        }
    }

    // A connection and the one at the other end of the same socket. Both ends
    // agree on the pair of addresses and disagree on which is which, and a
    // (address, port) pair identifies one socket, so the match is unique.
    //
    // The handshake's identities are a second opinion on the same question, and
    // where both ends have one they must agree: `a` names `b`'s process and
    // shard, and `b` names `a`'s. That rules out the case the addresses cannot,
    // which is a socket whose far end belongs to a node that has since
    // restarted and taken the address back -- two snapshots from the same
    // address, one of them stale. A peer that did not answer the feature leaves
    // zeroes, and a zero matches anything: the addresses are then all there is.
    const auto identity_agrees = [](const rpc_endpoint& a, const rpc_endpoint& b) {
        if (a.peer_boot_msb == 0 && a.peer_boot_lsb == 0) {
            return true;
        }
        if (b.key.first >= node_boot_ids.size()) {
            return true;  // a node whose files carried no metadata
        }
        const auto& [msb, lsb] = node_boot_ids[b.key.first];
        if (msb == 0 && lsb == 0) {
            return true;
        }
        return a.peer_boot_msb == msb && a.peer_boot_lsb == lsb && a.peer_shard == b.shard;
    };
    std::map<rpc_key, rpc_key> peers;
    for (const auto& [a_key, a] : endpoints) {
        for (const auto& [b_key, b] : endpoints) {
            if (a_key != b_key && a.local == b.remote && a.remote == b.local &&
                identity_agrees(a, b) && identity_agrees(b, a)) {
                peers[a_key] = b_key;
                break;
            }
        }
    }

    struct message_key {
        rpc_key connection;
        uint64_t sequence;
        auto operator<=>(const message_key&) const = default;
    };
    std::map<message_key, const rpc_event*> received;
    std::map<message_key, const rpc_event*> handled;
    std::map<message_key, int64_t> reply_ids;
    for (const rpc_event& e : ordered) {
        const message_key key{{e.node, e.connection}, e.sequence};
        if (e.kind == rpc_event_kind::message_received) {
            received.try_emplace(key, &e);
        } else if (e.kind == rpc_event_kind::request_handled) {
            handled.try_emplace(key, &e);
        } else if (e.kind == rpc_event_kind::reply_sent) {
            reply_ids[key] = e.msg_id;
        }
    }

    std::vector<rpc_span> all;
    for (const rpc_event& e : ordered) {
        if (e.kind != rpc_event_kind::message_sent) {
            continue;
        }
        const rpc_key source{e.node, e.connection};
        const auto peer = peers.find(source);
        if (peer == peers.end()) {
            continue;
        }
        const message_key far{peer->second, e.sequence};
        const auto target = received.find(far);
        if (target == received.end()) {
            continue;
        }
        const rpc_event& r = *target->second;
        rpc_span span{source, peer->second, e.shard, r.shard, e.task, 0, e.ts, r.ts, e.sequence};
        // A reply is a message too, and it has no request_handled: the client
        // resumes the task that was waiting on it rather than opening a chain.
        if (const auto h = handled.find(far); h != handled.end()) {
            span.destination_task = h->second->task;
        }
        if (const auto reply = reply_ids.find({source, e.sequence}); reply != reply_ids.end()) {
            span.reply_id = reply->second;
        }
        all.push_back(std::move(span));
    }

    std::vector<rpc_span> selected;
    std::vector<char> included(all.size(), 0);
    std::unordered_set<uint64_t> reached{task_id};
    bool changed = true;
    while (changed) {
        changed = false;
        for (size_t i = 0; i < all.size(); ++i) {
            const rpc_span& span = all[i];
            if (included[i] || !reached.contains(span.source_task)) {
                continue;
            }
            included[i] = 1;
            selected.push_back(span);
            if (span.destination_task != 0) {
                reached.insert(span.destination_task);
            }
            changed = true;
        }
    }
    std::ranges::sort(selected, {}, &rpc_span::sent);
    return selected;
}

template <> struct fmt::formatter<entry> : formatter<string_view> {
    auto format(const entry& e, auto& ctx) const -> decltype(ctx.out()) {
        // ctx.out() is an output iterator to write to.
        return fmt::format_to(ctx.out(), "({:016x} {:016x} {:016x} {:016x})", e.event, e.id, e.arg, e.ts);
    }
};

int main(int argc, char** argv) {
    if (argc < 2) {
        fprintf(stderr, "usage: %s SNAPSHOT-DIR [SNAPSHOT-DIR ...]\n", argv[0]);
        fprintf(stderr, "  a directory of <uuid>.trace files and their <uuid>.metadata.json,\n"
                        "  as written by Scylla's POST /system/trace_snapshot into\n"
                        "  <workdir>/traces/<stamp>/\n"
                        "  pass one directory per node to align and correlate RPC traffic\n");
        return 2;
    }

    // One file per shard *and level*, and one metadata stream per file: a trace
    // describes the objects its own thread saw loaded, so the files are decoded
    // separately and merged afterwards rather than concatenated.
    //
    // Which shard and which process a file came from is in its metadata now, not
    // in its name -- see snapshot_metadata above. The node numbering that the
    // rest of the viewer keys off is therefore over *boot ids*: one node is one
    // traced process, whichever directory its files were handed over in, and two
    // snapshots of the same node taken minutes apart merge into one timeline
    // instead of pretending to be two machines.
    struct trace_file {
        std::filesystem::path path;
        snapshot_metadata meta;
        uint32_t node = 0;
        uint32_t shard = 0;
    };
    std::vector<trace_file> files;
    for (int dir = 0; dir < argc - 1; ++dir) {
        std::vector<trace_file> in_dir;
        for (const auto& e : std::filesystem::directory_iterator(argv[dir + 1])) {
            if (e.path().extension() == ".trace") {
                in_dir.push_back({e.path(), read_metadata(e.path()), 0, 0});
            }
        }
        // Within a directory by path, so that the order a filesystem happens to
        // report is not part of the answer.
        std::ranges::sort(in_dir, {}, &trace_file::path);
        files.insert(files.end(), in_dir.begin(), in_dir.end());
    }
    if (files.empty()) {
        fprintf(stderr, "no *.trace files in the supplied snapshot directories\n");
        return 1;
    }

    // Node ids in first-seen order, which -- because the directories were walked
    // in the order they were given -- keeps the first supplied directory's node
    // as node 0, the reference clock the others are converted into.
    //
    // A file whose metadata is missing falls back to its directory: an old
    // snapshot, from before the metadata files existed, still loads and still
    // has its shards kept apart from another directory's.
    {
        std::map<std::string, uint32_t> by_boot_id;
        size_t at = 0;
        for (trace_file& file : files) {
            const std::string identity = file.meta.present && !file.meta.boot_id.empty()
                    ? file.meta.boot_id
                    : std::string("directory:") + file.path.parent_path().string();
            const auto [it, fresh] = by_boot_id.emplace(identity, uint32_t(node_boot_ids.size()));
            if (fresh) {
                node_boot_ids.push_back(boot_id_halves(identity));
                node_boot_id_strings.push_back(identity);
            }
            file.node = it->second;
            file.shard = file.meta.present ? file.meta.shard
                                           : shard_of(file.path).value_or(uint32_t(at));
            ++at;
        }
    }
    // By node, then shard, then path, so that the load order and the counts
    // printed below read in the order somebody thinks about them.
    std::ranges::sort(files, [](const trace_file& a, const trace_file& b) {
        return std::tie(a.node, a.shard, a.path) < std::tie(b.node, b.shard, b.path);
    });

    // Kept for the Nodes window as well as printed: which process each node
    // number is, so that a row labelled "node 2" in the plot can be tied back to
    // a machine and a boot without going to the shell.
    struct node_summary {
        std::string boot_id;
        std::string build_id;
        std::string shards;
        double seconds = 0;
        uint64_t first_ns = 0;
        uint64_t last_ns = 0;
    };
    std::vector<node_summary> node_summaries(node_boot_ids.size());

    for (uint32_t node = 0; node < node_boot_ids.size(); ++node) {
        uint64_t first = std::numeric_limits<uint64_t>::max();
        uint64_t last = 0;
        std::set<uint32_t> shards;
        std::set<std::string> builds;
        for (const trace_file& file : files) {
            if (file.node != node || !file.meta.present) {
                continue;
            }
            shards.insert(file.shard);
            builds.insert(file.meta.build_id);
            first = std::min(first, file.meta.first_record_ns);
            last = std::max(last, file.meta.last_record_ns);
        }
        if (first > last) {
            node_summaries[node] = {node_boot_id_strings[node], "?", "?", 0, 0, 0};
            fmt::print("node {}: {} (no metadata)\n", node, node_boot_id_strings[node]);
            continue;
        }
        node_summaries[node] = {node_boot_id_strings[node], fmt::format("{}", fmt::join(builds, ",")),
                                fmt::format("{}", fmt::join(shards, ",")),
                                double(last - first) / 1e9, first, last};
        fmt::print("node {}: boot {} build {} shards {} covering {:.3f} s\n", node,
                   node_boot_id_strings[node], fmt::join(builds, ","), fmt::join(shards, ","),
                   double(last - first) / 1e9);
    }

    // The objects the trace's source locations point into, as gathered beside
    // the traces by tools/gather-dsos. Nothing in the traced process writes
    // them: an address is read back against the object it is in, and finding
    // that object is the reader's job, not the writer's. $TRACE_DSO_DIR
    // overrides; without either, every location decodes as <unresolved 0x...>
    // and the rest of the trace is unaffected.
    const std::filesystem::path dso_dir = std::filesystem::path(argv[1]) / "dsos";
    trace::dso_directory dsos =
        std::getenv("TRACE_DSO_DIR") != nullptr || !std::filesystem::exists(dso_dir)
            ? trace::dso_directory()
            : trace::dso_directory(dso_dir.string());

    the_dsos = &dsos;

    // The symbolizer pool. Constructing it starts nothing -- the first address
    // that falls in an object is what spawns that object's worker -- so a trace
    // nobody symbolises pays nothing for this.
    the_decoder = std::make_unique<addrdec::address_decoder>();

    std::vector<entry> entries;
    std::vector<rpc_event> rpc_events;
    for (const auto& file : files) {
        load_trace(file.path, file.shard, file.node, entries, rpc_events, dsos);
        fmt::print("{} (node {} shard {} {}): {} records so far\n", file.path.filename().string(),
                   file.node, file.shard,
                   file.meta.level.empty() ? "all levels" : file.meta.level, entries.size());
    }
    if (entries.empty()) {
        fprintf(stderr, "no records in the supplied snapshot directories\n");
        return 1;
    }
    fmt::print("{} RPC transport records\n", rpc_events.size());

    // Every file has been read, so every sync record in the trace is in hand.
    // Build one clock per node first: rdtsc values from different machines are
    // unrelated.  The first supplied snapshot is the reference timeline, and
    // all records are then converted through CLOCK_REALTIME into that clock.
    std::unordered_map<uint32_t, wall_clock> node_clocks;
    for (const auto& [node, points] : clock_syncs) {
        node_clocks[node].build(points);
    }
    // Node numbering is by boot id in first-seen order, and the directories are
    // walked in the order they were given, so node 0 is the first supplied
    // directory's process however the paths happen to sort.
    constexpr uint32_t reference_node = 0;
    if (const auto found = node_clocks.find(reference_node); found != node_clocks.end()) {
        the_clock = found->second;
    }
    size_t sync_count = 0;
    for (const auto& [node, points] : clock_syncs) {
        sync_count += points.size();
    }
    for (entry& e : entries) {
        const auto node = node_clocks.find(e.node);
        if (node != node_clocks.end()) {
            if (const auto ns = node->second.realtime_ns(e.ts)) {
                if (const auto ticks = the_clock.ticks_from_realtime(*ns)) {
                    e.ts = *ticks;
                }
            }
        }
    }
    for (rpc_event& e : rpc_events) {
        const auto node = node_clocks.find(e.node);
        if (node != node_clocks.end()) {
            if (const auto ns = node->second.realtime_ns(e.ts)) {
                if (const auto ticks = the_clock.ticks_from_realtime(*ns)) {
                    e.ts = *ticks;
                }
            }
        }
    }
    if (const auto ns_per_tick = the_clock.ns_per_tick()) {
        MULTIPLIER = *ns_per_tick;
    }
    fmt::print("{} clock sync records, {:.6f} ns/tick ({:.4f} GHz){}\n", sync_count,
               MULTIPLIER, 1.0 / MULTIPLIER,
               the_clock.empty() ? " -- no sync records, times unavailable" : "");

    // Stack samples become entries only now, because where a sample belongs in
    // the timeline is its own CLOCK_REALTIME timestamp read back as ticks, and
    // that needs the clock the pass above built. Without a clock they fall back
    fmt::print("{} distinct source locations: {} resolved, {} not\n",
               locations_resolved + locations_unresolved, locations_resolved,
               locations_unresolved);

    // to the tick count they already sort at -- which is when the poll loop
    // drained them, a poll period late, and the best that can be done.
    if (!samples.empty()) {
        std::ranges::sort(samples, {}, &stack_sample::realtime_ns);
        size_t dated = 0;
        for (size_t i = 0; i < samples.size(); ++i) {
            if (const auto ticks = the_clock.ticks_from_realtime(samples[i].realtime_ns)) {
                samples[i].ts = *ticks;
                ++dated;
            } else if (!entries.empty()) {
                samples[i].ts = entries.back().ts;
            }
            entries.push_back({0xc, 0, i, samples[i].ts, 0, samples[i].shard, samples[i].node});
        }
        fmt::print("{} stack samples, {} placed on the trace's clock\n", samples.size(), dated);
        fmt::print("  (a cpu-clock event only ticks while the shard is on the cpu, so a mostly "
                   "idle node has far fewer than {} Hz x shards x seconds)\n",
                   100);
    }

    // The analysis below walks `span` as a global timeline -- it was reading a
    // single thread's ring in file order -- so the shards have to be merged
    // into one before it can, and the timestamps are rdtsc from one machine,
    // which makes that meaningful.
    std::ranges::sort(entries, {}, &entry::ts);

    // Keep the timestamp-ordered records for each node/shard. Distributed
    // rows use these same records as the ordinary Full log plot, but restrict
    // the view to the task running on that shard.
    using task_lane = std::pair<uint32_t, uint32_t>;
    std::map<task_lane, std::vector<const entry*>> entries_by_lane;
    for (const entry& e : entries) {
        entries_by_lane[{e.node, e.shard}].push_back(&e);
    }

    // Reconstruct prepared statements at every prepared-query run. The latest
    // snapshot is the anchor for this reverse walk: a forward add is undone by
    // removing the entry, while a forward remove is undone by adding its
    // metadata back. Snapshot entries are encountered in reverse order and
    // therefore seed the set as the walk crosses the snapshot.
    {
        struct prepared_metadata {
            std::string keyspace;
            std::string statement;
        };
        using prepared_set = std::unordered_map<std::string, prepared_metadata>;
        std::map<std::pair<uint32_t, uint32_t>, prepared_set> sets;

        for (auto it = entries.rbegin(); it != entries.rend(); ++it) {
            entry& e = *it;
            auto& set = sets[{e.node, e.shard}];
            switch (e.event) {
            case 0xd: {
                const auto found = set.find(e.prepared_id);
                if (found != set.end()) {
                    e.prepared_keyspace = found->second.keyspace;
                    e.prepared_statement = found->second.statement;
                }
                break;
            }
            case 0xe:
                set.erase(e.prepared_id);
                break;
            case 0xf:
            case 0x11:
                set[e.prepared_id] = {e.prepared_keyspace, e.prepared_statement};
                break;
            default:
                break;
            }
        }
    }

    // Which task each sample interrupted. A sample says which cpu it was on and
    // when; the switches say which task each cpu was running from when. So one
    // walk of the merged timeline, carrying the current task per shard, answers
    // it for every sample at once -- and that is the whole of the link between
    // the two windows below. A sample taken while the shard was between tasks
    // keeps task 0 and appears only in the sample list.
    {
        std::map<std::pair<uint32_t, uint32_t>, uint64_t> running;
        for (entry& e : entries) {
            const auto cpu = std::make_pair(e.node, e.shard);
            if (e.event == 0 || e.event == 1 || e.event == 0xa || e.event == 0xb) {
                running[cpu] = e.arg;
            } else if (e.event == 0xd || e.event == 0xe || e.event == 0xf ||
                       e.event == 0x10 || e.event == 0x11 || e.event == 0x12) {
                const auto found = running.find(cpu);
                e.id = found == running.end() ? 0 : found->second;
            } else if (e.event == 0xc) {
                const auto found = running.find(cpu);
                e.id = found == running.end() ? 0 : found->second;
                samples[e.arg].task = e.id;
            }
        }
    }

    // Symbolising is otherwise reachable only by clicking, and a backtrace that
    // comes out as bare addresses is usually a missing or stripped dsos/
    // directory rather than anything in the trace. This prints one and stops, so
    // that can be told apart without opening a window. See "Debugging a trace
    // without the GUI" in the README.
    if (const char* const which = std::getenv("TRACE_DUMP_SAMPLE")) {
        // A comma-separated list rather than one index, because the cost that
        // matters here is not the first sample's -- it is the second's. The
        // first pays for indexing the object, which is tens of seconds on the
        // Dev binary and unavoidable; every sample after it should be
        // effectively free, because the symbolizer is still alive and still
        // holding that index. Two indices and the elapsed time beside each is
        // what shows whether that is true.
        std::vector<size_t> indices;
        for (const char* p = which; *p != '\0';) {
            char* end = nullptr;
            const unsigned long value = std::strtoul(p, &end, 0);
            if (end == p) {
                break;
            }
            indices.push_back(size_t(value));
            p = end;
            while (*p == ',' || *p == ' ') {
                ++p;
            }
        }
        for (const size_t index : indices) {
            if (index >= samples.size()) {
                fmt::print("no sample {} ({} in the trace)\n", index, samples.size());
                return 1;
            }
            const auto begin = std::chrono::steady_clock::now();
            const std::vector<std::string> lines = sample_backtrace_blocking(index);
            const auto took = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - begin);
            fmt::print("sample {}: cpu{} task {:x} at {} (symbolised in {} ms)\n", index,
                       samples[index].shard, samples[index].task,
                       format_realtime(samples[index].realtime_ns), took.count());
            for (const std::string& line : lines) {
                fmt::print("{}\n", line);
            }
        }
        return 0;
    }

    fmt::print("{} of {} samples fell inside a task\n",
               std::ranges::count_if(samples, [](const auto& x) { return x.task != 0; }),
               samples.size());

    auto span = std::span<const entry>(entries);
    auto sorted = std::vector<entry>(span.begin(), span.end());
    std::ranges::sort(sorted, std::ranges::less(), [] (const auto &x) {return std::make_pair(x.query(), x.ts);});
#if 0
    for (const auto &x : sorted) {
        fmt::print("{:016x} {}\n", x.query(), x) ;
    }
#endif

    struct query {
        std::chrono::duration<double> latency;
        uint64_t id;
        std::chrono::duration<double> cputime;
        std::chrono::duration<double> iotime;
        std::chrono::duration<double> starvetime;
    };
    std::vector<query> queries;
    {
        size_t i = 0;
        while (i < sorted.size()) {
            while (i < sorted.size() && sorted[i].event != 1) {
                ++i;
            }
            if (i == sorted.size()) {
                break;
            }
            auto current_query = sorted[i].query();
            auto start = sorted[i].ts;
            while (i + 1 < sorted.size() && sorted[i + 1].query() == current_query) {
                ++i;
            }
            auto end = sorted[i].ts;
            auto time = std::chrono::duration<double, std::nano>(double(end - start) * MULTIPLIER);
            queries.push_back(query{time, current_query, {}, {}, {}});
            ++i;
        }
    }
    std::ranges::sort(queries, std::ranges::less(), [] (const auto &x) {return x.latency;});
#if 0
    for (const auto &x : queries) {
        fmt::print("{} {}\n", x.latency.count(), x.id) ;
    }
#endif

    {
        for (auto &x : queries) {
            auto id = x.id;
            auto sorted_range = std::ranges::equal_range(sorted, id, std::ranges::less(), [] (const auto& e) {return e.query();});
            //fmt::print("tsrange: {} {}\n", sorted_range.front().ts, sorted_range.back().ts);
            auto span_range = std::ranges::equal_range(span, 1, std::ranges::less(), [&sorted_range] (const auto& e) {return (e.ts >= sorted_range.front().ts) + (e.ts > sorted_range.back().ts);});

            uint64_t iostack = 0;
            bool cpu = true;
            uint64_t prev_ts = sorted_range.begin()->ts;
            uint64_t cputime = 0;
            uint64_t starvetime = 0;
            uint64_t iotime = 0;
            size_t i;
            //fmt::print("range: {} {}\n", span_range.begin() - span.begin(), span_range.end() - span.begin());
            for (i = span_range.begin() - span.begin(); i < size_t(span_range.end() - span.begin()); ++i) {
                //fmt::print("looping: {}\n", i);
                uint64_t dt = span[i].ts - prev_ts;
                if (iostack == 0 && !cpu) {
                    starvetime += dt;
                }
                if (cpu) {
                    cputime += dt;
                }
                if (iostack) {
                    iotime += dt;
                }
                if (span[i].query() == id) {
                    if (span[i].event != 0x5) {
                        cpu = true;
                    }
                    if (span[i].event == 0x4) {
                        iostack += 1;
                    } else if (span[i].event == 0x5) {
                        iostack -= 1;
                    }
                } else {
                    cpu = false;
                }
                prev_ts = span[i].ts;
            }
            auto conv = [] (uint64_t ticks) {
                return std::chrono::duration<double, std::nano>(ticks * MULTIPLIER);
            };
            x.iotime = conv(iotime);
            x.starvetime = conv(starvetime);
            x.cputime = conv(cputime);
            //fmt::print("cputime: {}", cputime);
        }
    }

    if (std::getenv("TRACE_DUMP_RPC") != nullptr) {
        // What the joins had to work with, before any request is picked. A walk
        // that comes back empty is nearly always one of these counts being zero,
        // and which one says where to look: no paired connections means the
        // connection snapshot is missing -- switching the tracepoints off before
        // taking the snapshot writes it through a tracepoint that is no longer
        // recording -- while sends without a task mean the send records are not
        // carrying the caller's id.
        std::map<rpc_key, std::pair<std::string, std::string>> known;
        // What the handshake said about the far end, by connection, and which
        // of those processes are in the snapshots that were opened. A peer that
        // is *not* is the thing an endpoint pairing could never report: the
        // connection is real, the node it goes to is named, and its trace is
        // simply not here.
        std::map<rpc_key, std::pair<uint64_t, uint64_t>> peer_identity;
        std::set<std::pair<uint64_t, uint64_t>> loaded_boots(node_boot_ids.begin(),
                                                             node_boot_ids.end());
        size_t sent = 0, sent_with_task = 0, received = 0, handled = 0;
        for (const rpc_event& e : rpc_events) {
            switch (e.kind) {
            case rpc_event_kind::connection_open:
            case rpc_event_kind::snapshot_entry:
                known[{e.node, e.connection}] = {e.local, e.remote};
                if (e.peer_boot_msb != 0 || e.peer_boot_lsb != 0) {
                    peer_identity[{e.node, e.connection}] = {e.peer_boot_msb, e.peer_boot_lsb};
                }
                break;
            case rpc_event_kind::message_sent:
                ++sent;
                sent_with_task += e.task != 0;
                break;
            case rpc_event_kind::message_received: ++received; break;
            case rpc_event_kind::request_handled: ++handled; break;
            default: break;
            }
        }
        size_t paired = 0;
        for (const auto& [a_key, a] : known) {
            for (const auto& [b_key, b] : known) {
                if (a_key != b_key && a.first == b.second && a.second == b.first) {
                    ++paired;
                    break;
                }
            }
        }
        size_t peer_off_snapshot = 0;
        for (const auto& [key, boot] : peer_identity) {
            peer_off_snapshot += !loaded_boots.contains(boot);
        }
        fmt::print("{} connections known, {} paired with the far end\n", known.size(), paired);
        fmt::print("{} named their peer in the handshake, {} of those to a node not in these"
                   " snapshots\n", peer_identity.size(), peer_off_snapshot);
        fmt::print("{} messages sent ({} from a task), {} received, {} opened a task chain\n",
                   sent, sent_with_task, received, handled);

        // `queries` is in ascending latency, so the last that correlates is the
        // slowest one the walk can say anything about. The slowest *overall* is
        // usually some local request that never left the node.
        std::optional<query> slowest;
        size_t reached = 0;
        for (const auto& q : queries) {
            if (make_rpc_spans(rpc_events, q.id).empty()) {
                continue;
            }
            ++reached;
            slowest = q;
        }
        fmt::print("{} of {} CQL requests reach at least one other node\n",
                   reached, queries.size());
        if (!slowest) {
            fmt::print("nothing to correlate\n");
            return 0;
        }

        const auto spans = make_rpc_spans(rpc_events, slowest->id);
        std::set<std::pair<uint32_t, uint32_t>> shards;
        std::set<uint64_t> tasks;
        for (const rpc_span& span : spans) {
            shards.emplace(span.source.first, span.source_shard);
            shards.emplace(span.destination.first, span.destination_shard);
            tasks.insert(span.source_task);
            if (span.destination_task != 0) {
                tasks.insert(span.destination_task);
            }
        }
        fmt::print("\nslowest distributed request {:x} ({:.3f} ms): {} RPC messages over"
                   " {} node/shards, {} tasks\n",
                   slowest->id, slowest->latency.count() * 1e3, spans.size(), shards.size(),
                   tasks.size());
        for (const rpc_span& span : spans) {
            fmt::print("  node{}:shard{} task {:x} -> node{}:shard{} task {:x}"
                       "  {:.3f} ms on the wire, sequence {}{}\n",
                       span.source.first, span.source_shard, span.source_task,
                       span.destination.first, span.destination_shard, span.destination_task,
                       double(span.received - span.sent) * MULTIPLIER / 1e6, span.sequence,
                       span.reply_id ? fmt::format(", reply-msg-id {}", *span.reply_id) : "");
        }
        return 0;
    }

    std::vector<double> xx;
    std::vector<double> yy;
    if (queries.size()) {
        for (int i = 0; i <= 1000; ++i) {
            double x = pow(100000.0, i/1000.0);
            size_t w = queries.size() - size_t(1.0 / x * queries.size());
            xx.push_back(x);
            yy.push_back(queries[std::clamp(w, size_t(0), queries.size() - 1)].latency.count());
        }
        for (size_t i = 0; i < xx.size(); ++i) {
            //fmt::print("{} {}\n", xx[i], yy[i]);
        }
    }

#if 0
    {
        double m_timerMul = 1.;

        std::atomic_signal_fence( std::memory_order_acq_rel );
        const auto t0 = std::chrono::high_resolution_clock::now();
        const auto r0 = rdtsc();
        std::atomic_signal_fence( std::memory_order_acq_rel );
        std::this_thread::sleep_for( std::chrono::milliseconds( 200 ) );
        std::atomic_signal_fence( std::memory_order_acq_rel );
        const auto t1 = std::chrono::high_resolution_clock::now();
        const auto r1 = rdtsc();
        std::atomic_signal_fence( std::memory_order_acq_rel );

        const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>( t1 - t0 ).count();
        const auto dr = r1 - r0;

        m_timerMul = double( dt ) / double( dr );
        fmt::print("dt: {}, dr: {}, MULT: {}\n", dt, dr, m_timerMul);
    }
#endif

    if (!SDL_Init(SDL_INIT_VIDEO | SDL_INIT_GAMEPAD)) {
        fprintf(stderr, "SDL_Init failed: %s\n", SDL_GetError());
        return 1;
    }

    // GL 3.0 + GLSL 130. SDL owns the window and context; the renderer
    // backend remains Dear ImGui's current OpenGL3 implementation.
    const char* glsl_version = "#version 130";
    SDL_GL_SetAttribute(SDL_GL_CONTEXT_MAJOR_VERSION, 3);
    SDL_GL_SetAttribute(SDL_GL_CONTEXT_MINOR_VERSION, 0);
    SDL_GL_SetAttribute(SDL_GL_CONTEXT_PROFILE_MASK, SDL_GL_CONTEXT_PROFILE_CORE);
    SDL_GL_SetAttribute(SDL_GL_DOUBLEBUFFER, 1);
    SDL_GL_SetAttribute(SDL_GL_DEPTH_SIZE, 24);
    SDL_GL_SetAttribute(SDL_GL_STENCIL_SIZE, 8);

    // Create window with graphics context
    SDL_Window* window = SDL_CreateWindow(
        "Latency analyzer", 1280, 720, SDL_WINDOW_OPENGL | SDL_WINDOW_RESIZABLE | SDL_WINDOW_MAXIMIZED | SDL_WINDOW_HIGH_PIXEL_DENSITY);
    if (window == nullptr) {
        fprintf(stderr, "SDL_CreateWindow failed: %s\n", SDL_GetError());
        SDL_Quit();
        return 1;
    }
    SDL_GLContext gl_context = SDL_GL_CreateContext(window);
    if (gl_context == nullptr) {
        fprintf(stderr, "SDL_GL_CreateContext failed: %s\n", SDL_GetError());
        SDL_DestroyWindow(window);
        SDL_Quit();
        return 1;
    }
    SDL_GL_MakeCurrent(window, gl_context);
    SDL_GL_SetSwapInterval(1); // Enable vsync

    // Setup Dear ImGui context
    IMGUI_CHECKVERSION();
    ImGui::CreateContext();
    ImPlot::CreateContext();
    ImGuiIO& io = ImGui::GetIO(); (void)io;
    io.ConfigFlags |= ImGuiConfigFlags_NavEnableKeyboard;     // Enable Keyboard Controls
    io.ConfigFlags |= ImGuiConfigFlags_NavEnableGamepad;      // Enable Gamepad Controls
    io.ConfigFlags |= ImGuiConfigFlags_DockingEnable;         // Enable Docking

    // Setup Dear ImGui style
    ImGui::StyleColorsDark();
    //ImGui::StyleColorsLight();

    // Setup Platform/Renderer backends
    ImGui_ImplSDL3_InitForOpenGL(window, gl_context);
    ImGui_ImplOpenGL3_Init(glsl_version);

    // Load Fonts
    // - If no fonts are loaded, dear imgui will use the default font. You can also load multiple fonts and use ImGui::PushFont()/PopFont() to select them.
    // - AddFontFromFileTTF() will return the ImFont* so you can store it if you need to select the font among multiple.
    // - If the file cannot be loaded, the function will return a nullptr. Please handle those errors in your application (e.g. use an assertion, or display an error and quit).
    // - The fonts will be rasterized at a given size (w/ oversampling) and stored into a texture when calling ImFontAtlas::Build()/GetTexDataAsXXXX(), which ImGui_ImplXXXX_NewFrame below will call.
    // - Use '#define IMGUI_ENABLE_FREETYPE' in your imconfig file to use Freetype for higher quality font rendering.
    // - Read 'docs/FONTS.md' for more instructions and details.
    // - Remember that in C/C++ if you want to include a backslash \ in a string literal you need to write a double backslash \\ !
    // - Our Emscripten build process allows embedding fonts to be accessible at runtime from the "fonts/" folder. See Makefile.emscripten for details.
    //io.Fonts->AddFontDefault();
    //io.Fonts->AddFontFromFileTTF("c:\\Windows\\Fonts\\segoeui.ttf", 18.0f);
    //io.Fonts->AddFontFromFileTTF("../../misc/fonts/DroidSans.ttf", 16.0f);
    //io.Fonts->AddFontFromFileTTF("../../misc/fonts/Roboto-Medium.ttf", 16.0f);
    //io.Fonts->AddFontFromFileTTF("../../misc/fonts/Cousine-Regular.ttf", 15.0f);
    //ImFont* font = io.Fonts->AddFontFromFileTTF("c:\\Windows\\Fonts\\ArialUni.ttf", 18.0f, nullptr, io.Fonts->GetGlyphRangesJapanese());
    //IM_ASSERT(font != nullptr);

    // Our state
    bool show_demo_window = false;
    bool show_config_window = true;
    ImVec4 clear_color = ImVec4(0.45f, 0.55f, 0.60f, 1.00f);
    int log_task_threshold = 10000;
    log_cache log_cache_state;
    full_log_cache full_log_cache_state;

    // Keep all views of the selected task in sync.  In particular, the
    // histogram index is what drives the timing header in the log window.
    uint64_t id_log = queries.empty() ? 0 : queries.front().id;
    uint64_t id_full_log = id_log;
    size_t w = 0;
    double line_x = 1.0;
    // Whether `id_log` names a CQL request, and so whether queries[w] describes
    // it. A replica's task chain is not one -- it was opened by an inbound RPC,
    // not by a CQL frame -- and before this the header just kept showing
    // whichever request was selected last, which reads as the window having
    // ignored the click.
    bool id_log_is_query = !queries.empty();
    auto select_task = [&] (uint64_t task_id, bool update_full_log) {
        id_log = task_id;
        if (update_full_log) {
            id_full_log = task_id;
        }
        id_log_is_query = false;
        for (size_t i = 0; i < queries.size(); ++i) {
            if (queries[i].id != task_id) {
                continue;
            }
            w = i;
            id_log_is_query = true;
            // Put the marker in the middle of the histogram bucket for this
            // query.  The half-bucket offset avoids floating-point rounding
            // making the histogram's inverse mapping select the next query.
            line_x = i == 0
                         ? 1.0
                         : static_cast<double>(queries.size()) /
                               (static_cast<double>(queries.size() - i) - 0.5);
            line_x = std::clamp(line_x, 1.0, 100000.0);
            return;
        }
    };

    // Main loop
    bool done = false;
    while (!done)
    {
        // Poll and handle events (inputs, window resize, etc.)
        // You can read the io.WantCaptureMouse, io.WantCaptureKeyboard flags to tell if dear imgui wants to use your inputs.
        // - When io.WantCaptureMouse is true, do not dispatch mouse input data to your main application, or clear/overwrite your copy of the mouse data.
        // - When io.WantCaptureKeyboard is true, do not dispatch keyboard input data to your main application, or clear/overwrite your copy of the keyboard data.
        // Generally you may always pass all inputs to dear imgui, and hide them from your application based on those two flags.
        SDL_Event event;
        while (SDL_PollEvent(&event)) {
            ImGui_ImplSDL3_ProcessEvent(&event);
            if (event.type == SDL_EVENT_QUIT ||
                (event.type == SDL_EVENT_WINDOW_CLOSE_REQUESTED &&
                 event.window.windowID == SDL_GetWindowID(window))) {
                done = true;
            }
        }

        // Start the Dear ImGui frame
        ImGui_ImplOpenGL3_NewFrame();
        ImGui_ImplSDL3_NewFrame();
        ImGui::NewFrame();

        // Everything the symbolizer workers finished since the last frame,
        // taken in one go and unconditionally -- the sample window may be shut
        // or collapsed, and results left in the queue would then never be
        // moved into the decoder's own record of what it knows. Addresses that
        // belong to a sample nobody is looking at are picked up too; they are
        // kept, so nothing is lost by reaping them here.
        std::unordered_set<uint64_t> just_decoded;
        for (const addrdec::decoded_address& d : the_decoder->reap()) {
            just_decoded.insert(d.address);
        }

        if (ImGui::BeginMainMenuBar()) {
            if (ImGui::BeginMenu("View")) {
                if (ImGui::BeginMenu("Dockers")) {
                    ImGui::MenuItem("Config", nullptr, &show_config_window);
                    ImGui::MenuItem("Demo", nullptr, &show_demo_window);
                    ImGui::EndMenu();
                }
                ImGui::EndMenu();
            }
            ImGui::EndMainMenuBar();
        }

        ImGui::DockSpaceOverViewport();

        if (show_config_window) {
            ImGui::Begin("Config", &show_config_window);
            ImGui::InputInt("Log task threshold", &log_task_threshold);
            log_task_threshold = std::max(log_task_threshold, 0);
            ImGui::End();
        }

        // What each node number is. The plot rows and the RPC edges name nodes
        // by index; this is the one place that says which process an index is,
        // which build it ran, and what stretch of time its files cover.
        {
            ImGui::Begin("Nodes");
            if (ImGui::BeginTable("nodes", 5,
                                  ImGuiTableFlags_Borders | ImGuiTableFlags_RowBg |
                                          ImGuiTableFlags_SizingFixedFit)) {
                ImGui::TableSetupColumn("node");
                ImGui::TableSetupColumn("boot id");
                ImGui::TableSetupColumn("build id");
                ImGui::TableSetupColumn("shards");
                ImGui::TableSetupColumn("covers");
                ImGui::TableHeadersRow();
                for (size_t i = 0; i < node_summaries.size(); ++i) {
                    const node_summary& n = node_summaries[i];
                    ImGui::TableNextRow();
                    ImGui::TableNextColumn();
                    ImGui::Text("%zu", i);
                    ImGui::TableNextColumn();
                    ImGui::TextUnformatted(n.boot_id.c_str());
                    ImGui::TableNextColumn();
                    ImGui::TextUnformatted(n.build_id.c_str());
                    ImGui::TableNextColumn();
                    ImGui::TextUnformatted(n.shards.c_str());
                    ImGui::TableNextColumn();
                    ImGui::Text("%.3f s", n.seconds);
                }
                ImGui::EndTable();
            }
            ImGui::End();
        }

        // 1. Show the big demo window (Most of the sample code is in ImGui::ShowDemoWindow()! You can browse its code to learn more about Dear ImGui!).
        if (show_demo_window) {
            ImGui::ShowDemoWindow(&show_demo_window);
        }

#if 0
        // 2. Show a simple window that we create ourselves. We use a Begin/End pair to create a named window.
        {
            static float f = 0.0f;
            static int counter = 0;

            ImGui::Begin("Hello, world!");                          // Create a window called "Hello, world!" and append into it.

            ImGui::Text("This is some useful text.");               // Display some text (you can use a format strings too)
            ImGui::Checkbox("Demo Window", &show_demo_window);      // Edit bools storing our window open/close state
            ImGui::Checkbox("Another Window", &show_another_window);

            ImGui::SliderFloat("float", &f, 0.0f, 1.0f);            // Edit 1 float using a slider from 0.0f to 1.0f
            ImGui::ColorEdit3("clear color", (float*)&clear_color); // Edit 3 floats representing a color

            if (ImGui::Button("Button"))                            // Buttons return true when clicked (most widgets return true when edited/activated)
                counter++;
            ImGui::SameLine();
            ImGui::Text("counter = %d", counter);

            ImGui::Text("Application average %.3f ms/frame (%.1f FPS)", 1000.0f / io.Framerate, io.Framerate);

            {
                static bool animate = true;
                ImGui::Checkbox("Animate", &animate);

                // Plot as lines and plot as histogram
                //IMGUI_DEMO_MARKER("Widgets/Plotting/PlotLines, PlotHistogram");
                static float arr[] = { 0.6f, 0.1f, 1.0f, 0.5f, 0.92f, 0.1f, 0.2f };
                ImGui::PlotLines("Frame Times", arr, IM_ARRAYSIZE(arr));
                ImGui::PlotHistogram("Histogram", arr, IM_ARRAYSIZE(arr), 0, NULL, 0.0f, 1.0f, ImVec2(0, 80.0f));

                // Fill an array of contiguous float values to plot
                // Tip: If your float aren't contiguous but part of a structure, you can pass a pointer to your first float
                // and the sizeof() of your structure in the "stride" parameter.
                static float values[90] = {};
                static int values_offset = 0;
                static double refresh_time = 0.0;
                if (!animate || refresh_time == 0.0)
                    refresh_time = ImGui::GetTime();
                while (refresh_time < ImGui::GetTime()) // Create data at fixed 60 Hz rate for the demo
                {
                    static float phase = 0.0f;
                    values[values_offset] = cosf(phase);
                    values_offset = (values_offset + 1) % IM_ARRAYSIZE(values);
                    phase += 0.10f * values_offset;
                    refresh_time += 1.0f / 60.0f;
                }

                // Plots can display overlay texts
                // (in this example, we will display an average value)
                {
                    float average = 0.0f;
                    for (int n = 0; n < IM_ARRAYSIZE(values); n++)
                        average += values[n];
                    average /= (float)IM_ARRAYSIZE(values);
                    char overlay[32];
                    sprintf(overlay, "avg %f", average);
                    ImGui::PlotLines("Lines", values, IM_ARRAYSIZE(values), values_offset, overlay, -1.0f, 1.0f, ImVec2(0, 80.0f));
                }

                // Use functions to generate output
                // FIXME: This is rather awkward because current plot API only pass in indices.
                // We probably want an API passing floats and user provide sample rate/count.
                struct Funcs
                {
                    static float Sin(void*, int i) { return sinf(i * 0.1f); }
                    static float Saw(void*, int i) { return (i & 1) ? 1.0f : -1.0f; }
                };
                static int func_type = 0, display_count = 70;
                ImGui::Separator();
                ImGui::SetNextItemWidth(ImGui::GetFontSize() * 8);
                ImGui::Combo("func", &func_type, "Sin\0Saw\0");
                ImGui::SameLine();
                ImGui::SliderInt("Sample count", &display_count, 1, 400);
                float (*func)(void*, int) = (func_type == 0) ? Funcs::Sin : Funcs::Saw;
                ImGui::PlotLines("Lines", func, NULL, display_count, 0, NULL, -1.0f, 1.0f, ImVec2(0, 80));
                ImGui::PlotHistogram("Histogram", func, NULL, display_count, 0, NULL, -1.0f, 1.0f, ImVec2(0, 80));
                ImGui::Separator();

                // Animate a simple progress bar
                //IMGUI_DEMO_MARKER("Widgets/Plotting/ProgressBar");
                static float progress = 0.0f, progress_dir = 1.0f;
                if (animate)
                {
                    progress += progress_dir * 0.4f * ImGui::GetIO().DeltaTime;
                    if (progress >= +1.1f) { progress = +1.1f; progress_dir *= -1.0f; }
                    if (progress <= -0.1f) { progress = -0.1f; progress_dir *= -1.0f; }
                }

                // Typically we would use ImVec2(-1.0f,0.0f) or ImVec2(-FLT_MIN,0.0f) to use all available width,
                // or ImVec2(width,0.0f) for a specified width. ImVec2(0.0f,0.0f) uses ItemWidth.
                ImGui::ProgressBar(progress, ImVec2(0.0f, 0.0f));
                ImGui::SameLine(0.0f, ImGui::GetStyle().ItemInnerSpacing.x);
                ImGui::Text("Progress Bar");

                float progress_saturated = std::clamp(progress, 0.0f, 1.0f);
                char buf[32];
                sprintf(buf, "%d/%d", (int)(progress_saturated * 1753), 1753);
                ImGui::ProgressBar(progress, ImVec2(0.f, 0.f), buf);
            }
            ImGui::End();
        }
#endif

        //ImPlot::ShowDemoWindow();

#if 0
        // 3. Show another simple window.
        if (show_another_window)
        {
            ImGui::Begin("Another Window", &show_another_window);   // Pass a pointer to our bool variable (the window will have a closing button that will clear the bool when clicked)
            ImGui::Text("Hello from another window!");
            if (ImGui::Button("Close Me"))
                show_another_window = false;
            ImGui::End();
        }
#endif

        if (!queries.empty()) {
            static size_t chosen_one = -1;
            static bool just_chosen = true;
            static size_t chosen_unfull = -1;
            static bool just_chosen_unfull = true;
            {
            ImGui::Begin("Graph");
            static double rect[] = {100.0, 0.001, 141.2, 0.003};

            if (ImPlot::BeginPlot("HdrHistogram", ImVec2(-1,0))) {
                ImPlot::SetupAxes(nullptr, nullptr, ImPlotAxisFlags_Lock, ImPlotAxisFlags_Lock);
                ImPlot::SetupAxisScale(ImAxis_X1, ImPlotScale_Log10);
                ImPlot::SetupAxisScale(ImAxis_Y1, ImPlotScale_Log10);
                ImPlot::SetupAxesLimits(1, 100000, 0.0001, queries.back().latency.count());
                ImPlot::PlotLine("Latency", xx.data(), yy.data(), 1001);

                if (ImPlot::IsPlotHovered() && ImGui::IsMouseDown(0)) {
                    ImPlotPoint pt = ImPlot::GetPlotMousePos();
                    line_x = std::clamp(pt.x, 1.0, 100000.0);
                    w = std::clamp(queries.size() - size_t(1.0 / line_x * queries.size()), size_t(0), size_t(queries.size() - 1));
                    select_task(queries[w].id, true);
                }
                ImPlotDragToolFlags flags = ImPlotDragToolFlags_NoCursors | ImPlotDragToolFlags_NoFit | ImPlotDragToolFlags_NoInputs;
                ImPlot::DragLineX(0, &line_x, ImVec4(1,1,1,1), 1, flags);

                rect[1] = 0.0001;
                rect[3] = 0.001;
                ImPlot::DragRect(0,&rect[0],&rect[1],&rect[2],&rect[3],ImVec4(1,0,1,1), ImPlotDragToolFlags_Delayed);

                ImPlot::EndPlot();
            }

            ImGui::End();

            ImGui::Begin("TimeDist");
            {
                static size_t w1g = -1;
                static size_t w2g = -1;
                size_t w1 = std::clamp(queries.size() - size_t(1.0 / rect[0] * queries.size()), size_t(0), size_t(queries.size() - 1));
                size_t w2 = std::clamp(queries.size() - size_t(1.0 / rect[2] * queries.size()), size_t(0), size_t(queries.size() - 1));
                using t = std::chrono::duration<double>;
                static std::vector<double> plot_x = std::invoke([&] {
                    std::vector<double> v;
                    for (int i = 0; i < 1024; ++i) {
                        v.push_back(i * (1.0/1024));
                    }
                    return v;
                });
                static std::vector<double> iotimes_y, cputimes_y, latencies_y, starvetimes_y;
                static t avgiotime, avgcputime, avgstarvetime, avglatency;

                if (w1 != w1g || w2 != w2g) {
                    w1g = w1;
                    w2g = w2;
                    avgiotime = avgcputime = avglatency = avgstarvetime = t::zero();
                    std::vector<t> iotimes, cputimes, latencies, starvetimes;
                    for (size_t i = w1; i <= w2; ++i) {
                        avgiotime += queries[i].iotime / (w2 - w1 + 1);
                        avgcputime += queries[i].cputime / (w2 - w1 + 1);
                        avgstarvetime += queries[i].starvetime / (w2 - w1 + 1);
                        avglatency += queries[i].latency / (w2 - w1 + 1);

                        iotimes.push_back(queries[i].iotime);
                        cputimes.push_back(queries[i].cputime);
                        starvetimes.push_back(queries[i].starvetime);
                        latencies.push_back(queries[i].latency);
                    }
                    std::ranges::sort(iotimes);
                    std::ranges::sort(cputimes);
                    std::ranges::sort(starvetimes);
                    std::ranges::sort(latencies);

                    auto sample = [&] (std::vector<t>& vec) {
                        auto res = std::vector<double>();
                        if (vec.empty()) {
                            return res;
                        }
                        for (const auto& p : plot_x) {
                            size_t ww = (vec.size() - 1) * p;
                            res.push_back(std::chrono::duration<double, std::milli>(vec[ww]).count());
                        }
                        return res;
                    };
                    iotimes_y = sample(iotimes);
                    starvetimes_y = sample(starvetimes);
                    cputimes_y = sample(cputimes);
                    latencies_y = sample(latencies);
                }

                ImGui::Text("%s", fmt::format("{:10s} {:12.9f}", "CPU", std::chrono::duration<double, std::milli>(avgcputime).count()).c_str());
                ImGui::Text("%s", fmt::format("{:10s} {:12.9f}", "STARVE", std::chrono::duration<double, std::milli>(avgstarvetime).count()).c_str());
                ImGui::Text("%s", fmt::format("{:10s} {:12.9f}", "IO", std::chrono::duration<double, std::milli>(avgiotime).count()).c_str());
                ImGui::Text("%s", fmt::format("{:10s} {:12.9f}", "TOTAL", std::chrono::duration<double, std::milli>(avglatency).count()).c_str());

                if (ImPlot::BeginSubplots("My Subplot",2,2,ImVec2(-1, -1))) {
                    if (ImPlot::BeginPlot("iotime cdf", ImVec2(-1,0))) {
                        ImPlot::SetupAxes(NULL,NULL,0,ImPlotAxisFlags_AutoFit|ImPlotAxisFlags_RangeFit);
                        ImPlot::PlotLine("iotime cdf", plot_x.data(), iotimes_y.data(), iotimes_y.size());
                        ImPlot::EndPlot();
                    }
                    if (ImPlot::BeginPlot("starvetime cdf", ImVec2(-1,0))) {
                        ImPlot::SetupAxes(NULL,NULL,0,ImPlotAxisFlags_AutoFit|ImPlotAxisFlags_RangeFit);
                        ImPlot::PlotLine("starvetime cdf", plot_x.data(), starvetimes_y.data(), starvetimes_y.size());
                        ImPlot::EndPlot();
                    }
                    if (ImPlot::BeginPlot("cputime cdf", ImVec2(-1,0))) {
                        ImPlot::SetupAxes(NULL,NULL,0,ImPlotAxisFlags_AutoFit|ImPlotAxisFlags_RangeFit);
                        ImPlot::PlotLine("cputime cdf", plot_x.data(), cputimes_y.data(), cputimes_y.size());
                        ImPlot::EndPlot();
                    }
                    if (ImPlot::BeginPlot("latency cdf", ImVec2(-1,0))) {
                        ImPlot::SetupAxes(NULL,NULL,0,ImPlotAxisFlags_AutoFit|ImPlotAxisFlags_RangeFit);
                        ImPlot::PlotLine("latency cdf", plot_x.data(), latencies_y.data(), latencies_y.size());
                        ImPlot::EndPlot();
                    }
                    ImPlot::EndSubplots();
                }
            }
            ImGui::End();

            {
                ImGui::Begin("Log");
                // Every record of one *task*, wherever it ran -- not everything
                // that ran on one shard. A task that hops shards brings its
                // records with it, and its neighbours on those shards are not
                // here.
                ImGui::Text("%s", fmt::format("task {:16x}", id_log).c_str());
                if (id_log_is_query) {
                    ImGui::Text("%s", fmt::format("{:10s} {:12.9f}", "CPU", std::chrono::duration<double, std::milli>(queries[w].cputime).count()).c_str());
                    ImGui::Text("%s", fmt::format("{:10s} {:12.9f}", "STARVE", std::chrono::duration<double, std::milli>(queries[w].starvetime).count()).c_str());
                    ImGui::Text("%s", fmt::format("{:10s} {:12.9f}", "IO", std::chrono::duration<double, std::milli>(queries[w].iotime).count()).c_str());
                    ImGui::Text("%s", fmt::format("{:10s} {:12.9f}", "TOTAL", std::chrono::duration<double, std::milli>(queries[w].latency).count()).c_str());
                } else {
                    ImGui::Text("not a CQL request: no latency breakdown");
                }
                update_log_cache(log_cache_state, id_log, log_task_threshold, sorted);
                if (log_cache_state.item_count > static_cast<size_t>(log_task_threshold)) {
                    render_truncation_warning(log_cache_state.item_count, log_task_threshold, false);
                }
                if (ImGui::BeginChild("Log entries", ImVec2(0, 0), ImGuiChildFlags_None, ImGuiWindowFlags_HorizontalScrollbar)) {
                    {
                        static size_t selected = -1;
                        ImGuiListClipper clipper;
                        clipper.Begin(static_cast<int>(log_cache_state.lines.size()));
                        if (chosen_unfull >= log_cache_state.source_begin &&
                            chosen_unfull < log_cache_state.source_begin + log_cache_state.lines.size()) {
                            clipper.IncludeItemByIndex(static_cast<int>(chosen_unfull - log_cache_state.source_begin));
                        }
                        while (clipper.Step()) {
                            for (int visible_index = clipper.DisplayStart;
                                 visible_index < clipper.DisplayEnd; ++visible_index) {
                                const auto& line = log_cache_state.lines[visible_index];
                                const size_t i = line.source_index;
                                const bool selected_in_range =
                                    selected >= log_cache_state.source_begin &&
                                    selected < log_cache_state.source_begin + log_cache_state.lines.size() &&
                                    selected < sorted.size();
                                const bool highlighted =
                                    selected_in_range &&
                                    (line.record.event == 0x4 || line.record.event == 0x5) &&
                                    (line.record.arg == sorted[selected].arg) &&
                                    (sorted[selected].event == 0x4 || sorted[selected].event == 0x5);
                                if (i == chosen_unfull) {
                                    if (just_chosen_unfull) {
                                        just_chosen_unfull = false;
                                        ImGui::SetScrollHereY();
                                    }
                                    ImGui::PushStyleColor(ImGuiCol_Text, ImVec4(1.f, 0.f, 0.0f, 1.f));
                                }
                                if (ImGui::Selectable(line.text.c_str(), highlighted)) {
                                    selected = highlighted ? size_t(-1) : i;
                                    if (line.record.event == 0xc) {
                                        selected_sample = line.record.arg;
                                        sample_needs_scroll = true;
                                    }
                                }
                                if (i == chosen_unfull) {
                                    ImGui::PopStyleColor();
                                }
                            }
                        }
                    }
                }
                ImGui::EndChild();
                ImGui::End();
            }
#if 1
            {
                ImGui::Begin("Full log");
                update_full_log_cache(full_log_cache_state, id_full_log, log_task_threshold, sorted, span);
                if (full_log_cache_state.task_count > static_cast<size_t>(log_task_threshold)) {
                    render_truncation_warning(full_log_cache_state.task_count, log_task_threshold, true);
                }
                if (ImGui::BeginChild("Full log entries", ImVec2(0, 0), ImGuiChildFlags_None, ImGuiWindowFlags_HorizontalScrollbar)) {
                    {
                        static size_t selected = 0;
                        ImGuiListClipper clipper;
                        clipper.Begin(static_cast<int>(full_log_cache_state.lines.size()));
                        if (chosen_one >= full_log_cache_state.source_begin &&
                            chosen_one < full_log_cache_state.source_begin + full_log_cache_state.lines.size()) {
                            clipper.IncludeItemByIndex(static_cast<int>(chosen_one - full_log_cache_state.source_begin));
                        }
                        while (clipper.Step()) {
                            for (int visible_index = clipper.DisplayStart;
                                 visible_index < clipper.DisplayEnd; ++visible_index) {
                                const auto& line = full_log_cache_state.lines[visible_index];
                                const size_t i = line.source_index;
                                const bool selected_in_range =
                                    selected >= full_log_cache_state.source_begin &&
                                    selected < full_log_cache_state.source_begin + full_log_cache_state.lines.size() &&
                                    selected < span.size();
                                const bool highlighted =
                                    selected_in_range && line.record.query() == id_log;
                                const bool is_active = line.record.query() == id_full_log;
                                if (is_active) {
                                    ImGui::PushStyleColor(ImGuiCol_Text, ImVec4(0.f, 1.f, 0.24f, 1.f));
                                }
                                if (i == chosen_one) {
                                    if (just_chosen) {
                                        just_chosen = false;
                                        ImGui::SetScrollHereY();
                                    }
                                    ImGui::PushStyleColor(ImGuiCol_Text, ImVec4(1.f, 0.f, 0.0f, 1.f));
                                }
                                if (ImGui::Selectable(line.text.c_str(), highlighted)) {
                                    auto x = line.record.query();
                                    if (x) {
                                        // Keep the full-log anchor unchanged,
                                        // but update the selected task and all
                                        // of its derived views.
                                        select_task(x, false);
                                    }
                                    selected = highlighted ? size_t(-1) : i;
                                    if (line.record.event == 0xc) {
                                        selected_sample = line.record.arg;
                                        sample_needs_scroll = true;
                                    }
                                }
                                if (i == chosen_one) {
                                    ImGui::PopStyleColor();
                                }
                                if (is_active) {
                                    ImGui::PopStyleColor();
                                }
                            }
                        }
                    }
                }
                ImGui::EndChild();
                ImGui::End();
            }
#endif
            {
                static uint64_t distributed_for = 0;
                static bool distributed_initialized = false;
                static std::vector<rpc_span> distributed_spans;
                static std::vector<distributed_plot_row> distributed_rows;
                if (!distributed_initialized || distributed_for != id_full_log) {
                    distributed_initialized = true;
                    distributed_for = id_full_log;
                    distributed_spans = make_rpc_spans(rpc_events, id_full_log);

                    // One row per (node, shard, task), and the selected task is
                    // not exempt from that. It used to be drawn as a row of its
                    // own above these, scoped to a whole *node* rather than to
                    // one shard -- which made it the one row whose bars counted
                    // another cpu's work as an interruption, and the one row
                    // whose name could not be a cpu. A request that hops shards
                    // now gets a row per shard it ran on, like everything else.
                    distributed_rows.clear();
                    const auto add_row = [](uint32_t node, uint32_t shard, uint64_t task_id) {
                        if (task_id == 0 || std::ranges::any_of(
                                distributed_rows, [=](const distributed_plot_row& row) {
                                    return row.node == node && row.shard == shard &&
                                           row.task_id == task_id;
                                })) {
                            return;
                        }
                        distributed_rows.push_back({node, shard, task_id, {}});
                    };
                    // The selected task's own lanes first, in shard order, so
                    // the request is still what the top of the plot is about.
                    // Every lane it left a record on: a continuation inherits
                    // its id across a cross-shard hop, so "which cpu was this
                    // request on" has more than one answer.
                    {
                        const auto own = std::ranges::equal_range(
                            sorted, id_full_log, std::ranges::less(),
                            [](const entry& e) { return e.query(); });
                        std::set<std::pair<uint32_t, uint32_t>> lanes;
                        for (const entry& e : own) {
                            lanes.emplace(e.node, e.shard);
                        }
                        for (const auto& [node, shard] : lanes) {
                            add_row(node, shard, id_full_log);
                        }
                    }
                    // Then the rest, in the order the request reached them --
                    // the spans are sorted by send time, so the rows come out
                    // roughly top-down in causal order.
                    for (const rpc_span& rpc : distributed_spans) {
                        add_row(rpc.source.first, rpc.source_shard, rpc.source_task);
                        add_row(rpc.destination.first, rpc.destination_shard,
                                rpc.destination_task);
                    }
                }

                for (distributed_plot_row& row : distributed_rows) {
                    const auto lane = entries_by_lane.find({row.node, row.shard});
                    if (lane != entries_by_lane.end()) {
                        update_task_plot_cache(row.cache, row.task_id, log_task_threshold,
                                               lane->second);
                    }
                }

                int64_t plot_start_ts = full_log_cache_state.start_ts;
                int64_t plot_end_ts = full_log_cache_state.end_ts;
                for (const distributed_plot_row& row : distributed_rows) {
                    if (!row.cache.plot_items.empty()) {
                        plot_start_ts = std::min(plot_start_ts, row.cache.start_ts);
                        plot_end_ts = std::max(plot_end_ts, row.cache.end_ts);
                    }
                }
                const double plot_width = std::max(
                    0.001, double(plot_end_ts - plot_start_ts) * MULTIPLIER / 1e6);

                ImGui::Begin("Full log plot");
                if (full_log_cache_state.task_count > static_cast<size_t>(log_task_threshold)) {
                    render_truncation_warning(full_log_cache_state.task_count, log_task_threshold, true);
                }

                const std::vector<distributed_plot_row>& row_views = distributed_rows;
                const size_t plot_rows = row_views.size();
                const float plot_height = std::max(150.f, 55.f * float(plot_rows));
                if (plot_rows == 0) {
                    ImGui::TextUnformatted("no records for the selected task");
                } else if (ImPlot::BeginPlot("Full log plot", ImVec2(-1, plot_height),
                                             ImPlotFlags_NoTitle)) {
                    static uint64_t prev_id;
                    auto flag = prev_id == id_full_log ? ImPlotCond_Once : ImPlotCond_Always;
                    prev_id = id_full_log;

                    // The row names are y-axis tick labels rather than text
                    // drawn inside the plot. Text at x=0 is clipped by the plot
                    // rect the moment somebody pans, which is how "selected
                    // task" came to read "ed task"; a tick label lives in the
                    // axis gutter and ImPlot sizes the gutter to fit it.
                    std::vector<std::string> row_labels;
                    std::vector<const char*> row_label_ptrs;
                    std::vector<double> row_ticks;
                    row_labels.reserve(plot_rows);
                    for (size_t i = 0; i < plot_rows; ++i) {
                        row_labels.push_back(lane_label(row_views[i].node, row_views[i].shard));
                        row_ticks.push_back(double(i) + 0.5);
                    }
                    for (const std::string& label : row_labels) {
                        row_label_ptrs.push_back(label.c_str());
                    }

                    ImPlot::SetupAxes(nullptr, nullptr, ImPlotAxisFlags_NoGridLines,
                                      ImPlotAxisFlags_Lock | ImPlotAxisFlags_NoGridLines);
                    ImPlot::SetupAxisTicks(ImAxis_Y1, row_ticks.data(), int(row_ticks.size()),
                                           row_label_ptrs.data());
                    ImPlot::SetupAxisLimitsConstraints(ImAxis_X1, 0, plot_width);
                    ImPlot::SetupAxesLimits(0, plot_width, 0, double(plot_rows), flag);
                    ImPlot::PushPlotClipRect();

                    const auto draw_row = [&](const std::vector<cached_plot_item>& items,
                                              int64_t row_start_ts, double row) {
                        const double x_offset = double(row_start_ts - plot_start_ts) * MULTIPLIER / 1e6;
                        for (const auto& item : items) {
                            const ImPlotPoint min{item.min.x + x_offset, row + item.min.y};
                            const ImPlotPoint max{item.max.x + x_offset, row + item.max.y};
                            ImVec2 rmin = ImPlot::PlotToPixels(min);
                            ImVec2 rmax = ImPlot::PlotToPixels(max);
                            if (item.draw_line) {
                                const ImPlotPoint line_end{item.line_end.x + x_offset,
                                                          row + item.line_end.y};
                                ImPlot::GetPlotDrawList()->AddLine(
                                    rmin, ImPlot::PlotToPixels(line_end), IM_COL32(0, 128, 0, 255));
                            }
                            ImPlot::GetPlotDrawList()->AddRectFilled(rmin, rmax, item.color);
                        }
                    };

                    for (size_t row_index = 0; row_index < row_views.size(); ++row_index) {
                        draw_row(row_views[row_index].cache.plot_items,
                                 row_views[row_index].cache.start_ts, double(row_index));
                    }

                    // What is under the pointer: which row, and -- from that
                    // row's own node and shard -- which task the reactor was
                    // actually running there. That last part is the point of the
                    // tooltip. A row's bars are its *own* task's, so the blue
                    // stretch between two green ones says only "something else
                    // ran here"; the lane's switch records say what.
                    if (ImPlot::IsPlotHovered()) {
                        const ImPlotPoint pt = ImPlot::GetPlotMousePos();
                        const auto row_index = size_t(std::max(0.0, std::floor(pt.y)));
                        if (pt.y >= 0 && row_index < row_views.size()) {
                            const distributed_plot_row& row = row_views[row_index];
                            const int64_t ts =
                                plot_start_ts + int64_t(pt.x * 1e6 / MULTIPLIER);
                            const auto lane = entries_by_lane.find({row.node, row.shard});
                            const entry* running = nullptr;
                            int64_t running_from = 0;
                            int64_t running_to = 0;
                            if (lane != entries_by_lane.end()) {
                                const auto& records = lane->second;
                                const auto after = std::ranges::upper_bound(
                                    records, ts, std::ranges::less(),
                                    [](const entry* e) { return e->ts; });
                                for (auto it = after; it != records.begin();) {
                                    --it;
                                    if (is_switch(**it)) {
                                        running = *it;
                                        running_from = (*it)->ts;
                                        break;
                                    }
                                }
                                if (running != nullptr) {
                                    running_to = plot_end_ts;
                                    for (auto it = after; it != records.end(); ++it) {
                                        if (is_switch(**it)) {
                                            running_to = (*it)->ts;
                                            break;
                                        }
                                    }
                                }
                            }

                            // A light wash over the stretch that task held the
                            // cpu, so that what the tooltip is talking about is
                            // visible rather than inferred from the pointer.
                            if (running != nullptr && running_to > running_from) {
                                const double x0 =
                                    double(running_from - plot_start_ts) * MULTIPLIER / 1e6;
                                const double x1 =
                                    double(running_to - plot_start_ts) * MULTIPLIER / 1e6;
                                ImPlot::GetPlotDrawList()->AddRectFilled(
                                    ImPlot::PlotToPixels(ImPlotPoint{x0, double(row_index) + 1.0}),
                                    ImPlot::PlotToPixels(ImPlotPoint{x1, double(row_index)}),
                                    IM_COL32(255, 255, 255, 36));
                            }

                            ImGui::BeginTooltip();
                            ImGui::Text("boot %s", node_boot_id_text(row.node).c_str());
                            ImGui::Text("node %u  shard %u", row.node, row.shard);
                            ImGui::Text("row task %016" PRIx64, row.task_id);
                            ImGui::Separator();
                            if (running == nullptr) {
                                ImGui::TextUnformatted("no switch record on this shard here");
                            } else {
                                const uint64_t task = running->query();
                                ImGui::Text("running  %016" PRIx64, task);
                                ImGui::Text("%s", entry_message(*running).c_str());
                                ImGui::Text("for %.6f ms",
                                            double(running_to - running_from) * MULTIPLIER / 1e6);
                                if (running->loc != 0) {
                                    ImGui::Text("at %s", location_detail(running->loc).c_str());
                                } else {
                                    ImGui::TextUnformatted("at <no source location>");
                                }
                            }
                            ImGui::EndTooltip();
                        }
                    }
                    ImPlot::PopPlotClipRect();

                    // IsMouseDown, not IsMouseClicked: holding the button and
                    // dragging scrubs the selection along the row, which is how
                    // this plot has always been read.
                    if (ImPlot::IsPlotHovered() && ImGui::IsMouseDown(0)) {
                        const ImPlotPoint pt = ImPlot::GetPlotMousePos();
                        const auto row_index = size_t(std::max(0.0, std::floor(pt.y)));
                        if (pt.y >= 0 && row_index < row_views.size()) {
                            const distributed_plot_row& row = row_views[row_index];
                            const int64_t ts =
                                plot_start_ts + int64_t(pt.x * 1e6 / MULTIPLIER);

                            // The row's task, and the record of it at or before
                            // the click. Anywhere in the row, not only on one of
                            // its rectangles: the gaps are the stretches the
                            // task was preempted or in an I/O, and "what was
                            // this request doing then" is a fair question to
                            // click on.
                            select_task(row.task_id, false);
                            const auto task_range = std::ranges::equal_range(
                                sorted, row.task_id, std::ranges::less(),
                                [](const auto& e) { return e.query(); });
                            if (!task_range.empty()) {
                                const auto after = std::ranges::upper_bound(
                                    task_range, ts, std::ranges::less(),
                                    [](const auto& e) { return e.ts; });
                                const auto at =
                                    after == task_range.begin() ? after : after - 1;
                                chosen_unfull = at - sorted.begin();
                                just_chosen_unfull = true;
                            }

                            // And the full log, which spans the whole request
                            // rather than one task, so it follows a click on any
                            // row rather than only on the one that used to be
                            // row 0.
                            if (!span.empty()) {
                                chosen_one = std::ranges::lower_bound(
                                                 span, ts, std::ranges::less(),
                                                 [](const auto& e) { return e.ts; }) -
                                             span.begin();
                                chosen_one = std::clamp(chosen_one == 0 ? 0 : chosen_one - 1,
                                                        size_t(0), span.size() - 1);
                                just_chosen = true;
                            }
                        }
                    }
                    ImPlot::EndPlot();
                }
                ImGui::End();
            }

            // The samples, and the backtrace of whichever one is selected.
            //
            // Both directions of the link between this window and the logs run
            // through selected_sample: clicking a row here selects the task the
            // sample interrupted, which is what the other windows are keyed on,
            // and clicking a SAMPLE line there selects the row here.
            {
                ImGui::Begin("Stack samples");
                if (samples.empty()) {
                    ImGui::Text("no stacktrace_sample records in this trace");
                } else {
                    ImGui::Text("%s", fmt::format("{} samples, {} objects mapped, {}",
                                                  samples.size(), object_mappings.size(),
                                                  the_dsos == nullptr || object_mappings.empty()
                                                      ? "no objects to decode against"
                                                      : "decoded on demand")
                                          .c_str());
                    const float list_width = ImGui::GetContentRegionAvail().x * 0.5f;
                    if (ImGui::BeginChild("Sample list", ImVec2(list_width, 0),
                                          ImGuiChildFlags_ResizeX,
                                          ImGuiWindowFlags_HorizontalScrollbar)) {
                        ImGuiListClipper clipper;
                        clipper.Begin(static_cast<int>(samples.size()));
                        if (sample_needs_scroll && selected_sample < samples.size()) {
                            clipper.IncludeItemByIndex(static_cast<int>(selected_sample));
                        }
                        while (clipper.Step()) {
                            for (int i = clipper.DisplayStart; i < clipper.DisplayEnd; ++i) {
                                const stack_sample& sample = samples[i];
                                const std::string text = fmt::format(
                                    "{}  cpu{:<2} {:16x}  {:3} frames##{}",
                                    format_realtime(sample.realtime_ns), sample.shard, sample.task,
                                    sample.frames.size(), i);
                                const bool chosen = size_t(i) == selected_sample;
                                if (chosen && sample_needs_scroll) {
                                    sample_needs_scroll = false;
                                    ImGui::SetScrollHereY();
                                }
                                if (ImGui::Selectable(text.c_str(), chosen)) {
                                    selected_sample = size_t(i);
                                    // Selecting the task the sample was taken
                                    // in is the point of the link: the logs and
                                    // the plot are all keyed on it.
                                    if (sample.task != 0) {
                                        select_task(sample.task, true);
                                        chosen_unfull = std::ranges::lower_bound(
                                                            sorted,
                                                            std::make_pair(sample.task,
                                                                           uint64_t(sample.ts)),
                                                            std::ranges::less(),
                                                            [](const auto& e) {
                                                                return std::make_pair(
                                                                    e.query(), uint64_t(e.ts));
                                                            }) -
                                                        sorted.begin();
                                        chosen_unfull = std::clamp(chosen_unfull, size_t(0),
                                                                   sorted.size() - 1);
                                        just_chosen_unfull = true;
                                        chosen_one =
                                            std::ranges::lower_bound(span, sample.ts,
                                                                     std::ranges::less(),
                                                                     [](const auto& e) {
                                                                         return e.ts;
                                                                     }) -
                                            span.begin();
                                        chosen_one =
                                            std::clamp(chosen_one, size_t(0), span.size() - 1);
                                        just_chosen = true;
                                    }
                                }
                            }
                        }
                    }
                    ImGui::EndChild();
                    ImGui::SameLine();
                    if (ImGui::BeginChild("Backtrace", ImVec2(0, 0), ImGuiChildFlags_None,
                                          ImGuiWindowFlags_HorizontalScrollbar)) {
                        if (selected_sample >= samples.size()) {
                            ImGui::Text("pick a sample on the left");
                        } else {
                            const stack_sample& sample = samples[selected_sample];
                            ImGui::Text("%s", fmt::format("cpu{} task {:x} at {}", sample.shard,
                                                          sample.task,
                                                          format_realtime(sample.realtime_ns))
                                                  .c_str());
                            ImGui::Separator();

                            // A sample switch invalidates the row states, and
                            // nothing else does: the vector is indexed by
                            // position in *this* sample's frames.
                            if (frame_states_for != selected_sample) {
                                frame_states_for = selected_sample;
                                frame_states.assign(sample.frames.size(), frame_state::fresh);
                            }

                            size_t pending = 0;
                            for (size_t i = 0; i < sample.frames.size(); ++i) {
                                const uint64_t key = frame_key(sample, i);
                                switch (frame_states[i]) {
                                case frame_state::fresh:
                                    request_frame(key);
                                    frame_states[i] = frame_state::sent;
                                    break;
                                case frame_state::sent:
                                    // Either it came back just now, or it was
                                    // already known -- a call site another
                                    // sample went through, or this same sample
                                    // looked at a moment ago -- in which case
                                    // request_frame() dropped the request and
                                    // there is nothing to wait for.
                                    if (just_decoded.contains(key) ||
                                        the_decoder->lookup(key) != nullptr) {
                                        frame_states[i] = frame_state::decoded;
                                    }
                                    break;
                                case frame_state::decoded:
                                    break;
                                }
                                if (frame_states[i] != frame_state::decoded) {
                                    ++pending;
                                }
                                ImGui::TextUnformatted(
                                    frame_line(sample, i, frame_states[i] == frame_state::decoded)
                                        .c_str());
                            }
                            if (pending != 0) {
                                ImGui::Separator();
                                ImGui::Text("%s", fmt::format("symbolising {} of {} frames...",
                                                              pending, sample.frames.size())
                                                      .c_str());
                            }
                        }
                    }
                    ImGui::EndChild();
                }
                ImGui::End();
            }

            }
        } else {
            ImGui::Begin("Trace viewer");
            ImGui::Text("No query records found in %s.", argv[1]);
            ImGui::End();
        }

        // Rendering
        ImGui::Render();
        int display_w, display_h;
        SDL_GetWindowSizeInPixels(window, &display_w, &display_h);
        glViewport(0, 0, display_w, display_h);
        glClearColor(clear_color.x * clear_color.w, clear_color.y * clear_color.w, clear_color.z * clear_color.w, clear_color.w);
        glClear(GL_COLOR_BUFFER_BIT);
        ImGui_ImplOpenGL3_RenderDrawData(ImGui::GetDrawData());

        SDL_GL_SwapWindow(window);
    }

    // Cleanup
    ImGui_ImplOpenGL3_Shutdown();
    ImGui_ImplSDL3_Shutdown();
    ImPlot::DestroyContext();
    ImGui::DestroyContext();

    SDL_GL_DestroyContext(gl_context);
    SDL_DestroyWindow(window);
    SDL_Quit();

    return 0;
}
