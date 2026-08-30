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
#include <atomic>
#include <stdexcept>
#include <system_error>
#include <fmt/core.h>
#include <fmt/ranges.h>
#include <cstdint>
#include <ctime>
#include <optional>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <unordered_map>
#include <map>
#include <array>
#include <cstdio>
#include <cstdlib>
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

// Every shard's rings are stamped by the same rdtsc and the same wall clock, so
// the syncs out of all the files go into one list rather than one per shard.
static std::vector<clock_sync_point> clock_syncs;

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
//   0xb execution_stage{prev, task} an execution stage ran a queued work item
//   0xc stacktrace_sample             the shard was interrupted for a stack sample
//
// (0x3 and 0xa, the reader-concurrency-semaphore events of the original
// experiment, are not emitted by this build; the formatters for them are left
// in place.)
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

    // Which request this record belongs to. For a *switch* that is the task
    // being switched to; for everything else, the task it happened under.
    uint64_t query() const {
        if (event == 0 || event == 1 || event == 0xa || event == 0xb) {
            return arg;
        } else {
            return id;
        }
    }
};

// The decoded source locations, interned.
//
// run_task carries one per record and the same call site turns up thousands of
// times -- every continuation the reactor runs off one `then()` -- so the entries
// hold an index into this and not a string. Index 0 is the empty location, which
// is what an unlocated event and a task with no resume point both get.
static std::vector<std::string> location_strings{""};

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
    }
    return it->second;
}

static const std::string& location_string(uint32_t index) {
    return location_strings[index];
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

// One symbolised frame: what llvm-addr2line said, or the bare address.
//
// Keyed by the address as looked up rather than as recorded -- see below -- so
// that the same call site costs one lookup however many samples caught it.
static std::unordered_map<uint64_t, std::string> frame_cache;

// Which addresses have been asked about at all. A frame in an object the
// directory does not have gets an entry too, so it is not re-attempted.
static std::string addr2line_binary() {
    if (const char* const from_env = std::getenv("TRACE_ADDR2LINE")) {
        return from_env;
    }
    return "llvm-addr2line";
}

// Symbolise a batch of addresses out of one object, filling frame_cache.
//
// One process for the whole batch: addr2line takes a list, and a stack of
// thirty frames spread over two objects is two spawns rather than thirty. The
// output is read in the form `-a -f -i` gives it -- an `0x...` line opening each
// address, then function/file pairs, one pair per inlined frame -- so the
// inlining is kept and shown as the several lines it is.
static void symbolise_batch(const std::string& path,
                            const std::vector<std::pair<uint64_t, uint64_t>>& batch) {
    // process address, file offset
    std::string command = fmt::format("{} -e '{}' -f -C -i -a", addr2line_binary(), path);
    for (const auto& [process_address, file_offset] : batch) {
        command += fmt::format(" {:#x}", file_offset);
    }
    command += " 2>/dev/null";

    std::vector<std::vector<std::string>> per_address;
    FILE* const pipe = popen(command.c_str(), "r");
    if (pipe != nullptr) {
        std::array<char, 4096> line{};
        while (fgets(line.data(), int(line.size()), pipe) != nullptr) {
            std::string text(line.data());
            while (!text.empty() && (text.back() == '\n' || text.back() == '\r')) {
                text.pop_back();
            }
            // An address line opens the block for the next input address; -a
            // prints one whether or not anything was found.
            if (text.rfind("0x", 0) == 0 && text.find(' ') == std::string::npos) {
                per_address.emplace_back();
                continue;
            }
            if (!per_address.empty()) {
                per_address.back().push_back(std::move(text));
            }
        }
        pclose(pipe);
    }

    for (size_t i = 0; i < batch.size(); ++i) {
        const uint64_t key = batch[i].first;
        if (i >= per_address.size() || per_address[i].empty()) {
            frame_cache.emplace(key, std::string());
            continue;
        }
        // Function then file:line, repeated once per inlined frame. Rendered
        // innermost first, which is the order addr2line prints them in.
        std::string text;
        const auto& lines = per_address[i];
        for (size_t j = 0; j + 1 < lines.size(); j += 2) {
            if (!text.empty()) {
                text += "\n                       (inlined by) ";
            }
            text += fmt::format("{} at {}", lines[j], lines[j + 1]);
        }
        if (text.empty()) {
            text = lines.front();
        }
        frame_cache.emplace(key, std::move(text));
    }
}

// The decoded backtrace of one sample, computed once and kept.
//
// A frame above the innermost is a *return* address -- the instruction after
// the call -- and looking it up as it stands attributes the call to whatever
// follows it, which for a call in tail position is the next function
// altogether. One byte back is inside the call instruction, which is what every
// unwinder does and what makes the line numbers right.
static const std::vector<std::string>& sample_backtrace(size_t index) {
    static std::unordered_map<size_t, std::vector<std::string>> decoded;
    if (const auto found = decoded.find(index); found != decoded.end()) {
        return found->second;
    }

    const stack_sample& sample = samples[index];

    // Group the frames this sample needs by object, so each object is one
    // addr2line run.
    std::map<std::string, std::vector<std::pair<uint64_t, uint64_t>>> wanted;
    std::vector<uint64_t> keys;
    keys.reserve(sample.frames.size());
    for (size_t i = 0; i < sample.frames.size(); ++i) {
        const uint64_t key = i == 0 ? sample.frames[i] : sample.frames[i] - 1;
        keys.push_back(key);
        if (frame_cache.contains(key)) {
            continue;
        }
        const trace::object_mapping* const in = trace::mapping_of(object_mappings, key);
        if (in == nullptr) {
            frame_cache.emplace(key, std::string());
            continue;
        }
        const std::string path = the_dsos != nullptr ? the_dsos->path(in->build_id) : std::string();
        if (path.empty()) {
            frame_cache.emplace(key, std::string());
            continue;
        }
        // The offset within the object's own address space, which is what its
        // ELF file is written in -- the same subtraction the decoder does to
        // read a source location.
        wanted[path].push_back({key, key - in->base});
    }
    for (const auto& [path, batch] : wanted) {
        symbolise_batch(path, batch);
    }

    std::vector<std::string> out;
    out.reserve(sample.frames.size());
    for (size_t i = 0; i < sample.frames.size(); ++i) {
        static const std::string nothing;
        const auto found = frame_cache.find(keys[i]);
        const std::string& text = found == frame_cache.end() ? nothing : found->second;
        out.push_back(text.empty() ? fmt::format("#{:<3} {:#018x}", i, sample.frames[i])
                                   : fmt::format("#{:<3} {:#018x}  {}", i, sample.frames[i], text));
    }
    return decoded.emplace(index, std::move(out)).first->second;
}

// The log line a sample gets, which must not symbolise anything: it is built for
// every sample in a task's log, and symbolising is what clicking one is for.
static std::string sample_message(uint64_t index) {
    const stack_sample& sample = samples[index];
    return fmt::format("{:10s} {} frames from {:#x}", "SAMPLE", sample.frames.size(),
                       sample.frames.empty() ? 0 : sample.frames.front());
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
    std::vector<cached_plot_item> plot_items;
};

static std::string log_line_text(const entry& e, int64_t start_ts, bool include_task_id) {
    auto dt_nano = std::chrono::duration<double, std::nano>(double(e.ts - start_ts) * MULTIPLIER);
    auto dt = std::chrono::duration<double, std::milli>(dt_nano);
    // The wall clock first and the offset from the start of the request second:
    // one says when this happened, the other how far into the request it is, and
    // a line reading a latency breakdown wants both.
    const std::string when = realtime_column(e.ts);
    if (include_task_id) {
        return fmt::format("{}  {:12.9f}: {:16x}: {}", when, dt.count(), e.query(),
                           entry_message(e));
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
    cache.plot_items.reserve(cached_count);
    for (size_t source_index = cache.source_begin;
         source_index < cache.source_begin + cached_count; ++source_index) {
        const auto& record = span[source_index];
        cache.lines.push_back({source_index, record, log_line_text(record, cache.start_ts, true)});
    }

    uint64_t iostack = 0;
    int64_t iostart = 0;
    int64_t prev_ts = cache.start_ts;
    bool cpu = true;
    for (size_t source_index = cache.source_begin;
         source_index < cache.source_begin + cached_count; ++source_index) {
        const auto& record = span[source_index];
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

// Task ids are *not* namespaced by shard here, deliberately. A request
// coordinated on one shard reaches a tablet on another, and the continuations
// that run there inherit its id -- so one request's records are spread over two
// shards' files under a single id, and separating them by shard would cut every
// cross-shard request in half. Scylla mints the ids with the shard already in
// the top bits, which is what makes that safe; see fresh_task_id in
// seastar/src/core/scylla_tracer.cc.

// The callback the generated decode() hands each record to: one overload per
// tracepoint the viewer has a use for, and a template that swallows the rest.
struct sink {
    std::vector<entry>& out;
    // Which file this is, so that every record knows which cpu wrote it. See
    // entry::shard.
    uint32_t shard;

    void operator()(const trace::run_task& e, const trace::tracepoint_metadata& m) const {
        out.push_back({0, e.prev, e.task, int64_t(m.timestamp), intern_location(e.at), shard});
    }
    void operator()(const trace::cql_request& e, const trace::tracepoint_metadata& m) const {
        out.push_back({1, e.prev, e.task, int64_t(m.timestamp), 0, shard});
    }
    void operator()(const trace::execution_stage& e, const trace::tracepoint_metadata& m) const {
        out.push_back({0xb, e.prev, e.task, int64_t(m.timestamp), 0, shard});
    }
    void operator()(const trace::io_begin& e, const trace::tracepoint_metadata& m) const {
        out.push_back({0x4, e.task, e.io, int64_t(m.timestamp), 0, shard});
    }
    void operator()(const trace::io_end& e, const trace::tracepoint_metadata& m) const {
        out.push_back({0x5, e.task, e.io, int64_t(m.timestamp), 0, shard});
    }
    // A sample is put aside rather than turned into an entry here: its place in
    // the timeline is its *own* timestamp converted to ticks, and no sync record
    // has been read yet. main() makes the entries once the clock is built.
    void operator()(const trace::stacktrace_sample& e, const trace::tracepoint_metadata&) const {
        stack_sample sample;
        sample.realtime_ns = e.time_ns;
        sample.shard = e.shard;
        const auto* const words = reinterpret_cast<const uint64_t*>(e.frames.data());
        sample.frames.assign(words, words + e.frames.size() / sizeof(uint64_t));
        samples.push_back(std::move(sample));
    }
    // Pass one of the wall clock conversion: a sync record is not an event of
    // the program's own, so it never becomes an entry -- it is put aside, and
    // the whole collection is handed to the_clock once every file is decoded.
    void operator()(const trace::clock_sync& e, const trace::tracepoint_metadata& m) const {
        clock_syncs.push_back({int64_t(m.timestamp), e.realtime_ns, e.ticks_per_second});
    }
    template <typename Event>
    void operator()(const Event&, const trace::tracepoint_metadata&) const {}
};

// One shard's trace file, decoded into the records above.
//
// `shard` is which file this is rather than anything the file says: a trace has
// no field for the cpu it came off, and it does not need one, because there is
// one file per shard.
static void load_trace(const std::filesystem::path& path, uint32_t shard,
                       std::vector<entry>& out, trace::dso_directory& dsos) {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        throw std::system_error(errno, std::generic_category(), path.string());
    }
    const std::vector<char> raw{std::istreambuf_iterator<char>(in),
                                std::istreambuf_iterator<char>()};
    const std::span<const std::byte> bytes{reinterpret_cast<const std::byte*>(raw.data()),
                                           raw.size()};
    trace::decode(bytes, sink{out, shard}, dsos);

    // The objects this thread had mapped, for the raw addresses in a stack
    // sample. Every shard of one process saw the same objects at the same
    // addresses, so the first file that has any is enough.
    if (object_mappings.empty()) {
        object_mappings = trace::trace_mappings(bytes);
    }
}

// The number in `shard-N.trace`, or nothing if the name is not that shape.
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
template <> struct fmt::formatter<entry> : formatter<string_view> {
    auto format(const entry& e, auto& ctx) const -> decltype(ctx.out()) {
        // ctx.out() is an output iterator to write to.
        return fmt::format_to(ctx.out(), "({:016x} {:016x} {:016x} {:016x})", e.event, e.id, e.arg, e.ts);
    }
};

int main(int argc, char** argv) {
    if (argc != 2) {
        fprintf(stderr, "usage: %s SNAPSHOT-DIR\n", argv[0]);
        fprintf(stderr, "  a directory of shard-N.trace files, as written by Scylla's\n"
                        "  POST /system/trace_snapshot into <workdir>/traces/<stamp>/\n");
        return 2;
    }

    // One file per shard, and one metadata stream per file: a trace describes
    // the objects *its own* thread saw loaded, so the shards are decoded
    // separately and merged afterwards rather than concatenated.
    std::vector<std::filesystem::path> files;
    for (const auto& e : std::filesystem::directory_iterator(argv[1])) {
        if (e.path().extension() == ".trace") {
            files.push_back(e.path());
        }
    }
    std::ranges::sort(files);
    if (files.empty()) {
        fprintf(stderr, "no *.trace files in %s\n", argv[1]);
        return 1;
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

    std::vector<entry> entries;
    for (size_t i = 0; i < files.size(); ++i) {
        const auto& file = files[i];
        // The name if it has a number in it, and the position in the sorted
        // list otherwise -- all this has to be is distinct per file.
        const uint32_t shard = shard_of(file).value_or(uint32_t(i));
        load_trace(file, shard, entries, dsos);
        fmt::print("{}: {} records so far\n", file.string(), entries.size());
    }
    if (entries.empty()) {
        fprintf(stderr, "no records in %s\n", argv[1]);
        return 1;
    }

    // Every file has been read, so every sync record in the trace is in hand:
    // pass two can now convert any record against the syncs either side of it.
    // A trace from a tracer older than the sync records -- or from one with them
    // switched off -- simply has none, and the log columns come out blank while
    // everything else works as before.
    the_clock.build(std::move(clock_syncs));
    if (const auto ns_per_tick = the_clock.ns_per_tick()) {
        MULTIPLIER = *ns_per_tick;
    }
    fmt::print("{} clock sync records, {:.6f} ns/tick ({:.4f} GHz){}\n", the_clock.size(),
               MULTIPLIER, 1.0 / MULTIPLIER,
               the_clock.empty() ? " -- no sync records, times unavailable" : "");

    // Stack samples become entries only now, because where a sample belongs in
    // the timeline is its own CLOCK_REALTIME timestamp read back as ticks, and
    // that needs the clock the pass above built. Without a clock they fall back
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
            entries.push_back({0xc, 0, i, samples[i].ts, 0, samples[i].shard});
        }
        fmt::print("{} distinct source locations: {} resolved, {} not\n",
                   locations_resolved + locations_unresolved, locations_resolved,
                   locations_unresolved);
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

    // Which task each sample interrupted. A sample says which cpu it was on and
    // when; the switches say which task each cpu was running from when. So one
    // walk of the merged timeline, carrying the current task per shard, answers
    // it for every sample at once -- and that is the whole of the link between
    // the two windows below. A sample taken while the shard was between tasks
    // keeps task 0 and appears only in the sample list.
    {
        std::unordered_map<uint32_t, uint64_t> running;
        for (entry& e : entries) {
            if (e.event == 0 || e.event == 1 || e.event == 0xa || e.event == 0xb) {
                running[e.shard] = e.arg;
            } else if (e.event == 0xc) {
                const auto found = running.find(e.shard);
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
        const size_t index = size_t(std::strtoul(which, nullptr, 0));
        if (index >= samples.size()) {
            fmt::print("no sample {} ({} in the trace)\n", index, samples.size());
            return 1;
        }
        fmt::print("sample {}: cpu{} task {:x} at {}\n", index, samples[index].shard,
                   samples[index].task, format_realtime(samples[index].realtime_ns));
        for (const std::string& line : sample_backtrace(index)) {
            fmt::print("{}\n", line);
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
        "Latency analyzer", 1280, 720, SDL_WINDOW_OPENGL | SDL_WINDOW_RESIZABLE | SDL_WINDOW_HIGH_PIXEL_DENSITY);
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
    bool show_demo_window = true;
    bool show_config_window = true;
    ImVec4 clear_color = ImVec4(0.45f, 0.55f, 0.60f, 1.00f);
    int log_task_threshold = 10000;
    log_cache log_cache_state;
    full_log_cache full_log_cache_state;

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

        if (ImGui::BeginMainMenuBar()) {
            if (ImGui::BeginMenu("View")) {
                if (ImGui::BeginMenu("Dockers")) {
                    ImGui::MenuItem("Config", nullptr, &show_config_window);
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

        // 1. Show the big demo window (Most of the sample code is in ImGui::ShowDemoWindow()! You can browse its code to learn more about Dear ImGui!).
        if (show_demo_window) {
            // ImGui::ShowDemoWindow(&show_demo_window);
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
            static uint64_t id_log = queries[0].id;
            {
            ImGui::Begin("Graph");
            static double line_x;
            static size_t w = 0;
            static uint64_t id_full_log = id_log;
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
                    id_log = queries[w].id;
                    id_full_log = id_log;
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
                ImGui::Text("%s", fmt::format("{:10s} {:12.9f}", "CPU", std::chrono::duration<double, std::milli>(queries[w].cputime).count()).c_str());
                ImGui::Text("%s", fmt::format("{:10s} {:12.9f}", "STARVE", std::chrono::duration<double, std::milli>(queries[w].starvetime).count()).c_str());
                ImGui::Text("%s", fmt::format("{:10s} {:12.9f}", "IO", std::chrono::duration<double, std::milli>(queries[w].iotime).count()).c_str());
                ImGui::Text("%s", fmt::format("{:10s} {:12.9f}", "TOTAL", std::chrono::duration<double, std::milli>(queries[w].latency).count()).c_str());
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
                                        id_log = x;
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
                ImGui::Begin("Full log plot");
                if (full_log_cache_state.task_count > static_cast<size_t>(log_task_threshold)) {
                    render_truncation_warning(full_log_cache_state.task_count, log_task_threshold, true);
                }
                if (ImPlot::BeginPlot("Full log plot", ImVec2(-1, 100), ImPlotFlags_NoTitle)) {
                    static uint64_t prev_id;
                    auto flag = prev_id == id_full_log ? ImPlotCond_Once : ImPlotCond_Always;
                    prev_id = id_full_log;

                    const int64_t start_ts = full_log_cache_state.start_ts;
                    const int64_t end_ts = full_log_cache_state.end_ts;
                    ImPlot::SetupAxes(nullptr, nullptr, ImPlotAxisFlags_NoGridLines, ImPlotAxisFlags_Lock | ImPlotAxisFlags_NoDecorations);
                    ImPlot::SetupAxisLimitsConstraints(ImAxis_X1, 0, double(end_ts - start_ts)*MULTIPLIER/1e6);
                    ImPlot::SetupAxesLimits(0, double(end_ts - start_ts)*MULTIPLIER/1e6, 0, 1, flag);
                    ImPlot::PushPlotClipRect();

                    for (const auto& item : full_log_cache_state.plot_items) {
                        ImVec2 rmin = ImPlot::PlotToPixels(item.min);
                        ImVec2 rmax = ImPlot::PlotToPixels(item.max);
                        if (item.draw_line) {
                            ImVec2 line_end = ImPlot::PlotToPixels(item.line_end);
                            ImPlot::GetPlotDrawList()->AddLine(rmin, line_end, IM_COL32(0,128,0,255));
                        }
                        ImPlot::GetPlotDrawList()->AddRectFilled(rmin, rmax, item.color);
                    }
                    ImPlot::PopPlotClipRect();

                    if (ImPlot::IsPlotHovered() && ImGui::IsMouseDown(0)) {
                        ImPlotPoint pt = ImPlot::GetPlotMousePos();
                        uint64_t ts = start_ts + pt.x * 1e6 / MULTIPLIER;
                        chosen_one = std::ranges::lower_bound(span, ts, std::ranges::less(), [] (const auto& e) {return e.ts;}) - span.begin() - 1;
                        chosen_one = std::clamp(chosen_one, size_t(0), span.size() - 1);
                        just_chosen = true;
                        chosen_unfull = std::ranges::lower_bound(sorted, std::make_pair<uint64_t, uint64_t>(uint64_t(id_log), uint64_t(ts)), std::ranges::less(), [] (const auto& e) {return std::make_pair<uint64_t, uint64_t>(e.query(), e.ts);}) - sorted.begin() - 1;
                        chosen_unfull = std::clamp(chosen_unfull, size_t(0), sorted.size() - 1);
                        just_chosen_unfull = true;
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
                                        id_log = sample.task;
                                        id_full_log = sample.task;
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
                            // Symbolised here and nowhere earlier: this is the
                            // one sample anybody asked about, and the answer is
                            // kept for the next time it is asked about.
                            for (const std::string& line : sample_backtrace(selected_sample)) {
                                ImGui::TextUnformatted(line.c_str());
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
