// A latency trace viewer, rebuilt around its tables.
//
// The viewer this replaced -- main.cc, deleted with the generated decoder it
// read -- grew as a chain of logic: a step that could only run after another
// step, reaching into globals that another step had filled in. This one is the
// same job written the other way round. It is a
// catalogue of arrays, and a sequence of passes each of which says which arrays
// it reads and which arrays it writes. A pass is expendable -- rewrite it,
// replace its algorithm, drop it and lose exactly the columns it filled -- and
// nothing else has to know.
//
// ============================================================================
//  THE TABLES
// ============================================================================
//
// Nothing here is encapsulated. Every table is a std::vector of a POD row, and
// every reference between tables is an index into another table. Strings are
// `str` handles into one bump arena, so a table of a million rows is a
// million-row allocation and not a million allocations.
//
// Static shape (one row per thing the snapshot directories describe):
//
//   trace::files        one *.trace file, and what its metadata.json says
//   trace::nodes        one traced process, by boot id
//   trace::cpus         one (node, shard): a reactor. "cpu" throughout.
//
// Events (per cpu -- trace::cpus[c] owns trace::tables[c]):
//
//   .switches           the reactor picked up a task: run_task, cql_request,
//                       semaphore_execute, execution_stage, rpc_request_handled
//   .io_begins/.io_ends a task submitted an I/O, and that I/O completed
//   .prep_runs          a prepared statement was executed
//   .prep_deltas        the prepared-statement cache changed, or was dumped
//   .conns              an RPC connection opened, closed, or was dumped
//   .rpcs               a message crossed the wire, or opened a task chain
//   .timeline           (ts, table, index) for every row above, in time order.
//                       This is what the log window walks and what a click on
//                       the plot resolves against.
//
// Joins and derived facts (global):
//
//   trace::locations    interned source locations; index 0 is "none"
//   trace::statements   interned (id, keyspace, text) of a prepared statement
//   trace::connections  one end of an RPC connection, joined to the other end
//   trace::queries      one CQL request, its time range and its cost
//   trace::parts        the (cpu, task) pairs one query was worked on under
//   trace::by_latency   query indices, sorted by latency. The histogram's x.
//
// ============================================================================
//  THE PASSES
// ============================================================================
//
// Each is a free function taking the tables it needs. The comment on each says
// reads/writes; that comment is the dependency graph, and there is no other.
//
//   pass_gather       argv                     -> files, nodes, cpus
//   pass_decode       files                    -> every event table, syncs,
//                                                 locations. Records that carry
//                                                 no timestamp are given one
//                                                 here; see interpolate_untimed
//   pass_order        event tables             -> the same, in timestamp order
//   pass_retime       syncs + event tables     -> every ts in node 0's clock
//   pass_attribute    switches + the rest      -> row.task where the record
//                                                 did not carry one
//   pass_index        the event tables         -> per-cpu task indices
//   pass_io_spans     io_begins + io_ends      -> io_begin.end
//   pass_statements   prep_deltas + prep_runs  -> statements, prep_run.statement
//   pass_connections  conns                    -> connections (paired)
//   pass_rpc_pair     rpcs + connections       -> rpc.peer_cpu/.peer_row
//   pass_queries      switches + rpcs          -> queries, parts
//   pass_query_rows   parts                    -> row.query everywhere
//   pass_cost         switches + io + parts     -> query.t1, query.cpu_ticks,
//                                                 by_latency
//   pass_prefix_sums  by_latency + queries      -> latency/cpu prefix sums
//   pass_query_statement  prep_runs + queries  -> query.statement
//   pass_render       every event table        -> log_lines, slices: the text
//                                                 and the rectangles, for the
//                                                 whole trace, once
//   pass_io_stack     slices                   -> slices, io_slices: the I/O
//                                                 band flattened to the span
//                                                 on top, originals kept
//   pass_lod          slices                   -> lods: the same rectangles at
//                                                 coarser scales, with what is
//                                                 too thin to draw summarised
//
// Everything above happens once, at startup, for the whole trace -- including
// pass_render, which turns every record into the line of text and the
// rectangles that will be drawn for it. The UI then only ever reads. There is
// no cache to invalidate: selecting a request moves the windows and recolours
// what is in them, and what exists on screen does not depend on it. See "the
// view" near the bottom, which is a selection and two scroll positions and
// nothing else.

#include <implot.h>
#include <imgui/imgui.h>
#include <imgui/backends/imgui_impl_sdl3.h>
#include <imgui/backends/imgui_impl_opengl3.h>
#include <SDL3/SDL.h>
#include <SDL3/SDL_opengl.h>

#include <fmt/core.h>
#include <fmt/ranges.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iterator>
#include <limits>
#include <map>
#include <numeric>
#include <optional>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "decoder_plugin.h"
#include "events.h"

namespace {

// ============================================================================
//  1. the arena
// ============================================================================
//
// Every string in every table is a `str`: an offset and a length into one
// growing block of bytes. Rows stay POD and copyable, a table is one
// allocation, and nothing in the event tables ever calls the allocator per
// event. The offset is not a pointer precisely so that the block may grow.

struct str {
    uint32_t off = 0;
    uint32_t len = 0;
};

struct arena {
    std::vector<char> bytes{'\0'};  // offset 0 is the empty string

    str put(std::string_view text) {
        if (text.empty()) {
            return {};
        }
        const auto off = uint32_t(bytes.size());
        bytes.insert(bytes.end(), text.begin(), text.end());
        return {off, uint32_t(text.size())};
    }
    str put(std::span<const std::byte> raw) {
        return put(std::string_view(reinterpret_cast<const char*>(raw.data()), raw.size()));
    }
    [[nodiscard]] std::string_view get(str s) const {
        return {bytes.data() + s.off, s.len};
    }
};

// ============================================================================
//  2. the rows
// ============================================================================

// Which table a timeline entry points into. The order is the order the tables
// are declared in cpu_tables, and for_each_table below visits them in it.
enum table_id : uint16_t {
    tab_switch = 0,
    tab_io_begin,
    tab_io_end,
    tab_prep_run,
    tab_prep_delta,
    tab_conn,
    tab_rpc,
    tab_tq_run,
    n_tables,
};

// Which tracepoint a switch row came from. A switch is "the reactor is running
// this task from here on", and five different records say it.
enum switch_cause : uint8_t {
    sw_run_task = 0,
    sw_cql_request,
    sw_semaphore,
    sw_execution_stage,
    sw_rpc_handled,
};

inline const char* switch_cause_name(uint8_t c) {
    switch (c) {
        case sw_run_task: return "RUN";
        case sw_cql_request: return "CQL";
        case sw_semaphore: return "SEM";
        case sw_execution_stage: return "STAGE";
        case sw_rpc_handled: return "RPC-IN";
        default: return "?";
    }
}

// Which end of a task queue's stretch of the cpu a tq_run row is. Only the
// begin carries a scheduling group: the end is the same queue by construction.
enum tq_kind : uint8_t { tq_begin = 0, tq_end };

enum prep_kind : uint8_t { prep_added = 0, prep_removed, prep_snapshot };
enum conn_kind : uint8_t { conn_open = 0, conn_close, conn_snapshot };
enum rpc_kind : uint8_t {
    rpc_sent = 0, rpc_received, rpc_reply_sent, rpc_reply_received, rpc_handled,
};

inline const char* rpc_kind_name(uint8_t k) {
    switch (k) {
        case rpc_sent: return "RPC-SENT";
        case rpc_received: return "RPC-RECV";
        case rpc_reply_sent: return "RPC-REPLY-SENT";
        case rpc_reply_received: return "RPC-REPLY-RECV";
        case rpc_handled: return "RPC-HANDLED";
        default: return "?";
    }
}

// Sentinel for "no row of that table". Signed, so that `< 0` is the test.
constexpr int32_t none = -1;

// Every row starts with these two, and the generic passes rely on it:
//   ts     the record's timestamp, in ticks -- its own node's until
//          pass_retime, node 0's afterwards.
//   task   which request the record belongs to. Carried by the record for most
//          kinds; filled in from the switches by pass_attribute for the rest.
//   query  index into trace::queries, or `none`. Filled by pass_query_rows.
#define ROW_COMMON     \
    int64_t ts = 0;    \
    uint32_t task = 0; \
    int32_t query = none

struct switch_row {
    ROW_COMMON;
    uint32_t loc = 0;     // trace::locations, where `task` was created
    uint8_t cause = 0;    // switch_cause
    int32_t rpc = none;   // for sw_rpc_handled: the rpc row that opened it
    int32_t run = none;   // the tq_begin row of the run this is in, or none
    int32_t group = none; // that run's scheduling group. Both by pass_task_queue_runs
};

// The reactor gave the cpu to one task queue, or took it back. A run is a
// begin and the end that follows it: every switch between them ran under the
// begin's scheduling group, and the end is the moment the reactor stopped
// running them. pass_task_queue_runs is what joins the pair and hands both
// facts to the switches inside.
struct tq_run_row {
    ROW_COMMON;
    int32_t group = none;  // the scheduling group id, on a tq_begin
    int32_t end = none;    // on a tq_begin: the tq_end that closed it
    uint8_t kind = 0;      // tq_kind
};

struct io_begin_row {
    ROW_COMMON;
    uint64_t io = 0;     // seastar's io descriptor id
    int32_t end = none;  // the io_end row that closed it, on this cpu
};

struct io_end_row {
    ROW_COMMON;
    uint64_t io = 0;
    int32_t begin = none;
};

struct prep_run_row {
    ROW_COMMON;
    str id;                    // the raw prepared-statement id, as bytes
    int32_t statement = none;  // trace::statements, once pass_statements ran
};

struct prep_delta_row {
    ROW_COMMON;
    str id;
    str keyspace;
    str statement;
    uint8_t kind = 0;          // prep_kind
    int32_t statement_index = none;  // trace::statements, filled by pass_statements
};

struct conn_row {
    ROW_COMMON;
    uint64_t connection = 0;  // the process-local connection id
    str local;                // the socket's two ends, as the handshake saw them
    str remote;
    uint64_t peer_boot_msb = 0;  // who the far end said it was
    uint64_t peer_boot_lsb = 0;
    uint32_t peer_shard = 0;
    uint8_t kind = 0;             // conn_kind
    int32_t connection_id = none; // trace::connections
};

struct rpc_row {
    ROW_COMMON;
    uint64_t connection = 0;
    uint64_t sequence = 0;  // counted per direction of each connection
    int64_t msg_id = 0;     // the RPC-level one, on the two reply records
    uint8_t kind = 0;       // rpc_kind
    int32_t connection_id = none;  // trace::connections
    int32_t peer_cpu = none;       // the far end of this message, once paired
    int32_t peer_row = none;       // its row in that cpu's .rpcs
};

// The one entry every record above also gets, and the only table that is a
// merge of the others. `index` is a row of the table `table` names.
//
// `timed` is false for a record of a tracepoint that carries no timestamp: the
// decoder hands such a record the moment of the record before it in its buffer,
// and says that it did. pass_decode spreads a run of them out between the timed
// records either side -- see interpolate_untimed -- so by the time anything
// draws them their `ts` is made up, and this is what says so.
struct timeline_row {
    int64_t ts = 0;
    uint16_t table = 0;
    uint32_t index = 0;
    bool timed = true;
};

// A rectangle. Which kind it is comes from the table it was drawn from, and
// which colour it takes comes from `query` against the selection, so the same
// row serves a request that is selected and one that is not.
//
// It is also what a *summary* is: one rectangle standing for all the ones too
// narrow to draw at some zoom (see pass_lod). A summary covers no single
// record, and says with `density` how much of its span the rectangles it
// replaced covered. Its `query` is the first request represented by those
// rectangles, or none if they were all unattributed. The fields that change
// meaning are marked below; everything that draws a rectangle draws both kinds
// the same way and only asks about `summary` to pick the colour.
struct slice_row {
    double t0 = 0;  // milliseconds from the start of the trace
    double t1 = 0;
    int32_t query = none;  // summary: first query represented, or none
    uint32_t index = 0;    // the row of `table` it was drawn from.
                           // summary: how many rectangles it stands for
    float density = 1.0f;  // summary: the fraction of its span they covered
    uint16_t table = 0;    // tab_switch: on the cpu. tab_io_begin: in an I/O.
    bool summary = false;
};

// One level of detail: the same reactor's rectangles, with everything narrower
// than `scale` replaced by summaries exactly `scale` wide. Built by pass_lod,
// which is where the shape of this is explained.
struct lod_level {
    double scale = 0;               // ms
    std::vector<slice_row> slices;  // sorted by t0, as cpu_tables::slices is
    std::vector<double> reach;      // running maximum of their ends, as slice_reach is
};

struct cpu_tables {
    std::vector<switch_row> switches;
    std::vector<tq_run_row> tq_runs;
    std::vector<io_begin_row> io_begins;
    std::vector<io_end_row> io_ends;
    std::vector<prep_run_row> prep_runs;
    std::vector<prep_delta_row> prep_deltas;
    std::vector<conn_row> conns;
    std::vector<rpc_row> rpcs;

    std::vector<timeline_row> timeline;

    // Built by pass_index: (task, row) pairs sorted by task, for the two
    // tables a query walk has to ask "what did task T do here" of. A sorted
    // array rather than a hash map because it is built once, read many times,
    // and equal_range over it is two cache lines.
    std::vector<std::pair<uint32_t, uint32_t>> switch_by_task;
    std::vector<std::pair<uint32_t, uint32_t>> rpc_by_task;

    // Built by pass_query_rows: which query each task on this cpu belongs to.
    std::unordered_map<uint32_t, int32_t> query_of_task;

    // Built by pass_render: this reactor's whole trace, drawn. One log line
    // per timeline entry, in this table's own arena, and every rectangle of
    // its timeline sorted by where it starts. slice_reach is a running maximum
    // of the slices' ends, which is what makes culling to the visible x range
    // a binary search.
    std::vector<str> log_lines;
    arena log_text;
    std::vector<slice_row> slices;
    std::vector<double> slice_reach;

    // Built by pass_render and kept whole by pass_io_stack, which flattens the
    // copies of them in .slices: every I/O rectangle as it really happened,
    // overlaps and all, so that a picked request's I/O can be drawn over the
    // flattened band it was buried in. Read only by that overlay -- never by
    // the plot's main pass, and never coarsened into a level of detail.
    std::vector<slice_row> io_slices;
    std::vector<double> io_reach;

    // Built by pass_lod: the same rectangles again at coarser and coarser
    // scales, finest first, so that a zoomed-out frame draws one summary per
    // pixel instead of a hundred thousand rectangles it cannot show.
    // Empty is a legal state: the plot then draws .slices, as it always did.
    std::vector<lod_level> lods;
};

// Visit every event table of a cpu. This is what lets the generic passes --
// sorting, retiming, counting -- be three lines rather than seven copies, and
// what makes adding a table a matter of adding it here and to table_id.
template <typename F>
void for_each_table(cpu_tables& t, F&& f) {
    f(tab_switch, t.switches);
    f(tab_io_begin, t.io_begins);
    f(tab_io_end, t.io_ends);
    f(tab_prep_run, t.prep_runs);
    f(tab_prep_delta, t.prep_deltas);
    f(tab_conn, t.conns);
    f(tab_rpc, t.rpcs);
    f(tab_tq_run, t.tq_runs);
}

// --- the static shape ---------------------------------------------------------

struct file_row {
    std::filesystem::path path;
    std::string build_id;
    std::string boot_id;
    std::string level;
    uint32_t shard = 0;
    uint64_t first_record_ns = 0;
    uint64_t last_record_ns = 0;
    bool has_metadata = false;
    int32_t node = none;
    int32_t cpu = none;
};

struct node_row {
    std::string boot_id;
    uint64_t boot_msb = 0;  // the same id as the two words a tracepoint carries
    uint64_t boot_lsb = 0;
    std::string build_id;
    std::set<uint32_t> shards;
    uint64_t first_ns = 0;
    uint64_t last_ns = 0;
};

struct cpu_row {
    uint32_t node = 0;
    uint32_t shard = 0;
    std::string label;  // "<boot-id-head>/shard<N>", for the plot axis
};

struct location_row {
    uint64_t address = 0;
    str file;
    str function;
    uint32_t line = 0;
    bool resolved = false;
};

struct statement_row {
    str id;
    str keyspace;
    str text;
};

// One end of a connection: a (node, connection id) pair, which is what a
// connection is process-locally, joined to the row that is the other end.
struct connection_row {
    uint32_t cpu = 0;
    uint64_t connection = 0;
    str local;
    str remote;
    uint64_t peer_boot_msb = 0;
    uint64_t peer_boot_lsb = 0;
    uint32_t peer_shard = 0;
    int32_t peer = none;  // trace::connections, the far end
};

// A (cpu, task) the query was worked on under. One query has many: a task id
// is inherited by continuations on the same shard, carried to other shards of
// the same node, and re-minted on the far side of every RPC.
struct part_row {
    uint32_t cpu = 0;
    uint32_t task = 0;
    int32_t query = none;
};

struct query_row {
    int64_t t0 = 0;  // the cql_request record: when the frame arrived
    int64_t t1 = 0;  // the last record of any of its parts
    uint32_t root_cpu = 0;
    uint32_t root_task = 0;
    int32_t statement = none;  // trace::statements, if a prepared query ran
    uint32_t parts_begin = 0;  // [begin, end) into trace::parts
    uint32_t parts_end = 0;
    int64_t cpu_ticks = 0;     // summed on-cpu time, over every part
};

struct clock_sync_row {
    int64_t ticks = 0;
    uint64_t realtime_ns = 0;
    uint64_t ticks_per_second = 0;
};

// The whole program's state, and the only thing a pass is handed. There are no
// globals below this line except the two the UI needs to draw with.
struct trace_data {
    arena strings;

    std::vector<file_row> files;
    std::vector<node_row> nodes;
    std::vector<cpu_row> cpus;
    std::vector<cpu_tables> tables;  // parallel to cpus

    std::vector<location_row> locations{location_row{}};  // 0 is "none"
    std::vector<statement_row> statements;
    std::vector<connection_row> connections;
    std::vector<query_row> queries;
    std::vector<part_row> parts;
    std::vector<uint32_t> by_latency;
    // Built by pass_prefix_sums: cumulative latency and cpu time in
    // by_latency order. The extra element at the front makes a selection's
    // aggregate a pair of range queries.
    std::vector<double> latency_prefix;
    std::vector<double> cpu_prefix;

    std::vector<std::vector<clock_sync_row>> syncs;  // parallel to nodes

    double ns_per_tick = 0.2941171840072451;  // until the syncs say otherwise
    // The earliest record in the trace. Both plot axes and every rendered
    // rectangle are milliseconds from here, so that two reactors' rows are the
    // same axis and a rectangle never has to be rebuilt for a new selection.
    int64_t origin = 0;
    int64_t last = 0;  // and the latest, for a plot with no request on it

    [[nodiscard]] std::string_view text(str s) const { return strings.get(s); }
    [[nodiscard]] double seconds(int64_t ticks) const {
        return double(ticks) * ns_per_tick * 1e-9;
    }
    [[nodiscard]] double ms(int64_t ticks) const {
        return double(ticks) * ns_per_tick * 1e-6;
    }
};

// ============================================================================
//  3. the clock
// ============================================================================
//
// A record's timestamp is an rdtsc reading: a duration away from another
// rdtsc reading and nothing at all on its own. What dates it is the clock_sync
// record -- a tick count beside a CLOCK_REALTIME reading -- which the tracer
// writes into the head of every ring and again at every rotation.
//
// Two shards of one process share a clock; two nodes do not. So there is one
// of these per node, and pass_retime converts every node's ticks into node 0's
// through the wall clock they have in common.

class wall_clock {
public:
    void build(std::vector<clock_sync_row> points) {
        std::ranges::sort(points, {}, &clock_sync_row::ticks);
        const auto dup = std::ranges::unique(points, {}, &clock_sync_row::ticks);
        points.erase(dup.begin(), dup.end());
        points_ = std::move(points);
    }

    [[nodiscard]] bool empty() const { return points_.empty(); }

    // The arithmetic is 128-bit and relative to a sync rather than double: a
    // nanosecond count since 1970 is ~2^61, which a double holds only to a few
    // hundred nanoseconds -- coarser than the columns this ends up in.
    [[nodiscard]] std::optional<uint64_t> realtime_ns(int64_t ticks) const {
        if (points_.empty()) {
            return std::nullopt;
        }
        const auto after = std::ranges::lower_bound(points_, ticks, {}, &clock_sync_row::ticks);
        if (after == points_.begin()) {
            return extrapolate_ns(points_.front(), ticks);
        }
        const clock_sync_row& before = *(after - 1);
        if (after == points_.end()) {
            return extrapolate_ns(before, ticks);
        }
        const __int128 span_ticks = __int128(after->ticks) - before.ticks;
        const __int128 span_ns = __int128(after->realtime_ns) - before.realtime_ns;
        if (span_ticks == 0) {
            return before.realtime_ns;
        }
        const __int128 offset = (__int128(ticks - before.ticks) * span_ns) / span_ticks;
        return uint64_t(__int128(before.realtime_ns) + offset);
    }

    [[nodiscard]] std::optional<int64_t> ticks_from_realtime(uint64_t ns) const {
        if (points_.empty()) {
            return std::nullopt;
        }
        const auto after = std::ranges::lower_bound(points_, ns, {}, &clock_sync_row::realtime_ns);
        if (after == points_.begin()) {
            return extrapolate_ticks(points_.front(), ns);
        }
        const clock_sync_row& before = *(after - 1);
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

    // What a tick is worth, measured over the longest baseline the trace
    // offers rather than taken from the rate field, which may well be a
    // default nobody ever calibrated.
    [[nodiscard]] std::optional<double> ns_per_tick() const {
        if (points_.size() >= 2 && points_.back().ticks != points_.front().ticks) {
            return double(points_.back().realtime_ns - points_.front().realtime_ns) /
                   double(points_.back().ticks - points_.front().ticks);
        }
        if (points_.size() == 1 && points_.front().ticks_per_second != 0) {
            return 1e9 / double(points_.front().ticks_per_second);
        }
        return std::nullopt;
    }

private:
    static std::optional<uint64_t> extrapolate_ns(const clock_sync_row& sync, int64_t ticks) {
        if (sync.ticks_per_second == 0) {
            return std::nullopt;
        }
        const __int128 offset =
            (__int128(ticks - sync.ticks) * 1'000'000'000) / __int128(sync.ticks_per_second);
        const __int128 ns = __int128(sync.realtime_ns) + offset;
        return ns < 0 ? std::nullopt : std::optional<uint64_t>(uint64_t(ns));
    }
    static std::optional<int64_t> extrapolate_ticks(const clock_sync_row& sync, uint64_t ns) {
        if (sync.ticks_per_second == 0) {
            return std::nullopt;
        }
        const __int128 offset =
            ((__int128(ns) - sync.realtime_ns) * __int128(sync.ticks_per_second)) / 1'000'000'000;
        return int64_t(__int128(sync.ticks) + offset);
    }

    std::vector<clock_sync_row> points_;
};

// "YYYY-MM-DD HH:MM:SS.NNNNNNNNN", UTC -- what Scylla's own logs are stamped
// in, and the only reading of a wall clock that means the same thing on the
// node and on the machine looking at its trace.
std::string format_realtime(uint64_t ns) {
    const std::time_t seconds = std::time_t(ns / 1'000'000'000ull);
    const uint64_t fraction = ns % 1'000'000'000ull;
    std::tm tm{};
    gmtime_r(&seconds, &tm);
    char buf[32];
    std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tm);
    return fmt::format("{}.{:09}", buf, fraction);
}

// ============================================================================
//  4. pass_gather -- argv -> files, nodes, cpus
// ============================================================================
//
// A trace file is named after a fresh UUID and says nothing about itself in
// its name; the `<uuid>.metadata.json` beside it says which process, which
// shard, which level and which stretch of time. A `shard-N.trace` with no
// metadata beside it is an old snapshot, and falls back to its directory for
// the node and to its name for the shard.

std::optional<std::string> json_field(const std::string& text, const char* key) {
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
        return end == std::string::npos ? std::nullopt
                                        : std::optional(text.substr(p + 1, end - p - 1));
    }
    const auto end = text.find_first_of(",}\n", p);
    return text.substr(p, (end == std::string::npos ? text.size() : end) - p);
}

// A boot id's 32 hex digits as the two words a tracepoint carries. Zero for
// anything that is not a UUID -- which is also what a peer that did not answer
// the handshake leaves in a connection record, so "unparsed" and "unknown"
// compare equal and both mean "no idea".
std::pair<uint64_t, uint64_t> boot_id_halves(std::string_view text) {
    uint64_t halves[2] = {0, 0};
    unsigned digits = 0;
    for (const char c : text) {
        if (c == '-') {
            continue;
        }
        unsigned value = 0;
        if (c >= '0' && c <= '9') value = unsigned(c - '0');
        else if (c >= 'a' && c <= 'f') value = unsigned(c - 'a') + 10;
        else if (c >= 'A' && c <= 'F') value = unsigned(c - 'A') + 10;
        else return {0, 0};
        if (digits >= 32) {
            return {0, 0};
        }
        halves[digits / 16] = (halves[digits / 16] << 4) | value;
        ++digits;
    }
    return digits == 32 ? std::pair{halves[0], halves[1]} : std::pair<uint64_t, uint64_t>{0, 0};
}

void pass_gather(trace_data& d, int argc, char** argv) {
    for (int dir = 1; dir < argc; ++dir) {
        std::vector<file_row> in_dir;
        for (const auto& e : std::filesystem::directory_iterator(argv[dir])) {
            if (e.path().extension() != ".trace") {
                continue;
            }
            file_row f;
            f.path = e.path();
            std::filesystem::path meta_path = f.path;
            meta_path.replace_extension();
            meta_path += ".metadata.json";
            if (std::ifstream in(meta_path); in) {
                const std::string text{std::istreambuf_iterator<char>(in),
                                       std::istreambuf_iterator<char>()};
                f.has_metadata = true;
                if (const auto v = json_field(text, "build_id")) f.build_id = *v;
                if (const auto v = json_field(text, "boot_id")) f.boot_id = *v;
                if (const auto v = json_field(text, "level")) f.level = *v;
                const auto number = [&](const char* key, uint64_t& into) {
                    if (const auto v = json_field(text, key)) {
                        try { into = std::stoull(*v); } catch (const std::exception&) {}
                    }
                };
                uint64_t shard = 0;
                number("shard", shard);
                f.shard = uint32_t(shard);
                number("first_record_ns", f.first_record_ns);
                number("last_record_ns", f.last_record_ns);
            } else {
                // shard-N.trace, and the directory for the process.
                f.boot_id = std::string("directory:") + f.path.parent_path().string();
                const std::string stem = f.path.stem().string();
                if (const auto dash = stem.rfind('-'); dash != std::string::npos) {
                    try { f.shard = uint32_t(std::stoul(stem.substr(dash + 1))); }
                    catch (const std::exception&) {}
                }
            }
            in_dir.push_back(std::move(f));
        }
        // Within a directory by path, so that the order a filesystem happens
        // to report is not part of the answer.
        std::ranges::sort(in_dir, {}, &file_row::path);
        for (auto& f : in_dir) {
            d.files.push_back(std::move(f));
        }
    }

    // Node ids by boot id in first-seen order, which -- the directories having
    // been walked in the order they were given -- keeps the first directory's
    // process as node 0, the reference clock everything else is converted into.
    // Two snapshots of one node therefore merge into one timeline instead of
    // pretending to be two machines.
    std::map<std::string, uint32_t> node_of_boot;
    std::map<std::pair<uint32_t, uint32_t>, uint32_t> cpu_of;
    for (file_row& f : d.files) {
        const auto [it, fresh] = node_of_boot.emplace(f.boot_id, uint32_t(d.nodes.size()));
        if (fresh) {
            node_row n;
            n.boot_id = f.boot_id;
            std::tie(n.boot_msb, n.boot_lsb) = boot_id_halves(f.boot_id);
            n.first_ns = std::numeric_limits<uint64_t>::max();
            d.nodes.push_back(std::move(n));
        }
        f.node = int32_t(it->second);
        node_row& n = d.nodes[it->second];
        n.shards.insert(f.shard);
        if (n.build_id.empty()) {
            n.build_id = f.build_id;
        }
        if (f.has_metadata) {
            n.first_ns = std::min(n.first_ns, f.first_record_ns);
            n.last_ns = std::max(n.last_ns, f.last_record_ns);
        }

        const auto [cit, cfresh] =
            cpu_of.emplace(std::pair{uint32_t(f.node), f.shard}, uint32_t(d.cpus.size()));
        if (cfresh) {
            d.cpus.push_back({uint32_t(f.node), f.shard, {}});
        }
        f.cpu = int32_t(cit->second);
    }
    d.tables.resize(d.cpus.size());
    d.syncs.resize(d.nodes.size());

    // A row's name in the plot. The boot id is cut to its first group -- a
    // time-based UUID's time_low, which differs between two nodes booted a
    // second apart -- because a whole one is 36 characters of axis.
    for (cpu_row& c : d.cpus) {
        const std::string& boot = d.nodes[c.node].boot_id;
        std::string head = fmt::format("node{}", c.node);
        if (const auto dash = boot.find('-'); dash != std::string::npos && boot.size() >= 8) {
            head = boot.substr(0, dash);
        }
        c.label = fmt::format("{}/shard{}", head, c.shard);
    }

    for (const node_row& n : d.nodes) {
        fmt::print("node {}: boot {} build {} shards {} covering {:.3f} s\n",
                   &n - d.nodes.data(), n.boot_id, n.build_id.empty() ? "?" : n.build_id,
                   fmt::join(n.shards, ","),
                   n.last_ns > n.first_ns ? double(n.last_ns - n.first_ns) / 1e9 : 0.0);
    }
}

// ============================================================================
//  5. decoding -- files -> the event tables, syncs, locations
// ============================================================================
//
// Four things, in this order: the sink a record lands in, the symbols the
// decoder plugin calls to reach it, the pass that builds that plugin, and the
// pass that reads the files through it.
//
// One file is one (node, shard, level), and it carries its own metadata stream
// saying where that process's objects were mapped, so it decodes on its own.
// Every record it holds is appended to the table for its kind on its cpu, and
// an entry naming that row is appended to the cpu's timeline. Nothing is
// sorted here -- see pass_order.
//
// What a record *is* comes from the tracepoint tables of the objects it was
// written by, which are read out of `dsos/` and compiled into one plugin; see
// decoder_plugin.h. A tracepoint those tables have and events.h has not is
// dropped by the plugin and never arrives here. A field events.h wants and a
// build has not got arrives at its default, and pass_decoder has said so by
// name.

struct decode_sink {
    trace_data& d;
    cpu_tables& t;
    uint32_t cpu;
    uint32_t node;
    std::unordered_map<uint64_t, uint32_t>& interned;

    // Append a row, and the timeline entry that points at it. Every event in
    // the trace goes through here, which is what makes the timeline complete
    // by construction rather than by a pass that has to be remembered.
    //
    // The row is stamped here rather than by its caller, because what a record
    // says about when it happened is the metadata's answer and not the event's
    // -- and for a record that carries no timestamp it is only half an answer;
    // see interpolate_untimed.
    template <typename Row>
    void push(table_id which, std::vector<Row>& into, Row row,
              const viewer::event_meta& m) const {
        row.ts = int64_t(m.timestamp);
        t.timeline.push_back(
            {row.ts, uint16_t(which), uint32_t(into.size()), m.has_timestamp});
        into.push_back(row);
    }

    // A source location is one address, and the same call site turns up
    // thousands of times -- every continuation off one `then()` -- so the rows
    // hold an index and the strings are stored once.
    uint32_t intern(const viewer::source_location& loc) const {
        if (loc.address == 0 && !loc.resolved) {
            return 0;
        }
        const auto [it, fresh] = interned.emplace(loc.address, uint32_t(d.locations.size()));
        if (fresh) {
            std::string_view file = loc.file;
            if (const auto slash = file.rfind('/'); slash != std::string_view::npos) {
                file = file.substr(slash + 1);
            }
            d.locations.push_back({loc.address, d.strings.put(file),
                                   d.strings.put(loc.function), loc.line, loc.resolved});
        }
        return it->second;
    }

    void switch_to(uint8_t cause, uint32_t task,
                   const viewer::event_meta& m, uint32_t loc) const {
        switch_row r;
        r.task = task;
        r.cause = cause;
        r.loc = loc;
        push(tab_switch, t.switches, r, m);
    }

    void operator()(const viewer::events::run_task& e, const viewer::event_meta& m) const {
        switch_to(sw_run_task, e.task, m, intern(e.at));
    }
    void operator()(const viewer::events::cql_request& e, const viewer::event_meta& m) const {
        switch_to(sw_cql_request, e.task, m, 0);
    }
    void operator()(const viewer::events::semaphore_execute& e, const viewer::event_meta& m) const {
        switch_to(sw_semaphore, e.task, m, 0);
    }
    void operator()(const viewer::events::execution_stage& e, const viewer::event_meta& m) const {
        switch_to(sw_execution_stage, e.task, m, 0);
    }
    // An inbound request opens a task chain on this shard, which is a switch in
    // exactly the sense the four above are -- and it is also the far end of a
    // message, which is what joins two nodes. So it becomes two rows: the
    // switch the timeline and the plot see, and an rpc row carrying the
    // (connection, sequence) that pass_rpc_pair joins on. The rpc row is the
    // one exception to "every row has a timeline entry": the switch is what
    // the log shows, and two lines for one record would be a lie.
    void operator()(const viewer::events::rpc_request_handled& e,
                    const viewer::event_meta& m) const {
        rpc_row r;
        r.ts = int64_t(m.timestamp);
        r.task = e.task;
        r.connection = e.connection;
        r.sequence = e.sequence;
        r.kind = rpc_handled;
        const auto rpc_index = int32_t(t.rpcs.size());
        t.rpcs.push_back(r);

        switch_row s;
        s.task = e.task;
        s.cause = sw_rpc_handled;
        s.rpc = rpc_index;
        push(tab_switch, t.switches, s, m);
    }

    void tq_run(uint8_t kind, int32_t group, const viewer::event_meta& m) const {
        tq_run_row r;
        r.kind = kind;
        r.group = group;
        push(tab_tq_run, t.tq_runs, r, m);
    }
    void operator()(const viewer::events::task_queue_run_begin& e,
                    const viewer::event_meta& m) const {
        tq_run(tq_begin, int32_t(e.scheduling_group), m);
    }
    void operator()(const viewer::events::task_queue_run_end&,
                    const viewer::event_meta& m) const {
        tq_run(tq_end, none, m);
    }

    void operator()(const viewer::events::io_begin& e, const viewer::event_meta& m) const {
        io_begin_row r;
        r.task = e.task;
        r.io = e.io;
        push(tab_io_begin, t.io_begins, r, m);
    }
    void operator()(const viewer::events::io_end& e, const viewer::event_meta& m) const {
        io_end_row r;
        r.task = e.task;
        r.io = e.io;
        push(tab_io_end, t.io_ends, r, m);
    }

    void operator()(const viewer::events::prepared_query_run& e,
                    const viewer::event_meta& m) const {
        prep_run_row r;
        r.id = d.strings.put(e.id);
        push(tab_prep_run, t.prep_runs, r, m);
    }
    void delta(uint8_t kind, std::string_view keyspace, std::string_view statement,
               std::span<const std::byte> id, const viewer::event_meta& m) const {
        prep_delta_row r;
        r.kind = kind;
        r.id = d.strings.put(id);
        r.keyspace = d.strings.put(keyspace);
        r.statement = d.strings.put(statement);
        push(tab_prep_delta, t.prep_deltas, r, m);
    }
    void operator()(const viewer::events::prepared_statement_added& e,
                    const viewer::event_meta& m) const {
        delta(prep_added, e.keyspace, e.statement, e.id, m);
    }
    void operator()(const viewer::events::prepared_statement_removed& e,
                    const viewer::event_meta& m) const {
        delta(prep_removed, e.keyspace, e.statement, e.id, m);
    }
    void operator()(const viewer::events::prepared_statement_snapshot_entry& e,
                    const viewer::event_meta& m) const {
        delta(prep_snapshot, e.keyspace, e.statement, e.id, m);
    }

    void connection(uint8_t kind, uint64_t id, std::string_view local, std::string_view remote,
                    uint64_t msb, uint64_t lsb, uint32_t peer_shard,
                    const viewer::event_meta& m) const {
        conn_row r;
        r.kind = kind;
        r.connection = id;
        r.local = d.strings.put(local);
        r.remote = d.strings.put(remote);
        r.peer_boot_msb = msb;
        r.peer_boot_lsb = lsb;
        r.peer_shard = peer_shard;
        push(tab_conn, t.conns, r, m);
    }
    void operator()(const viewer::events::rpc_connection_open& e,
                    const viewer::event_meta& m) const {
        connection(conn_open, e.connection, e.local, e.remote, e.peer_boot_msb, e.peer_boot_lsb,
                   e.peer_shard, m);
    }
    void operator()(const viewer::events::rpc_connection_close& e,
                    const viewer::event_meta& m) const {
        connection(conn_close, e.connection, {}, {}, e.peer_boot_msb, e.peer_boot_lsb,
                   e.peer_shard, m);
    }
    void operator()(const viewer::events::rpc_connection_snapshot_entry& e,
                    const viewer::event_meta& m) const {
        connection(conn_snapshot, e.connection, e.local, e.remote, e.peer_boot_msb,
                   e.peer_boot_lsb, e.peer_shard, m);
    }

    void message(uint8_t kind, uint64_t conn, uint64_t seq, int64_t msg_id, uint32_t task,
                 const viewer::event_meta& m) const {
        rpc_row r;
        r.task = task;
        r.connection = conn;
        r.sequence = seq;
        r.msg_id = msg_id;
        r.kind = kind;
        push(tab_rpc, t.rpcs, r, m);
    }
    // The task on a send is the one that *queued* the buffer, carried by the
    // record: the connection's send loop is what actually writes it, and
    // asking what was running on the shard would answer "the connection" and
    // pull every unrelated request into the walk.
    void operator()(const viewer::events::rpc_message_sent& e, const viewer::event_meta& m) const {
        message(rpc_sent, e.connection, e.sequence, 0, e.task, m);
    }
    void operator()(const viewer::events::rpc_message_received& e,
                    const viewer::event_meta& m) const {
        message(rpc_received, e.connection, e.sequence, 0, 0, m);
    }
    void operator()(const viewer::events::rpc_reply_sent& e, const viewer::event_meta& m) const {
        message(rpc_reply_sent, e.connection, e.sequence, e.msg_id, e.task, m);
    }
    void operator()(const viewer::events::rpc_reply_received& e,
                    const viewer::event_meta& m) const {
        message(rpc_reply_received, e.connection, e.sequence, e.msg_id, 0, m);
    }

    // Not an event of the program's own: it is how pass_retime dates the rest.
    void operator()(const viewer::events::clock_sync& e, const viewer::event_meta& m) const {
        d.syncs[node].push_back({int64_t(m.timestamp), e.realtime_ns, e.ticks_per_second});
    }

};

}  // namespace

// --- the viewer's side of the plugin boundary --------------------------------
//
// One exported symbol per event in events.h. A decoder plugin is compiled at
// startup and leaves these undefined; dlopen() binds them to these, which is
// why the viewer is linked -rdynamic. `sink` is the decode_sink the plugin was
// handed, passed back untouched.
//
// **This list is events.h's list**, and it is read from there: a struct added
// to `viewer::events` and to VIEWER_EVENT_LIST gets its symbol here, and gets
// bridged by decoder_plugin.cc, with nothing to keep in step by hand.
#define ON_DECODE(name)                                                             \
    extern "C" void on_decode_##name(void* sink, const viewer::events::name& event, \
                                     const viewer::event_meta& meta) {              \
        (*static_cast<const decode_sink*>(sink))(event, meta);                      \
    }

VIEWER_EVENT_LIST(ON_DECODE)

#undef ON_DECODE

namespace {

// The `ts` of one row, whichever table it is in. A visit of the eight rather
// than a switch, for the same reason the generic passes are: a table is added
// in one place.
void set_row_ts(cpu_tables& t, uint16_t table, uint32_t index, int64_t ts) {
    for_each_table(t, [&](table_id which, auto& rows) {
        if (uint16_t(which) == table) {
            rows[index].ts = ts;
        }
    });
}

// Give the records that carried no timestamp times of their own.
//
// A tracepoint declared TRACEPOINT_UNTIMED() writes none, and the decoder hands
// such a record the moment of the record before it in its buffer -- so a run of
// them arrives as several events stamped identically, at the moment the run
// opened. That is the truth about them and it is not something this viewer can
// draw: a slice needs a width, the log needs an order, and a dozen events at
// one instant are a dozen rectangles on top of each other.
//
// So a run of n of them between two timed records is spread evenly across the
// gap: the i'th is placed at t1 + (t2 - t1) * i / (n + 1). The times are made
// up, and the only thing they claim is what the trace does claim -- that these
// happened after t1, in this order, and before t2. A run with nothing timed
// after it keeps what it was given, there being nothing to interpolate towards.
//
// Over the entries one file's decode appended, because that is the span in
// which "the record before it" means anything: a shard's levels are separate
// files, read one after the other and put in order later by pass_order.
//
// The rows the timeline does not name keep the decoder's timestamp: today that
// is the rpc row of an rpc_request_handled, whose tracepoint is timed anyway.
void interpolate_untimed(cpu_tables& t, size_t from) {
    for (size_t at = from; at < t.timeline.size();) {
        if (t.timeline[at].timed) {
            ++at;
            continue;
        }
        size_t end = at;
        while (end < t.timeline.size() && !t.timeline[end].timed) {
            ++end;
        }
        if (end == t.timeline.size()) {
            break;
        }
        // t1 is what the decoder gave the run, which is the moment of the last
        // timed record before it -- and is not necessarily the entry before it
        // here, because not every record becomes a timeline entry.
        const int64_t t1 = t.timeline[at].ts;
        const int64_t t2 = t.timeline[end].ts;
        const auto n = int64_t(end - at);
        for (size_t i = at; i < end; ++i) {
            const int64_t ts = t1 + (t2 - t1) * int64_t(i - at + 1) / (n + 1);
            t.timeline[i].ts = ts;
            set_row_ts(t, t.timeline[i].table, t.timeline[i].index, ts);
        }
        at = end;
    }
}

// --- pass_decoder -- the objects -> one decoder ------------------------------

// The decoder for these snapshots, and a line each about what the objects'
// tracepoint tables and this viewer's events.h disagree about.
//
// Its own pass because it is the expensive part of reading a trace the first
// time -- a plugin is a C++ file clang compiles -- and because it is the pass
// that fails when a snapshot arrives without the objects it was written by.
// Nothing here reads a record; see decoder_plugin.h.
void pass_decoder(const plugin::decoder& dec) {
    if (dec.decode == nullptr) {
        fmt::print("no decoder: {}\n", dec.error);
    } else {
        fmt::print("decoder: {} tracepoints in {} object{}, {} of them bridged into events.h "
                   "({})\n",
                   dec.tracepoints, dec.objects, dec.objects == 1 ? "" : "s", dec.bridged,
                   dec.from_cache ? "cached" : "compiled");
    }
    // What the viewer will not know about these traces, in full: an object it
    // could not read, a tracepoint whose records are dropped, a field that will
    // stay at its default for every record of its kind. Printed either way,
    // because when there is no decoder at all these are why.
    for (const std::string& note : dec.notes) {
        fmt::print("    {}\n", note);
    }
}

// --- pass_decode -- files + the plugin -> the event tables -------------------

void pass_decode(trace_data& d, const plugin::decoder& dec, const std::string& dso_root) {
    if (dec.decode == nullptr) {
        return;  // pass_decoder said so already
    }
    std::unordered_map<uint64_t, uint32_t> interned;
    for (const file_row& f : d.files) {
        // Sized, rather than the std::istreambuf_iterator pair this used to be.
        // The iterator pair reads a byte at a time through the streambuf and
        // grows the vector as it goes, and a shard's debug file is tens of
        // megabytes; one file_size, one allocation and one read is about 6% of
        // the whole startup back.
        const auto size = std::filesystem::file_size(f.path);
        std::ifstream in(f.path, std::ios::binary);
        std::vector<char> raw(size);
        if (!in || (size != 0 && !in.read(raw.data(), std::streamsize(size)))) {
            throw std::system_error(errno, std::generic_category(), f.path.string());
        }
        const std::span<const std::byte> bytes{
            reinterpret_cast<const std::byte*>(raw.data()), raw.size()};
        cpu_tables& t = d.tables[f.cpu];
        // A record is not self-delimiting, so a decode that fails cannot be
        // resynchronised past -- but what it read before that point is in the
        // tables and consistent, and the other files are unaffected. The
        // failure worth expecting is a `dsos/` that does not go with these
        // traces: see "Decoders" in the README.
        const size_t first = t.timeline.size();
        decode_sink sink{d, t, uint32_t(f.cpu), uint32_t(f.node), interned};
        // No exception crosses the plugin boundary -- see plugin_abi.h -- so a
        // failure comes back as a message rather than as a throw.
        std::array<char, 1024> failure{};
        if (dec.decode(bytes.data(), bytes.size(), const_cast<decode_sink*>(&sink),
                       dso_root.c_str(), failure.data(), failure.size()) != 0) {
            fmt::print("{}: {}\n", f.path.filename().string(), failure.data());
        }
        // Over what this file appended, decode order and all, before the next
        // file's records are put after it. A decode that threw part way through
        // still leaves the records it did read, and they are interpolated like
        // any others.
        interpolate_untimed(t, first);
        fmt::print("{} (node {} shard {} {}): {} events on this cpu so far\n",
                   f.path.filename().string(), f.node, f.shard,
                   f.level.empty() ? "all levels" : f.level, t.timeline.size());
    }
    size_t resolved = 0;
    for (const location_row& l : d.locations) {
        resolved += l.resolved ? 1 : 0;
    }
    fmt::print("{} distinct source locations: {} resolved, {} not\n", d.locations.size() - 1,
               resolved, d.locations.size() - 1 - resolved);
}

// ============================================================================
//  6. pass_order -- the event tables -> the same, in timestamp order
// ============================================================================
//
// Files within one stream have disjoint time ranges and are read in order, but
// a shard's levels are separate files and their records interleave. Rather
// than assume which level holds which event -- which would be a fact about
// today's tracepoint table, wired into the reader -- every table is checked
// and put in order if it needs it, and the timeline entries pointing into it
// are remapped.
//
// Nothing here is *sorted*, though, and that is the point. What arrives is a
// cpu's files read one after another, so a table out of order is not disorder
// but a handful of sorted runs laid end to end -- one per file it drew from --
// and merging runs is linear where sorting them is not.
//
// One table is fed from both rings by construction, so this is the ordinary
// case rather than the odd one: .switches takes cql_request, which the tracer
// writes at *info*, alongside run_task and the other three, which it writes at
// debug. Measured on latte-run-task32, the first fixture here whose
// cql_request records decode: every unsorted table is exactly two runs,
// breaking at row 98,528 of 1.8 million -- the 98.8k CQL requests that cpu saw
// -- and every timeline is two as well. Sorting them cost 1.10 s of an 8.1 s
// startup; merging them costs 0.26 s, for output identical record for record.
//
// The timeline is worth the same treatment for the same reason. It is the
// bigger array -- one entry per record rather than per record of one kind --
// and it is two runs whenever any of its tables is.

// The runs, merged left to right. One merge for the two-run case this is
// really about, and the fold keeps it correct rather than merely fast when a
// future capture has more of them. Ties keep the order they arrived in, which
// is what the tables' own indices are numbered by.
template <typename T, typename Key>
void merge_runs(std::vector<T>& v, Key key) {
    std::vector<size_t> runs;
    for (size_t i = 1; i < v.size(); ++i) {
        if (std::invoke(key, v[i]) < std::invoke(key, v[i - 1])) {
            runs.push_back(i);
        }
    }
    if (runs.empty()) {
        return;
    }
    runs.push_back(v.size());
    size_t merged = runs.front();
    for (size_t r = 1; r < runs.size(); ++r) {
        std::ranges::inplace_merge(v.begin(), v.begin() + merged, v.begin() + runs[r], {}, key);
        merged = runs[r];
    }
}

template <typename Row>
bool sort_table(std::vector<Row>& rows, std::vector<uint32_t>& old_to_new) {
    if (std::ranges::is_sorted(rows, {}, &Row::ts)) {
        return false;
    }
    std::vector<uint32_t> order(rows.size());
    for (uint32_t i = 0; i < order.size(); ++i) {
        order[i] = i;
    }
    merge_runs(order, [&](uint32_t i) { return rows[i].ts; });

    std::vector<Row> sorted;
    sorted.reserve(rows.size());
    old_to_new.assign(rows.size(), 0);
    for (uint32_t at = 0; at < order.size(); ++at) {
        old_to_new[order[at]] = at;
        sorted.push_back(rows[order[at]]);
    }
    rows.swap(sorted);
    return true;
}

void pass_order(trace_data& d) {
    for (cpu_tables& t : d.tables) {
        std::vector<uint32_t> remap[n_tables];
        bool moved[n_tables] = {};
        for_each_table(t, [&](table_id which, auto& rows) {
            moved[which] = sort_table(rows, remap[which]);
        });
        // The switch rows' `rpc` index survives, because rpc rows carrying a
        // request_handled are appended without a timeline entry and so are not
        // covered by the remap loop below.
        if (moved[tab_rpc]) {
            for (switch_row& s : t.switches) {
                if (s.rpc >= 0) {
                    s.rpc = int32_t(remap[tab_rpc][s.rpc]);
                }
            }
        }
        for (timeline_row& e : t.timeline) {
            if (moved[e.table]) {
                e.index = remap[e.table][e.index];
            }
        }
        // Merged, not sorted, like the tables above and for the same reason.
        // Its runs are found by the same scan that would have checked whether
        // it was in order at all, so the second run of this pass -- after
        // pass_retime, where nothing has moved unless a node's clock went
        // backwards -- still costs one pass over the entries and nothing else.
        merge_runs(t.timeline, &timeline_row::ts);
    }
}

// ============================================================================
//  7. pass_retime -- syncs + event tables -> every ts in node 0's clock
// ============================================================================
//
// rdtsc values from two machines are unrelated numbers. Each node's syncs pair
// its ticks with CLOCK_REALTIME, so a record is converted by going out through
// the wall clock of the node that wrote it and back in through node 0's.

void pass_retime(trace_data& d) {
    std::vector<wall_clock> clocks(d.nodes.size());
    size_t sync_count = 0;
    for (size_t node = 0; node < d.nodes.size(); ++node) {
        sync_count += d.syncs[node].size();
        clocks[node].build(d.syncs[node]);
    }
    if (clocks.empty()) {
        return;
    }
    const wall_clock& reference = clocks.front();
    if (const auto rate = reference.ns_per_tick()) {
        d.ns_per_tick = *rate;
    }
    fmt::print("{} clock sync records, {:.6f} ns/tick ({:.4f} GHz){}\n", sync_count,
               d.ns_per_tick, 1.0 / d.ns_per_tick,
               reference.empty() ? " -- no sync records, times unavailable" : "");

    for (size_t cpu = 0; cpu < d.cpus.size(); ++cpu) {
        const uint32_t node = d.cpus[cpu].node;
        if (node == 0 || clocks[node].empty() || reference.empty()) {
            continue;
        }
        const auto convert = [&](int64_t ticks) {
            if (const auto ns = clocks[node].realtime_ns(ticks)) {
                if (const auto out = reference.ticks_from_realtime(*ns)) {
                    return *out;
                }
            }
            return ticks;
        };
        cpu_tables& t = d.tables[cpu];
        for_each_table(t, [&](table_id, auto& rows) {
            for (auto& r : rows) {
                r.ts = convert(r.ts);
            }
        });
        for (timeline_row& e : t.timeline) {
            e.ts = convert(e.ts);
        }
    }
    // Converting is monotone, so the order pass_order established holds -- but
    // only if the clocks are, and a sync pair that went backwards would
    // otherwise leave a table that lies about being sorted. So main runs
    // pass_order again after this one rather than trusting that.
}

// ============================================================================
//  8. pass_index -- the event tables -> the per-cpu task indices
// ============================================================================
//
// "What did task T do on this cpu" is asked once per part per query walk, and
// a linear scan of a shard's switches would make the walk quadratic. Two
// sorted (task, row) arrays answer it with an equal_range.
//
// There was a third, over the I/O begins, for when pass_render and pass_cost
// subtracted a task's own I/O from its stretch of cpu. They no longer do --
// the task queue run's end says when the cpu was given back, and the I/O is
// drawn over the cpu rather than out of it -- so nothing asks that question
// any more and the index is gone with it.

void pass_index(trace_data& d) {
    for (cpu_tables& t : d.tables) {
        t.switch_by_task.clear();
        t.switch_by_task.reserve(t.switches.size());
        for (uint32_t i = 0; i < t.switches.size(); ++i) {
            t.switch_by_task.emplace_back(t.switches[i].task, i);
        }
        t.rpc_by_task.clear();
        t.rpc_by_task.reserve(t.rpcs.size());
        for (uint32_t i = 0; i < t.rpcs.size(); ++i) {
            t.rpc_by_task.emplace_back(t.rpcs[i].task, i);
        }
        // By task, and by row within a task, so a range is also in time order.
        std::ranges::sort(t.switch_by_task);
        std::ranges::sort(t.rpc_by_task);
    }
}

// The rows of `index` whose task is `task`, as a [first, last) pair of
// iterators. The values are row numbers into whichever table the index was
// built from.
inline auto rows_of_task(const std::vector<std::pair<uint32_t, uint32_t>>& index, uint32_t task) {
    return std::ranges::equal_range(index, task, {},
                                    &std::pair<uint32_t, uint32_t>::first);
}

// Which task the reactor was running at `ts`: the switch at or before it. The
// switches are sorted, so this is a binary search, and `none` means the trace
// starts after the moment asked about.
int32_t switch_at(const cpu_tables& t, int64_t ts) {
    const auto after = std::ranges::upper_bound(t.switches, ts, {}, &switch_row::ts);
    if (after == t.switches.begin()) {
        return none;
    }
    return int32_t((after - 1) - t.switches.begin());
}

// ============================================================================
//  9. pass_attribute -- switches -> row.task where the record carried none
// ============================================================================
//
// A prepared-query run, a connection record and an inbound message do not name
// a task: nothing plumbs one to them. What they have instead is a shard and a
// moment, and the switch at or before that moment says what that shard was
// running. That is the honest answer, and it is what puts these records into
// the right request's log.
//
// It is deliberately not used for the two send records: those carry the task
// that queued the buffer, and the ambient task there is the connection's send
// loop.

void pass_attribute(trace_data& d) {
    for (cpu_tables& t : d.tables) {
        const auto ambient = [&](int64_t ts) -> uint64_t {
            const int32_t s = switch_at(t, ts);
            return s < 0 ? 0 : t.switches[s].task;
        };
        for (prep_run_row& r : t.prep_runs) {
            r.task = ambient(r.ts);
        }
        for (prep_delta_row& r : t.prep_deltas) {
            r.task = ambient(r.ts);
        }
        for (conn_row& r : t.conns) {
            r.task = ambient(r.ts);
        }
        for (rpc_row& r : t.rpcs) {
            if (r.kind == rpc_received || r.kind == rpc_reply_received) {
                r.task = ambient(r.ts);
            }
        }
    }
}

// ============================================================================
//  10. pass_task_queue_runs -- tq_runs + timeline -> switch.run, .group, tq.end
// ============================================================================
//
// The reactor gives the cpu to one task queue, runs whatever that queue holds,
// and takes it back. Two records bracket that, and joining them answers two
// questions at once for every switch in between:
//
//   which scheduling group ran it    the begin carries the id. A run_task
//                                    record does not, and making it would be a
//                                    field on the hottest record in the trace
//                                    for something that changes a few times a
//                                    millisecond.
//   when it stopped being on the cpu the end. This is the record the trace
//                                    never had: there is no "task ended"
//                                    tracepoint, but the *reactor* says when it
//                                    gave the cpu back, and nothing it picked
//                                    up is running after that.
//
// So a switch gets `run` -- the tq_begin row it is inside -- and the begin gets
// `end`, and switch_ends() below turns the pair into the one number both
// pass_render and pass_cost want.
//
// The walk is over the *timeline* rather than over the two tables in parallel,
// and that is the reason to prefer it: two records of one shard can share an
// rdtsc tick -- a begin and the first run_task under it routinely do -- and the
// timeline is the one place their order survives, because it is stable-sorted
// and so still in the order the ring holds them.
//
// Both sentinels stay `none` rather than being guessed at, and a snapshot is a
// ring of a running system, so both happen at its edges: a switch older than
// the first begin in the file is the tail of a run that was evicted, and the
// run that was open when the snapshot was taken has no end in it.

void pass_task_queue_runs(trace_data& d) {
    size_t runs = 0, closed = 0, attributed = 0, orphaned = 0;
    for (cpu_tables& t : d.tables) {
        int32_t open = none;
        for (const timeline_row& e : t.timeline) {
            if (e.table == tab_tq_run) {
                if (t.tq_runs[e.index].kind == tq_begin) {
                    open = int32_t(e.index);
                    ++runs;
                } else {
                    if (open >= 0) {
                        t.tq_runs[open].end = int32_t(e.index);
                        ++closed;
                    }
                    open = none;
                }
            } else if (e.table == tab_switch) {
                switch_row& sw = t.switches[e.index];
                sw.run = open;
                sw.group = open >= 0 ? t.tq_runs[open].group : none;
                ++(open >= 0 ? attributed : orphaned);
            }
        }
    }
    fmt::print("{} task queue runs, {} of them closed in the snapshot: {} switches in one, "
               "{} before the first\n", runs, closed, attributed, orphaned);
}

// When the reactor stopped running the task a switch picked up.
//
// The end of its task queue run is the honest answer -- that record is the
// reactor saying it gave the cpu back, and nothing it picked up is running
// after it. The next switch on the shard bounds it too, and is all there is
// when the run's end is not in the snapshot; `cpu_end` is the reactor's last
// record, for the switch that is still the newest one.
//
// Reads switch.run and tq_run.end, so without pass_task_queue_runs it falls
// back to the next switch on its own -- which is all this could do before there
// were task queue records at all.
inline int64_t switch_ends(const cpu_tables& t, uint32_t i, int64_t cpu_end) {
    int64_t to = i + 1 < t.switches.size() ? t.switches[i + 1].ts : cpu_end;
    const int32_t run = t.switches[i].run;
    if (run >= 0 && t.tq_runs[run].end >= 0) {
        to = std::min(to, t.tq_runs[t.tq_runs[run].end].ts);
    }
    return to;
}

// ============================================================================
//  11. pass_io_spans -- io_begins + io_ends -> io_begin.end
// ============================================================================
//
// An I/O is a pair of records sharing a descriptor id. The id is reused once
// the descriptor is freed, so the pairing is "the open begin with this id",
// which one sweep in timestamp order answers. A begin whose end is not in the
// trace keeps `none`: the ring evicted it, or the snapshot was taken while the
// I/O was still in flight, and both are worth seeing as an unclosed bar.

void pass_io_spans(trace_data& d) {
    size_t paired = 0;
    size_t open = 0;
    for (cpu_tables& t : d.tables) {
        std::unordered_map<uint64_t, uint32_t> pending;  // io id -> io_begin row
        size_t at_begin = 0;
        size_t at_end = 0;
        while (at_begin < t.io_begins.size() || at_end < t.io_ends.size()) {
            const bool take_begin =
                at_end == t.io_ends.size() ||
                (at_begin < t.io_begins.size() && t.io_begins[at_begin].ts <= t.io_ends[at_end].ts);
            if (take_begin) {
                pending[t.io_begins[at_begin].io] = uint32_t(at_begin);
                ++at_begin;
            } else {
                const auto found = pending.find(t.io_ends[at_end].io);
                if (found != pending.end()) {
                    t.io_begins[found->second].end = int32_t(at_end);
                    t.io_ends[at_end].begin = int32_t(found->second);
                    pending.erase(found);
                    ++paired;
                }
                ++at_end;
            }
        }
        open += pending.size();
    }
    fmt::print("{} I/Os paired, {} still in flight at the end of their file\n", paired, open);
}

// ============================================================================
//  12. pass_statements -- prep_deltas + prep_runs -> statements, .statement
// ============================================================================
//
// A prepared_query_run carries an id and nothing else. What that id meant is
// the state of the shard's statement cache *at that moment*, and the trace
// describes that cache in two ways: a snapshot, written when the trace is
// taken, and the additions and removals on either side of it.
//
// So this is a sweep in both directions from the snapshot. Backwards, an
// addition is undone by removing and a removal is undone by adding -- which
// works because both delta records carry the keyspace and the text, not just
// the id. Forwards it is the obvious thing. A separate walk per run would be
// quadratic; one sweep with the map kept current is linear.

void pass_statements(trace_data& d) {
    // Step one: every statement the trace names anywhere, interned, and the
    // index written back into the delta row that named it. It is done first
    // and on its own because it is the only thing here that grows the arena,
    // and the sweeps below key their maps on views into it.
    {
        std::map<std::pair<std::string, std::string>, int32_t> seen;
        for (cpu_tables& t : d.tables) {
            for (prep_delta_row& r : t.prep_deltas) {
                std::pair<std::string, std::string> key{std::string(d.text(r.id)),
                                                        std::string(d.text(r.statement))};
                const auto [it, fresh] = seen.emplace(std::move(key), int32_t(d.statements.size()));
                if (fresh) {
                    d.statements.push_back({r.id, r.keyspace, r.statement});
                }
                r.statement_index = it->second;
            }
        }
    }

    size_t named = 0;
    size_t unnamed = 0;
    for (cpu_tables& t : d.tables) {
        // Where the snapshot is: the run of prep_snapshot deltas. Everything
        // before it is reconstructed backwards, everything after forwards.
        size_t snapshot_begin = t.prep_deltas.size();
        size_t snapshot_end = t.prep_deltas.size();
        for (size_t i = 0; i < t.prep_deltas.size(); ++i) {
            if (t.prep_deltas[i].kind == prep_snapshot) {
                if (snapshot_begin == t.prep_deltas.size()) {
                    snapshot_begin = i;
                }
                snapshot_end = i + 1;
            }
        }

        // The cache as the snapshot found it, keyed by the raw id.
        std::unordered_map<std::string_view, int32_t> cache;
        for (size_t i = snapshot_begin; i < snapshot_end; ++i) {
            const prep_delta_row& r = t.prep_deltas[i];
            if (r.kind == prep_snapshot) {
                cache[d.text(r.id)] = r.statement_index;
            }
        }

        const auto snapshot_ts = snapshot_begin < t.prep_deltas.size()
                                     ? t.prep_deltas[snapshot_begin].ts
                                     : std::numeric_limits<int64_t>::max();

        // Backwards, from the snapshot to the start of the trace.
        {
            auto state = cache;
            size_t run = t.prep_runs.size();
            while (run > 0 && t.prep_runs[run - 1].ts >= snapshot_ts) {
                --run;
            }
            size_t delta = snapshot_begin;
            while (run > 0) {
                --run;
                // Undo every delta later than this run, then read the map.
                while (delta > 0 && t.prep_deltas[delta - 1].ts > t.prep_runs[run].ts) {
                    --delta;
                    const prep_delta_row& r = t.prep_deltas[delta];
                    if (r.kind == prep_added) {
                        state.erase(d.text(r.id));
                    } else if (r.kind == prep_removed) {
                        state[d.text(r.id)] = r.statement_index;
                    }
                }
                const auto found = state.find(d.text(t.prep_runs[run].id));
                if (found != state.end()) {
                    t.prep_runs[run].statement = found->second;
                }
            }
        }

        // Forwards, from the snapshot to the end.
        {
            auto state = std::move(cache);
            size_t delta = snapshot_end;
            for (size_t run = 0; run < t.prep_runs.size(); ++run) {
                if (t.prep_runs[run].ts < snapshot_ts) {
                    continue;
                }
                while (delta < t.prep_deltas.size() &&
                       t.prep_deltas[delta].ts <= t.prep_runs[run].ts) {
                    const prep_delta_row& r = t.prep_deltas[delta];
                    if (r.kind == prep_added) {
                        state[d.text(r.id)] = r.statement_index;
                    } else if (r.kind == prep_removed) {
                        state.erase(d.text(r.id));
                    }
                    ++delta;
                }
                const auto found = state.find(d.text(t.prep_runs[run].id));
                if (found != state.end()) {
                    t.prep_runs[run].statement = found->second;
                }
            }
        }

        for (const prep_run_row& r : t.prep_runs) {
            (r.statement >= 0 ? named : unnamed) += 1;
        }
    }
    fmt::print("{} prepared statements known; {} runs named, {} not\n", d.statements.size(),
               named, unnamed);
}

// ============================================================================
//  13. pass_connections -- conns -> trace::connections, joined end to end
// ============================================================================
//
// A connection id is process-local, so one socket is two rows here and the
// join is what makes it one thing. Both ends name the same pair of addresses
// and disagree about which is which, so the addresses pair them; the boot id
// and shard each end learned in the handshake then check the pairing, which
// rules out what addresses cannot -- a socket whose far end belongs to a node
// that has since restarted and taken the address back.

void pass_connections(trace_data& d) {
    std::map<std::pair<uint32_t, uint64_t>, int32_t> index;  // (node, id) -> row
    for (uint32_t cpu = 0; cpu < d.tables.size(); ++cpu) {
        const uint32_t node = d.cpus[cpu].node;
        for (conn_row& r : d.tables[cpu].conns) {
            const auto [it, fresh] =
                index.emplace(std::pair{node, r.connection}, int32_t(d.connections.size()));
            if (fresh) {
                d.connections.push_back({cpu, r.connection, r.local, r.remote, r.peer_boot_msb,
                                         r.peer_boot_lsb, r.peer_shard, none});
            } else {
                // A close carries no addresses and an open carries no more
                // than the snapshot did: whichever row has them wins.
                connection_row& c = d.connections[it->second];
                if (c.local.len == 0) {
                    c.local = r.local;
                    c.remote = r.remote;
                }
                if (c.peer_boot_msb == 0 && c.peer_boot_lsb == 0) {
                    c.peer_boot_msb = r.peer_boot_msb;
                    c.peer_boot_lsb = r.peer_boot_lsb;
                    c.peer_shard = r.peer_shard;
                }
            }
            r.connection_id = it->second;
        }
    }

    // By the pair of addresses as this end sees them, so the far end is looked
    // up by the same pair swapped.
    std::map<std::pair<std::string_view, std::string_view>, std::vector<int32_t>> by_endpoints;
    for (int32_t i = 0; i < int32_t(d.connections.size()); ++i) {
        const connection_row& c = d.connections[i];
        if (c.local.len != 0) {
            by_endpoints[{d.text(c.local), d.text(c.remote)}].push_back(i);
        }
    }

    const auto identities_agree = [&](const connection_row& a, const connection_row& b) {
        const node_row& bn = d.nodes[d.cpus[b.cpu].node];
        if (a.peer_boot_msb == 0 && a.peer_boot_lsb == 0) {
            return true;  // this end did not learn who the other one was
        }
        return a.peer_boot_msb == bn.boot_msb && a.peer_boot_lsb == bn.boot_lsb &&
               a.peer_shard == d.cpus[b.cpu].shard;
    };

    size_t paired = 0;
    size_t elsewhere = 0;
    for (int32_t i = 0; i < int32_t(d.connections.size()); ++i) {
        connection_row& a = d.connections[i];
        if (a.peer >= 0 || a.local.len == 0) {
            continue;
        }
        const auto found = by_endpoints.find({d.text(a.remote), d.text(a.local)});
        if (found == by_endpoints.end()) {
            // Named a peer this trace does not contain, which is a different
            // thing from failing to pair: that node's snapshot is not here.
            if (a.peer_boot_msb != 0 || a.peer_boot_lsb != 0) {
                ++elsewhere;
            }
            continue;
        }
        for (const int32_t j : found->second) {
            connection_row& b = d.connections[j];
            if (b.peer >= 0 || j == i) {
                continue;
            }
            if (identities_agree(a, b) && identities_agree(b, a)) {
                a.peer = j;
                b.peer = i;
                paired += 2;
                break;
            }
        }
    }
    fmt::print("{} connection ends known, {} paired, {} to a node not in these snapshots\n",
               d.connections.size(), paired, elsewhere);
}

// ============================================================================
//  14. pass_rpc_pair -- rpcs + connections -> rpc.peer_cpu, rpc.peer_row
// ============================================================================
//
// No tracing id goes on the wire, so a message is joined to its arrival by the
// sequence number each direction of each connection counts locally over the
// frames it writes and reads. One frame is therefore (connection end,
// sequence) on the sending side and (the far end, the same sequence) on the
// receiving side.
//
// Where the arrival opened a task chain -- an inbound request, not a reply --
// the rpc_request_handled row is what the send is linked to, because that row
// is the one carrying the task the work continues under. That link is the
// whole of how a request is followed onto another node.

void pass_rpc_pair(trace_data& d) {
    struct frame {
        int32_t send_cpu = none, send_row = none;
        int32_t recv_cpu = none, recv_row = none;
        int32_t handled_cpu = none, handled_row = none;
    };
    std::map<std::pair<int32_t, uint64_t>, frame> frames;  // (connection end, sequence)

    // A message record names the connection by its process-local id; look up
    // which end of which socket that is.
    std::map<std::pair<uint32_t, uint64_t>, int32_t> end_of;
    for (int32_t i = 0; i < int32_t(d.connections.size()); ++i) {
        end_of[{d.cpus[d.connections[i].cpu].node, d.connections[i].connection}] = i;
    }

    for (uint32_t cpu = 0; cpu < d.tables.size(); ++cpu) {
        const uint32_t node = d.cpus[cpu].node;
        for (uint32_t i = 0; i < d.tables[cpu].rpcs.size(); ++i) {
            rpc_row& r = d.tables[cpu].rpcs[i];
            const auto found = end_of.find({node, r.connection});
            if (found == end_of.end()) {
                continue;  // a connection whose records the ring evicted
            }
            r.connection_id = found->second;
            frame& f = frames[{r.connection_id, r.sequence}];
            switch (r.kind) {
                case rpc_sent:
                case rpc_reply_sent:
                    f.send_cpu = int32_t(cpu);
                    f.send_row = int32_t(i);
                    break;
                case rpc_received:
                case rpc_reply_received:
                    f.recv_cpu = int32_t(cpu);
                    f.recv_row = int32_t(i);
                    break;
                case rpc_handled:
                    f.handled_cpu = int32_t(cpu);
                    f.handled_row = int32_t(i);
                    break;
                default:
                    break;
            }
        }
    }

    size_t joined = 0;
    size_t opened_chain = 0;
    for (const auto& [key, sender] : frames) {
        if (sender.send_row < 0) {
            continue;
        }
        const connection_row& c = d.connections[key.first];
        if (c.peer < 0) {
            continue;
        }
        const auto other = frames.find({c.peer, key.second});
        if (other == frames.end()) {
            continue;
        }
        const frame& receiver = other->second;
        // The handled row where there is one, because that is the row with the
        // task the work continues under; the plain receive otherwise, which is
        // all a reply ever has.
        const int32_t to_cpu =
            receiver.handled_row >= 0 ? receiver.handled_cpu : receiver.recv_cpu;
        const int32_t to_row =
            receiver.handled_row >= 0 ? receiver.handled_row : receiver.recv_row;
        if (to_row < 0) {
            continue;
        }
        rpc_row& from = d.tables[sender.send_cpu].rpcs[sender.send_row];
        rpc_row& to = d.tables[to_cpu].rpcs[to_row];
        from.peer_cpu = to_cpu;
        from.peer_row = to_row;
        to.peer_cpu = sender.send_cpu;
        to.peer_row = sender.send_row;
        ++joined;
        opened_chain += receiver.handled_row >= 0 ? 1 : 0;
    }
    fmt::print("{} frames, {} joined to the far end, {} of those opened a task chain\n",
               frames.size(), joined, opened_chain);
}

// ============================================================================
//  15. pass_queries -- switches + rpcs -> trace::queries, trace::parts
// ============================================================================
//
// There is no global request id. A CQL frame mints a task id, that id is
// inherited by every continuation on the shard and carried to other shards of
// the same node, and on the far side of an RPC a *different* id is minted for
// the work the message caused. So a query is a set of (cpu, task) pairs, grown
// from the frame's own by two rules:
//
//   * the same task id on another cpu of the same node -- a continuation that
//     hopped shards keeps its id, and only the file it landed in says where it
//     ran;
//   * the task an inbound message opened on the far end of a connection, which
//     is what pass_rpc_pair joined.
//
// A part is claimed by the first query that reaches it, which is what stops a
// shared task -- were there one -- merging two requests into one.

void pass_queries(trace_data& d) {
    std::map<std::pair<uint32_t, uint32_t>, int32_t> claimed;  // (cpu, task) -> query
    std::vector<std::pair<uint32_t, uint32_t>> frontier;

    // Seeds, in time order across the whole trace, so that query indices read
    // in the order the requests arrived.
    struct seed {
        int64_t ts;
        uint32_t cpu;
        uint32_t task;
    };
    std::vector<seed> seeds;
    for (uint32_t cpu = 0; cpu < d.tables.size(); ++cpu) {
        for (const switch_row& s : d.tables[cpu].switches) {
            if (s.cause == sw_cql_request && s.task != 0) {
                seeds.push_back({s.ts, cpu, s.task});
            }
        }
    }
    std::ranges::sort(seeds, {}, &seed::ts);

    for (const seed& sd : seeds) {
        if (claimed.contains({sd.cpu, sd.task})) {
            continue;
        }
        const auto q = int32_t(d.queries.size());
        query_row row;
        row.t0 = sd.ts;
        row.t1 = sd.ts;
        row.root_cpu = sd.cpu;
        row.root_task = sd.task;
        row.parts_begin = uint32_t(d.parts.size());
        d.queries.push_back(row);

        frontier.clear();
        const auto reach = [&](uint32_t cpu, uint32_t task) {
            if (task == 0) {
                return;
            }
            const auto [it, fresh] = claimed.emplace(std::pair{cpu, task}, q);
            if (!fresh) {
                return;
            }
            d.parts.push_back({cpu, task, q});
            frontier.push_back({cpu, task});
        };
        reach(sd.cpu, sd.task);

        while (!frontier.empty()) {
            const auto [cpu, task] = frontier.back();
            frontier.pop_back();

            // The same id on another cpu of this node.
            for (uint32_t other = 0; other < d.cpus.size(); ++other) {
                if (other == cpu || d.cpus[other].node != d.cpus[cpu].node) {
                    continue;
                }
                const auto found = rows_of_task(d.tables[other].switch_by_task, task);
                if (!found.empty()) {
                    reach(other, task);
                }
            }

            // Every message this task queued, followed into the task it opened
            // on the far end.
            const cpu_tables& t = d.tables[cpu];
            for (const auto& [ignored, at] : rows_of_task(t.rpc_by_task, task)) {
                const rpc_row& r = t.rpcs[at];
                if (r.kind != rpc_sent && r.kind != rpc_reply_sent) {
                    continue;
                }
                if (r.peer_row < 0) {
                    continue;
                }
                const rpc_row& far = d.tables[r.peer_cpu].rpcs[r.peer_row];
                if (far.kind == rpc_handled) {
                    reach(uint32_t(r.peer_cpu), far.task);
                }
            }
        }
        d.queries.back().parts_end = uint32_t(d.parts.size());
    }

    size_t distributed = 0;
    for (const query_row& q : d.queries) {
        std::set<uint32_t> nodes;
        for (uint32_t p = q.parts_begin; p < q.parts_end; ++p) {
            nodes.insert(d.cpus[d.parts[p].cpu].node);
        }
        distributed += nodes.size() > 1 ? 1 : 0;
    }
    fmt::print("{} CQL requests, {} of them reaching another node, {} (cpu, task) parts\n",
               d.queries.size(), distributed, d.parts.size());
}

// ============================================================================
//  16. pass_query_rows -- parts -> row.query, everywhere
// ============================================================================
//
// The parts say which task on which cpu belongs to which query; this writes
// that back onto every row, so that the log's highlight and the plot's bars
// are a field read rather than a lookup per frame.

void pass_query_rows(trace_data& d) {
    for (const part_row& p : d.parts) {
        d.tables[p.cpu].query_of_task[p.task] = p.query;
    }
    for (cpu_tables& t : d.tables) {
        const auto& map = t.query_of_task;
        for_each_table(t, [&](table_id, auto& rows) {
            for (auto& r : rows) {
                if (r.task == 0) {
                    continue;
                }
                const auto found = map.find(r.task);
                r.query = found == map.end() ? none : found->second;
            }
        });
    }
}

// ============================================================================
//  17. pass_cost -- switches + io spans + rows -> query.t1, query.cpu_ticks
// ============================================================================
//
// Two numbers per query, and only two, because for a distributed request they
// are the two that mean something:
//
//   latency   t1 - t0: from the frame arriving to the last record any of its
//             parts wrote.
//   cpu time  summed over every part, on every cpu it touched.
//
// On-cpu is bounded by *another task's record on the same shard*: there is no
// "task ended" tracepoint, so a stretch where the reactor ran nothing else is
// counted to the last task it picked up. The I/O it issued is cut back out --
// a shard waiting for a disk is not on the cpu, even when nothing else runs.

void pass_cost(trace_data& d) {
    // The range first, and over every cpu before any cpu's cost is summed:
    // the clamp below needs the final t1, not the one this loop is halfway
    // through building.
    //
    // Only records that *carry* a task extend a query. The ones pass_attribute
    // gave an ambient task to do not, and that distinction is load-bearing:
    // the connection and statement-cache snapshots are written when the trace
    // is taken, and whichever task a shard last picked up before going idle is
    // still the ambient one there. Letting those extend a query would stretch
    // every request that happened to be last on a shard to the end of the
    // trace.
    for (const cpu_tables& t : d.tables) {
        const auto extend = [&](int32_t query, int64_t ts) {
            if (query >= 0) {
                d.queries[query].t1 = std::max(d.queries[query].t1, ts);
                d.queries[query].t0 = std::min(d.queries[query].t0, ts);
            }
        };
        for (const switch_row& r : t.switches) {
            extend(r.query, r.ts);
        }
        for (const io_begin_row& r : t.io_begins) {
            extend(r.query, r.end >= 0 ? t.io_ends[r.end].ts : r.ts);
        }
        for (const io_end_row& r : t.io_ends) {
            extend(r.query, r.ts);
        }
        for (const rpc_row& r : t.rpcs) {
            if (r.kind != rpc_received && r.kind != rpc_reply_received) {
                extend(r.query, r.ts);
            }
        }
    }

    // What a task holds the cpu for is switch_ends() -- the end of the task
    // queue run it was picked up in, or the next switch on the shard when the
    // snapshot does not have that end. One further thing cuts it back: the
    // query's own end, because a request cannot be on the cpu after the last
    // record it wrote.
    //
    // Its I/O does not. A shard is on the cpu or it is not, and the run's end
    // already says which; time a task spends with a read outstanding *and* the
    // reactor still running it is cpu time, and the plot draws the overlap for
    // the same reason.
    for (const cpu_tables& t : d.tables) {
        const int64_t cpu_end = t.timeline.empty() ? 0 : t.timeline.back().ts;
        for (uint32_t i = 0; i < t.switches.size(); ++i) {
            const switch_row& s = t.switches[i];
            if (s.query < 0) {
                continue;
            }
            const int64_t from = s.ts;
            const int64_t to = std::min(switch_ends(t, i, cpu_end), d.queries[s.query].t1);
            if (to <= from) {
                continue;
            }
            d.queries[s.query].cpu_ticks += to - from;
        }
    }

    d.by_latency.resize(d.queries.size());
    for (uint32_t i = 0; i < d.by_latency.size(); ++i) {
        d.by_latency[i] = i;
    }
    std::ranges::sort(d.by_latency, {},
                      [&](uint32_t i) { return d.queries[i].t1 - d.queries[i].t0; });

    if (!d.queries.empty()) {
        const auto at = [&](double p) -> const query_row& {
            return d.queries[d.by_latency[std::min(size_t(p * double(d.by_latency.size())),
                                                   d.by_latency.size() - 1)]];
        };
        for (const double p : {0.5, 0.99, 1.0}) {
            const query_row& q = at(p);
            fmt::print("p{:<5} latency {:8.3f} ms, cpu {:8.3f} ms, {} parts\n", p,
                       d.seconds(q.t1 - q.t0) * 1e3, d.seconds(q.cpu_ticks) * 1e3,
                       q.parts_end - q.parts_begin);
        }
    }
}

// ============================================================================
//  18. pass_prefix_sums -- by_latency + query costs -> prefix sums
// ============================================================================
//
// The histogram's selection is a contiguous range in by_latency. Keep the
// two quantities it aggregates in that same order, so changing the selection
// only needs two prefix-sum range queries rather than a walk over its queries.

void pass_prefix_sums(trace_data& d) {
    d.latency_prefix.assign(d.by_latency.size() + 1, 0.0);
    d.cpu_prefix.assign(d.by_latency.size() + 1, 0.0);
    for (size_t i = 0; i < d.by_latency.size(); ++i) {
        const query_row& q = d.queries[d.by_latency[i]];
        d.latency_prefix[i + 1] = d.latency_prefix[i] + d.seconds(q.t1 - q.t0);
        d.cpu_prefix[i + 1] = d.cpu_prefix[i] + d.seconds(q.cpu_ticks);
    }
}

// ============================================================================
//  19. pass_query_statement -- prep_runs + queries -> query.statement
// ============================================================================

void pass_query_statement(trace_data& d) {
    for (const cpu_tables& t : d.tables) {
        for (const prep_run_row& r : t.prep_runs) {
            if (r.query >= 0 && r.statement >= 0 && d.queries[r.query].statement < 0) {
                d.queries[r.query].statement = r.statement;
            }
        }
    }
}

// ============================================================================
//  20. rendering a record as text
// ============================================================================
//
// One line per record, for the log. Everything a record is joined to -- the
// statement behind a prepared id, the far end of a message -- is already a
// field by the time this runs, so this is formatting and nothing else.

std::string format_location(const trace_data& d, uint32_t loc) {
    if (loc == 0 || loc >= d.locations.size()) {
        return {};
    }
    const location_row& l = d.locations[loc];
    if (!l.resolved) {
        return fmt::format("<unresolved {:#x}>", l.address);
    }
    return fmt::format("{}:{}", d.text(l.file), l.line);
}

std::string format_bytes(std::string_view raw) {
    std::string out;
    out.reserve(raw.size() * 2);
    for (const char c : raw) {
        fmt::format_to(std::back_inserter(out), "{:02x}", uint8_t(c));
    }
    return out;
}

std::string format_event(const trace_data& d, uint32_t cpu, uint16_t table, uint32_t index) {
    const cpu_tables& t = d.tables[cpu];
    switch (table) {
        case tab_switch: {
            const switch_row& r = t.switches[index];
            std::string line = fmt::format("{:<7} task {:08x}",
                                           switch_cause_name(r.cause), r.task);
            if (r.group >= 0) {
                fmt::format_to(std::back_inserter(line), "  sg {}", r.group);
            }
            if (const std::string at = format_location(d, r.loc); !at.empty()) {
                fmt::format_to(std::back_inserter(line), "  at {}", at);
            }
            return line;
        }
        case tab_tq_run: {
            const tq_run_row& r = t.tq_runs[index];
            if (r.kind == tq_begin) {
                return fmt::format("{:<7} scheduling group {}", "TQ+", r.group);
            }
            return fmt::format("{:<7}", "TQ-");
        }
        case tab_io_begin: {
            const io_begin_row& r = t.io_begins[index];
            std::string line = fmt::format("{:<7} task {:08x} io {:016x}", "IO-BEGIN", r.task, r.io);
            if (r.end >= 0) {
                fmt::format_to(std::back_inserter(line), "  {:.6f} ms",
                               d.seconds(t.io_ends[r.end].ts - r.ts) * 1e3);
            } else {
                line += "  (never completed in this trace)";
            }
            return line;
        }
        case tab_io_end: {
            const io_end_row& r = t.io_ends[index];
            return fmt::format("{:<7} task {:08x} io {:016x}", "IO-END", r.task, r.io);
        }
        case tab_prep_run: {
            const prep_run_row& r = t.prep_runs[index];
            if (r.statement >= 0) {
                const statement_row& s = d.statements[r.statement];
                return fmt::format("{:<7} {} {}", "PREPARED", d.text(s.keyspace), d.text(s.text));
            }
            return fmt::format("{:<7} id {} (not in the cache at this point)", "PREPARED",
                               format_bytes(d.text(r.id)));
        }
        case tab_prep_delta: {
            const prep_delta_row& r = t.prep_deltas[index];
            const char* what = r.kind == prep_added     ? "PREP+"
                               : r.kind == prep_removed ? "PREP-"
                                                        : "PREP=";
            return fmt::format("{:<7} {} {}", what, d.text(r.keyspace), d.text(r.statement));
        }
        case tab_conn: {
            const conn_row& r = t.conns[index];
            const char* what = r.kind == conn_open    ? "CONN+"
                               : r.kind == conn_close ? "CONN-"
                                                      : "CONN=";
            return fmt::format("{:<7} {} {} -> {} peer shard {}", what, r.connection,
                               d.text(r.local), d.text(r.remote), r.peer_shard);
        }
        case tab_rpc: {
            const rpc_row& r = t.rpcs[index];
            std::string line = fmt::format("{:<7} conn {} seq {} task {:08x}",
                                           rpc_kind_name(r.kind), r.connection, r.sequence, r.task);
            if (r.peer_row >= 0) {
                const rpc_row& far = d.tables[r.peer_cpu].rpcs[r.peer_row];
                fmt::format_to(std::back_inserter(line), "  <-> {} task {:08x} ({:+.6f} ms)",
                               d.cpus[r.peer_cpu].label, far.task,
                               d.seconds(far.ts - r.ts) * 1e3);
            }
            return line;
        }
        default:
            return "?";
    }
}

const char* event_kind_name(const trace_data& d, uint32_t cpu, uint16_t table,
                            uint32_t index) {
    const cpu_tables& t = d.tables[cpu];
    switch (table) {
        case tab_switch: return switch_cause_name(t.switches[index].cause);
        case tab_tq_run: return t.tq_runs[index].kind == tq_begin ? "TQ+" : "TQ-";
        case tab_io_begin: return "IO-BEGIN";
        case tab_io_end: return "IO-END";
        case tab_prep_run: return "PREPARED";
        case tab_prep_delta: {
            const uint8_t kind = t.prep_deltas[index].kind;
            return kind == prep_added ? "PREP+" : kind == prep_removed ? "PREP-" : "PREP=";
        }
        case tab_conn: {
            const uint8_t kind = t.conns[index].kind;
            return kind == conn_open ? "CONN+" : kind == conn_close ? "CONN-" : "CONN=";
        }
        case tab_rpc: return rpc_kind_name(t.rpcs[index].kind);
        default: return "?";
    }
}

std::string format_event_details(const trace_data& d, uint32_t cpu, uint16_t table,
                                 uint32_t index) {
    const cpu_tables& t = d.tables[cpu];
    switch (table) {
        case tab_switch: {
            const switch_row& r = t.switches[index];
            std::string details;
            if (r.group >= 0) {
                details = fmt::format("scheduling group {}", r.group);
            }
            if (const std::string at = format_location(d, r.loc); !at.empty()) {
                if (!details.empty()) {
                    details += ", ";
                }
                details += fmt::format("at {}", at);
            }
            return details;
        }
        case tab_tq_run: {
            const tq_run_row& r = t.tq_runs[index];
            return r.kind == tq_begin ? fmt::format("scheduling group {}", r.group) : "";
        }
        case tab_io_begin: {
            const io_begin_row& r = t.io_begins[index];
            std::string details = fmt::format("I/O {:016x}", r.io);
            if (r.end >= 0) {
                fmt::format_to(std::back_inserter(details), ", {:.6f} ms",
                               d.seconds(t.io_ends[r.end].ts - r.ts) * 1e3);
            } else {
                details += ", never completed in this trace";
            }
            return details;
        }
        case tab_io_end:
            return fmt::format("I/O {:016x}", t.io_ends[index].io);
        case tab_prep_run: {
            const prep_run_row& r = t.prep_runs[index];
            if (r.statement >= 0) {
                const statement_row& s = d.statements[r.statement];
                return fmt::format("{}: {}", d.text(s.keyspace), d.text(s.text));
            }
            return fmt::format("id {} (not in the cache at this point)", format_bytes(d.text(r.id)));
        }
        case tab_prep_delta: {
            const prep_delta_row& r = t.prep_deltas[index];
            return fmt::format("{}: {}", d.text(r.keyspace), d.text(r.statement));
        }
        case tab_conn: {
            const conn_row& r = t.conns[index];
            return fmt::format("connection {}: {} -> {}, peer shard {}", r.connection,
                               d.text(r.local), d.text(r.remote), r.peer_shard);
        }
        case tab_rpc: {
            const rpc_row& r = t.rpcs[index];
            std::string details = fmt::format("connection {}, sequence {}", r.connection,
                                              r.sequence);
            if (r.peer_row >= 0) {
                const rpc_row& far = d.tables[r.peer_cpu].rpcs[r.peer_row];
                fmt::format_to(std::back_inserter(details), ", <-> {} task {:08x} ({:+.6f} ms)",
                               d.cpus[r.peer_cpu].label, far.task,
                               d.seconds(far.ts - r.ts) * 1e3);
            }
            return details;
        }
        default: return {};
    }
}

// The task a row is about, whichever table it is in. Used by the log's
// highlight and by the plot's tooltip.
uint32_t task_of(const trace_data& d, uint32_t cpu, uint16_t table, uint32_t index) {
    const cpu_tables& t = d.tables[cpu];
    switch (table) {
        case tab_switch: return t.switches[index].task;
        case tab_io_begin: return t.io_begins[index].task;
        case tab_io_end: return t.io_ends[index].task;
        case tab_prep_run: return t.prep_runs[index].task;
        case tab_prep_delta: return t.prep_deltas[index].task;
        case tab_conn: return t.conns[index].task;
        case tab_rpc: return t.rpcs[index].task;
        case tab_tq_run: return t.tq_runs[index].task;
        default: return 0;
    }
}

int32_t query_of(const trace_data& d, uint32_t cpu, uint16_t table, uint32_t index) {
    const cpu_tables& t = d.tables[cpu];
    switch (table) {
        case tab_switch: return t.switches[index].query;
        case tab_io_begin: return t.io_begins[index].query;
        case tab_io_end: return t.io_ends[index].query;
        case tab_prep_run: return t.prep_runs[index].query;
        case tab_prep_delta: return t.prep_deltas[index].query;
        case tab_conn: return t.conns[index].query;
        case tab_rpc: return t.rpcs[index].query;
        case tab_tq_run: return t.tq_runs[index].query;
        default: return none;
    }
}

// ============================================================================
//  21. pass_render -- the event tables -> the text and the rectangles
// ============================================================================
//
// The last preprocessing pass, and the one the UI draws straight out of. Every
// record's log line and every rectangle of every reactor's whole timeline is
// built here, once, for the entire trace -- not for a selected request.
//
// That is what lets the log and the plot be scrolled *past* the request that
// is selected. A selection moves the window and recolours what is in it; it
// never decides what exists. Nothing is rebuilt when it changes.
//
// Two tables per cpu:
//
//   .log_lines   one str per timeline entry, in the cpu's own arena
//   .slices      every rectangle: a stretch on the cpu, or an I/O in flight,
//                in milliseconds from the start of the trace, carrying the
//                query it belongs to so a frame's only decision is the colour
//
// Kept alongside the slices, .slice_reach is a running maximum of their ends.
// It is what makes culling to the visible x range a binary search rather than
// a scan: the slices are sorted by where they start, so the first one that can
// possibly reach into view is the first whose reach does.

// What a slice looks like, which is two independent questions.
//
// Its *shape* comes from what it is: a stretch on the cpu is the full height
// of its row, an I/O is a narrow bar inside that, so an I/O in flight over a
// stretch of cpu leaves the stretch visible -- and hoverable -- above and
// below it. Every reactor's whole timeline is drawn this way, whoever the work
// belonged to.
//
// Its *colour* comes from whether it is the selected request's: the same two
// colours either way, washed out to about a third of their saturation for
// everything else. A row is then the reactor's real timeline, with this
// request picked out of it, rather than a request over a grey band.

struct band {
    double top;
    double bottom;
};

inline band band_of(uint16_t table) {
    return table == tab_io_begin ? band{0.40, 0.60} : band{0.08, 0.92};
}

// Drawn in this order, back to front: an I/O sits inside the stretch of cpu it
// interrupts, so it goes over it.
inline int layer_of(uint16_t table) {
    return table == tab_io_begin ? 1 : 0;
}

// Three states, not two: the picked request, the one under the pointer, and
// everything else. Green and blue are what is picked; amber and violet are
// what is being pointed at, so that a hovered request is picked out of every
// row it touches at once rather than only under the pointer; and the washed
// pair is the reactor's other work.
inline ImU32 colour_of(uint16_t table, int32_t query, int32_t primary, int32_t secondary) {
    const bool is_io = table == tab_io_begin;
    if (query >= 0 && query == primary) {
        return is_io ? IM_COL32(90, 140, 240, 255) : IM_COL32(64, 200, 64, 255);
    }
    if (query >= 0 && query == secondary) {
        return is_io ? IM_COL32(196, 128, 224, 255) : IM_COL32(230, 172, 64, 255);
    }
    return is_io ? IM_COL32(88, 100, 128, 255) : IM_COL32(84, 108, 84, 255);
}

// A summary is nobody's work -- it stands for everything that was in that
// stretch -- so it takes the washed colour of its band and says how busy the
// stretch was with its alpha. Never transparent: a bin with something in it
// must not read as one with nothing in it, however little that something was.
inline ImU32 summary_colour(uint16_t table, float density) {
    const ImU32 base = colour_of(table, none, none, none);
    const auto alpha = ImU32(255.0f * (0.25f + 0.75f * std::clamp(density, 0.0f, 1.0f)));
    return (base & ~IM_COL32_A_MASK) | (alpha << IM_COL32_A_SHIFT);
}

// The rightmost pixel of a bar, darkened. Two stretches of cpu that meet --
// which is most of them, a reactor running one task after another -- are
// otherwise one unbroken block of colour with no boundary in it.
inline ImU32 darker(ImU32 colour, float by) {
    const auto channel = [&](int shift) {
        return ImU32(float((colour >> shift) & 0xff) * by) << shift;
    };
    return channel(IM_COL32_R_SHIFT) | channel(IM_COL32_G_SHIFT) | channel(IM_COL32_B_SHIFT) |
           (colour & (0xffu << IM_COL32_A_SHIFT));
}

void pass_render(trace_data& d) {
    // One origin for every reactor, so that two rows of the plot are the same
    // axis and a time in the log is a time in the plot.
    d.origin = std::numeric_limits<int64_t>::max();
    for (const cpu_tables& t : d.tables) {
        if (!t.timeline.empty()) {
            d.origin = std::min(d.origin, t.timeline.front().ts);
        }
    }
    if (d.origin == std::numeric_limits<int64_t>::max()) {
        d.origin = 0;
    }
    for (const cpu_tables& t : d.tables) {
        if (!t.timeline.empty()) {
            d.last = std::max(d.last, t.timeline.back().ts);
        }
    }

    size_t bytes = 0;
    size_t slices = 0;
    for (uint32_t cpu = 0; cpu < d.tables.size(); ++cpu) {
        cpu_tables& t = d.tables[cpu];

        t.log_lines.clear();
        t.log_lines.reserve(t.timeline.size());
        for (const timeline_row& e : t.timeline) {
            t.log_lines.push_back(t.log_text.put(format_event(d, cpu, e.table, e.index)));
        }

        // A stretch on the cpu runs from the switch that picked the task up to
        // switch_ends() -- the end of its task queue run, or the next switch on
        // the shard, whichever the snapshot has. The same bound pass_cost uses,
        // so the picture and the number agree.
        //
        // Nothing is cut out of it. An I/O is drawn *over* the cpu it overlaps,
        // as a narrow bar inside the row (see band_of), because that overlap is
        // the thing worth seeing: a task holding the cpu while its own read is
        // outstanding looks different from one blocked on it, and subtracting
        // one from the other would hide both.
        t.slices.clear();
        const int64_t cpu_end = t.timeline.empty() ? 0 : t.timeline.back().ts;
        const auto emit = [&](int64_t from, int64_t to, int32_t query, uint16_t table,
                              uint32_t index) {
            if (to > from) {
                t.slices.push_back({d.ms(from - d.origin), d.ms(to - d.origin), query, index,
                                    1.0f, table, false});
            }
        };
        for (uint32_t i = 0; i < t.switches.size(); ++i) {
            const switch_row& sw = t.switches[i];
            emit(sw.ts, switch_ends(t, i, cpu_end), sw.query, tab_switch, i);
        }
        for (uint32_t i = 0; i < t.io_begins.size(); ++i) {
            const io_begin_row& b = t.io_begins[i];
            emit(b.ts, b.end >= 0 ? t.io_ends[b.end].ts : cpu_end, b.query, tab_io_begin, i);
        }

        std::ranges::sort(t.slices, {}, &slice_row::t0);
        t.slice_reach.clear();
        t.slice_reach.reserve(t.slices.size());
        double reach = -std::numeric_limits<double>::infinity();
        for (const slice_row& sl : t.slices) {
            reach = std::max(reach, sl.t1);
            t.slice_reach.push_back(reach);
        }

        bytes += t.log_text.bytes.size();
        slices += t.slices.size();
    }
    fmt::print("{} log lines ({:.1f} MB of text), {} rectangles, all rendered up front\n",
               std::accumulate(d.tables.begin(), d.tables.end(), size_t(0),
                               [](size_t n, const cpu_tables& t) {
                                   return n + t.log_lines.size();
                               }),
               double(bytes) / (1 << 20), slices);
}

// ============================================================================
//  22. pass_io_stack -- overlapping I/O rectangles -> the topmost one only
// ============================================================================
//
// reads: .slices (of every cpu)  ->  writes: .slices, .io_slices (of every cpu)
//
// A reactor has many I/Os in flight at once -- a hundred of them on a loaded
// shard -- and each one is a rectangle in the same narrow band of the row.
// Drawn as they are, they stack: every pixel column of that band carries as
// many rectangles as there were outstanding requests under it, all but the
// last of them painted over. That is the one thing the pyramid of levels
// cannot bound, because it only summarises what is *too narrow* to draw: a
// hundred overlapping five-millisecond spans are each far wider than a pixel
// and every one of them is passed through verbatim. Measured on latte-run,
// the worst frame drew 285k I/O rectangles against 42k summaries.
//
// So the band is flattened first, to the one span a reader can actually see:
// at any instant, the *most recently opened* I/O still in flight. An I/O that
// starts while another is open covers it and gives it back when it ends, which
// is a stack -- the same shape as the call stack that issued them, and the
// same reading as a flame graph's top edge. A span therefore comes out as one
// rectangle where nothing interrupted it and several where something did, each
// segment keeping the row it was drawn from, so hovering one still names the
// I/O it belongs to. What is lost is the *count* of what is underneath, which
// is what the plot never showed anyway: it showed the newest one, over the
// others, at whatever cost.
//
// The originals are kept in .io_slices, whole and overlapping. A picked
// request's own I/O may be buried under a newer one that is not its own, so
// the plot draws it from there, over the flattened band -- see the overlay in
// draw_plot_window.

void pass_io_stack(trace_data& d) {
    size_t before = 0;
    size_t after = 0;
    for (cpu_tables& t : d.tables) {
        t.io_slices.clear();
        for (const slice_row& s : t.slices) {
            if (s.table == tab_io_begin) {
                t.io_slices.push_back(s);
            }
        }
        t.io_reach.clear();
        t.io_reach.reserve(t.io_slices.size());
        double io_reach = -std::numeric_limits<double>::infinity();
        for (const slice_row& s : t.io_slices) {
            io_reach = std::max(io_reach, s.t1);
            t.io_reach.push_back(io_reach);
        }
        before += t.io_slices.size();
        if (t.io_slices.empty()) {
            continue;
        }
        // The sweep. Two streams of events in time order -- the spans opening,
        // which .io_slices already is, and the same spans closing, which is it
        // sorted by end -- and a stack of what is open, newest last. Only a
        // change of what is on top ends a segment: an I/O closing under
        // another one is invisible while it happens and invisible when it
        // goes, so it must not cut the rectangle above it in two.
        std::vector<uint32_t> by_end(t.io_slices.size());
        std::iota(by_end.begin(), by_end.end(), 0u);
        std::ranges::sort(by_end, {}, [&](uint32_t i) { return t.io_slices[i].t1; });
        std::vector<uint32_t> open;  // indices into .io_slices, in opening order
        std::vector<slice_row> flat;
        flat.reserve(t.io_slices.size());
        double segment = 0;  // when the one on top became the one on top
        size_t at_open = 0;
        size_t at_close = 0;
        const auto top = [&] { return open.empty() ? uint32_t(-1) : open.back(); };
        while (at_open < t.io_slices.size() || at_close < by_end.size()) {
            const bool opening =
                at_open < t.io_slices.size() &&
                t.io_slices[at_open].t0 <= t.io_slices[by_end[at_close]].t1;
            const double now =
                opening ? t.io_slices[at_open].t0 : t.io_slices[by_end[at_close]].t1;
            const uint32_t was = top();
            if (opening) {
                open.push_back(uint32_t(at_open++));
            } else {
                const uint32_t done = by_end[at_close++];
                open.erase(std::find(open.begin(), open.end(), done));
            }
            if (top() == was) {
                continue;  // something closed under the one on top: nothing to draw
            }
            if (was != uint32_t(-1) && now > segment) {
                flat.push_back(t.io_slices[was]);
                flat.back().t0 = segment;
                flat.back().t1 = now;
            }
            segment = now;
        }
        after += flat.size();
        // Back into .slices, which the flattened band leaves sorted: the cpu
        // rectangles keep their order and the segments come out in time order,
        // so the two only have to be merged.
        std::erase_if(t.slices, [](const slice_row& s) { return s.table == tab_io_begin; });
        std::vector<slice_row> merged;
        merged.reserve(t.slices.size() + flat.size());
        std::ranges::merge(t.slices, flat, std::back_inserter(merged), {}, &slice_row::t0,
                           &slice_row::t0);
        t.slices = std::move(merged);
        t.slice_reach.clear();
        t.slice_reach.reserve(t.slices.size());
        double reach = -std::numeric_limits<double>::infinity();
        for (const slice_row& s : t.slices) {
            reach = std::max(reach, s.t1);
            t.slice_reach.push_back(reach);
        }
    }
    fmt::print("{} I/O rectangles flattened to {} the eye can see\n", before, after);
}

// ============================================================================
//  23. pass_lod -- the same rectangles, at coarser and coarser scales
// ============================================================================
//
// reads: .slices (of every cpu)   ->  writes: .lods (of every cpu)
//
// Zoomed all the way out, a reactor's hundred thousand rectangles land on two
// thousand pixels. Drawing them all is most of a frame, and nine tenths of the
// work is invisible: a rectangle a fifth of a pixel wide is a smear the next
// one overwrites. But it cannot simply be dropped -- a stretch where a reactor
// was flat out and a stretch where it was idle must not come out looking the
// same -- so what is not drawn has to be *said* rather than omitted.
//
// So each cpu gets a pyramid. The level with scale `s` holds:
//
//   - every rectangle at least `s` wide, verbatim, exactly as .slices has it;
//   - one *summary* per `s`-wide bin (aligned to multiples of `s` from the
//     start of the trace) standing for the narrower ones that fall in it,
//     carrying the fraction of the bin they covered, how many they were, and
//     the first query represented in it.
//
// A frame picks the coarsest level whose scale is still under a pixel and
// draws it the way it drew .slices: one binary search, then a scan. Not a
// query per gap, and no merging of levels -- one array per row per frame is
// the whole point of paying for the pyramid up front.
//
// Two things keep the build cheap. Every level above the finest is coarsened
// from the one below it rather than from .slices, so the coarse half of the
// pyramid is a geometric tail; and because the scales double and every bin is
// aligned to a multiple of its own scale, the bins nest exactly, which makes a
// coarse summary an exact sum of finer ones rather than a resampling of them.
// The pyramid *starts* at the scale where a level first costs more than half
// of .slices, because below that a level is nearly a copy of .slices under
// another name and the plot may as well read .slices, which is what it does.
// That floor is found by building levels until one stops paying, not guessed
// from how far apart the rectangles are on average: they are not spread out
// evenly, and guessing put the fall-through to .slices five octaves of zoom
// too early. pass_lod has the measurement.
//
// A summary does not retain all the requests whose work it represents. That
// matters in exactly one place: the selected request would vanish from a
// zoomed-out plot, which is where you most want to see where it went. The plot
// draws that one request's own rectangles again, over the summaries -- see the
// timeline window. It does retain the first request represented, so hovering
// a summary can pick something useful.

// Everything in `src` narrower than `scale` collapsed into summaries `scale`
// wide, everything wider carried over as it stands. `src` is in t0 order and
// so is what comes out.
//
// Two accumulators rather than one, because a stretch of cpu and an I/O drawn
// over it are different bands of the picture: adding them together would say
// the reactor was busier than it was.
//
// Within a band the density is the time covered over the width of the bin,
// clamped at one. A reactor runs one task at a time, so the cpu band never
// reaches that clamp and on-cpu time is conserved exactly by every level. Two
// I/Os in flight together do overlap, so an I/O summary says how much of the
// bin had some I/O outstanding rather than how many I/O-seconds went into it
// -- which is what the row draws anyway when it draws them one over the other.
std::vector<slice_row> coarsen(const std::vector<slice_row>& src, double scale) {
    // What one band has accumulated in the bin being filled. `carry` is the
    // part that belongs to the *next* bin: a rectangle narrower than a bin can
    // still straddle two of them, and it is clipped between them rather than
    // credited to whichever it started in.
    struct band_acc {
        double busy = 0;
        uint32_t count = 0;
        double carry = 0;
        int32_t query = none;
        int32_t carry_query = none;
    };
    band_acc acc[2];
    int64_t cur = std::numeric_limits<int64_t>::min();
    std::vector<slice_row> out;
    std::vector<slice_row> wide;  // the wide rectangles of `cur`, held back
    out.reserve(src.size() / 2 + 16);

    const auto summary = [&](int k, int64_t bin, double busy, uint32_t count,
                             int32_t query) {
        if (busy <= 0) {
            return;
        }
        const double t0 = double(bin) * scale;
        out.push_back({t0, t0 + scale, query, count, float(std::min(1.0, busy / scale)),
                       uint16_t(k == 1 ? tab_io_begin : tab_switch), true});
    };
    // Close the bin being filled and open `next`. Its summaries go out first
    // and the wide rectangles that start inside it after, which is what keeps
    // the output in t0 order without ever sorting it.
    const auto close = [&](int64_t next) {
        summary(0, cur, acc[0].busy, acc[0].count, acc[0].query);
        summary(1, cur, acc[1].busy, acc[1].count, acc[1].query);
        out.insert(out.end(), wide.begin(), wide.end());
        wide.clear();
        for (int k = 0; k < 2; ++k) {
            band_acc& a = acc[k];
            if (next == cur + 1) {
                a.busy = a.carry;  // what straddled into the bin we are opening
                a.query = a.carry_query;
            } else {
                summary(k, cur + 1, a.carry, 0, a.carry_query);
                a.busy = 0;
                a.query = none;
            }
            a.count = 0;
            a.carry = 0;
            a.carry_query = none;
        }
        cur = next;
    };

    for (const slice_row& s : src) {
        const int64_t bin = int64_t(std::floor(s.t0 / scale));
        if (bin != cur) {
            close(bin);
        }
        if (s.t1 - s.t0 >= scale) {
            wide.push_back(s);
            continue;
        }
        // A summary of the level below covered `density` of its own span; a
        // rectangle covers all of its own. Either way it is "how much time
        // does this stand for", which is what the coarser bin adds up.
        band_acc& a = acc[s.table == tab_io_begin ? 1 : 0];
        const double covered = s.summary ? double(s.density) : 1.0;
        const double bin_end = double(bin + 1) * scale;
        if (a.query == none && s.query >= 0) {
            a.query = s.query;
        }
        a.busy += covered * (std::min(s.t1, bin_end) - s.t0);
        a.count += s.summary ? s.index : 1;
        if (s.t1 > bin_end) {
            a.carry += covered * (s.t1 - bin_end);
            if (a.carry_query == none && s.query >= 0) {
                a.carry_query = s.query;
            }
        }
    }
    if (cur != std::numeric_limits<int64_t>::min()) {
        close(cur + 2);  // + 2 so that the last bin's carry is emitted too
    }
    return out;
}

void pass_lod(trace_data& d) {
    size_t rows = 0;
    size_t levels = 0;
    double finest = std::numeric_limits<double>::infinity();
    for (cpu_tables& t : d.tables) {
        t.lods.clear();
        if (t.slices.size() < 64) {
            continue;  // a reactor with this little on it is never the problem
        }
        // From the first rectangle to the last one to *end*, which is what
        // .slice_reach has: the rectangles are in the order they start in, and
        // the one that starts last is not the one that finishes last.
        const double span = t.slice_reach.back() - t.slices.front().t0;
        if (!(span > 0)) {
            continue;
        }
        // Where the pyramid meets the rectangles themselves. A level is
        // picked at the zoom where its scale is about a pixel, so the level
        // with scale `s` draws one summary per pixel and the .slices under it
        // draw `n * w * s / span` rectangles on a `w`-pixel plot -- which is
        // on budget, at a plot's width, when `s` is about the average time
        // between one rectangle and the next.
        //
        // That average is a bad place to stop, and stopping there is what put
        // a cliff in the middle of the zoom range. Rectangles are not spread
        // out evenly: on a real reactor 99% of them are narrower than that
        // scale and they arrive in bursts, so the frame that first falls
        // through to .slices draws a few thousand rectangles all of which are
        // a thousandth of a pixel wide, each widened to a pixel and each
        // overwriting the last -- the same smear the pyramid exists to avoid,
        // and a step from "nothing thinner than a pixel" straight to "one
        // pixel standing for a hundred rectangles".
        //
        // So the floor is not a guess about the average, it is measured: keep
        // going finer while a level still halves the rectangles it stands for,
        // and stop at the first one that does not. Below that level the plot
        // reads .slices and draws at most twice what the level would have --
        // which is the point where the pyramid has nothing left to buy.
        const double coarsest = span * 2;
        const double scale = std::ldexp(1.0, int(std::ceil(std::log2(
            span / double(t.slices.size())))));
        if (!(scale > 0) || scale >= span) {
            continue;
        }
        // Finer first, each level built from .slices because there is no level
        // below it yet, into a vector that is reversed into .lods. The
        // sixteen only bounds the descent when a reactor's rectangles pile up
        // on so few distinct instants that halving never stops paying.
        std::vector<lod_level> finer;
        for (double s = scale / 2; finer.size() < 16; s /= 2) {
            std::vector<slice_row> made = coarsen(t.slices, s);
            if (made.size() * 2 > t.slices.size()) {
                break;
            }
            finer.push_back({s, std::move(made), {}});
        }
        for (auto it = finer.rbegin(); it != finer.rend(); ++it) {
            t.lods.push_back(std::move(*it));
        }
        // Then coarser until one bin covers the whole trace: past that there
        // is nothing left to summarise and no zoom that could ask for it.
        const std::vector<slice_row>* below =
            t.lods.empty() ? &t.slices : &t.lods.back().slices;
        for (double s = scale; s < coarsest; s *= 2) {
            t.lods.push_back({s, coarsen(*below, s), {}});
            below = &t.lods.back().slices;
        }
        for (lod_level& l : t.lods) {
            l.reach.reserve(l.slices.size());
            double reach = -std::numeric_limits<double>::infinity();
            for (const slice_row& s : l.slices) {
                reach = std::max(reach, s.t1);
                l.reach.push_back(reach);
            }
            rows += l.slices.size();
        }
        levels += t.lods.size();
        finest = std::min(finest, t.lods.front().scale);
    }
    fmt::print("{} levels of detail over {} reactors, {} rectangles in them, finest {:.3g} ms\n",
               levels, d.tables.size(), rows, finest);
}

// ============================================================================
//  24. the view -- which part of all that is on screen
// ============================================================================
//
// What is left once everything is rendered in advance: a selection, and where
// each window is looking. Changing the selection recolours the plot and the
// log and moves them to the request; it does not rebuild anything, and both
// windows can then be scrolled anywhere in the trace.

// What is picked, and by which hand.
//
// Two of these. `clicked` is the selection proper: it survives until something
// else is clicked. `hover` is what the pointer is over *right now*, and every
// field of it is cleared at the top of each frame and refilled by whichever
// window the pointer is in. Everything downstream reads the two through
// query(), log_cpu() and focus() below, which prefer the hover where there is
// one -- so passing the pointer over the histogram previews a request in the
// plot and in the log without losing the one you picked, and taking the
// pointer away puts it back.
struct selection {
    int32_t query = none;
    int32_t log_cpu = none;
    int32_t focus = none;  // a timeline entry on log_cpu
    // Set when this selection came from the timeline itself, which is what
    // tells the two apart where it matters: a request picked off the timeline
    // does not move the x axis, because it is already on screen and moving the
    // axis would take it out from under the pointer.
    bool from_timeline = false;
};

struct view {
    selection clicked;
    selection hover;
    // Where a window writes the hover it has just found. It becomes `hover` at
    // the top of the *next* frame, and that indirection is the point: a window
    // discovers what the pointer is over while it draws, which is too late for
    // itself and for every window drawn before it. One frame's lag, and in
    // exchange every window in a frame sees the same hover.
    selection pending;

    [[nodiscard]] int32_t query() const {
        return hover.query >= 0 ? hover.query : clicked.query;
    }
    [[nodiscard]] int32_t log_cpu() const {
        return hover.log_cpu >= 0 ? hover.log_cpu : clicked.log_cpu;
    }
    [[nodiscard]] int32_t focus() const {
        return hover.log_cpu >= 0 ? hover.focus : clicked.focus;
    }

    // Reactors kept on the plot whatever is selected. A row that is worth
    // watching -- the shard a request keeps waiting on -- otherwise comes and
    // goes with the selection.
    std::vector<uint8_t> pinned;  // parallel to trace::cpus
    uint32_t pins = 0;            // bumped on every change, so rows notice

    double t0 = 0;  // the picked request, in the plot's milliseconds
    double t1 = 0;
    std::vector<uint32_t> rows;  // one cpu per plot row

    // What the plot's rows have already been built for. Three things move
    // them: either selection, and the pins.
    int32_t rows_primary = none;
    int32_t rows_secondary = none;
    uint32_t rows_pins = uint32_t(-1);
    // ... and which request its x axis was last sent to.
    int32_t range_for = none - 1;

    int32_t scrolled_cpu = none;
    int32_t scrolled_to = none;
    // Set only by the histogram: picking a request there is asking to look at
    // it, so the x axis goes to it. Picking one off the timeline is asking
    // about something already on screen, and moving the axis under the pointer
    // would take it away.
    bool refit = false;

    // The plot's x axis and the log's scroll as the last frame actually drew
    // them -- not what they were asked to be. Where the user has panned to is
    // only knowable this way, and a preview has to know it to give it back.
    double axis_lo = 0;
    double axis_hi = 0;
    float log_scroll = 0;
    // Where the pointer was on that axis, if it was over the plot at all.
    // Kept for the same reason and from the same frame: the keyboard zoom runs
    // before the plot begins and has no other way to ask. See apply_keys.
    bool axis_hovered = false;
    double axis_mouse = 0;

    // Set while a preview is holding the view, with what it took. See the
    // borrow/return in follow_selection.
    bool borrowed = false;
    double borrowed_lo = 0;
    double borrowed_hi = 0;
    float borrowed_log_scroll = 0;
    int32_t borrowed_range_for = none - 1;
    int32_t borrowed_clicked = none;
    // A transient hover borrows the log position. Histogram previews already
    // borrow the plot axis too; timeline hovers only borrow the log, so they
    // can follow the hovered record and then return to wherever the reader was
    // before the hover.
    bool borrowed_log = false;
    int32_t borrowed_log_query = none;
    bool follow_log = false;
    bool histogram_picked = false;
    // One-shots, consumed by the plot and the log in the frame a preview ends.
    bool restore_axis = false;
    bool restore_log = false;

    // Where the keyboard has moved the timeline to, and whether it moved it
    // this frame. The same shape as `refit`: an instruction the plot consumes
    // and forgets, so that the axis is the user's again the moment the key
    // comes up. See apply_keys.
    bool key_axis = false;
    double key_lo = 0;
    double key_hi = 0;

    // The plot's geometry, kept from the frame that drew it: the row
    // checkboxes are put beside the rows they belong to, and the widget is
    // sized so that a row is the same height whatever the row count.
    float plot_top = 0;
    float plot_left = 0;
    float row_height = 0;
    float plot_chrome = 40;  // what ImPlot spends on axes and padding
    size_t visible_summary_rectangles = 0;
    size_t visible_nonsummary_rectangles = 0;
    double thinnest_visible_rectangle_pixels = 0;
    // The counts above are totals over every row of the plot, which is what
    // makes them hard to read on their own: the budget a level of detail
    // bounds is per row, and only the summarised half of it -- rectangles at
    // least a scale wide are drawn verbatim, and the cpu band tiles the row
    // while I/O spans overlap each other. These two say what to divide by.
    size_t visible_rows = 0;
    double lod_scale_pixels = 0;  // 0 when the row is drawing .slices
};

// The plot's rows: the pinned reactors, then the picked request's, then
// whatever the hovered request needs that is not there yet.
//
// In that order and no other. The pins are at the top because they were put
// there deliberately; the picked request's rows are stable for as long as it
// is picked; and the hovered request's extra rows come and go at the bottom,
// where rows appearing and disappearing does not move anything above them out
// from under the pointer.
void build_rows(const trace_data& d, view& v) {
    v.rows.clear();
    std::vector<uint8_t> taken(d.cpus.size(), 0);
    const auto add = [&](uint32_t cpu) {
        if (!taken[cpu]) {
            taken[cpu] = 1;
            v.rows.push_back(cpu);
        }
    };
    const auto add_query = [&](int32_t query) {
        if (query < 0) {
            return;
        }
        const query_row& q = d.queries[query];
        std::vector<uint32_t> cpus;
        for (uint32_t p = q.parts_begin; p < q.parts_end; ++p) {
            cpus.push_back(d.parts[p].cpu);
        }
        std::ranges::sort(cpus, {}, [&](uint32_t c) {
            return std::pair{d.cpus[c].node, d.cpus[c].shard};
        });
        for (const uint32_t cpu : cpus) {
            add(cpu);
        }
    };

    for (uint32_t cpu = 0; cpu < d.cpus.size(); ++cpu) {
        if (cpu < v.pinned.size() && v.pinned[cpu] != 0) {
            add(cpu);
        }
    }
    add_query(v.rows_primary);
    add_query(v.rows_secondary);
}

// The rectangles of one array that can be seen between two milliseconds. The
// first one that can reach into view is found by binary search over the
// running maximum of their ends; from there they are scanned until they start
// past the right edge.
//
// The two arrays are the same shape whether they are a reactor's rectangles or
// one of its levels of detail, which is what lets the plot draw a level with
// the code it already had.
std::span<const slice_row> slices_in(std::span<const slice_row> slices,
                                     std::span<const double> reach, double from, double to) {
    const auto first = std::ranges::lower_bound(reach, from);
    const auto begin = slices.begin() + (first - reach.begin());
    const auto end = std::upper_bound(begin, slices.end(), to,
                                      [](double x, const slice_row& s) { return x < s.t0; });
    return {begin, end};
}

std::span<const slice_row> slices_in(const cpu_tables& t, double from, double to) {
    return slices_in(t.slices, t.slice_reach, from, to);
}

// The level to draw a reactor at when a rectangle narrower than `thinnest`
// milliseconds is not worth drawing: the coarsest one that still draws
// everything wider than that verbatim. Null means there is no such level and
// the rectangles themselves are what to draw -- which is the answer whenever
// the plot is zoomed in far enough to show them.
const lod_level* level_for(const cpu_tables& t, double thinnest) {
    const lod_level* pick = nullptr;
    for (const lod_level& l : t.lods) {
        if (l.scale > thinnest) {
            break;  // .lods is in increasing scale order
        }
        pick = &l;
    }
    return pick;
}

// The record a click at `ts` on `cpu` landed on: the last one at or before it,
// which is the event that "owns" that moment.
int32_t owning_line(const trace_data& d, uint32_t cpu, int64_t ts) {
    const cpu_tables& t = d.tables[cpu];
    if (t.timeline.empty()) {
        return none;
    }
    const auto after = std::ranges::upper_bound(t.timeline, ts, {}, &timeline_row::ts);
    return after == t.timeline.begin() ? 0 : int32_t((after - 1) - t.timeline.begin());
}

// Where a request is looked at from before anything else is picked: its root
// reactor, at the first record on that reactor belonging to the request.
selection selection_of_query(const trace_data& d, int32_t query) {
    selection out;
    if (query < 0 || query >= int32_t(d.queries.size())) {
        return out;
    }
    out.query = query;
    const query_row& q = d.queries[query];
    out.log_cpu = int32_t(q.root_cpu);
    const cpu_tables& t = d.tables[q.root_cpu];
    const auto first = std::ranges::lower_bound(t.timeline, q.t0, {}, &timeline_row::ts);
    for (auto it = first; it != t.timeline.end() && it->ts <= q.t1; ++it) {
        if (query_of(d, q.root_cpu, it->table, it->index) == query) {
            out.focus = int32_t(it - t.timeline.begin());
            break;
        }
    }
    if (out.log_cpu >= 0) {
        if (out.focus < 0) {
            out.focus = owning_line(d, uint32_t(out.log_cpu), q.t0);
        }
    }
    return out;
}

// Run once a frame, before anything is drawn: the plot's rows and its time
// range follow both selections and the pins.
void follow_selection(const trace_data& d, view& v) {
    // The two hovers are not the same gesture.
    //
    // A hover from the histogram is a *preview*: it asks "what does this
    // request look like", and the answer is the plot it would get if it were
    // picked -- its rows, and its stretch of time -- with the picked request's
    // rows out of the way. A hover from the timeline is a highlight on the
    // plot that is already there: it asks "whose is this bar", and moving the
    // plot to answer would take the bar out from under the pointer.
    const bool preview = v.hover.query >= 0 && !v.hover.from_timeline;
    const bool transient_log_hover = v.hover.log_cpu >= 0;
    const int32_t primary = preview ? none : v.clicked.query;
    const int32_t secondary = v.hover.query;

    if (primary != v.rows_primary || secondary != v.rows_secondary || v.pins != v.rows_pins) {
        v.rows_primary = primary;
        v.rows_secondary = secondary;
        v.rows_pins = v.pins;
        build_rows(d, v);
    }

    // A preview borrows the view, and has to give it back.
    //
    // Moving the plot to the request under the pointer is the whole gesture,
    // but the pan and the zoom it displaces are the *user's* and not the
    // selection's. So they are kept on the way in and put back on the way out,
    // rather than the view being recomputed from `clicked` as though the
    // pointer leaving the histogram were a new selection. Recomputing is what
    // threw away wherever you had scrolled to, every time the pointer crossed
    // the histogram on its way somewhere else.
    //
    // A click in the histogram commits its preview. The click is recorded by
    // draw_queries_window and consumed once the hover handoff makes the new
    // clicked selection effective.
    if (v.histogram_picked && !preview) {
        v.histogram_picked = false;
        v.borrowed = false;
        v.borrowed_log = false;
        v.borrowed_log_query = none;
        v.restore_log = false;
    }

    const bool new_log_hover = transient_log_hover && !v.borrowed_log;
    const bool changed_log_hover_query =
        transient_log_hover && v.hover.query >= 0 && v.hover.query != v.borrowed_log_query;
    if (new_log_hover) {
        v.borrowed_log = true;
        v.borrowed_log_scroll = v.log_scroll;
        v.borrowed_log_query = v.hover.query;
        v.follow_log = true;
    } else if (changed_log_hover_query) {
        v.borrowed_log_query = v.hover.query;
        v.follow_log = true;
    } else if (!transient_log_hover && v.borrowed_log) {
        v.borrowed_log = false;
        v.borrowed_log_query = none;
        v.restore_log = true;
    }

    if (preview && !v.borrowed) {
        v.borrowed = true;
        v.borrowed_lo = v.axis_lo;
        v.borrowed_hi = v.axis_hi;
        v.borrowed_range_for = v.range_for;
        v.borrowed_clicked = v.clicked.query;
    } else if (!preview && v.borrowed) {
        v.borrowed = false;
        if (v.clicked.query == v.borrowed_clicked && v.borrowed_hi > v.borrowed_lo) {
            v.restore_axis = true;
            v.restore_log = true;
            // ... and put back what the axis was last *sent* to as well, so
            // that the next frame does not read the restore as a change and
            // refit over it.
            v.range_for = v.borrowed_range_for;
            return;
        }
    }

    // What the times are measured from: the previewed request if there is one,
    // the picked one otherwise. It is also what the x axis is sent to -- but
    // only when the request did not come off the timeline itself, where it was
    // already on screen and moving the axis would take it away.
    const int32_t against = preview                ? v.hover.query
                            : v.clicked.query >= 0 ? v.clicked.query
                                                   : v.hover.query;
    if (against == v.range_for) {
        return;
    }
    v.range_for = against;
    if (against >= 0) {
        const query_row& q = d.queries[against];
        v.t0 = d.ms(q.t0 - d.origin);
        v.t1 = d.ms(std::max(q.t1, q.t0 + 1) - d.origin);
        v.refit = !(preview ? v.hover.from_timeline : v.clicked.from_timeline);
    } else {
        // Nothing picked, which happens only with pinned rows on the plot:
        // the whole trace, then.
        v.t0 = 0;
        v.t1 = d.ms(d.last - d.origin);
    }
}

// The keyboard's hold on the timeline: w and s zoom, a and d pan.
//
// Rates rather than steps, scaled by the frame's own delta time: a doubling of
// the zoom per second, one screenful of pan per second, for as long as the key
// is held. That way the speed is the same on a 60 Hz display and a 144 Hz one,
// and a tap is a small movement rather than a fixed jump.
//
// It works off the axis the plot last *drew* (view::axis_lo/axis_hi) rather
// than off the selection, for the same reason a preview borrows the view: the
// pan and the zoom are the user's. It writes an instruction for the plot to
// consume rather than moving anything itself, because the axis belongs to
// ImPlot and the frame has not begun the plot yet.
//
// Zoom is about the pointer when the pointer is over the timeline, and about
// the centre of what is on screen when it is not. Pointing at a stretch and
// holding w is the same gesture as scrolling the wheel over it, and it should
// keep the same thing under the cursor; but a hand that is nowhere near the
// plot has said nothing about where to zoom, and the middle of the screen is
// the only place left that does not throw the view somewhere arbitrary.
void apply_keys(view& v) {
    v.key_axis = false;
    const ImGuiIO& io = ImGui::GetIO();
    // WantTextInput and not WantCaptureKeyboard: the latter is also true
    // whenever ImGui's keyboard navigation is live, which it is as soon as any
    // window has focus, and guarding on it would leave these keys permanently
    // dead. What must not be stolen is a field someone is typing into.
    if (io.WantTextInput) {
        return;
    }
    if (!(v.axis_hi > v.axis_lo)) {
        return;  // nothing has been drawn yet, so there is nothing to move
    }
    const double zoom = (ImGui::IsKeyDown(ImGuiKey_S) ? 1.0 : 0.0) -
                        (ImGui::IsKeyDown(ImGuiKey_W) ? 1.0 : 0.0);
    const double pan = (ImGui::IsKeyDown(ImGuiKey_D) ? 1.0 : 0.0) -
                       (ImGui::IsKeyDown(ImGuiKey_A) ? 1.0 : 0.0);
    if (zoom == 0 && pan == 0) {
        return;
    }
    const double dt = std::clamp(double(io.DeltaTime), 0.0, 0.25);
    // A floor on the width, and no ceiling: zoomed in far enough the axis is
    // millionths of a millisecond apart and the plot has nothing left to say,
    // whereas zooming out past the trace is how you find out you have panned
    // off the end of it.
    const double was = v.axis_hi - v.axis_lo;
    const double width = std::max(was * std::pow(100.0, zoom * dt), 1e-6);
    // The point that stays where it is. Clamped to the axis because the
    // pointer can be over the plot's padding, a hair outside the limits, and
    // a pivot outside the view drags it rather than holding it still.
    const double pivot = v.axis_hovered
                             ? std::clamp(v.axis_mouse, v.axis_lo, v.axis_hi)
                             : 0.5 * (v.axis_lo + v.axis_hi);
    // Ratio rather than std::pow(100, ...) again: the width has been through a
    // floor, and the pivot only stays put if the shrink applied to the offset
    // is the one the width actually took.
    const double shrink = width / was;
    const double lo = pivot - (pivot - v.axis_lo) * shrink + pan * dt * width;
    v.key_axis = true;
    v.key_lo = lo;
    v.key_hi = lo + width;
}

// ============================================================================
//  25. the latency histogram
// ============================================================================
//
// The picture the whole tool hangs off. x is 1/(1-quantile) on a log axis, so
// a click picks a *tier* rather than a request: the median at 2, the 99th at
// 100, the tail at 10000. Two requests from two tiers, side by side, is how a
// latency problem is read.

constexpr int quantile_points = 1001;
constexpr double quantile_max = 100000.0;

struct histogram {
    std::vector<double> x;        // 1/(1-p)
    std::vector<double> latency;  // seconds
    std::vector<double> cpu;      // seconds
};

// Which query sits at 1/(1-p) == x.
size_t query_at_quantile(const trace_data& d, double x) {
    const double p = 1.0 - 1.0 / std::clamp(x, 1.0, quantile_max);
    const auto at = size_t(p * double(d.by_latency.size()));
    return std::min(at, d.by_latency.size() - 1);
}

histogram build_histogram(const trace_data& d) {
    histogram h;
    for (int i = 0; i < quantile_points; ++i) {
        const double x = std::pow(10.0, 5.0 * double(i) / double(quantile_points - 1));
        const query_row& q = d.queries[d.by_latency[query_at_quantile(d, x)]];
        h.x.push_back(x);
        h.latency.push_back(d.seconds(q.t1 - q.t0));
        h.cpu.push_back(d.seconds(q.cpu_ticks));
    }
    return h;
}

// The aggregate over a range of quantiles: what the DragRect selects.
struct aggregate {
    size_t count = 0;
    double latency_mean = 0;
    double cpu_mean = 0;
};

aggregate build_aggregate(const trace_data& d, size_t from, size_t to) {
    aggregate a;
    if (from > to) {
        std::swap(from, to);
    }
    if (d.by_latency.empty()) {
        return a;
    }
    from = std::min(from, d.by_latency.size());
    to = std::min(to, d.by_latency.size() - 1);
    if (from > to) {
        return a;
    }
    const size_t end = to + 1;
    a.count = end - from;
    a.latency_mean = (d.latency_prefix[end] - d.latency_prefix[from]) / double(a.count);
    a.cpu_mean = (d.cpu_prefix[end] - d.cpu_prefix[from]) / double(a.count);
    return a;
}

// ============================================================================
//  26. the windows
// ============================================================================

constexpr ImU32 colour_hover = IM_COL32(255, 255, 255, 70);

void draw_queries_window(const trace_data& d, view& v, const histogram& h, double* rect) {
    ImGui::Begin("Queries");

    // The left button belongs to the quantile picker here, not to the plot:
    // dragging across the histogram is how a tier is scrubbed through, and a
    // plot that panned under the pointer at the same time would fight it.
    // Panning moves to the middle button and the scroll wheel still zooms.
    ImPlotInputMap& input = ImPlot::GetInputMap();
    const ImGuiMouseButton was_pan = input.Pan;
    input.Pan = ImGuiMouseButton_Middle;

    if (ImPlot::BeginPlot("latency by quantile", ImVec2(-1, 260))) {
        ImPlot::SetupAxes("1 / (1 - quantile)", "seconds", ImPlotAxisFlags_None,
                          ImPlotAxisFlags_None);
        ImPlot::SetupAxisScale(ImAxis_X1, ImPlotScale_Log10);
        ImPlot::SetupAxisScale(ImAxis_Y1, ImPlotScale_Log10);
        ImPlot::SetupAxesLimits(1, quantile_max, std::max(1e-6, h.latency.front() / 2),
                                std::max(1e-6, h.latency.back() * 2));
        ImPlot::PlotLine("latency", h.x.data(), h.latency.data(), quantile_points);
        ImPlot::PlotLine("cpu time", h.x.data(), h.cpu.data(), quantile_points);

        // Hovering previews a request; holding the button picks it. The
        // preview is the same selection the click would make, put in the
        // hover slot instead of the clicked one, so what you see while
        // scrubbing is exactly what you get when you let go.
        if (ImPlot::IsPlotHovered()) {
            const ImPlotPoint pt = ImPlot::GetPlotMousePos();
            const auto under = int32_t(d.by_latency[query_at_quantile(d, pt.x)]);
            if (ImGui::IsMouseClicked(0)) {
                v.histogram_picked = true;
            }
            if (ImGui::IsMouseDown(0)) {
                v.clicked = selection_of_query(d, under);
            } else {
                v.pending = selection_of_query(d, under);
            }
        }
        // Both selections, drawn: the hovered one first and in grey, so the
        // picked one stands on top of it and stays visible while the pointer
        // runs over the rest of the distribution.
        const auto marker = [&](int32_t query, ImVec4 colour, int id) {
            const auto at = std::ranges::find(d.by_latency, uint32_t(query));
            if (query < 0 || at == d.by_latency.end()) {
                return;
            }
            const double p = double(at - d.by_latency.begin()) / double(d.by_latency.size());
            double x = 1.0 / std::max(1e-6, 1.0 - p);
            ImPlot::DragLineX(id, &x, colour, 1,
                              ImPlotDragToolFlags_NoInputs | ImPlotDragToolFlags_NoFit);
        };
        marker(v.hover.query, ImVec4(0.55f, 0.55f, 0.55f, 1), 2);
        marker(v.clicked.query, ImVec4(1, 1, 1, 1), 0);
        rect[1] = 1e-9;
        rect[3] = 1e9;
        ImPlot::DragRect(1, &rect[0], &rect[1], &rect[2], &rect[3], ImVec4(1, 0, 1, 0.3f),
                         ImPlotDragToolFlags_NoFit);
        ImPlot::EndPlot();
    }
    input.Pan = was_pan;

    const aggregate a = build_aggregate(d, query_at_quantile(d, rect[0]),
                                        query_at_quantile(d, rect[2]));
    ImGui::Text("%zu queries between quantiles %.5f and %.5f", a.count,
                1.0 - 1.0 / std::clamp(rect[0], 1.0, quantile_max),
                1.0 - 1.0 / std::clamp(rect[2], 1.0, quantile_max));
    ImGui::Text("mean latency %.3f ms, mean cpu time %.3f ms", a.latency_mean * 1e3,
                a.cpu_mean * 1e3);
    ImGui::End();
}

// What is picked and what the pointer is over, one under the other. Both,
// because comparing two requests is the whole method -- and the second of them
// is gone the moment the pointer moves, so it has to be readable while it is
// there.
void describe_query(const trace_data& d, int32_t query, const char* what) {
    ImGui::SeparatorText(what);
    if (query < 0) {
        ImGui::TextUnformatted("(none)");
        return;
    }
    const query_row& q = d.queries[query];
    ImGui::Text("query %d, task %08" PRIx32 " on %s", query, q.root_task,
                d.cpus[q.root_cpu].label.c_str());
    ImGui::Text("latency %.3f ms, cpu time %.3f ms", d.seconds(q.t1 - q.t0) * 1e3,
                d.seconds(q.cpu_ticks) * 1e3);
    std::set<uint32_t> cpus;
    for (uint32_t p = q.parts_begin; p < q.parts_end; ++p) {
        cpus.insert(d.parts[p].cpu);
    }
    ImGui::Text("%u parts over %zu reactors", q.parts_end - q.parts_begin, cpus.size());
    if (q.statement >= 0) {
        const statement_row& st = d.statements[q.statement];
        ImGui::TextWrapped("%s: %s", std::string(d.text(st.keyspace)).c_str(),
                           std::string(d.text(st.text)).c_str());
    } else {
        ImGui::TextUnformatted("(no prepared statement recorded for this request)");
    }
}

void draw_selected_query(const trace_data& d, const view& v) {
    ImGui::Begin("Selected query");
    if (v.clicked.query < 0 && v.hover.query < 0) {
        ImGui::TextUnformatted("click the histogram or a bar of the timeline to pick a query");
        ImGui::End();
        return;
    }
    describe_query(d, v.clicked.query, "picked");
    describe_query(d, v.hover.query, "under the pointer");
    ImGui::End();
}

void draw_plot_window(const trace_data& d, view& v) {
    v.visible_summary_rectangles = 0;
    v.visible_nonsummary_rectangles = 0;
    v.thinnest_visible_rectangle_pixels = 0;
    v.visible_rows = 0;
    v.lod_scale_pixels = 0;
    ImGui::Begin("Timeline");
    if (v.rows.empty()) {
        ImGui::TextUnformatted("no query selected, and no reactor pinned");
        ImGui::End();
        return;
    }
    // A row is `row_pixels` tall, exactly, however many rows there are.
    //
    // The widget's height is not a row's height: ImPlot spends some of it on
    // the x axis' labels and its own padding, and what is left over is the
    // data area the rows are divided out of. Sizing the widget at
    // rows * 34 + 40 -- a *guess* at that overhead -- left a row
    // 34 + (40 - overhead)/rows tall, so every row changed height slightly
    // whenever a row was added, and the boundary under the pointer moved with
    // it. The overhead is measured below instead, from the frame that drew it,
    // and it is stable after the first.
    constexpr float row_pixels = 34.0f;
    const float height = float(v.rows.size()) * row_pixels + v.plot_chrome;

    std::vector<double> ticks;
    std::vector<const char*> labels;
    for (size_t i = 0; i < v.rows.size(); ++i) {
        ticks.push_back(double(i) + 0.5);
        labels.push_back(d.cpus[v.rows[i]].label.c_str());
    }

    // A gutter for the pin checkboxes, which are ordinary ImGui widgets put
    // beside their rows after the plot has been drawn and has said where its
    // rows are. Indenting the plot is what reserves the room for them.
    constexpr float gutter = 22.0f;
    const float gutter_x = ImGui::GetCursorScreenPos().x;
    ImGui::Indent(gutter);
    if (ImPlot::BeginPlot("##timeline", ImVec2(-1, height))) {
        // The y axis is a list of reactors rather than a quantity. Locked, so
        // that a drag or a scroll moves along the trace and never shears the
        // rows off the plot -- and inverted, so that row 0 is the top one.
        //
        // Inverted with the flag rather than by handing SetupAxisLimits its
        // bounds the other way round, which does not invert anything: the
        // limits come back normalised, row 0 lands at the bottom, and rows
        // appended for a hovered request appear above the rows they were
        // supposed to go under.
        ImPlot::SetupAxes("ms from the start of the trace", nullptr, ImPlotAxisFlags_None,
                          ImPlotAxisFlags_NoGridLines | ImPlotAxisFlags_Lock |
                              ImPlotAxisFlags_Invert);
        ImPlot::SetupAxisTicks(ImAxis_Y1, ticks.data(), int(ticks.size()), labels.data());
        // Pinned to the request when it has just changed, and the user's after
        // that: everything is drawn already, so panning and zooming out of the
        // request and into what the reactor did before and after it costs the
        // same as looking at the request itself.
        // The x axis goes to the request only when the histogram sent it
        // there; the y axis always spans the rows, which is not a choice the
        // user has -- it is locked, and rows appear and disappear under it.
        const double pad = std::max(0.02 * (v.t1 - v.t0), 0.001);
        ImPlot::SetupAxesLimits(v.t0 - pad, v.t1 + pad, 0, 1, ImPlotCond_Once);
        if (v.refit) {
            ImPlot::SetupAxisLimits(ImAxis_X1, v.t0 - pad, v.t1 + pad, ImPlotCond_Always);
            v.refit = false;
        } else if (v.restore_axis) {
            // Where the user was before a preview took the plot. Not v.t0/v.t1
            // -- those are the selected request's stretch, and the whole point
            // is that the axis need not be on it.
            ImPlot::SetupAxisLimits(ImAxis_X1, v.borrowed_lo, v.borrowed_hi,
                                    ImPlotCond_Always);
        } else if (v.key_axis) {
            // Where w/a/s/d have moved it to. Last of the three, so that a
            // request picked this frame still wins the axis: a key held down
            // is a continuous thing and picks itself up again next frame.
            ImPlot::SetupAxisLimits(ImAxis_X1, v.key_lo, v.key_hi, ImPlotCond_Always);
        }
        v.restore_axis = false;
        ImPlot::SetupAxisLimits(ImAxis_Y1, 0, double(v.rows.size()), ImPlotCond_Always);
        ImPlot::PushPlotClipRect();
        ImDrawList* draw = ImPlot::GetPlotDrawList();

        const ImPlotRect limits = ImPlot::GetPlotLimits();
        // What was actually drawn, for a preview to borrow next frame.
        v.axis_lo = limits.X.Min;
        v.axis_hi = limits.X.Max;
        // Two passes over each row rather than one, because what is drawn on
        // top has to be drawn last and the slices are in time order, not in
        // depth order. Two is enough: cpu, then the I/O that interrupts it.
        struct hit_row {
            const slice_row* slice;
            size_t row;
        };
        hit_row hit{nullptr, 0};
        const ImPlotPoint pt = ImPlot::GetPlotMousePos();
        const bool hovering = ImPlot::IsPlotHovered();
        // For the keyboard zoom next frame, which pivots on the pointer when
        // it is over the plot.
        v.axis_hovered = hovering;
        v.axis_mouse = pt.x;

        // One device pixel, in the units ImPlot hands back: 1 on an ordinary
        // display, 0.5 where the window has two device pixels to the unit.
        const float device_pixel =
            1.0f / std::max(1.0f, ImGui::GetIO().DisplayFramebufferScale.x);

        // One rectangle, wherever it came from. A verbatim one is coloured by
        // whose work it is; a summary, which has no single owner, by how busy
        // it says that stretch was.
        //
        // `hoverable` is false for the I/O drawn from .io_slices over a picked
        // request: those rectangles are a second copy of a span the flattened
        // band already offers, and the pointer must keep landing on the band
        // the plot is really made of rather than on whichever overlay happens
        // to be painted over it.
        const auto draw_slice = [&](const slice_row& s, size_t row, bool hoverable = true) {
            const ImU32 colour =
                s.summary ? summary_colour(s.table, s.density)
                          : colour_of(s.table, s.query, v.clicked.query, v.hover.query);
            const band at = band_of(s.table);
            ImVec2 a = ImPlot::PlotToPixels(ImPlotPoint{s.t0, double(row) + at.top});
            ImVec2 b = ImPlot::PlotToPixels(ImPlotPoint{s.t1, double(row) + at.bottom});
            if (b.x - a.x < 1.0f) {
                b.x = a.x + 1.0f;  // a slice thinner than a pixel is still a slice
            }
            draw->AddRectFilled(a, b, colour);
            // Its own trailing edge, so that a run of stretches on one reactor
            // reads as a run of them rather than as one block. Only where
            // there is room for it: at one pixel wide the bar *is* its edge,
            // and darkening it would turn a dense stretch of the plot into a
            // dark smear. Summaries tile, and are never that wide.
            //
            // One *device* pixel of it: ImPlot works in ImGui's units, and on a
            // display with two device pixels to the unit a one-unit edge comes
            // out twice as thick as it reads in this file. It stays where the
            // slice really ends rather than being snapped to the pixel grid --
            // a fractional edge is filled across the two columns it falls
            // between, which is what keeps a run of slices in their places
            // instead of jittering by a pixel as the plot is panned.
            if (b.x - a.x >= 3.0f * device_pixel && !s.summary) {
                draw->AddRectFilled(ImVec2{b.x - device_pixel, a.y}, b, darker(colour, 0.45f));
            }
            // The topmost bar the pointer is inside, in both axes: an I/O
            // drawn over a stretch of cpu leaves that stretch hoverable above
            // and below it.
            if (hoverable && hovering && int(std::floor(pt.y)) == int(row) && pt.x >= s.t0 &&
                pt.x <= s.t1 && pt.y >= double(row) + at.top &&
                pt.y <= double(row) + at.bottom) {
                hit = {&s, row};
            }
        };

        // What is too narrow to be worth a rectangle, in milliseconds: a
        // pixel. Below this the plot reads a level of detail instead, where
        // everything narrower has been summarised into how busy it was --
        // which is what keeps a frame's work proportional to the width of the
        // plot rather than to the length of the trace. A pixel and not half of
        // one because a summary narrower than a pixel is widened to a pixel by
        // draw_slice, so half-pixel summaries come out as two rectangles on
        // the same pixel, of which only the second is seen.
        const double per_pixel =
            (limits.X.Max - limits.X.Min) / double(std::max(1.0f, ImPlot::GetPlotSize().x));
        const double thinnest = per_pixel;
        const auto is_subpixel = [&](const slice_row& s) {
            return s.t1 - s.t0 < per_pixel;
        };

        v.visible_rows = v.rows.size();
        for (size_t row = 0; row < v.rows.size(); ++row) {
            const cpu_tables& t = d.tables[v.rows[row]];
            const lod_level* const lod = level_for(t, thinnest);
            if (lod != nullptr) {
                v.lod_scale_pixels = lod->scale / per_pixel;
            }
            const std::span<const slice_row> visible =
                lod != nullptr ? slices_in(lod->slices, lod->reach, limits.X.Min, limits.X.Max)
                               : slices_in(t, limits.X.Min, limits.X.Max);
            for (const slice_row& s : visible) {
                if (s.summary) {
                    ++v.visible_summary_rectangles;
                } else {
                    ++v.visible_nonsummary_rectangles;
                }
                const double pixels = (s.t1 - s.t0) / per_pixel;
                if (v.thinnest_visible_rectangle_pixels == 0 ||
                    pixels < v.thinnest_visible_rectangle_pixels) {
                    v.thinnest_visible_rectangle_pixels = pixels;
                }
            }
            for (int layer = 0; layer < 2; ++layer) {
                for (const slice_row& s : visible) {
                    if (layer_of(s.table) == layer) {
                        draw_slice(s, row);
                    }
                }
            }
            // The two selected requests, drawn again over what buried them.
            // Three things bury a request, and this puts it back on top of all
            // of them. A summary has no single request, so a request whose
            // every rectangle is thinner than a pixel would be invisible on a
            // zoomed-out plot -- which is exactly the plot you are looking at
            // when you ask where a request went. draw_slice widens a subpixel
            // rectangle to a pixel, but a later neighbour can still cover that
            // pixel. And pass_io_stack flattened the I/O band to the span on
            // top, which may be somebody else's: a request's own I/O is drawn
            // from .io_slices, where the spans are whole and overlapping, and
            // so it is drawn whether or not the row is on a level of detail.
            // An overlay is one request on one reactor -- a few rectangles --
            // so it does not need the pyramid and would gain nothing from it.
            //
            // It costs a binary search and a scan of the request's own stretch
            // of time, not of the row: a request's rectangles are contiguous
            // in time, because a stretch of time is what a request is.
            if (lod != nullptr || v.clicked.query >= 0 || v.hover.query >= 0) {
                for (const int32_t q : {v.clicked.query, v.hover.query}) {
                    if (q < 0) {
                        continue;
                    }
                    const query_row& qr = d.queries[q];
                    const double from = d.ms(qr.t0 - d.origin);
                    const double to = d.ms(qr.t1 - d.origin);
                    for (const slice_row& s : slices_in(t, from, to)) {
                        if (s.query == q && layer_of(s.table) == 0 &&
                            (lod != nullptr || is_subpixel(s))) {
                            draw_slice(s, row);
                        }
                    }
                    for (const slice_row& s : slices_in(t.io_slices, t.io_reach, from, to)) {
                        if (s.query == q) {
                            draw_slice(s, row, false);
                        }
                    }
                }
            }
        }

        if (hovering) {
            const auto row = int(std::floor(pt.y));
            if (row >= 0 && row < int(v.rows.size())) {
                const uint32_t cpu = v.rows[row];
                if (hit.slice != nullptr) {
                    const slice_row& s = *hit.slice;
                    const band at = band_of(s.table);
                    draw->AddRectFilled(
                        ImPlot::PlotToPixels(ImPlotPoint{s.t0, double(hit.row) + at.top}),
                        ImPlot::PlotToPixels(ImPlotPoint{s.t1, double(hit.row) + at.bottom}),
                        colour_hover);
                    ImGui::BeginTooltip();
                    ImGui::Text("%s  %.6f ms", d.cpus[v.rows[hit.row]].label.c_str(),
                                s.t1 - s.t0);
                    // A summary stands for records rather than being one, so
                    // it says how many and how much of the stretch they took,
                    // and the way to see them is to zoom until they are drawn.
                    if (s.summary) {
                        ImGui::Text("%u %s here, %.0f%% of the time", s.index,
                                    s.table == tab_io_begin ? "I/Os" : "stretches on the cpu",
                                    100.0 * double(s.density));
                        ImGui::TextUnformatted("too narrow to draw -- zoom in for the records");
                        ImGui::EndTooltip();
                    } else {
                        ImGui::TextUnformatted(
                            format_event(d, v.rows[hit.row], s.table, s.index).c_str());
                        if (s.table == tab_switch) {
                            const switch_row& sw = d.tables[v.rows[hit.row]].switches[s.index];
                            if (sw.loc != 0 && d.locations[sw.loc].resolved) {
                                const location_row& l = d.locations[sw.loc];
                                ImGui::Text("created at %s:%u in %s",
                                            std::string(d.text(l.file)).c_str(), l.line,
                                            std::string(d.text(l.function)).c_str());
                            }
                        }
                        ImGui::Text("%s%s", s.table == tab_io_begin ? "waiting for this I/O"
                                                                   : "on the cpu",
                                    s.query < 0 ? ", no request"
                                    : s.query == v.clicked.query ? ", the picked request"
                                    : s.query == v.hover.query
                                        ? ", the request under the pointer"
                                        : ", another request");
                        ImGui::EndTooltip();
                    }
                }
                // Hovering a row previews the record under the pointer in
                // the log; clicking keeps it. Both come from the same place:
                // the last record at or before where the pointer is, which is
                // the event that owns that moment on that reactor.
                const int64_t ts = d.origin + int64_t(pt.x * 1e6 / d.ns_per_tick);
                selection at;
                at.log_cpu = int32_t(cpu);
                at.focus = owning_line(d, cpu, ts);
                at.from_timeline = true;
                // A bar belongs to a request, so pointing at one selects that
                // request as well as that record -- which is how a row of
                // washed-out work is followed back to whatever it was for.
                // Only where the bar has a request: a stretch the trace could
                // not attribute leaves the selection where it was.
                if (hit.slice != nullptr && hit.slice->query >= 0) {
                    at.query = hit.slice->query;
                }
                if (ImGui::IsMouseClicked(0)) {
                    v.clicked.log_cpu = at.log_cpu;
                    v.clicked.focus = at.focus;
                    if (at.query >= 0) {
                        v.clicked.query = at.query;
                    }
                    v.clicked.from_timeline = true;
                } else {
                    v.pending = at;
                }
            }
        }
        v.plot_top = ImPlot::GetPlotPos().y;
        v.plot_left = gutter_x;
        v.row_height = ImPlot::GetPlotSize().y / float(v.rows.size());
        v.plot_chrome = height - ImPlot::GetPlotSize().y;

        ImPlot::PopPlotClipRect();
        ImPlot::EndPlot();
    }
    ImGui::Unindent(gutter);

    // The pins. Drawn last and placed by hand, in the gutter indented above,
    // each beside the row it holds: a checkbox cannot be a y-axis tick label,
    // and a list of them somewhere else would not say which row is which.
    if (v.row_height > 0) {
        const ImVec2 cursor = ImGui::GetCursorScreenPos();
        for (size_t row = 0; row < v.rows.size(); ++row) {
            const uint32_t cpu = v.rows[row];
            ImGui::SetCursorScreenPos(
                ImVec2(v.plot_left,
                       v.plot_top + (float(row) + 0.5f) * v.row_height -
                           ImGui::GetFrameHeight() * 0.5f));
            ImGui::PushID(int(cpu));
            bool on = cpu < v.pinned.size() && v.pinned[cpu] != 0;
            if (ImGui::Checkbox("##pin", &on)) {
                v.pinned.resize(d.cpus.size(), 0);
                v.pinned[cpu] = on ? 1 : 0;
                ++v.pins;
            }
            if (ImGui::IsItemHovered()) {
                ImGui::SetTooltip("keep %s on the plot whatever is selected",
                                  d.cpus[cpu].label.c_str());
            }
            ImGui::PopID();
        }
        // Back where the plot left it, and an empty item to say so: ImGui
        // tracks a window's extent through the items in it, and a cursor moved
        // by hand and not followed by one trips its check.
        ImGui::SetCursorScreenPos(cursor);
        ImGui::Dummy(ImVec2(0, 0));
    }
    ImGui::End();
}

// One reactor's whole trace, scrolled to the request. Every line is rendered
// already, so the clipper draws the handful on screen out of a hundred
// thousand and the scrollbar reaches the rest of the trace -- what the shard
// was doing before the request arrived and after it answered is one drag away.
void draw_log_window(const trace_data& d, view& v) {
    ImGui::Begin("Log");
    const int32_t cpu = v.log_cpu();
    const int32_t focus = v.focus();
    if (cpu < 0) {
        ImGui::TextUnformatted("click a row of the timeline to read that shard's log");
        ImGui::End();
        return;
    }
    const cpu_tables& t = d.tables[cpu];
    ImGui::Text("%s, %zu records over the whole trace", d.cpus[cpu].label.c_str(),
                t.timeline.size());
    ImGui::SameLine();
    // Scroll where the effective selection points whenever that moves -- which
    // under a hover is every time the pointer does -- and stay put otherwise,
    // so the window can be read and scrolled without being dragged back.
    bool scroll = v.follow_log || cpu != v.scrolled_cpu || focus != v.scrolled_to;
    if (v.restore_log) {
        // A preview just ended: the selection going back to the picked request
        // is not a move to follow, it is the undoing of one.
        scroll = false;
    }
    if (ImGui::SmallButton("back to the selection")) {
        scroll = true;
    }
    v.scrolled_cpu = cpu;
    v.scrolled_to = focus;
    ImGui::Separator();

    if (ImGui::BeginTable("##events", 4,
                          ImGuiTableFlags_Borders | ImGuiTableFlags_RowBg |
                              ImGuiTableFlags_Resizable | ImGuiTableFlags_SizingStretchProp |
                              ImGuiTableFlags_ScrollX | ImGuiTableFlags_ScrollY,
                          ImVec2(0, 0))) {
        ImGui::TableSetupColumn("Time", ImGuiTableColumnFlags_WidthFixed, 135.0f);
        ImGui::TableSetupColumn("Kind", ImGuiTableColumnFlags_WidthFixed, 90.0f);
        ImGui::TableSetupColumn("Task", ImGuiTableColumnFlags_WidthFixed, 95.0f);
        ImGui::TableSetupColumn("Details", ImGuiTableColumnFlags_WidthStretch);
        ImGui::TableSetupScrollFreeze(0, 1);
        ImGui::TableHeadersRow();

        if (v.restore_log) {
            ImGui::SetScrollY(v.borrowed_log_scroll);
            v.restore_log = false;
        } else {
            // Where the reader is, for a preview to borrow. Read rather than
            // remembered, because the table's scrollbar and the wheel move it too.
            v.log_scroll = ImGui::GetScrollY();
        }
        v.follow_log = false;

        // The details column is deliberately one line: the clipper needs a
        // stable row height when the histogram changes the included range.
        const float event_row_height =
            ImGui::GetTextLineHeight() + 2.0f * ImGui::GetStyle().CellPadding.y;
        ImGuiListClipper clipper;
        clipper.Begin(int(t.timeline.size()), event_row_height);
        if (scroll && focus >= 0) {
            clipper.IncludeItemByIndex(focus);
        }
        while (clipper.Step()) {
            for (int i = clipper.DisplayStart; i < clipper.DisplayEnd; ++i) {
                const timeline_row& e = t.timeline[i];
                const bool focused = i == focus;
                const bool in_query =
                    v.query() >= 0 && query_of(d, uint32_t(cpu), e.table, e.index) == v.query();
                ImGui::TableNextRow(ImGuiTableRowFlags_None, event_row_height);
                if (focused || in_query) {
                    ImGui::TableSetBgColor(ImGuiTableBgTarget_RowBg1,
                                           focused ? IM_COL32(100, 78, 20, 180)
                                                   : IM_COL32(20, 90, 45, 140));
                }
                ImGui::PushStyleColor(ImGuiCol_Text,
                                      focused    ? IM_COL32(255, 220, 100, 255)
                                      : in_query ? IM_COL32(120, 240, 120, 255)
                                                 : IM_COL32(150, 150, 158, 255));

                ImGui::TableSetColumnIndex(0);
                // Relative to the request, wherever in the trace the row is,
                // so scrolling away from it reads as a distance from it.
                ImGui::Text("%+.6f ms", d.ms(e.ts - d.origin) - v.t0);
                ImGui::TableSetColumnIndex(1);
                ImGui::TextUnformatted(event_kind_name(d, uint32_t(cpu), e.table, e.index));
                ImGui::TableSetColumnIndex(2);
                ImGui::Text("%08" PRIx32, task_of(d, uint32_t(cpu), e.table, e.index));
                ImGui::TableSetColumnIndex(3);
                std::string details = format_event_details(d, uint32_t(cpu), e.table, e.index);
                std::ranges::replace_if(details, [](char c) { return c == '\r' || c == '\n'; }, ' ');
                ImGui::TextUnformatted(details.empty() ? "—" : details.c_str());
                ImGui::PopStyleColor();

                if (focused && scroll) {
                    // From the row itself rather than an estimate of where it
                    // is, which is the one way to get it right when the
                    // clipper means most rows were never laid out.
                    ImGui::SetScrollHereY(0.4f);
                }
            }
        }
        ImGui::EndTable();
    }
    ImGui::End();
}

void draw_nodes_window(const trace_data& d) {
    ImGui::Begin("Nodes");
    for (size_t i = 0; i < d.nodes.size(); ++i) {
        const node_row& n = d.nodes[i];
        ImGui::Text("node %zu  %s", i, n.boot_id.c_str());
        ImGui::Text("    build %s, shards %s, %.3f s", n.build_id.c_str(),
                    fmt::format("{}", fmt::join(n.shards, ",")).c_str(),
                    n.last_ns > n.first_ns ? double(n.last_ns - n.first_ns) / 1e9 : 0.0);
        if (n.first_ns != std::numeric_limits<uint64_t>::max() && n.first_ns != 0) {
            ImGui::Text("    from %s", format_realtime(n.first_ns).c_str());
        }
    }
    ImGui::Separator();
    size_t events = 0;
    for (const cpu_tables& t : d.tables) {
        events += t.timeline.size();
    }
    ImGui::Text("%zu events over %zu reactors, %zu queries", events, d.cpus.size(),
                d.queries.size());
    ImGui::End();
}

void draw_debug_window(const view& v, bool* open) {
    if (!*open) {
        return;
    }
    ImGui::Begin("Debug", open);
    const size_t rects = v.visible_summary_rectangles + v.visible_nonsummary_rectangles;
    ImGui::Text("visible rectangles: %zu over %zu rows (%.0f per row)", rects, v.visible_rows,
                v.visible_rows == 0 ? 0.0 : double(rects) / double(v.visible_rows));
    ImGui::Text("  summaries: %zu", v.visible_summary_rectangles);
    ImGui::Text("  nonsummaries: %zu", v.visible_nonsummary_rectangles);
    if (v.lod_scale_pixels > 0) {
        ImGui::Text("  level of detail: %.2f px per summary", v.lod_scale_pixels);
    } else {
        ImGui::TextUnformatted("  level of detail: none, drawing the rectangles");
    }
    if (v.thinnest_visible_rectangle_pixels > 0) {
        ImGui::Text("  thinnest rectangle: %.3f px", v.thinnest_visible_rectangle_pixels);
    } else {
        ImGui::TextUnformatted("  thinnest rectangle: none");
    }
    ImGui::End();
}

// --- what the startup cost ----------------------------------------------------
//
// Every pass says what it *found* on the way in; this says what it cost. It is
// deliberately neither a table nor a column: the numbers are about this run of
// the program rather than about the trace, and nothing downstream may read
// them. So a pass stays what it was -- a free function handed tables, returning
// nothing -- and the timing is a wrapper the caller puts around the call.
//
// Two outputs, because they answer different questions. A line under each pass
// as it finishes says where a run that is still going has got to; the
// breakdown at the end, most expensive first, says what to attack.

struct pass_clock {
    std::vector<std::pair<const char*, double>> costs;

    // Runs `pass` and prints its cost beneath whatever the pass itself printed,
    // so the log reads as "what it found, what it cost" per step.
    template <typename F>
    void run(const char* name, F&& pass) {
        const auto started = std::chrono::steady_clock::now();
        pass();
        const double ms = std::chrono::duration<double, std::milli>(
                std::chrono::steady_clock::now() - started).count();
        costs.emplace_back(name, ms);
        fmt::print("  [{:8.1f} ms] {}\n", ms, name);
    }

    void report() const {
        auto sorted = costs;
        std::ranges::stable_sort(sorted, std::greater<>{},
                                 &std::pair<const char*, double>::second);
        const double total = std::accumulate(costs.begin(), costs.end(), 0.0,
                [](double sum, const auto& c) { return sum + c.second; });
        fmt::print("\nstartup: {:.1f} ms in {} passes, most expensive first\n", total,
                   costs.size());
        for (const auto& [name, ms] : sorted) {
            fmt::print("  {:8.1f} ms  {:4.1f}%  {}\n", ms,
                       total > 0 ? 100.0 * ms / total : 0.0, name);
        }
    }
};

}  // namespace

// ============================================================================
//  27. main
// ============================================================================

// The whole of the program, so that main is the one place that has to decide
// what to do about an exception: say what went wrong and stop. Nothing below
// catches, because there is nothing below that could do better.
static int run(int argc, char** argv) {
    if (argc < 2) {
        fprintf(stderr, "usage: %s SNAPSHOT-DIR [SNAPSHOT-DIR ...]\n", argv[0]);
        fprintf(stderr,
                "  a directory of <uuid>.trace files and their <uuid>.metadata.json,\n"
                "  as written by Scylla's POST /system/trace_snapshot.\n"
                "  one directory per node, in a stable order: the first is the\n"
                "  reference clock the others are converted into.\n");
        return 2;
    }

    trace_data d;
    pass_clock timing;
    timing.run("pass_gather", [&] { pass_gather(d, argc, argv); });
    if (d.files.empty()) {
        fprintf(stderr, "no *.trace files in the supplied snapshot directories\n");
        return 1;
    }

    // The objects these traces were written by, found by build id. They are
    // both halves of reading a trace: what its tracepoints *are*, which is the
    // table in each object's `tracepoints` section, and what its source
    // locations point at, which is the object's .rodata. Nothing in the traced
    // process writes them -- see tools/gather-dsos.
    const char* const dso_env = std::getenv("TRACE_DSO_DIR");
    const std::filesystem::path dso_dir = std::filesystem::path(argv[1]) / "dsos";
    const std::string dso_root = dso_env != nullptr ? std::string(dso_env)
                                 : std::filesystem::exists(dso_dir)
                                     ? dso_dir.string()
                                     : std::string(".");

    plugin::decoder decoder;
    timing.run("pass_decoder", [&] {
        decoder = plugin::build(dso_root);
        pass_decoder(decoder);
    });
    timing.run("pass_decode", [&] { pass_decode(d, decoder, dso_root); });
    timing.run("pass_order", [&] { pass_order(d); });
    timing.run("pass_retime", [&] { pass_retime(d); });
    timing.run("pass_order (again)", [&] { pass_order(d); });
    // Before pass_index, because it is what fills in the task of a record that
    // did not carry one -- and the index is keyed on that task.
    timing.run("pass_attribute", [&] { pass_attribute(d); });
    // After the second pass_order, because the walk is over the timeline and
    // relies on it being in the order the rings hold the records.
    timing.run("pass_task_queue_runs", [&] { pass_task_queue_runs(d); });
    timing.run("pass_index", [&] { pass_index(d); });
    timing.run("pass_io_spans", [&] { pass_io_spans(d); });
    timing.run("pass_statements", [&] { pass_statements(d); });
    timing.run("pass_connections", [&] { pass_connections(d); });
    timing.run("pass_rpc_pair", [&] { pass_rpc_pair(d); });
    timing.run("pass_queries", [&] { pass_queries(d); });
    timing.run("pass_query_rows", [&] { pass_query_rows(d); });
    timing.run("pass_cost", [&] { pass_cost(d); });
    timing.run("pass_prefix_sums", [&] { pass_prefix_sums(d); });
    timing.run("pass_query_statement", [&] { pass_query_statement(d); });
    timing.run("pass_render", [&] { pass_render(d); });
    // Both after pass_render, which is where the rectangles come from, and in
    // this order: the pyramid must be built from the flattened I/O band, not
    // from the overlapping one it replaces.
    timing.run("pass_io_stack", [&] { pass_io_stack(d); });
    timing.run("pass_lod", [&] { pass_lod(d); });
    timing.report();

    if (d.queries.empty()) {
        fprintf(stderr, "no CQL requests in these snapshots: nothing to look at\n");
        return 1;
    }
    // The headless path, for asking what a trace holds without a window --
    // and for the checks that a GUI is a poor place to make. $TRACE_DUMP_QUERY
    // is a quantile: 0.5 is the median request, 1 the slowest.
    if (const char* const at = std::getenv("TRACE_DUMP_QUERY"); at != nullptr) {
        view v;
        const auto rank = std::min(size_t(std::atof(at) * double(d.by_latency.size())),
                                   d.by_latency.size() - 1);
        v.clicked = selection_of_query(d, int32_t(d.by_latency[rank]));
        v.pinned.assign(d.cpus.size(), 0);
        follow_selection(d, v);
        const query_row& q = d.queries[v.query()];
        fmt::print("\nquery {} at quantile {}: task {:08x} on {}, {:.3f} ms, {:.3f} ms of cpu\n",
                   v.query(), at, q.root_task, d.cpus[q.root_cpu].label,
                   d.seconds(q.t1 - q.t0) * 1e3, d.seconds(q.cpu_ticks) * 1e3);
        if (q.statement >= 0) {
            fmt::print("  {} {}\n", d.text(d.statements[q.statement].keyspace),
                       d.text(d.statements[q.statement].text));
        }
        for (uint32_t p = q.parts_begin; p < q.parts_end; ++p) {
            fmt::print("  part {} task {:08x}\n", d.cpus[d.parts[p].cpu].label,
                       d.parts[p].task);
        }
        for (size_t row = 0; row < v.rows.size(); ++row) {
            const cpu_tables& t = d.tables[v.rows[row]];
            double on_cpu = 0;
            double in_io = 0;
            size_t bars = 0;
            for (const slice_row& sl : slices_in(t, v.t0, v.t1)) {
                const bool mine = sl.query == v.query();
                // Clipped to the request, because a bar is drawn to the next
                // record on the shard and the last one runs past the answer --
                // which is what pass_cost cuts back, so cutting it back here
                // too is what makes this agree with the headline number.
                const double covered =
                    std::max(0.0, std::min(sl.t1, v.t1) - std::max(sl.t0, v.t0));
                on_cpu += mine && sl.table == tab_switch ? covered : 0;
                in_io += mine && sl.table == tab_io_begin ? covered : 0;
                bars += mine ? 1 : 0;
            }
            fmt::print("  row {} {}: {} bars, {:.3f} ms on the cpu, {:.3f} ms in I/O\n", row,
                       d.cpus[v.rows[row]].label, bars, on_cpu, in_io);
        }
        for (const uint32_t cpu : v.rows) {
            const cpu_tables& t = d.tables[cpu];
            fmt::print("\n{}: this request's records\n", d.cpus[cpu].label);
            for (uint32_t i = 0; i < t.timeline.size(); ++i) {
                const timeline_row& e = t.timeline[i];
                if (query_of(d, cpu, e.table, e.index) != v.query()) {
                    continue;
                }
                fmt::print("  {:+13.6f} ms  {}\n", d.ms(e.ts - d.origin) - v.t0,
                           t.log_text.get(t.log_lines[i]));
            }
        }
        return 0;
    }
    if (std::getenv("TRACE_HEADLESS") != nullptr) {
        return 0;  // the counts above are the whole point of this path
    }

    const histogram hist = build_histogram(d);
    view v;
    v.clicked = selection_of_query(d, int32_t(d.by_latency[d.by_latency.size() / 2]));
    v.pinned.assign(d.cpus.size(), 0);
    v.refit = true;
    follow_selection(d, v);

    if (!SDL_Init(SDL_INIT_VIDEO)) {
        fprintf(stderr, "SDL_Init failed: %s\n", SDL_GetError());
        return 1;
    }
    const char* glsl_version = "#version 130";
    SDL_GL_SetAttribute(SDL_GL_CONTEXT_MAJOR_VERSION, 3);
    SDL_GL_SetAttribute(SDL_GL_CONTEXT_MINOR_VERSION, 0);
    SDL_GL_SetAttribute(SDL_GL_CONTEXT_PROFILE_MASK, SDL_GL_CONTEXT_PROFILE_CORE);
    SDL_GL_SetAttribute(SDL_GL_DOUBLEBUFFER, 1);
    SDL_Window* window = SDL_CreateWindow("Trace viewer", 1600, 900,
                                          SDL_WINDOW_OPENGL | SDL_WINDOW_RESIZABLE |
                                              SDL_WINDOW_MAXIMIZED |
                                              SDL_WINDOW_HIGH_PIXEL_DENSITY);
    if (window == nullptr) {
        fprintf(stderr, "SDL_CreateWindow failed: %s\n", SDL_GetError());
        SDL_Quit();
        return 1;
    }
    SDL_GLContext gl = SDL_GL_CreateContext(window);
    SDL_GL_MakeCurrent(window, gl);
    SDL_GL_SetSwapInterval(1);

    IMGUI_CHECKVERSION();
    ImGui::CreateContext();
    ImPlot::CreateContext();
    ImGui::GetIO().ConfigFlags |= ImGuiConfigFlags_NavEnableKeyboard;
    ImGui::GetIO().ConfigFlags |= ImGuiConfigFlags_DockingEnable;
    ImGui::StyleColorsDark();
    ImGui_ImplSDL3_InitForOpenGL(window, gl);
    ImGui_ImplOpenGL3_Init(glsl_version);

    // The quantile range the aggregate is taken over: a DragRect in x, with y
    // pinned, because the y of that plot is a latency and not a choice.
    double rect[4] = {1.0, 1e-9, 10.0, 1e9};

    bool show_debug = false;
    bool show_imgui_demo = false;
    bool done = false;
    while (!done) {
        SDL_Event event;
        while (SDL_PollEvent(&event)) {
            ImGui_ImplSDL3_ProcessEvent(&event);
            if (event.type == SDL_EVENT_QUIT) {
                done = true;
            }
            if (event.type == SDL_EVENT_WINDOW_CLOSE_REQUESTED &&
                event.window.windowID == SDL_GetWindowID(window)) {
                done = true;
            }
        }
        if ((SDL_GetWindowFlags(window) & SDL_WINDOW_MINIMIZED) != 0) {
            SDL_Delay(10);
            continue;
        }

        ImGui_ImplOpenGL3_NewFrame();
        ImGui_ImplSDL3_NewFrame();
        ImGui::NewFrame();
        if (ImGui::BeginMainMenuBar()) {
            if (ImGui::BeginMenu("View")) {
                ImGui::MenuItem("Debug", nullptr, &show_debug);
                ImGui::MenuItem("ImGui Demo", nullptr, &show_imgui_demo);
                ImGui::EndMenu();
            }
            ImGui::EndMainMenuBar();
        }
        ImGui::DockSpaceOverViewport(0, ImGui::GetMainViewport(),
                                     ImGuiDockNodeFlags_PassthruCentralNode);

        // What the pointer was over last frame becomes the hover for this
        // one, and the rows follow it, before anything is drawn. A window
        // finds out what the pointer is over *while* it draws -- too late for
        // itself, and far too late for the windows drawn before it -- so a
        // hover that took effect in the frame that found it would recolour
        // nothing and describe nothing. One frame's lag buys every window in a
        // frame the same answer.
        v.hover = v.pending;
        v.pending = {};

        // The histogram first, because a click in it picks a request *this*
        // frame -- and then the rows and the time range, so that everything
        // after them is looking at the request that was just picked rather
        // than at the one before it.
        draw_queries_window(d, v, hist, rect);
        follow_selection(d, v);
        // After it, so that a request picked this frame gets the axis and the
        // keys act on what was actually on screen last frame.
        apply_keys(v);

        draw_selected_query(d, v);
        draw_plot_window(d, v);
        draw_log_window(d, v);
        draw_nodes_window(d);
        draw_debug_window(v, &show_debug);
        if (show_imgui_demo) {
            ImGui::ShowDemoWindow(&show_imgui_demo);
        }

        ImGui::Render();
        glViewport(0, 0, int(ImGui::GetIO().DisplaySize.x), int(ImGui::GetIO().DisplaySize.y));
        glClearColor(0.10f, 0.10f, 0.12f, 1.0f);
        glClear(GL_COLOR_BUFFER_BIT);
        ImGui_ImplOpenGL3_RenderDrawData(ImGui::GetDrawData());
        SDL_GL_SwapWindow(window);
    }

    ImGui_ImplOpenGL3_Shutdown();
    ImGui_ImplSDL3_Shutdown();
    ImPlot::DestroyContext();
    ImGui::DestroyContext();
    SDL_GL_DestroyContext(gl);
    SDL_DestroyWindow(window);
    SDL_Quit();
    return 0;
}

int main(int argc, char** argv) {
    try {
        return run(argc, argv);
    } catch (const std::exception& e) {
        fprintf(stderr, "%s\n", e.what());
        return 1;
    }
}
