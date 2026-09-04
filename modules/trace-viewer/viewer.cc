// A latency trace viewer, rebuilt around its tables.
//
// The old viewer (main.cc beside this) grew as a chain of logic: a step that
// could only run after another step, reaching into globals that another step
// had filled in. This one is the same job written the other way round. It is a
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
//                                                 locations
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
//   pass_query_statement  prep_runs + queries  -> query.statement
//   pass_render       every event table        -> log_lines, slices: the text
//                                                 and the rectangles, for the
//                                                 whole trace, once
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
#include <chrono>
#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <fstream>
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

#include "decoder.h"

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
    uint64_t task = 0; \
    int32_t query = none

struct switch_row {
    ROW_COMMON;
    uint64_t prev = 0;    // the task that was running until now
    uint32_t loc = 0;     // trace::locations, where `task` was created
    uint8_t cause = 0;    // switch_cause
    int32_t rpc = none;   // for sw_rpc_handled: the rpc row that opened it
    int32_t group = none; // scheduling group, filled by pass_sched_group
};

// The reactor gave the cpu to one task queue, or took it back. A run is a
// begin and the end that follows it, and every switch between them ran under
// the begin's scheduling group -- which is what pass_sched_group makes of it.
struct tq_run_row {
    ROW_COMMON;
    int32_t group = none;  // the scheduling group id, on a tq_begin
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
struct timeline_row {
    int64_t ts = 0;
    uint16_t table = 0;
    uint32_t index = 0;
};

// A rectangle. Which kind it is comes from the table it was drawn from, and
// which colour it takes comes from `query` against the selection, so the same
// row serves a request that is selected and one that is not.
struct slice_row {
    double t0 = 0;  // milliseconds from the start of the trace
    double t1 = 0;
    int32_t query = none;
    uint16_t table = 0;  // tab_switch: on the cpu. tab_io_begin: in an I/O.
    uint32_t index = 0;
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

    // Built by pass_index: (task, row) pairs sorted by task, for the three
    // tables a query walk has to ask "what did task T do here" of. A sorted
    // array rather than a hash map because it is built once, read many times,
    // and equal_range over it is two cache lines.
    std::vector<std::pair<uint64_t, uint32_t>> switch_by_task;
    std::vector<std::pair<uint64_t, uint32_t>> io_by_task;
    std::vector<std::pair<uint64_t, uint32_t>> rpc_by_task;

    // Built by pass_query_rows: which query each task on this cpu belongs to.
    std::unordered_map<uint64_t, int32_t> query_of_task;

    // Built by pass_render: this reactor's whole trace, drawn. One log line
    // per timeline entry, in this table's own arena, and every rectangle of
    // its timeline sorted by where it starts. slice_reach is a running maximum
    // of the slices' ends, which is what makes culling to the visible x range
    // a binary search.
    std::vector<str> log_lines;
    arena log_text;
    std::vector<slice_row> slices;
    std::vector<double> slice_reach;
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
    uint64_t task = 0;
    int32_t query = none;
};

struct query_row {
    int64_t t0 = 0;  // the cql_request record: when the frame arrived
    int64_t t1 = 0;  // the last record of any of its parts
    uint32_t root_cpu = 0;
    uint64_t root_task = 0;
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
//  5. pass_decode -- files -> the event tables, syncs, locations
// ============================================================================
//
// One file is one (node, shard, level), and it carries its own metadata stream
// saying where that process's objects were mapped, so it decodes on its own.
// Every record it holds is appended to the table for its kind on its cpu, and
// an entry naming that row is appended to the cpu's timeline. Nothing is
// sorted here -- see pass_order.
//
// Records the viewer has no table for are dropped by the catch-all at the
// bottom. Stack samples are among them, deliberately.

struct decode_sink {
    trace_data& d;
    cpu_tables& t;
    uint32_t cpu;
    uint32_t node;
    std::unordered_map<uint64_t, uint32_t>& interned;

    // Append a row, and the timeline entry that points at it. Every event in
    // the trace goes through here, which is what makes the timeline complete
    // by construction rather than by a pass that has to be remembered.
    template <typename Row>
    void push(table_id which, std::vector<Row>& into, Row row) const {
        row.ts = int64_t(row.ts);
        t.timeline.push_back({row.ts, uint16_t(which), uint32_t(into.size())});
        into.push_back(row);
    }

    // A source location is one address, and the same call site turns up
    // thousands of times -- every continuation off one `then()` -- so the rows
    // hold an index and the strings are stored once.
    uint32_t intern(const trace::source_location& loc) const {
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

    void switch_to(uint8_t cause, uint64_t prev, uint64_t task, uint64_t ts, uint32_t loc) const {
        switch_row r;
        r.ts = int64_t(ts);
        r.task = task;
        r.prev = prev;
        r.cause = cause;
        r.loc = loc;
        push(tab_switch, t.switches, r);
    }

    void operator()(const trace::run_task& e, const trace::tracepoint_metadata& m) const {
        switch_to(sw_run_task, e.prev, e.task, m.timestamp, intern(e.at));
    }
    void operator()(const trace::cql_request& e, const trace::tracepoint_metadata& m) const {
        switch_to(sw_cql_request, e.prev, e.task, m.timestamp, 0);
    }
    void operator()(const trace::semaphore_execute& e, const trace::tracepoint_metadata& m) const {
        switch_to(sw_semaphore, e.prev, e.task, m.timestamp, 0);
    }
    void operator()(const trace::execution_stage& e, const trace::tracepoint_metadata& m) const {
        switch_to(sw_execution_stage, e.prev, e.task, m.timestamp, 0);
    }
    // An inbound request opens a task chain on this shard, which is a switch in
    // exactly the sense the four above are -- and it is also the far end of a
    // message, which is what joins two nodes. So it becomes two rows: the
    // switch the timeline and the plot see, and an rpc row carrying the
    // (connection, sequence) that pass_rpc_pair joins on. The rpc row is the
    // one exception to "every row has a timeline entry": the switch is what
    // the log shows, and two lines for one record would be a lie.
    void operator()(const trace::rpc_request_handled& e,
                    const trace::tracepoint_metadata& m) const {
        rpc_row r;
        r.ts = int64_t(m.timestamp);
        r.task = e.task;
        r.connection = e.connection;
        r.sequence = e.sequence;
        r.kind = rpc_handled;
        const auto rpc_index = int32_t(t.rpcs.size());
        t.rpcs.push_back(r);

        switch_row s;
        s.ts = int64_t(m.timestamp);
        s.task = e.task;
        s.prev = e.prev;
        s.cause = sw_rpc_handled;
        s.rpc = rpc_index;
        push(tab_switch, t.switches, s);
    }

    void tq_run(uint8_t kind, int32_t group, uint64_t ts) const {
        tq_run_row r;
        r.ts = int64_t(ts);
        r.kind = kind;
        r.group = group;
        push(tab_tq_run, t.tq_runs, r);
    }
    void operator()(const trace::task_queue_run_begin& e,
                    const trace::tracepoint_metadata& m) const {
        tq_run(tq_begin, int32_t(e.scheduling_group), m.timestamp);
    }
    void operator()(const trace::task_queue_run_end&,
                    const trace::tracepoint_metadata& m) const {
        tq_run(tq_end, none, m.timestamp);
    }

    void operator()(const trace::io_begin& e, const trace::tracepoint_metadata& m) const {
        io_begin_row r;
        r.ts = int64_t(m.timestamp);
        r.task = e.task;
        r.io = e.io;
        push(tab_io_begin, t.io_begins, r);
    }
    void operator()(const trace::io_end& e, const trace::tracepoint_metadata& m) const {
        io_end_row r;
        r.ts = int64_t(m.timestamp);
        r.task = e.task;
        r.io = e.io;
        push(tab_io_end, t.io_ends, r);
    }

    void operator()(const trace::prepared_query_run& e,
                    const trace::tracepoint_metadata& m) const {
        prep_run_row r;
        r.ts = int64_t(m.timestamp);
        r.id = d.strings.put(e.id);
        push(tab_prep_run, t.prep_runs, r);
    }
    void delta(uint8_t kind, std::string_view keyspace, std::string_view statement,
               std::span<const std::byte> id, uint64_t ts) const {
        prep_delta_row r;
        r.ts = int64_t(ts);
        r.kind = kind;
        r.id = d.strings.put(id);
        r.keyspace = d.strings.put(keyspace);
        r.statement = d.strings.put(statement);
        push(tab_prep_delta, t.prep_deltas, r);
    }
    void operator()(const trace::prepared_statement_added& e,
                    const trace::tracepoint_metadata& m) const {
        delta(prep_added, e.keyspace, e.statement, e.id, m.timestamp);
    }
    void operator()(const trace::prepared_statement_removed& e,
                    const trace::tracepoint_metadata& m) const {
        delta(prep_removed, e.keyspace, e.statement, e.id, m.timestamp);
    }
    void operator()(const trace::prepared_statement_snapshot_entry& e,
                    const trace::tracepoint_metadata& m) const {
        delta(prep_snapshot, e.keyspace, e.statement, e.id, m.timestamp);
    }

    void connection(uint8_t kind, uint64_t id, std::string_view local, std::string_view remote,
                    uint64_t msb, uint64_t lsb, uint32_t peer_shard, uint64_t ts) const {
        conn_row r;
        r.ts = int64_t(ts);
        r.kind = kind;
        r.connection = id;
        r.local = d.strings.put(local);
        r.remote = d.strings.put(remote);
        r.peer_boot_msb = msb;
        r.peer_boot_lsb = lsb;
        r.peer_shard = peer_shard;
        push(tab_conn, t.conns, r);
    }
    void operator()(const trace::rpc_connection_open& e,
                    const trace::tracepoint_metadata& m) const {
        connection(conn_open, e.connection, e.local, e.remote, e.peer_boot_msb, e.peer_boot_lsb,
                   e.peer_shard, m.timestamp);
    }
    void operator()(const trace::rpc_connection_close& e,
                    const trace::tracepoint_metadata& m) const {
        connection(conn_close, e.connection, {}, {}, e.peer_boot_msb, e.peer_boot_lsb,
                   e.peer_shard, m.timestamp);
    }
    void operator()(const trace::rpc_connection_snapshot_entry& e,
                    const trace::tracepoint_metadata& m) const {
        connection(conn_snapshot, e.connection, e.local, e.remote, e.peer_boot_msb,
                   e.peer_boot_lsb, e.peer_shard, m.timestamp);
    }

    void message(uint8_t kind, uint64_t conn, uint64_t seq, int64_t msg_id, uint64_t task,
                 uint64_t ts) const {
        rpc_row r;
        r.ts = int64_t(ts);
        r.task = task;
        r.connection = conn;
        r.sequence = seq;
        r.msg_id = msg_id;
        r.kind = kind;
        push(tab_rpc, t.rpcs, r);
    }
    // The task on a send is the one that *queued* the buffer, carried by the
    // record: the connection's send loop is what actually writes it, and
    // asking what was running on the shard would answer "the connection" and
    // pull every unrelated request into the walk.
    void operator()(const trace::rpc_message_sent& e, const trace::tracepoint_metadata& m) const {
        message(rpc_sent, e.connection, e.sequence, 0, e.task, m.timestamp);
    }
    void operator()(const trace::rpc_message_received& e,
                    const trace::tracepoint_metadata& m) const {
        message(rpc_received, e.connection, e.sequence, 0, 0, m.timestamp);
    }
    void operator()(const trace::rpc_reply_sent& e, const trace::tracepoint_metadata& m) const {
        message(rpc_reply_sent, e.connection, e.sequence, e.msg_id, e.task, m.timestamp);
    }
    void operator()(const trace::rpc_reply_received& e,
                    const trace::tracepoint_metadata& m) const {
        message(rpc_reply_received, e.connection, e.sequence, e.msg_id, 0, m.timestamp);
    }

    // Not an event of the program's own: it is how pass_retime dates the rest.
    void operator()(const trace::clock_sync& e, const trace::tracepoint_metadata& m) const {
        d.syncs[node].push_back({int64_t(m.timestamp), e.realtime_ns, e.ticks_per_second});
    }

    template <typename Event>
    void operator()(const Event&, const trace::tracepoint_metadata&) const {}
};

void pass_decode(trace_data& d, trace::dso_directory& dsos) {
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
        // failure worth expecting is a decoder.h that does not match these
        // traces: see "regenerating decoder.h" in the README.
        try {
            trace::decode(bytes,
                          decode_sink{d, t, uint32_t(f.cpu), uint32_t(f.node), interned}, dsos);
        } catch (const std::exception& e) {
            fmt::print("{}: {}\n", f.path.filename().string(), e.what());
        }
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
// and sorted if it needs it, and the timeline entries pointing into it are
// remapped.
//
// In practice a tracepoint is written at one level, so the check passes and
// this pass is a scan. It is here for the trace where it does not.

template <typename Row>
bool sort_table(std::vector<Row>& rows, std::vector<uint32_t>& old_to_new) {
    if (std::ranges::is_sorted(rows, {}, &Row::ts)) {
        return false;
    }
    std::vector<uint32_t> order(rows.size());
    for (uint32_t i = 0; i < order.size(); ++i) {
        order[i] = i;
    }
    std::ranges::stable_sort(order, {}, [&](uint32_t i) { return rows[i].ts; });

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
        // Checked first, like the tables above, and for the same reason: this
        // pass runs twice, and the second time -- after pass_retime -- the
        // timeline is already in order unless a node's clock went backwards.
        // Sorting it anyway was the one part of this pass that was not a scan,
        // and on a million-entry timeline it was most of the pass's cost.
        if (!std::ranges::is_sorted(t.timeline, {}, &timeline_row::ts)) {
            std::ranges::stable_sort(t.timeline, {}, &timeline_row::ts);
        }
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
// a linear scan of a shard's switches would make the walk quadratic. Three
// sorted (task, row) arrays answer it with an equal_range.

void pass_index(trace_data& d) {
    for (cpu_tables& t : d.tables) {
        t.switch_by_task.clear();
        t.switch_by_task.reserve(t.switches.size());
        for (uint32_t i = 0; i < t.switches.size(); ++i) {
            t.switch_by_task.emplace_back(t.switches[i].task, i);
        }
        t.io_by_task.clear();
        t.io_by_task.reserve(t.io_begins.size());
        for (uint32_t i = 0; i < t.io_begins.size(); ++i) {
            t.io_by_task.emplace_back(t.io_begins[i].task, i);
        }
        t.rpc_by_task.clear();
        t.rpc_by_task.reserve(t.rpcs.size());
        for (uint32_t i = 0; i < t.rpcs.size(); ++i) {
            t.rpc_by_task.emplace_back(t.rpcs[i].task, i);
        }
        // By task, and by row within a task, so a range is also in time order.
        std::ranges::sort(t.switch_by_task);
        std::ranges::sort(t.io_by_task);
        std::ranges::sort(t.rpc_by_task);
    }
}

// The rows of `index` whose task is `task`, as a [first, last) pair of
// iterators. The values are row numbers into whichever table the index was
// built from.
inline auto rows_of_task(const std::vector<std::pair<uint64_t, uint32_t>>& index, uint64_t task) {
    return std::ranges::equal_range(index, task, {},
                                    &std::pair<uint64_t, uint32_t>::first);
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
//  10. pass_sched_group -- tq_runs + switches -> switch.group
// ============================================================================
//
// A run_task record does not say which scheduling group ran it, and making it
// carry one would be a field on the hottest record in the trace for something
// that changes a few times a millisecond. What says it instead is the bracket
// around it: the reactor gives the cpu to one task queue, runs whatever that
// queue holds, and takes it back, so every switch between a tq_begin and the
// tq_end after it ran under that begin's group.
//
// The walk is over the *timeline* rather than over the two tables in parallel,
// and that is the reason to prefer it: two records of one shard can share an
// rdtsc tick -- a begin and the first run_task under it routinely do -- and the
// timeline is the one place their order survives, because it is stable-sorted
// and so still in the order the ring holds them.
//
// A switch outside any run keeps `none`. That is not a failure to explain: the
// debug ring evicts, and the oldest surviving records of a shard are the tail
// of a run whose begin is gone.

void pass_sched_group(trace_data& d) {
    size_t runs = 0, attributed = 0, orphaned = 0;
    for (cpu_tables& t : d.tables) {
        int32_t group = none;
        for (const timeline_row& e : t.timeline) {
            if (e.table == tab_tq_run) {
                const tq_run_row& r = t.tq_runs[e.index];
                group = r.kind == tq_begin ? r.group : none;
                runs += r.kind == tq_begin ? 1 : 0;
            } else if (e.table == tab_switch) {
                t.switches[e.index].group = group;
                ++(group >= 0 ? attributed : orphaned);
            }
        }
    }
    fmt::print("{} task queue runs: {} switches in one, {} before the first\n", runs,
               attributed, orphaned);
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
    std::map<std::pair<uint32_t, uint64_t>, int32_t> claimed;  // (cpu, task) -> query
    std::vector<std::pair<uint32_t, uint64_t>> frontier;

    // Seeds, in time order across the whole trace, so that query indices read
    // in the order the requests arrived.
    struct seed {
        int64_t ts;
        uint32_t cpu;
        uint64_t task;
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
        const auto reach = [&](uint32_t cpu, uint64_t task) {
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

    // What a task holds the cpu for is the stretch to the next record on the
    // same shard: there is no "task ended" tracepoint, so a stretch where the
    // reactor ran nothing else is counted to the last task it picked up. Two
    // things cut it back -- the query's own end, because a request cannot be
    // on the cpu after its last record, and its I/O, because a shard waiting
    // for a disk is not running even when nothing else is.
    for (const cpu_tables& t : d.tables) {
        const int64_t cpu_end = t.timeline.empty() ? 0 : t.timeline.back().ts;
        for (uint32_t i = 0; i < t.switches.size(); ++i) {
            const switch_row& s = t.switches[i];
            if (s.query < 0) {
                continue;
            }
            const int64_t from = s.ts;
            const int64_t to =
                std::min(i + 1 < t.switches.size() ? t.switches[i + 1].ts : cpu_end,
                         d.queries[s.query].t1);
            if (to <= from) {
                continue;
            }
            int64_t on_cpu = to - from;
            for (const auto& [ignored, at] : rows_of_task(t.io_by_task, s.task)) {
                const io_begin_row& b = t.io_begins[at];
                const int64_t b_end = b.end >= 0 ? t.io_ends[b.end].ts : to;
                on_cpu -= std::max<int64_t>(0, std::min(to, b_end) - std::max(from, b.ts));
            }
            d.queries[s.query].cpu_ticks += std::max<int64_t>(0, on_cpu);
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
//  18. pass_query_statement -- prep_runs + queries -> query.statement
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
//  19. rendering a record as text
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
            std::string line = fmt::format("{:<7} task {:016x} from {:016x}",
                                           switch_cause_name(r.cause), r.task, r.prev);
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
            std::string line = fmt::format("{:<7} task {:016x} io {:016x}", "IO-BEGIN", r.task, r.io);
            if (r.end >= 0) {
                fmt::format_to(std::back_inserter(line), "  {:.3f} ms",
                               d.seconds(t.io_ends[r.end].ts - r.ts) * 1e3);
            } else {
                line += "  (never completed in this trace)";
            }
            return line;
        }
        case tab_io_end: {
            const io_end_row& r = t.io_ends[index];
            return fmt::format("{:<7} task {:016x} io {:016x}", "IO-END", r.task, r.io);
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
            std::string line = fmt::format("{:<7} conn {} seq {} task {:016x}",
                                           rpc_kind_name(r.kind), r.connection, r.sequence, r.task);
            if (r.peer_row >= 0) {
                const rpc_row& far = d.tables[r.peer_cpu].rpcs[r.peer_row];
                fmt::format_to(std::back_inserter(line), "  <-> {} task {:016x} ({:+.3f} ms)",
                               d.cpus[r.peer_cpu].label, far.task,
                               d.seconds(far.ts - r.ts) * 1e3);
            }
            return line;
        }
        default:
            return "?";
    }
}

// The task a row is about, whichever table it is in. Used by the log's
// highlight and by the plot's tooltip.
uint64_t task_of(const trace_data& d, uint32_t cpu, uint16_t table, uint32_t index) {
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
//  20. pass_render -- the event tables -> the text and the rectangles
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

        // A stretch on the cpu is bounded by the next record of any kind on
        // the same shard -- there is no "task ended" tracepoint -- and is cut
        // back by whatever of that task's own I/O fell inside it, because a
        // shard waiting for a disk is not running even when nothing else is.
        // The cut is the same one pass_cost makes, so the picture and the
        // number agree.
        t.slices.clear();
        const int64_t cpu_end = t.timeline.empty() ? 0 : t.timeline.back().ts;
        const auto emit = [&](int64_t from, int64_t to, int32_t query, uint16_t table,
                              uint32_t index) {
            if (to > from) {
                t.slices.push_back({d.ms(from - d.origin), d.ms(to - d.origin), query, table,
                                    index});
            }
        };
        std::vector<std::pair<int64_t, int64_t>> waits;
        for (uint32_t i = 0; i < t.switches.size(); ++i) {
            const switch_row& sw = t.switches[i];
            const int64_t from = sw.ts;
            const int64_t to = i + 1 < t.switches.size() ? t.switches[i + 1].ts : cpu_end;
            if (to <= from) {
                continue;
            }
            waits.clear();
            for (const auto& [ignored, at] : rows_of_task(t.io_by_task, sw.task)) {
                const io_begin_row& b = t.io_begins[at];
                const int64_t b_end = b.end >= 0 ? t.io_ends[b.end].ts : to;
                const int64_t lo = std::max(from, b.ts);
                const int64_t hi = std::min(to, b_end);
                if (lo < hi) {
                    waits.emplace_back(lo, hi);
                }
            }
            std::ranges::sort(waits);
            int64_t at = from;
            for (const auto& [lo, hi] : waits) {
                emit(at, lo, sw.query, tab_switch, i);
                at = std::max(at, hi);
            }
            emit(at, to, sw.query, tab_switch, i);
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
//  21. the view -- which part of all that is on screen
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

    // The plot's geometry, kept from the frame that drew it: the row
    // checkboxes are put beside the rows they belong to, and the widget is
    // sized so that a row is the same height whatever the row count.
    float plot_top = 0;
    float plot_left = 0;
    float row_height = 0;
    float plot_chrome = 40;  // what ImPlot spends on axes and padding
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

// The slices of one cpu that can be seen between two milliseconds. The first
// one that can reach into view is found by binary search over the running
// maximum of their ends; from there they are scanned until they start past the
// right edge.
std::span<const slice_row> slices_in(const cpu_tables& t, double from, double to) {
    const auto first = std::ranges::lower_bound(t.slice_reach, from);
    const auto begin = t.slices.begin() + (first - t.slice_reach.begin());
    const auto end = std::upper_bound(begin, t.slices.end(), to,
                                      [](double x, const slice_row& s) { return x < s.t0; });
    return {begin, end};
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

// Where a request is looked at from before anything else is picked: its first
// reactor, at the record its own frame arrived on.
selection selection_of_query(const trace_data& d, int32_t query) {
    selection out;
    if (query < 0 || query >= int32_t(d.queries.size())) {
        return out;
    }
    out.query = query;
    const query_row& q = d.queries[query];
    for (uint32_t p = q.parts_begin; p < q.parts_end; ++p) {
        const uint32_t cpu = d.parts[p].cpu;
        if (out.log_cpu < 0 ||
            std::pair{d.cpus[cpu].node, d.cpus[cpu].shard} <
                std::pair{d.cpus[out.log_cpu].node, d.cpus[out.log_cpu].shard}) {
            out.log_cpu = int32_t(cpu);
        }
    }
    if (out.log_cpu >= 0) {
        out.focus = owning_line(d, uint32_t(out.log_cpu), q.t0);
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
    const int32_t primary = preview ? none : v.clicked.query;
    const int32_t secondary = v.hover.query;

    if (primary != v.rows_primary || secondary != v.rows_secondary || v.pins != v.rows_pins) {
        v.rows_primary = primary;
        v.rows_secondary = secondary;
        v.rows_pins = v.pins;
        build_rows(d, v);
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

// ============================================================================
//  22. the latency histogram
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

// The aggregate over a range of quantiles: what the DragRect selects. Two
// numbers per query, so two distributions, and they are drawn as CDFs over
// exactly the queries between the two quantile bounds.
struct aggregate {
    size_t count = 0;
    double latency_mean = 0;
    double cpu_mean = 0;
    std::vector<double> fraction;  // 0..1, the x of the two CDFs
    std::vector<double> latency;   // seconds
    std::vector<double> cpu;
};

aggregate build_aggregate(const trace_data& d, size_t from, size_t to) {
    aggregate a;
    if (from > to) {
        std::swap(from, to);
    }
    to = std::min(to + 1, d.by_latency.size());
    a.count = to - from;
    if (a.count == 0) {
        return a;
    }
    std::vector<double> cpu;
    cpu.reserve(a.count);
    for (size_t i = from; i < to; ++i) {
        const query_row& q = d.queries[d.by_latency[i]];
        a.latency_mean += d.seconds(q.t1 - q.t0);
        cpu.push_back(d.seconds(q.cpu_ticks));
        a.cpu_mean += cpu.back();
    }
    a.latency_mean /= double(a.count);
    a.cpu_mean /= double(a.count);
    // by_latency is already sorted by latency; cpu time is not.
    std::ranges::sort(cpu);
    constexpr int steps = 256;
    for (int i = 0; i < steps; ++i) {
        const double f = double(i) / double(steps - 1);
        const auto at = std::min(size_t(f * double(a.count - 1)), a.count - 1);
        a.fraction.push_back(f);
        a.latency.push_back(d.seconds(d.queries[d.by_latency[from + at]].t1 -
                                      d.queries[d.by_latency[from + at]].t0));
        a.cpu.push_back(cpu[at]);
    }
    return a;
}

// ============================================================================
//  23. the windows
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
    if (a.count > 0 && ImPlot::BeginPlot("distribution over the selection", ImVec2(-1, 200))) {
        ImPlot::SetupAxes("fraction of the selection", "seconds",
                          ImPlotAxisFlags_AutoFit, ImPlotAxisFlags_AutoFit);
        ImPlot::PlotLine("latency", a.fraction.data(), a.latency.data(), int(a.fraction.size()));
        ImPlot::PlotLine("cpu time", a.fraction.data(), a.cpu.data(), int(a.fraction.size()));
        ImPlot::EndPlot();
    }
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
    ImGui::Text("query %d, task %016" PRIx64 " on %s", query, q.root_task,
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
        }
        ImPlot::SetupAxisLimits(ImAxis_Y1, 0, double(v.rows.size()), ImPlotCond_Always);
        ImPlot::PushPlotClipRect();
        ImDrawList* draw = ImPlot::GetPlotDrawList();

        const ImPlotRect limits = ImPlot::GetPlotLimits();
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

        for (size_t row = 0; row < v.rows.size(); ++row) {
            const cpu_tables& t = d.tables[v.rows[row]];
            const std::span<const slice_row> visible =
                slices_in(t, limits.X.Min, limits.X.Max);
            for (int layer = 0; layer < 2; ++layer) {
                for (const slice_row& s : visible) {
                    if (layer_of(s.table) != layer) {
                        continue;
                    }
                    const ImU32 colour =
                        colour_of(s.table, s.query, v.clicked.query, v.hover.query);
                    const band at = band_of(s.table);
                    ImVec2 a = ImPlot::PlotToPixels(ImPlotPoint{s.t0, double(row) + at.top});
                    ImVec2 b = ImPlot::PlotToPixels(ImPlotPoint{s.t1, double(row) + at.bottom});
                    if (b.x - a.x < 1.0f) {
                        b.x = a.x + 1.0f;  // a slice thinner than a pixel is still a slice
                    }
                    draw->AddRectFilled(a, b, colour);
                    // Its own trailing edge, so that a run of stretches on one
                    // reactor reads as a run of them rather than as one block.
                    // Only where there is room for it: at one pixel wide the
                    // bar *is* its edge, and darkening it would turn a dense
                    // stretch of the plot into a dark smear.
                    if (b.x - a.x >= 3.0f) {
                        draw->AddRectFilled(ImVec2{b.x - 1.0f, a.y}, b, darker(colour, 0.45f));
                    }
                    // The topmost bar the pointer is inside, in both axes: an
                    // I/O drawn over a stretch of cpu leaves that stretch
                    // hoverable above and below it.
                    if (hovering && int(std::floor(pt.y)) == int(row) && pt.x >= s.t0 &&
                        pt.x <= s.t1 && pt.y >= double(row) + at.top &&
                        pt.y <= double(row) + at.bottom) {
                        hit = {&s, row};
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
                    ImGui::Text("%s  %.3f ms", d.cpus[v.rows[hit.row]].label.c_str(),
                                s.t1 - s.t0);
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
                                s.query < 0                  ? ", no request"
                                : s.query == v.clicked.query ? ", the picked request"
                                : s.query == v.hover.query   ? ", the request under the pointer"
                                                             : ", another request");
                    ImGui::EndTooltip();
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
    bool scroll = cpu != v.scrolled_cpu || focus != v.scrolled_to;
    if (ImGui::SmallButton("back to the selection")) {
        scroll = true;
    }
    v.scrolled_cpu = cpu;
    v.scrolled_to = focus;
    ImGui::Separator();

    ImGui::BeginChild("##lines", ImVec2(0, 0), false, ImGuiWindowFlags_HorizontalScrollbar);
    ImGuiListClipper clipper;
    clipper.Begin(int(t.timeline.size()));
    if (scroll && focus >= 0) {
        clipper.IncludeItemByIndex(focus);
    }
    while (clipper.Step()) {
        for (int i = clipper.DisplayStart; i < clipper.DisplayEnd; ++i) {
            const timeline_row& e = t.timeline[i];
            const bool focused = i == focus;
            const bool in_query =
                v.query() >= 0 && query_of(d, uint32_t(cpu), e.table, e.index) == v.query();
            ImGui::PushStyleColor(ImGuiCol_Text,
                                  focused    ? IM_COL32(255, 220, 100, 255)
                                  : in_query ? IM_COL32(120, 240, 120, 255)
                                             : IM_COL32(150, 150, 158, 255));
            // Relative to the request, wherever in the trace the line is, so
            // that scrolling away from it reads as a distance from it.
            ImGui::Text("%+10.3f ms  %s", d.ms(e.ts - d.origin) - v.t0,
                        std::string(t.log_text.get(t.log_lines[i])).c_str());
            ImGui::PopStyleColor();
            if (focused && scroll) {
                // From the line itself rather than from an estimate of where
                // it is, which is the one way of getting it right when the
                // clipper means most lines were never laid out.
                ImGui::SetScrollHereY(0.4f);
            }
        }
    }
    ImGui::EndChild();
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
//  24. main
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

    // The objects a source location points into, found by build id. Nothing in
    // the traced process writes them -- an address is read back against the
    // object it is in, and finding that object is the reader's job.
    const std::filesystem::path dso_dir = std::filesystem::path(argv[1]) / "dsos";
    trace::dso_directory dsos =
        std::getenv("TRACE_DSO_DIR") != nullptr || !std::filesystem::exists(dso_dir)
            ? trace::dso_directory()
            : trace::dso_directory(dso_dir.string());

    timing.run("pass_decode", [&] { pass_decode(d, dsos); });
    timing.run("pass_order", [&] { pass_order(d); });
    timing.run("pass_retime", [&] { pass_retime(d); });
    timing.run("pass_order (again)", [&] { pass_order(d); });
    // Before pass_index, because it is what fills in the task of a record that
    // did not carry one -- and the index is keyed on that task.
    timing.run("pass_attribute", [&] { pass_attribute(d); });
    // After the second pass_order, because the walk is over the timeline and
    // relies on it being in the order the rings hold the records.
    timing.run("pass_sched_group", [&] { pass_sched_group(d); });
    timing.run("pass_index", [&] { pass_index(d); });
    timing.run("pass_io_spans", [&] { pass_io_spans(d); });
    timing.run("pass_statements", [&] { pass_statements(d); });
    timing.run("pass_connections", [&] { pass_connections(d); });
    timing.run("pass_rpc_pair", [&] { pass_rpc_pair(d); });
    timing.run("pass_queries", [&] { pass_queries(d); });
    timing.run("pass_query_rows", [&] { pass_query_rows(d); });
    timing.run("pass_cost", [&] { pass_cost(d); });
    timing.run("pass_query_statement", [&] { pass_query_statement(d); });
    timing.run("pass_render", [&] { pass_render(d); });
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
        fmt::print("\nquery {} at quantile {}: task {:016x} on {}, {:.3f} ms, {:.3f} ms of cpu\n",
                   v.query(), at, q.root_task, d.cpus[q.root_cpu].label,
                   d.seconds(q.t1 - q.t0) * 1e3, d.seconds(q.cpu_ticks) * 1e3);
        if (q.statement >= 0) {
            fmt::print("  {} {}\n", d.text(d.statements[q.statement].keyspace),
                       d.text(d.statements[q.statement].text));
        }
        for (uint32_t p = q.parts_begin; p < q.parts_end; ++p) {
            fmt::print("  part {} task {:016x}\n", d.cpus[d.parts[p].cpu].label,
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
                fmt::print("  {:+10.3f} ms  {}\n", d.ms(e.ts - d.origin) - v.t0,
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

        draw_selected_query(d, v);
        draw_plot_window(d, v);
        draw_log_window(d, v);
        draw_nodes_window(d);

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
