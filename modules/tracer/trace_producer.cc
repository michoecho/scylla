// The binary the build takes a demo trace from, and the objects to read it
// against.
//
// Both jobs have to live in one executable: a trace only means anything beside
// the tracepoint tables of the binary that wrote it, and that table is this
// file's TRACEPOINT() call sites. Run it two ways --
//
//     trace_producer --emit-trace FILE     a trace of the demo workload
//     trace_producer --emit-dsos DIR       the objects that trace is read
//                                          against, by build ID
//
// -- and see modules/tracer/BUCK for how those become build steps.
//
// It is linked against a shared library that has tracepoints of its own, so both
// jobs cover the case that motivates the metadata stream in a trace: records
// from two objects, each with a `tracepoints` section of its own, and neither
// decodable from an address alone.

#include <cstdint>
#include <cstring>
#include <exception>
#include <fstream>
#include <iostream>
#include <span>
#include <string_view>
#include <vector>

// Before tracer.h, so its TRACER_TIMESTAMP wins the #ifndef. rdtsc would make
// every byte of the trace -- and so the snapshot of the decoded output --
// different on every run. The clock lives in the plugin, so that the two objects
// share one counter rather than each starting from zero; see demo_clock.h.
#include "tracer_demo/common_tracepoints.h"
#include "tracer_demo/demo_clock.h"
#include "tracer_demo/trace_plugin.h"

#include "source_location/source_location.h"
#include "tracer/tracer.h"

namespace {

using tracer::event_level;

std::span<const std::byte> as_bytes(std::string_view s) {
    return {reinterpret_cast<const std::byte*>(s.data()), s.size()};
}

// A function that records where it was called from, which is the shape a source
// location exists for: `open_table` says nothing about its call site and the
// tracepoint reports it anyway.
void open_table(std::string_view name, srcloc::location from = {}) {
    TRACEPOINT(event_level::info, "table_opened", "name", name, "opened_at", from);
}

// A workload chosen to cover every wire type there is: the integer
// widths, bool, a string, a length-prefixed byte span, a pointer, a source
// location, and no parameters at all.
void run_demo() {
    TRACEPOINT(event_level::info, "listening", "port", static_cast<std::uint16_t>(8080));

    for (std::uint32_t i = 0; i < 3; ++i) {
        TRACEPOINT(event_level::debug, "accepted_connection", "conn", i, "keepalive", i % 2 == 0);
        // The one in the loop, and so the one worth shortening: a static id
        // puts it on the wire in one byte rather than eight. See "static ids"
        // in tracer.h.
        TRACEPOINT_STATIC_ID(demo::request_header_id, event_level::debug, "request_header",
                             "method", "GET", "path", i == 1 ? "/index.html" : "/");
    }

    TRACEPOINT(event_level::info, "cache_miss", "key", as_bytes("session"), "slot",
               // A fixed value rather than a real address: under ASLR a real
               // one would differ between the two runs of this binary and the
               // decoded output would not be snapshottable.
               reinterpret_cast<const void*>(static_cast<std::uintptr_t>(0xdeadbeef)));

    TRACEPOINT(event_level::info, "clock_skew", "nanoseconds", static_cast<std::int64_t>(-4200),
               "retries", static_cast<std::uint8_t>(3));

    // The library's tracepoints, and -- through the header both objects include
    // -- a second copy of "shared_event" beside the one just below.
    demo::run_plugin_workload(2);
    demo::trace_shared_event(99);

    // Two call sites, so the decoded trace shows two different places -- and one
    // location that was never captured, which decodes as the nothing it is
    // rather than as an address.
    open_table("users");
    open_table("sessions");
    TRACEPOINT(event_level::info, "table_opened", "name", "anonymous", "opened_at",
               srcloc::location::none());

    // A snapshot: one record to open it, a row per table, and one to close it.
    // The rows go down back to back in a loop, between the same two moments,
    // and a timestamp on each would be a clock read and a vint spent saying
    // what the record before it already said -- so they are written with
    // TRACEPOINT_STATIC_ID_UNTIMED(), which is the smallest a record gets: one
    // byte of id and the arguments.
    //
    // What a consumer gets back is each row carrying the moment of the record
    // before it, and metadata saying that the moment is not the row's own. See
    // timestamp_encoding in tracer.h.
    TRACEPOINT(event_level::info, "table_snapshot_begin", "tables",
               static_cast<std::uint32_t>(3));
    for (std::uint32_t i = 0; i < 3; ++i) {
        TRACEPOINT_STATIC_ID_UNTIMED(demo::table_snapshot_row_id, event_level::info,
                                     "table_snapshot_row", "table",
                                     i == 0 ? "users" : (i == 1 ? "sessions" : "anonymous"),
                                     "rows", static_cast<std::uint32_t>(10 * (i + 1)));
    }
    // Untimed and identified by its entry, so the trace holds one of each shape.
    // It is also the last record of its ring, which is the case a consumer
    // interpolating times for a run of these has to have an answer for: there
    // is no timed record after it to interpolate towards.
    TRACEPOINT_UNTIMED(event_level::info, "table_snapshot_end");

    TRACEPOINT(event_level::info, "shutting_down");
}

int emit_trace(const char* path) {
    // Small rings: the demo writes a few hundred bytes, and the default 68 MiB
    // budget would only slow the build down.
    // Constructing it writes the metadata prologue -- the two objects this
    // binary is, as load events -- into its own metadata ring, and a clock sync
    // into each of the others.
    //
    // 192-byte buffers, which is smaller than the workload: the info ring
    // rotates part way through it, so the trace carries a clock sync record
    // written by a rotation as well as the two the constructor wrote. The
    // capacity is far larger, so nothing is evicted and the trace is still the
    // whole workload.
    tracer::trace_buffers buffers(64 * 1024, 64 * 1024, 4096, 192);
    tracer::local_tracer = &buffers;
    // Tracepoints are nops until their keys are flipped, so a run that turned
    // nothing on would write an empty trace. The demo wants all of them; a real
    // program would name the ones it wants with set_tracepoint_enabled().
    tracer::set_all_tracepoints_enabled(true);
    run_demo();
    tracer::set_all_tracepoints_enabled(false);
    tracer::local_tracer = nullptr;

    std::ofstream out(path, std::ios::binary);
    if (!out) {
        std::cerr << "cannot write " << path << "\n";
        return 1;
    }
    // Every ring as a chunk of its own. Which ring a record was in does not
    // survive -- the decoder merges them back into one timestamp order -- but
    // where each one ends does, because the merge has to know how far each
    // stream runs.
    const std::vector<std::byte> trace = tracer::collect_trace(buffers);
    out.write(reinterpret_cast<const char*>(trace.data()),
              static_cast<std::streamsize>(trace.size()));
    return out ? 0 : 1;
}

}  // namespace

int main(int argc, char** argv) {
    const std::string_view mode = argc > 1 ? argv[1] : "";

    if (mode == "--emit-trace" && argc == 3) {
        return emit_trace(argv[2]);
    }
    if (mode == "--emit-dsos" && argc == 3) {
        // This binary and the library it is linked against, filed by build ID.
        // Their tracepoint tables are what says what a record means, and a
        // source location is an address inside one of them, so neither can be
        // read back without the files themselves.
        try {
            tracer::write_dso_directory(argv[2]);
        } catch (const std::exception& e) {
            std::cerr << "cannot collect the objects: " << e.what() << "\n";
            return 1;
        }
        return 0;
    }

    std::cerr << "usage: " << argv[0]
              << " (--emit-trace FILE | --emit-dsos DIR)\n";
    return 2;
}
