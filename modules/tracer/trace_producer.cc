// The binary the build generates a decoder from, and takes a trace from.
//
// Both jobs have to live in one executable: a decoder is only valid for the
// tracepoint table of the binary that produced the trace, and that table is
// this file's TRACEPOINT() call sites. Run it two ways --
//
//     trace_producer --emit-decoder        C++ header of the matching decoder
//     trace_producer --emit-trace FILE     a trace of the demo workload
//     trace_producer --emit-dsos DIR       the objects a decoder needs, by build ID
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
#include "tracer/codegen.h"
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

// A workload chosen to cover every wire type the codegen can emit: the integer
// widths, bool, a string, a length-prefixed byte span, a pointer, a source
// location, and no parameters at all.
void run_demo() {
    TRACEPOINT(event_level::info, "listening", "port", static_cast<std::uint16_t>(8080));

    for (std::uint32_t i = 0; i < 3; ++i) {
        TRACEPOINT(event_level::debug, "accepted_connection", "conn", i, "keepalive", i % 2 == 0);
        TRACEPOINT(event_level::debug, "request_header", "method", "GET", "path",
                   i == 1 ? "/index.html" : "/");
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

    TRACEPOINT(event_level::info, "shutting_down");
}

int emit_trace(const char* path) {
    // Small rings: the demo writes a few hundred bytes, and the default 68 MiB
    // budget would only slow the build down.
    // Constructing it writes the metadata prologue -- the two objects this
    // binary is, as load events -- into its own metadata ring.
    tracer::trace_buffers buffers(64 * 1024, 64 * 1024, 4096, 4096);
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

    if (mode == "--emit-decoder" && argc == 2) {
        // A table this binary cannot describe is a build failure with a
        // diagnostic, not a generated header that fails to compile.
        try {
            std::cout << tracer::generate_decoder_source();
        } catch (const std::exception& e) {
            std::cerr << "cannot generate a decoder: " << e.what() << "\n";
            return 1;
        }
        return 0;
    }
    if (mode == "--emit-trace" && argc == 3) {
        return emit_trace(argv[2]);
    }
    if (mode == "--emit-dsos" && argc == 3) {
        // This binary and the library it is linked against, filed by build ID.
        // A source location is an address inside one of them, so a decoder
        // cannot read one back without the files themselves.
        try {
            tracer::write_dso_directory(argv[2]);
        } catch (const std::exception& e) {
            std::cerr << "cannot collect the objects: " << e.what() << "\n";
            return 1;
        }
        return 0;
    }

    std::cerr << "usage: " << argv[0]
              << " (--emit-decoder | --emit-trace FILE | --emit-dsos DIR)\n";
    return 2;
}
