// The binary the build generates a decoder from, and takes a trace from.
//
// Both jobs have to live in one executable: a decoder is only valid for the
// tracepoint table of the binary that produced the trace, and that table is
// this file's TRACEPOINT() call sites. Run it two ways --
//
//     trace_producer --emit-decoder        C++ header of the matching decoder
//     trace_producer --emit-trace FILE     a trace of the demo workload
//
// -- and see modules/tracer/BUCK for how those become build steps.

#include <cstdint>
#include <cstring>
#include <exception>
#include <fstream>
#include <iostream>
#include <span>
#include <string_view>
#include <vector>

// Before tracer.h, so it wins the #ifndef. rdtsc would make every byte of the
// trace -- and so the snapshot of the decoded output -- different on every run.
namespace demo {
inline std::uint64_t tick() noexcept {
    static std::uint64_t t = 0;
    t += 100;
    return t;
}
}  // namespace demo
#define TRACER_TIMESTAMP() ::demo::tick()

#include "tracer/codegen.h"
#include "tracer/tracer.h"

namespace {

using tracer::event_level;

std::span<const std::byte> as_bytes(std::string_view s) {
    return {reinterpret_cast<const std::byte*>(s.data()), s.size()};
}

// A workload chosen to cover every wire type the codegen can emit: the integer
// widths, bool, a string, a length-prefixed byte span, a pointer, and no
// parameters at all.
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

    TRACEPOINT(event_level::info, "shutting_down");
}

int emit_trace(const char* path) {
    // Small rings: the demo writes a few hundred bytes, and the default 68 MiB
    // budget would only slow the build down.
    tracer::trace_buffers buffers(64 * 1024, 64 * 1024, 4096);
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
    // Both rings into one stream, info first. Records are self-describing, so
    // the decoder does not need to know where one ring ends.
    for (event_level level : {event_level::info, event_level::debug}) {
        const std::vector<std::byte> bytes = buffers.group(level).collect();
        out.write(reinterpret_cast<const char*>(bytes.data()),
                  static_cast<std::streamsize>(bytes.size()));
    }
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

    std::cerr << "usage: " << argv[0] << " (--emit-decoder | --emit-trace FILE)\n";
    return 2;
}
