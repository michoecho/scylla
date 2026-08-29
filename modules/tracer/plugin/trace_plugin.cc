#include "tracer_demo/trace_plugin.h"

#include <cstddef>
#include <cstdint>

#include "tracer_demo/common_tracepoints.h"

#include "tracer/tracer.h"

namespace demo {

std::uint64_t tick() noexcept {
    static std::uint64_t t = 0;
    t += 100;
    return t;
}

void run_plugin_workload(std::uint32_t connections) {
    TRACEPOINT(::tracer::event_level::info, "plugin_loaded", "connections", connections);

    for (std::uint32_t i = 0; i < connections; ++i) {
        TRACEPOINT(::tracer::event_level::debug, "plugin_work", "step", i, "label", "handshake");
    }

    // The header's tracepoint, compiled into this library. The executable calls
    // the same function and records through its own copy.
    trace_shared_event(connections);
}

}  // namespace demo

extern "C" {

void tracer_plugin_run(std::uint32_t connections) { demo::run_plugin_workload(connections); }

std::size_t tracer_plugin_tracepoint_count() {
    return static_cast<std::size_t>(::tracer::__stop_tracepoints - ::tracer::__start_tracepoints);
}
}
