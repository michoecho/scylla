#pragma once

// The shared library half of the demo workload.
//
// It exists so that the pipeline in modules/tracer/BUCK covers a trace whose
// records come from more than one loaded object: the library has tracepoints of
// its own, in a `tracepoints` section of its own, mapped wherever the loader put
// it. Nothing about the trace it writes is decodable without the object header
// that names it.

#include <cstddef>
#include <cstdint>

namespace demo {

// Records the library's own tracepoints, plus the one from the shared header
// that the executable also compiles a copy of.
void run_plugin_workload(std::uint32_t connections);

}  // namespace demo

// The same two things again, for a test that reaches the library through
// dlsym() rather than by linking against it -- which needs names it can spell.
extern "C" {

void tracer_plugin_run(std::uint32_t connections);

// How many tracepoints this library's own table holds, so a test can watch the
// process's view of the tables change by exactly that much as the library
// arrives and leaves.
std::size_t tracer_plugin_tracepoint_count();
}
