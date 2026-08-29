#pragma once

// A tracepoint in a header, which is the case a shared library makes awkward.
//
// The call below is an inline function, so every object that includes this
// header compiles its own copy of the tracepoint: its own entry in its own
// `tracepoints` section, its own static key, its own everything. Two objects
// that include it therefore give the process two entries that are the same
// tracepoint -- same name, same file, same line, same parameters -- and a trace
// may hold records from either.
//
// That is what the code generator has to tolerate: the copies collapse into one
// struct, and only a *disagreement* about the parameter list is an error. See
// tracer/codegen.h.

#include <cstdint>

#include "tracer_demo/demo_clock.h"

#include "tracer/tracer.h"

namespace demo {

inline void trace_shared_event(std::uint32_t sequence) {
    TRACEPOINT(::tracer::event_level::info, "shared_event", "sequence", sequence);
}

}  // namespace demo
