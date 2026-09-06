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
// That is what a decoder has to tolerate: the copies collapse into one reader,
// and two that disagree about the parameter list are two readers delivering
// into one struct. See modules/trace-viewer/decoder_plugin.h.

#include <cstdint>

#include "tracer_demo/demo_clock.h"

#include "tracer/tracer.h"

namespace demo {

// The static ids the demo's cheap tracepoints go on the wire as, all in one
// place because that is the only thing keeping them apart: a static id is a
// number somebody chose, where an entry address is one the linker chose. Small
// ones, so they cost a byte each.
//
// The generator refuses a table in which one id names two different
// tracepoints, so the cost of getting this wrong is a failed build rather than
// a misread trace.
inline constexpr ::tracer::tracepoint_id shared_event_id{1};
inline constexpr ::tracer::tracepoint_id request_header_id{2};
inline constexpr ::tracer::tracepoint_id table_snapshot_row_id{3};

// The id is on the *tracepoint*, not on the copy, so both objects that compile
// this header write the same one -- which is the same thing they already do
// with the name, and what a decoder needs if a record from either is to mean
// the same event.
inline void trace_shared_event(std::uint32_t sequence) {
    TRACEPOINT_STATIC_ID(shared_event_id, ::tracer::event_level::info, "shared_event",
                         "sequence", sequence);
}

}  // namespace demo
