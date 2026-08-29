#pragma once

// The demo's clock, shared by the executable and its plugin.
//
// The trace the build takes is snapshotted, so its timestamps have to be
// reproducible: rdtsc would make every byte of the trace differ between runs.
// A counter would do, except that the workload spans two objects -- and two
// counters, one per object, would interleave into timestamps that go backwards
// and forwards depending on which side ran. So there is one counter, defined in
// the shared library and called from both sides.
//
// The macro is here rather than at each call site because TRACER_TIMESTAMP has
// to be defined before tracer.h is first included, and there are now several
// translation units that must agree on it. Including this header is how they do.

#include <cstdint>

namespace demo {

// Advances by 100 per call, starting at 100.
std::uint64_t tick() noexcept;

}  // namespace demo

#ifndef TRACER_TIMESTAMP
#define TRACER_TIMESTAMP() ::demo::tick()
#endif
