#pragma once

// Program side of the Intel PT control protocol driven by tools/pt-trace.
//
// The orchestrator creates two named pipes and hands their paths to us through
// the environment:
//
//   PERF_CTL_FIFO  - we WRITE "enable\n" / "disable\n" commands here; perf reads
//                    them off its --control ctl-fd and starts/stops the trace.
//   PERF_ACK_FIFO  - perf WRITES an ack back here once a command has taken
//                    effect; we READ it to synchronise (so the trace is
//                    guaranteed on/off by the time the call returns).
//
// If neither variable is set the helpers are no-ops, so instrumented code can
// be left in place and simply runs untraced when not launched under pt-trace.

namespace pt {

// Resolve the control FIFO environment once. This is normally called
// implicitly by enable()/disable(), but reporters that bracket a larger
// operation can resolve the environment explicitly during setup.
void resolve();

// Enable tracing and block until perf acknowledges. No-op (returns false) when
// not running under the orchestrator or if the control fifos are unavailable.
// Returns true if an ack was received.
bool enable();

// Disable tracing and block until perf acknowledges. A failure to disable
// (closed pipe, missing ack, ...) is treated as success: the worst case is a
// little extra trace, never a stuck program. Always "succeeds".
void disable();

// RAII scope: enable() on construction, disable() on destruction. Use to bound
// the trace to a lexical region:
//
//   {
//       pt::Trace _;
//       hot_function();
//   }
class Trace {
public:
    Trace() { enable(); }
    ~Trace() { disable(); }

    Trace(const Trace&) = delete;
    Trace& operator=(const Trace&) = delete;
};

} // namespace pt
