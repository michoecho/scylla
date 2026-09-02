#pragma once

#include <memory>

namespace pt {

// A process-wide Intel PT recording session used by Boost/Seastar tests.
//
// The session is deliberately separate from Trace: Trace controls an already
// running perf session, while this class owns perf and the post-processing
// steps.  It is normally created by the test entry point after the Seastar
// reactor has started and stopped before the reactor is finalized.
class perf_trace {
public:
    // Return an active session when PT_TRACE is present.  Missing tools,
    // permissions, or configuration disable tracing with a diagnostic rather
    // than changing the result of the test run.
    static std::unique_ptr<perf_trace> start_if_requested();

    ~perf_trace();

    perf_trace(const perf_trace&) = delete;
    perf_trace& operator=(const perf_trace&) = delete;

    // Stop recording, decode perf.data, and publish the resulting .ftf.  It
    // is safe to call this more than once.
    void stop();

private:
    perf_trace() = default;

    struct impl;
    std::unique_ptr<impl> _impl;
};

} // namespace pt
