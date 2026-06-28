#include "pt_control.h"

#include <cstdint>

#include "doctest/doctest.h"

// A clearly-named, non-inlinable function so it stands out in `perf script`
// output after decoding the Intel PT trace. The volatile loop gives the CPU
// some branches/instructions to record while tracing is enabled.
__attribute__((noinline)) static std::uint64_t pt_traced_function(int n) {
    std::uint64_t acc = 0;
    for (int i = 0; i < n; ++i) {
        // Data-dependent branch so the trace has something non-trivial.
        if ((i & 1) != 0)
            acc += static_cast<std::uint64_t>(i) * 2u;
        else
            acc += static_cast<std::uint64_t>(i);
    }
    return acc;
}

// Keep the optimiser from discarding the call.
volatile std::uint64_t pt_sink = 0;

// A small tree of noinline functions so the decoded trace has a visible call
// hierarchy. In Perfetto (via tools/pt-trace --perfetto) these nest as
// pt_outer -> pt_middle -> pt_leaf, which is what makes the flamegraph useful.
__attribute__((noinline)) static std::uint64_t pt_leaf(int n) {
    return pt_traced_function(n);
}

__attribute__((noinline)) static std::uint64_t pt_middle(int n) {
    std::uint64_t acc = 0;
    for (int i = 0; i < 3; ++i)
        acc += pt_leaf(n + i);
    return acc;
}

__attribute__((noinline)) static std::uint64_t pt_outer(int n) {
    return pt_middle(n) + pt_middle(n / 2);
}

// Exercises the scoped helper end-to-end. Run untraced it's just a normal test
// (the helpers no-op when PERF_CTL_FIFO/PERF_ACK_FIFO are unset). Run under
// tools/pt-trace, the body between enable() and disable() is what perf records:
//
//   tools/pt-trace --script -- ./out/build/Debug/cpp_template test \
//       --test-case='intel pt scoped trace'
//
// then look for pt_traced_function in the `perf script` output.
TEST_CASE("intel pt scoped trace") {
    std::uint64_t result;
    {
        pt::Trace _; // enable() now; disable() at end of scope
        result = pt_traced_function(100000);
    }
    pt_sink = result;

    // Independent of tracing: just confirms the work ran.
    CHECK(result != 0);
}

// Like above, but the traced region calls into a nested tree of functions, so
// the decoded trace shows a call hierarchy. Useful for eyeballing the Perfetto
// flamegraph:
//
//   tools/pt-trace --perfetto -- ./out/build/Debug/cpp_template test \
//       --test-case='intel pt nested calls'
TEST_CASE("intel pt nested calls") {
    std::uint64_t result;
    {
        pt::Trace _;
        result = pt_outer(1000);
    }
    pt_sink = result;

    CHECK(result != 0);
}
