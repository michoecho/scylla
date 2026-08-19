// libafl_c -- LibAFL's in-process fuzzer, behind a C ABI.
//
// This exists to be the `libafl` backend of modules/test_rng. The shape of the
// boundary is dictated by what that backend needs and by what can safely cross
// a language boundary, which are not the same thing; the difference is the
// reason this file is hand-written rather than generated.
//
// --- who calls whom ---------------------------------------------------------
//
// Inverted relative to every other backend, and this is the whole design.
// exhaustigen and Hegel are libraries the provider drives: the provider owns
// the loop and asks for values. LibAFL is a *fuzzer* -- it owns the loop, and
// the code under test is a callback it invokes. So the control flow is:
//
//     C++ calls libafl_c_run(...)          <- once, blocks for the whole search
//       Rust runs the fuzzing loop
//         Rust calls harness(input, ctx)   <- once per testcase
//           C++ runs the test body
//             C++ calls back into nothing -- draws come from `input`
//
// The provider therefore hands over control for the duration of the search and
// gets it back with a verdict, rather than pulling testcases one at a time.
// A pull API would have meant either a thread or a coroutine to suspend
// LibAFL's loop in, and neither is worth it for a callback that already
// re-enters C++ exactly where it is needed.
//
// --- why bytes rather than draws --------------------------------------------
//
// The harness receives an opaque byte buffer, not a sequence of drawn integers.
// That matches how the AFL backend already works (see AflRng in
// modules/test_rng/test_rng.cc): parameters are *carved out of* a testcase, so
// the fuzzer's mutations act on the same bytes the body compares against. It is
// also the only thing that can cross this boundary cheaply -- a per-draw
// callback into Rust would be one FFI transition per parameter, and would put
// the C++ side back in control of a loop LibAFL wants to own.
//
// The C++ side reuses its existing byte-carving RNG on this buffer, so the
// libafl and afl backends draw *identically* from a given input. That is
// deliberate: it means the two backends differ only in which fuzzer produces
// the bytes, which is what makes comparing them meaningful.
//
// --- how a consumer must be built -------------------------------------------
//
// Two flags, and the second one is not optional despite looking like it:
//
//     -fsanitize-coverage=trace-pc-guard    instrument edges
//     -fno-sanitize-link-runtime            do NOT link clang's own runtime
//
// The first is what makes the compiler emit calls to
// __sanitizer_cov_trace_pc_guard. The second is what makes those calls reach
// *this* library's implementation of it.
//
// Without the second flag the search silently does nothing, which is the worst
// failure mode available and is worth spelling out. clang links
// libclang_rt.ubsan_standalone.a, which carries its own weak definition of
// __sanitizer_cov_trace_pc_guard -- a stub that counts nothing. A static
// archive's definition binds ahead of a shared library's, so every guard call
// in the binary lands in clang's stub, the coverage map stays all-zero, and
// LibAFL runs a coverage-guided search with no coverage. It looks exactly like
// a working fuzzer that simply is not finding anything.
//
// libafl_c_edge_count() is the check that catches this: it returns the number
// of edges SanCov's _init registered, which is the *map size* rather than a
// real count when _init never ran. The C++ backend refuses to start when the
// count is implausible, for the same reason the afl backend refuses to run
// outside afl-fuzz.
//
// --- exceptions -------------------------------------------------------------
//
// A C++ exception must never unwind into Rust: the frames in between are Rust's
// and unwinding through them is undefined behaviour. So the harness callback is
// *not* allowed to throw -- the C++ side catches everything at the boundary and
// reports the outcome in its return value instead. See LibAflHarnessResult.
//
// This is why the callback returns a status rather than being void: "the body
// threw" is the signal that the fuzzer found something, and it has to travel as
// data.

#ifndef LIBAFL_C_H
#define LIBAFL_C_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// What one harness invocation concluded.
//
// Deliberately not a bool: "the body threw" and "the body ran fine" are the two
// ordinary outcomes, but a fuzzer also needs to distinguish a testcase it
// should *not* count as a find. Kept as an explicit enum so adding a third
// outcome later does not silently change the meaning of an existing value.
typedef enum LibAflHarnessResult {
    // The body ran to completion without signalling anything. The fuzzer keeps
    // this input only if it reached new coverage.
    LIBAFL_HARNESS_OK = 0,
    // The body threw, i.e. the property under test was falsified. LibAFL
    // records this as an objective ("solution") and the run stops.
    LIBAFL_HARNESS_FAILED = 1,
} LibAflHarnessResult;

// The test body, as seen from Rust.
//
// `data`/`len` is the testcase; `ctx` is the opaque pointer handed to
// libafl_c_run, through which the C++ side recovers its own state. The buffer
// is valid only for the duration of the call -- LibAFL owns it and will mutate
// it for the next testcase, so a harness that wants to keep bytes must copy
// them.
typedef LibAflHarnessResult (*LibAflHarnessFn)(const uint8_t* data,
                                               size_t len,
                                               void* ctx);

// How a whole run ended.
typedef struct LibAflRunReport {
    // How many times the harness was invoked. This is the honest count of body
    // invocations, including the ones LibAFL spends on its initial corpus.
    uint64_t invocations;
    // Whether any invocation returned LIBAFL_HARNESS_FAILED.
    uint8_t found_failure;
} LibAflRunReport;

// Run the in-process fuzzer against `harness` until it finds a failure or
// exhausts `max_invocations`.
//
// Blocks for the whole search. Returns 0 on success, or a negative value if the
// fuzzer itself could not be set up -- which is a configuration error (see
// libafl_c_last_error), not a test failure.
//
// `max_invocations` of 0 means "no cap", which is only sensible under a harness
// that is expected to fail; a passing property would search forever.
//
// `seed` fixes the RNG. Derandomized by default in the C++ layer, for the same
// reason Hegel is: a property that fails one run in fifty should fail every run
// or none.
int32_t libafl_c_run(LibAflHarnessFn harness,
                     void* ctx,
                     uint64_t max_invocations,
                     uint64_t seed,
                     LibAflRunReport* out_report);

// A human-readable description of the last failure from libafl_c_run, or NULL.
//
// Owned by the library and valid until the next call into it, so a caller that
// needs to keep the text must copy it. Thread-local, because the error belongs
// to the call that produced it.
const char* libafl_c_last_error(void);

// The number of SanCov edges the instrumented binary registered.
//
// Zero means the binary was built without -fsanitize-coverage=trace-pc-guard,
// which the C++ side treats as a configuration error: a coverage-guided backend
// with no coverage is a slow random search wearing its name, and silently
// degrading to that would look exactly like a passing run. This is the libafl
// analogue of the afl backend's afl_unusable_reason().
size_t libafl_c_edge_count(void);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // LIBAFL_C_H
