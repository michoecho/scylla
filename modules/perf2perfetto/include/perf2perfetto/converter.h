#pragma once

// Turning a decoded Intel PT branch stream into a nested call trace.
//
// `perf script` hands a dlfilter one sample per branch. A call pushes a frame,
// a return pops it, and the counters accumulated in between (instructions,
// cycles, touched cache lines) become the frame's arguments -- which is what
// the Perfetto UI draws as a flamegraph.
//
// Everything in here is independent of perf's ABI: the filter (dlfilter.cc)
// translates perf's structures into the Sample and Location below, so the
// state machine can be exercised in tests without a recording.

#include <cstdint>
#include <optional>
#include <ostream>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "perf2perfetto/ftf.h"

namespace perf2perfetto {

// Sample flags, mirroring PERF_DLFILTER_FLAG_* in perf_dlfilter.h. dlfilter.cc
// static_asserts that the two agree.
enum SampleFlag : uint32_t {
    kBranch = 1u << 0,
    kCall = 1u << 1,
    kReturn = 1u << 2,
    kConditional = 1u << 3,
    kSyscallRet = 1u << 4,
    kAsync = 1u << 5,
    kInterrupt = 1u << 6,
    kTraceBegin = 1u << 8,
    kTraceEnd = 1u << 9,
};

// One branch (or instruction) sample, as much of it as the conversion uses.
struct Sample {
    uint64_t ip = 0;
    uint64_t addr = 0;
    uint64_t time = 0;
    uint64_t insn_cnt = 0;
    uint64_t cyc_cnt = 0;
    uint32_t flags = 0;
    uint64_t pid = 0;
    uint64_t tid = 0;
    // Whether resolve_addr() may be called for this sample.
    bool addr_correlates_sym = false;
    // A 'branches' sample, as opposed to an 'instructions' one.
    bool is_branch = true;
};

// What perf's resolve_ip()/resolve_addr() report about an address. `addr`,
// `sym_start` and `sym_end` are dso-relative, so they are comparable with each
// other but never with a Sample's runtime addresses.
struct Location {
    bool has_symbol = false;
    std::string_view sym;
    uint64_t addr = 0;
    uint64_t sym_start = 0;
    uint64_t sym_end = 0;
    // The object the address falls in, or nullptr if perf could not say.
    const char* dso = nullptr;
};

// Symbolisation for the sample currently being handled.
class Resolver {
public:
    virtual ~Resolver() = default;
    virtual Location resolve_ip() = 0;
    virtual Location resolve_addr() = 0;
};

// What the trace's time axis measures. Instructions and cycles make a
// flamegraph whose widths are work done rather than time passed, which is
// usually what you want from a PT trace; wall-clock time is only as fine as
// the recording's timestamps.
enum class TimestampMode { Time, Cycles, Instructions };

// 't', 'c' or 'i'; anything else is Instructions.
TimestampMode parse_timestamp_mode(char c);

// Where and how a contiguous trace segment ended, kept until the next branch
// so that a resume can be matched against it.
struct TraceEnd {
    uint64_t ip = 0;
    // A syscall completes the instruction that ended the trace; anything else
    // (a preemption, a decoding failure) interrupts it. That decides which
    // resume address means "carried on from here" -- see resumes_in_place().
    bool was_syscall = false;
};

bool is_trace_end(uint32_t flags);
bool is_trace_begin(uint32_t flags);

// The `syscall` instruction is reported as a call that also ends the trace:
// PT stops at the kernel boundary, so the matching `sysret` never shows up as
// a branch of its own. Recognising the pattern lets us open a frame for the
// kernel and close it again when user space resumes, instead of leaving an
// unmatched call on the stack that swallows every later return.
//
// All three flags are required. A preemption also ends the trace but is not a
// call, and a recording that happens to stop on an ordinary call would be a
// call that never returns -- neither should open a kernel frame.
bool is_syscall_entry(uint32_t flags);

bool is_unconditional_jump(uint32_t flags);

// Whether a TRACE_BEGIN picks up where the preceding TRACE_END stopped, which
// is what tells us the call stack survived the gap.
//
// perf brackets every break in the trace the same way -- a TRACE_END branch
// with addr=0, then a TRACE_BEGIN branch with ip=0 -- whether the cause was a
// syscall, the scheduler, or a decoding failure. The flags alone do not say
// which, but the flags together with the resume address do, because the two
// harmless cases resume at an address fixed by how they were interrupted:
//
//   syscall     the instruction completed, so we resume just past it
//   preemption  the instruction never retired, so it re-executes: same address
//   lost data   the decoder regained sync at an unrelated address
//
// The exactness matters. Recording with /uk traces the kernel's own PT
// save/restore, so a context switch stops in pt_event_stop's `wrmsr` and
// resumes in pt_event_start's -- two bytes apart in the same function, but
// reached down entirely different call paths. Accepting a nearby address
// rather than the exact one would carry the outgoing thread's stack into the
// incoming one. An asynchronous interruption that truly resumed in place
// resumes at the very same byte.
bool resumes_in_place(std::optional<TraceEnd> trace_end, uint64_t sample_ip,
                      uint64_t sample_addr, uint32_t sample_flags);

// Converts a stream of samples into a .ftf trace on `out`.
//
// The stream may interleave threads; each is tracked separately and gets its
// own track in the trace.
class Converter {
public:
    // Writes the trace header immediately, so `out` must already be open.
    Converter(std::ostream& out, TimestampMode mode);

    Converter(const Converter&) = delete;
    Converter& operator=(const Converter&) = delete;

    // `resolver` symbolises this sample and is not retained.
    void handle_sample(const Sample& sample, Resolver& resolver);

    // Close every frame still open and flush. Called once, after the last
    // sample.
    void finish();

private:
    // A cache line, as the unit an instruction footprint is measured in.
    using CacheLine = uint64_t;

    struct FrameData {
        uint64_t start_insn_cnt = 0;
        uint64_t start_cyc_cnt = 0;
        uint64_t start_timestamp = 0;
        // The dso-relative bounds of the symbol this frame is executing, as
        // reported by resolve_addr() when the frame was opened, or (0, 0) when
        // we could not resolve it. Used to check that a suspected tail call
        // really is leaving the frame we are about to close.
        uint64_t symbol_start = 0;
        uint64_t symbol_end = 0;
        // Set on the frame we open for the kernel side of a syscall, so that
        // the resume can recognise and close it.
        bool is_kernel = false;
        std::unordered_set<CacheLine> footprint;
    };

    // A jump site, as the key of the tail-call cache.
    struct JumpSite {
        uint64_t ip = 0;
        uint64_t addr = 0;

        bool operator==(const JumpSite&) const = default;
    };

    struct JumpSiteHash {
        size_t operator()(const JumpSite& s) const {
            return std::hash<uint64_t>{}(s.ip) * 1099511628211u ^ std::hash<uint64_t>{}(s.addr);
        }
    };

    // The dso-relative bounds of a symbol.
    struct SymbolBounds {
        uint64_t start = 0;
        uint64_t end = 0;

        bool operator==(const SymbolBounds&) const = default;
    };

    struct ThreadState {
        uint64_t insn_cnt = 0;
        uint64_t cyc_cnt = 0;
        uint64_t ip = 0;
        // Set when the previous branch ended a contiguous trace segment, and
        // cleared by any other branch. It lets the next TRACE_BEGIN show that
        // execution picked up where it left off -- see resumes_in_place().
        std::optional<TraceEnd> trace_end;
        // Maps a jump site to whether it is a tail call, along with the bounds
        // of the symbol it jumps out of. Both sides of a jump resolve to the
        // same symbols every time, so this is resolved once.
        std::unordered_map<JumpSite, std::optional<SymbolBounds>, JumpSiteHash> tail_call_cache;
        std::vector<FrameData> stack;
        uint64_t last_seen_time = 0;
        ftf::ThreadId thread;
    };

    uint64_t timestamp(const ThreadState& t) const;
    void pop_frame(ThreadState& t);
    void pop_unknown_frame(ThreadState& t, const Sample& sample, Resolver& resolver);
    void push_frame(ThreadState& t, const Sample& sample, Resolver& resolver);
    void push_kernel_frame(ThreadState& t, const Sample& sample);
    std::optional<SymbolBounds> tail_call_source(ThreadState& t, const Sample& sample,
                                                 Resolver& resolver);

    std::ostream& out_;
    ftf::Caches caches_;
    TimestampMode mode_;
    // Set once an 'instructions' sample is seen, after which instructions are
    // counted one by one instead of taken from the branch samples.
    bool has_insns_events_ = false;
    std::unordered_map<uint64_t, ThreadState> threads_;
};

}  // namespace perf2perfetto
