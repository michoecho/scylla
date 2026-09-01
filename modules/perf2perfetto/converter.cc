#include "perf2perfetto/converter.h"

#include <cinttypes>
#include <cstdio>
#include <cstring>
#include <utility>

namespace perf2perfetto {
namespace {

// The granularity the instruction footprint is measured at. Every cache line
// executed inside a frame is recorded, and the frame reports the total.
constexpr uint64_t kCacheLineSize = 64;

// An address with no symbol is shown as zero-padded hex, which is at least
// something to correlate against a disassembly.
constexpr size_t kHexAddrLen = 16;

std::string_view format_addr(char (&buf)[kHexAddrLen + 1], uint64_t addr) {
    std::snprintf(buf, sizeof(buf), "%016" PRIx64, addr);
    return {buf, kHexAddrLen};
}

struct ResolvedSymbol {
    std::string_view name;
    uint64_t start = 0;
    uint64_t end = 0;
};

// The symbol a branch lands in, or its address if it has none.
ResolvedSymbol resolve_addr_symbol(const Sample& sample, Resolver& resolver,
                                   char (&buf)[kHexAddrLen + 1]) {
    if (sample.addr_correlates_sym) {
        const Location l = resolver.resolve_addr();
        if (l.has_symbol) {
            return {l.sym, l.sym_start, l.sym_end};
        }
    }
    return {format_addr(buf, sample.addr), 0, 0};
}

// The symbol a branch is taken from, or its address if it has none.
std::string_view resolve_ip_symbol(const Sample& sample, Resolver& resolver,
                                   char (&buf)[kHexAddrLen + 1]) {
    const Location l = resolver.resolve_ip();
    if (l.has_symbol) {
        return l.sym;
    }
    return format_addr(buf, sample.ip);
}

bool same_dso(const Location& a, const Location& b) {
    if (a.dso == nullptr || b.dso == nullptr) {
        // Unknown is not equal to known; two unknowns are the same only in
        // the sense that there is nothing to tell them apart by.
        return a.dso == b.dso;
    }
    return std::strcmp(a.dso, b.dso) == 0;
}

// Merge the smaller set into the larger one, and return the larger.
std::unordered_set<uint64_t> merge_sets(std::unordered_set<uint64_t> a,
                                        std::unordered_set<uint64_t> b) {
    if (a.size() < b.size()) {
        std::swap(a, b);
    }
    a.insert(b.begin(), b.end());
    return a;
}

}  // namespace

TimestampMode parse_timestamp_mode(char c) {
    switch (c) {
        case 't':
            return TimestampMode::Time;
        case 'c':
            return TimestampMode::Cycles;
        case 'i':
            return TimestampMode::Instructions;
        default:
            return TimestampMode::Instructions;
    }
}

bool is_trace_end(uint32_t flags) {
    return (flags & kTraceEnd) != 0;
}

bool is_trace_begin(uint32_t flags) {
    return (flags & kTraceBegin) != 0;
}

bool is_syscall_entry(uint32_t flags) {
    return is_trace_end(flags) && (flags & kCall) != 0 && (flags & kSyscallRet) != 0;
}

bool is_unconditional_jump(uint32_t flags) {
    return (flags & kBranch) != 0 &&
           (flags & (kCall | kReturn | kConditional | kTraceBegin | kTraceEnd | kAsync |
                     kInterrupt)) == 0;
}

bool resumes_in_place(std::optional<TraceEnd> trace_end, uint64_t sample_ip,
                      uint64_t sample_addr, uint32_t sample_flags) {
    if (!trace_end) {
        return false;
    }
    if (!is_trace_begin(sample_flags) || sample_ip != 0 || sample_addr == 0 ||
        trace_end->ip == 0) {
        return false;
    }
    const uint64_t advanced = sample_addr - trace_end->ip;
    if (trace_end->was_syscall) {
        // `syscall` is two bytes, but other entry instructions are not, so
        // allow any single instruction's worth of progress.
        constexpr uint64_t kMaxInsnLen = 15;
        return advanced >= 1 && advanced <= kMaxInsnLen;
    }
    return advanced == 0;
}

Converter::Converter(std::ostream& out, TimestampMode mode) : out_(out), mode_(mode) {
    ftf::write_header(out_);
}

uint64_t Converter::timestamp(const ThreadState& t) const {
    switch (mode_) {
        case TimestampMode::Time:
            return t.last_seen_time;
        case TimestampMode::Cycles:
            return t.cyc_cnt;
        case TimestampMode::Instructions:
            return t.insn_cnt;
    }
    return t.insn_cnt;
}

void Converter::pop_frame(ThreadState& t) {
    const FrameData& frame = t.stack.back();
    ftf::write_frame_end(out_, caches_, timestamp(t), t.thread, t.insn_cnt - frame.start_insn_cnt,
                         t.cyc_cnt - frame.start_cyc_cnt,
                         frame.footprint.size() * kCacheLineSize, frame.start_timestamp,
                         t.last_seen_time);
    // The caller's footprint includes everything the callee touched.
    std::unordered_set<CacheLine> top = std::move(t.stack.back().footprint);
    t.stack.pop_back();
    auto& parent = t.stack.back().footprint;
    parent = merge_sets(std::move(parent), std::move(top));
}

void Converter::pop_unknown_frame(ThreadState& t, const Sample& sample, Resolver& resolver) {
    const FrameData& frame = t.stack.back();
    char buf[kHexAddrLen + 1];
    const std::string_view sym = resolve_ip_symbol(sample, resolver, buf);
    const uint64_t ts = timestamp(t);
    ftf::write_frame_full(out_, caches_, ts, t.thread, t.insn_cnt - frame.start_insn_cnt,
                          t.cyc_cnt - frame.start_cyc_cnt,
                          frame.footprint.size() * kCacheLineSize, sym, ts,
                          frame.start_timestamp, sample.time);
}

void Converter::push_frame(ThreadState& t, const Sample& sample, Resolver& resolver) {
    char buf[kHexAddrLen + 1];
    // Frame 1 is the contiguous trace segment itself rather than a call, so it
    // is named for what it is instead of being symbolised.
    ResolvedSymbol symbol{"TRACE", 0, 0};
    if (t.stack.size() > 1) {
        symbol = resolve_addr_symbol(sample, resolver, buf);
    }

    ftf::write_frame_start(out_, caches_, timestamp(t), ftf::ThreadId{sample.pid, sample.tid},
                           symbol.name);

    t.stack.push_back(FrameData{
        .start_insn_cnt = t.insn_cnt,
        .start_cyc_cnt = t.cyc_cnt,
        .start_timestamp = sample.time,
        .symbol_start = symbol.start,
        .symbol_end = symbol.end,
        .is_kernel = false,
        .footprint = {},
    });
}

void Converter::push_kernel_frame(ThreadState& t, const Sample& sample) {
    // PT does not trace the kernel in the user-space-only mode we record in,
    // so this frame stands in for it: it keeps the syscall's unmatched call
    // from unbalancing the stack, and it shows how long the call took.
    ftf::write_frame_start(out_, caches_, timestamp(t), ftf::ThreadId{sample.pid, sample.tid},
                           "[kernel]");

    t.stack.push_back(FrameData{
        .start_insn_cnt = t.insn_cnt,
        .start_cyc_cnt = t.cyc_cnt,
        .start_timestamp = sample.time,
        .symbol_start = 0,
        .symbol_end = 0,
        .is_kernel = true,
        .footprint = {},
    });
}

// Whether an unconditional jump is a tail call, and if so the dso-relative
// bounds of the symbol it leaves.
//
// A tail call is a jump that lands on the *entry* of another symbol. The
// callee runs in the caller's stack frame, so its `ret` goes back to the
// caller's caller: by the time we are in the callee, the caller has for all
// practical purposes returned, and that is how we render it.
//
// Landing on the first instruction of the target is what separates a tail call
// from the other jumps that cross a symbol boundary -- a branch into an
// outlined cold part of the same routine, say, which lands in the middle of
// its target and does not consume the frame.
//
// PLT stubs match on purpose. `f@plt` is a one-instruction trampoline that
// jumps to the start of `f`, and `f` returns to whoever called the stub, so it
// is the tail-call shape exactly: treating it as one is what lets `f@plt`
// close and stops it swallowing the rest of the trace.
std::optional<Converter::SymbolBounds> Converter::tail_call_source(ThreadState& t,
                                                                   const Sample& sample,
                                                                   Resolver& resolver) {
    if (!is_unconditional_jump(sample.flags) || sample.ip == 0 || sample.addr == 0 ||
        !sample.addr_correlates_sym) {
        return std::nullopt;
    }

    const JumpSite key{sample.ip, sample.addr};
    if (auto it = t.tail_call_cache.find(key); it != t.tail_call_cache.end()) {
        return it->second;
    }

    const Location source = resolver.resolve_ip();
    const Location target = resolver.resolve_addr();

    std::optional<SymbolBounds> resolved;
    if (source.has_symbol && target.has_symbol) {
        // A jump inside one symbol is an ordinary loop or branch.
        const bool same_symbol = same_dso(source, target) &&
                                 source.sym_start == target.sym_start &&
                                 source.sym_end == target.sym_end;
        if (!same_symbol && target.addr == target.sym_start) {
            resolved = SymbolBounds{source.sym_start, source.sym_end};
        }
    }

    t.tail_call_cache.emplace(key, resolved);
    return resolved;
}

void Converter::handle_sample(const Sample& sample, Resolver& resolver) {
    // The semantics of `stack` in ThreadState are as follows:
    // Frame 0 contains counters for the entire trace.
    // Frame 1 contains counters for the current contiguous trace segment. It
    // is closed and reopened on `tr end` and errors.
    // Frames 2.. contain the call stack as it is known. They are opened on
    // calls (or interrupts) and closed (merged into the parent frame) on
    // returns.
    //
    // Since the current trace segment could have started in the middle of a
    // real stack frame, ancestors of the starting frame are not here. They are
    // only noticed when they return, and printed with counters taken from
    // frame 1.
    auto [it, inserted] = threads_.try_emplace(sample.tid);
    ThreadState& t = it->second;
    if (inserted) {
        t.thread = ftf::ThreadId{sample.pid, sample.tid};
        t.stack.resize(2);
    }

    t.last_seen_time = sample.time;

    if (!sample.is_branch) {
        // 'instructions' event.
        has_insns_events_ = true;
        t.insn_cnt += 1;
        return;
    }

    if (!has_insns_events_) {
        // If the user has piped instruction events to the filter, then we do
        // an exact count of instructions. Otherwise we use the approximate
        // (updated on CYC packets) count provided by perf.
        t.insn_cnt += sample.insn_cnt;
    }
    t.cyc_cnt += sample.cyc_cnt;

    // Not all decoding errors cause a 'tr end'. Some are recovered from, and
    // the output continues. Unfortunately perf doesn't notify the filter about
    // that, so we need to cope with gaps in the input that appear sometimes.
    //
    // Here we try to guess when a gap occurred. If `ip` became smaller since
    // the last sample, or it grew by more than BAD_JUMP_HEURISTIC, we guess
    // that an error has occurred.
    constexpr uint64_t kBadJumpHeuristic = 0x1000;
    const bool trace_end = is_trace_end(sample.flags);
    // A trace end explains itself, so it is not a gap to be guessed at. Its
    // addr is 0 by construction and the resume that follows is judged by
    // resumes_in_place() instead.
    if (!trace_end && sample.ip - t.ip > kBadJumpHeuristic) {
        t.ip = 0;
    }

    const bool trace_resume =
        resumes_in_place(t.trace_end, sample.ip, sample.addr, sample.flags);

    if (t.ip != 0 && sample.ip != 0) {
        // No errors. The normal path. We update the cache footprint info.
        constexpr uint64_t kCacheLineMask = ~(kCacheLineSize - 1);
        const uint64_t cache_line_end = sample.ip & kCacheLineMask;
        // The current implementation is dumb and just inserts all touched
        // lines into a set.
        for (uint64_t line = t.ip & kCacheLineMask; line <= cache_line_end;
             line += kCacheLineSize) {
            t.stack.back().footprint.insert(line);
        }
    } else if (trace_resume) {
        // Execution picked up where it stopped, so the stack behind us is
        // still good and we leave it alone. If the gap was a syscall, the
        // frame standing in for the kernel ends here.
        if (t.stack.back().is_kernel) {
            pop_frame(t);
        }
    } else {
        // `ip` equal to 0 means that a contiguous trace segment has ended
        // (`tr end`) or an error has occurred, and the resume did not continue
        // from where we stopped -- so whatever the stack held is no longer
        // trustworthy. We close all open stack frames. We also close the
        // special frame 1 (the current contiguous trace segment) and reopen it.
        while (t.stack.size() > 1) {
            pop_frame(t);
        }
        push_frame(t, sample, resolver);
    }

    t.ip = sample.addr;
    t.trace_end = trace_end ? std::optional{TraceEnd{sample.ip, is_syscall_entry(sample.flags)}}
                            : std::nullopt;

    // A tail call closes the frame it jumps out of, but only once we have
    // checked that the frame on top really is the symbol being left. If it is
    // not, our stack and the trace already disagree and unwinding on that
    // basis would only compound the error. A frame whose symbol we never
    // resolved is taken at the jump's word.
    bool tail_call = false;
    if (const auto source = tail_call_source(t, sample, resolver)) {
        const FrameData& top = t.stack.back();
        const SymbolBounds top_symbol{top.symbol_start, top.symbol_end};
        tail_call = !top.is_kernel && (top_symbol == *source || top_symbol == SymbolBounds{});
    }

    if (is_syscall_entry(sample.flags)) {
        push_kernel_frame(t, sample);
    } else if ((sample.flags & kCall) != 0) {
        push_frame(t, sample, resolver);
    } else if ((sample.flags & kReturn) != 0) {
        if (t.stack.size() > 2) {
            // This return matches a previously seen call.
            pop_frame(t);
        } else {
            // This return does not match a previous call, so the current trace
            // fragment started inside the frame.
            //
            // The current implementation handles that by writing a single
            // point in time (the end of the frame) to output. It would be
            // better to show the full known time span in the trace, but we
            // only learn about it at the end, and ftf requires spans to be
            // nested properly. In other words, to represent those
            // front-truncated frames properly we would have to delay all
            // output until the current trace fragment ends.
            pop_unknown_frame(t, sample, resolver);
        }
    } else if (tail_call) {
        // The jump leaves the current symbol for good, so close its frame as
        // though it had returned.
        if (t.stack.size() > 2) {
            pop_frame(t);
        } else {
            pop_unknown_frame(t, sample, resolver);
        }
        // The callee takes its place as a sibling rather than a child: it
        // inherited the frame, so the single `ret` that eventually comes
        // belongs to it and closes this frame, not the caller's.
        push_frame(t, sample, resolver);
    }
}

void Converter::finish() {
    for (auto& [tid, t] : threads_) {
        while (t.stack.size() > 1) {
            pop_frame(t);
        }
    }
    out_.flush();
}

}  // namespace perf2perfetto
