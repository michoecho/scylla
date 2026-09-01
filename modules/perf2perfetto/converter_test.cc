#include "perf2perfetto/converter.h"

#include <cstdint>
#include <cstring>
#include <optional>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include <doctest/doctest.h>

namespace {

using namespace perf2perfetto;

// The exact flag combinations perf reports at a trace boundary, taken from
// real Intel PT captures of tools/pt_repro.
constexpr uint32_t kSyscallEnd = kBranch | kCall | kSyscallRet | kTraceEnd;
constexpr uint32_t kAsyncEnd = kBranch | kAsync | kTraceEnd;
constexpr uint32_t kResume = kBranch | kTraceBegin;

std::optional<TraceEnd> syscall_end(uint64_t ip) {
    return TraceEnd{ip, true};
}

std::optional<TraceEnd> async_end(uint64_t ip) {
    return TraceEnd{ip, false};
}

// ---------------------------------------------------------------------------
// A trace read back
//
// The events the converter wrote, decoded far enough to see the shape of the
// call tree: the record walk only needs each record's type and size, which
// every Fuchsia record carries in its first word.

struct Event {
    uint64_t etype = 0;  // 2 begin, 3 end, 4 complete
    std::string name;
    uint64_t timestamp = 0;
};

std::vector<Event> parse_events(const std::string& bytes) {
    REQUIRE(bytes.size() % 8 == 0);
    std::vector<uint64_t> w(bytes.size() / 8);
    std::memcpy(w.data(), bytes.data(), bytes.size());

    std::unordered_map<uint64_t, std::string> strings;
    std::vector<Event> events;
    for (size_t i = 0; i < w.size();) {
        const uint64_t header = w[i];
        const uint64_t rtype = header & 0xf;
        const uint64_t rsize = (header >> 4) & 0xfff;
        REQUIRE(rsize >= 1);
        REQUIRE(i + rsize <= w.size());
        if (rtype == 2) {  // String record.
            const uint64_t index = (header >> 16) & 0xffff;
            const uint64_t len = (header >> 32) & 0x7fff;
            strings[index] = std::string(bytes.data() + (i + 1) * 8, len);
        } else if (rtype == 4) {  // Event record.
            const uint64_t name_ref = (header >> 48) & 0xffff;
            events.push_back(Event{
                .etype = (header >> 16) & 0xf,
                .name = strings.count(name_ref) ? strings[name_ref] : std::string(),
                .timestamp = w[i + 1],
            });
        }
        i += rsize;
    }
    return events;
}

// The frame names, in the order the events describe them, with the two kinds
// of close marked so nesting is visible: "foo{" opens a frame, "}" closes the
// one most recently opened, and "!bar" is a frame whose start was never seen.
std::vector<std::string> frame_shape(const std::string& bytes) {
    std::vector<std::string> out;
    for (const Event& e : parse_events(bytes)) {
        if (e.etype == 2) {
            out.push_back(e.name + "{");
        } else if (e.etype == 3) {
            out.push_back("}");
        } else if (e.etype == 4) {
            out.push_back("!" + e.name);
        }
    }
    return out;
}

// ---------------------------------------------------------------------------
// A fake symbol table

struct Sym {
    uint64_t start = 0;
    uint64_t end = 0;
    std::string name;
    const char* dso = "a.out";
};

// Symbolises whatever sample it was last handed. Addresses are their own
// dso-relative addresses here, which is all the conversion needs: it only ever
// compares them with each other.
class FakeResolver final : public Resolver {
public:
    explicit FakeResolver(std::vector<Sym> syms) : syms_(std::move(syms)) {}

    void set_sample(const Sample& s) {
        ip_ = s.ip;
        addr_ = s.addr;
    }

    Location resolve_ip() override { return lookup(ip_); }
    Location resolve_addr() override { return lookup(addr_); }

private:
    Location lookup(uint64_t address) const {
        Location out;
        out.addr = address;
        for (const Sym& s : syms_) {
            if (address >= s.start && address < s.end) {
                out.has_symbol = true;
                out.sym = s.name;
                out.sym_start = s.start;
                out.sym_end = s.end;
                out.dso = s.dso;
                break;
            }
        }
        return out;
    }

    std::vector<Sym> syms_;
    uint64_t ip_ = 0;
    uint64_t addr_ = 0;
};

// A branch sample on the one thread these tests use.
Sample branch(uint64_t ip, uint64_t addr, uint32_t flags) {
    Sample s;
    s.ip = ip;
    s.addr = addr;
    s.flags = flags;
    s.pid = 100;
    s.tid = 100;
    s.insn_cnt = 1;
    s.cyc_cnt = 1;
    s.addr_correlates_sym = addr != 0;
    return s;
}

// Runs a whole sample stream through a converter and returns the trace.
std::string convert(const std::vector<Sample>& samples, std::vector<Sym> syms,
                    TimestampMode mode = TimestampMode::Instructions) {
    std::ostringstream out;
    FakeResolver resolver(std::move(syms));
    Converter converter(out, mode);
    for (const Sample& s : samples) {
        resolver.set_sample(s);
        converter.handle_sample(s, resolver);
    }
    converter.finish();
    return out.str();
}

// Two functions and a PLT stub, laid out far enough apart to be distinct but
// close enough that a jump between them is not mistaken for a decoding gap.
const std::vector<Sym> kSyms = {
    {0x1000, 0x1100, "main"},
    {0x1100, 0x1200, "foo"},
    {0x1200, 0x1300, "bar"},
};

}  // namespace

TEST_CASE("perf2perfetto recognizes trace boundaries") {
    CHECK(is_trace_end(kSyscallEnd));
    CHECK(is_trace_end(kAsyncEnd));
    CHECK(!is_trace_end(kResume));
    CHECK(is_trace_begin(kResume));
    CHECK(!is_trace_begin(kSyscallEnd));
}

TEST_CASE("perf2perfetto recognizes only the syscall trace end as a call") {
    // The `syscall` instruction is the one trace end that perf also flags as a
    // call; a scheduler preemption is not, and must not open a frame.
    CHECK(is_syscall_entry(kSyscallEnd));
    CHECK(!is_syscall_entry(kAsyncEnd));
    CHECK(!is_syscall_entry(kBranch | kCall));
    // A recording that stops on an ordinary call is a call that never returns,
    // not a trip through the kernel.
    CHECK(!is_syscall_entry(kBranch | kCall | kTraceEnd));
}

TEST_CASE("perf2perfetto sees a syscall resume one instruction on") {
    // `syscall` retires, so user space picks up just past it -- two bytes.
    CHECK(resumes_in_place(syscall_end(0x401070), 0, 0x401072, kResume));
    // But not arbitrarily far past it.
    CHECK(!resumes_in_place(syscall_end(0x401070), 0, 0x401090, kResume));
}

TEST_CASE("perf2perfetto sees a preemption resume at the interrupted instruction") {
    // The instruction never retired, so it re-executes at the same byte.
    CHECK(resumes_in_place(async_end(0x401070), 0, 0x401070, kResume));
}

TEST_CASE("perf2perfetto requires an async resume to be exact") {
    // Recording with /uk traces the kernel's own PT save/restore, so a context
    // switch stops in pt_event_stop's `wrmsr` and resumes two bytes later in
    // pt_event_start's -- same function, different call path. Only an exact
    // match means the stack really survived.
    CHECK(!resumes_in_place(async_end(0x401070), 0, 0x401072, kResume));
    CHECK(!resumes_in_place(async_end(0x401070), 0, 0x40106f, kResume));
}

TEST_CASE("perf2perfetto treats lost trace data as not resuming in place") {
    // A decoder that regained sync somewhere else tells us nothing about the
    // stack, however soon after the gap it happened.
    CHECK(!resumes_in_place(syscall_end(0x401070), 0, 0x501070, kResume));
    CHECK(!resumes_in_place(async_end(0x401070), 0, 0x501070, kResume));
    // Backwards is just as unrelated as far away.
    CHECK(!resumes_in_place(syscall_end(0x401070), 0, 0x40106f, kResume));
}

TEST_CASE("perf2perfetto does not recognize an unrelated trace resume") {
    // Not preceded by a trace end.
    CHECK(!resumes_in_place(std::nullopt, 0, 0x401072, kResume));
    // Not a trace begin.
    CHECK(!resumes_in_place(syscall_end(0x401070), 0, 0x401072, 0));
    // A trace begin carries ip=0 and a real addr; anything else is not one.
    CHECK(!resumes_in_place(syscall_end(0x401070), 0x401072, 0x401072, kResume));
    CHECK(!resumes_in_place(syscall_end(0x401070), 0, 0, kResume));
    // Nothing to compare against.
    CHECK(!resumes_in_place(syscall_end(0), 0, 0x401072, kResume));
}

TEST_CASE("perf2perfetto recognizes an unconditional jump") {
    CHECK(is_unconditional_jump(kBranch));
    CHECK(!is_unconditional_jump(kBranch | kCall));
    CHECK(!is_unconditional_jump(kBranch | kConditional));
    CHECK(!is_unconditional_jump(kBranch | kTraceBegin));
    CHECK(!is_unconditional_jump(kSyscallEnd));
    CHECK(!is_unconditional_jump(kAsyncEnd));
}

TEST_CASE("perf2perfetto parses the timestamp mode") {
    CHECK(parse_timestamp_mode('t') == TimestampMode::Time);
    CHECK(parse_timestamp_mode('c') == TimestampMode::Cycles);
    CHECK(parse_timestamp_mode('i') == TimestampMode::Instructions);
    // Anything else falls back rather than failing the whole decode.
    CHECK(parse_timestamp_mode('x') == TimestampMode::Instructions);
}

TEST_CASE("perf2perfetto nests a call inside the trace frame") {
    const std::vector<Sample> samples = {
        // main calls foo, which returns.
        branch(0x1010, 0x1100, kBranch | kCall),
        branch(0x1140, 0x1015, kBranch | kReturn),
    };

    // The first sample opens the frame standing for the whole trace segment.
    // The stray close before it is the empty segment frame the converter
    // starts with, which the first sample retires.
    CHECK(frame_shape(convert(samples, kSyms)) ==
          std::vector<std::string>{"}", "TRACE{", "foo{", "}", "}"});
}

TEST_CASE("perf2perfetto completes a frame whose call was never seen") {
    const std::vector<Sample> samples = {
        // The segment starts inside foo, so foo's return has no matching call.
        branch(0x1010, 0x1020, kBranch | kConditional),
        branch(0x1140, 0x1015, kBranch | kReturn),
    };

    // A front-truncated frame is written as a single complete event named for
    // where the return came from, rather than being nested.
    CHECK(frame_shape(convert(samples, kSyms)) ==
          std::vector<std::string>{"}", "TRACE{", "!foo", "}"});
}

TEST_CASE("perf2perfetto opens a kernel frame for a syscall and closes it on resume") {
    const std::vector<Sample> samples = {
        branch(0x1010, 0x1020, kBranch | kConditional),
        // `syscall` at 0x1030: a call that also ends the trace.
        branch(0x1030, 0, kSyscallEnd),
        // User space picks up just past it, two bytes on.
        branch(0, 0x1032, kResume),
    };

    CHECK(frame_shape(convert(samples, kSyms)) ==
          std::vector<std::string>{"}", "TRACE{", "[kernel]{", "}", "}"});
}

TEST_CASE("perf2perfetto rebuilds the stack when a trace gap loses it") {
    const std::vector<Sample> samples = {
        branch(0x1010, 0x1020, kBranch | kConditional),
        branch(0x1020, 0x1100, kBranch | kCall),
        // The recording stops, and resumes somewhere unrelated: everything the
        // stack held is closed and the segment frame reopened.
        branch(0x1140, 0, kAsyncEnd),
        branch(0, 0x9000, kResume),
    };

    CHECK(frame_shape(convert(samples, kSyms)) ==
          std::vector<std::string>{"}", "TRACE{", "foo{", "}", "}", "TRACE{", "}"});
}

TEST_CASE("perf2perfetto closes the caller's frame on a tail call") {
    const std::vector<Sample> samples = {
        branch(0x1010, 0x1100, kBranch | kCall),
        // foo jumps to the first instruction of bar: bar inherits the frame,
        // so foo is closed and bar opened beside it rather than inside it.
        branch(0x1180, 0x1200, kBranch),
        branch(0x1280, 0x1015, kBranch | kReturn),
    };

    CHECK(frame_shape(convert(samples, kSyms)) ==
          std::vector<std::string>{"}", "TRACE{", "foo{", "}", "bar{", "}", "}"});
}

TEST_CASE("perf2perfetto keeps the frame on a jump into the middle of another symbol") {
    const std::vector<Sample> samples = {
        branch(0x1010, 0x1100, kBranch | kCall),
        // Not a tail call: the jump lands past bar's entry, as a branch into
        // an outlined cold path does.
        branch(0x1180, 0x1240, kBranch),
        branch(0x1280, 0x1015, kBranch | kReturn),
    };

    CHECK(frame_shape(convert(samples, kSyms)) ==
          std::vector<std::string>{"}", "TRACE{", "foo{", "}", "}"});
}

TEST_CASE("perf2perfetto names an unresolved target by its address") {
    const std::vector<Sample> samples = {
        // 0x2000 is in none of the symbols, so the frame is named for the
        // address, zero-padded, which is what a disassembly can be matched to.
        branch(0x1010, 0x2000, kBranch | kCall),
    };

    CHECK(frame_shape(convert(samples, kSyms)) ==
          std::vector<std::string>{"}", "TRACE{", "0000000000002000{", "}", "}"});
}

TEST_CASE("perf2perfetto tracks each thread separately") {
    Sample other = branch(0x1010, 0x1100, kBranch | kCall);
    other.tid = 200;
    const std::vector<Sample> samples = {
        branch(0x1010, 0x1100, kBranch | kCall),
        other,
    };

    const std::vector<Event> events = parse_events(convert(samples, kSyms));
    // Two threads, each with its own segment frame and its own call.
    size_t begins = 0;
    for (const Event& e : events) {
        begins += e.etype == 2;
    }
    CHECK(begins == 4);
}

TEST_CASE("perf2perfetto timestamps events with the chosen axis") {
    std::vector<Sample> samples = {
        branch(0x1010, 0x1020, kBranch | kConditional),
        branch(0x1020, 0x1100, kBranch | kCall),
    };
    // Each sample above carries one instruction and one cycle; give them
    // distinguishable wall-clock times too.
    samples[0].time = 1000;
    samples[1].time = 2000;
    samples[1].cyc_cnt = 7;

    auto timestamp_of_last_begin = [](const std::string& trace) {
        uint64_t last = 0;
        for (const Event& e : parse_events(trace)) {
            if (e.etype == 2) {
                last = e.timestamp;
            }
        }
        return last;
    };

    // The `foo` frame opens after two instructions, eight cycles, at t=2000.
    CHECK(timestamp_of_last_begin(convert(samples, kSyms, TimestampMode::Instructions)) == 2);
    CHECK(timestamp_of_last_begin(convert(samples, kSyms, TimestampMode::Cycles)) == 8);
    CHECK(timestamp_of_last_begin(convert(samples, kSyms, TimestampMode::Time)) == 2000);
}
