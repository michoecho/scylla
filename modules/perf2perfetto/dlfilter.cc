// The perf side of perf2perfetto: the entry points `perf script --dlfilter`
// dlopen()s and calls, and nothing else.
//
// perf loads this object, fills in `perf_dlfilter_fns` with the callbacks the
// filter may use, calls start() once, filter_event_early() for every sample,
// and stop() at the end. All this file does is own the output file, translate
// perf's structures into the module's own Sample/Location, and hand them to
// Converter -- so that everything worth testing can be tested without a perf
// recording.
//
// Returning 1 from filter_event_early() drops the sample from perf's own
// output: the trace we write is the only thing this run is for.

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <memory>
#include <new>
#include <string>

#include "perf2perfetto/converter.h"

// The header declares perf's entry points without an `extern "C"` of its own,
// so it is included inside one: the names below have to reach perf's dlsym
// unmangled.
extern "C" {
#include "perf_dlfilter.h"

// perf resolves this symbol in the loaded object and writes its callbacks into
// it before calling anything else. It has to be a definition under this exact
// name; being an object with static storage duration, it starts zeroed, so a
// callback perf does not provide stays null rather than garbage.
struct perf_dlfilter_fns perf_dlfilter_fns;
}

namespace {

using namespace perf2perfetto;

// The module's flag values are perf's; keep the two spellings in step.
static_assert(kBranch == uint32_t{PERF_DLFILTER_FLAG_BRANCH});
static_assert(kCall == uint32_t{PERF_DLFILTER_FLAG_CALL});
static_assert(kReturn == uint32_t{PERF_DLFILTER_FLAG_RETURN});
static_assert(kConditional == uint32_t{PERF_DLFILTER_FLAG_CONDITIONAL});
static_assert(kSyscallRet == uint32_t{PERF_DLFILTER_FLAG_SYSCALLRET});
static_assert(kAsync == uint32_t{PERF_DLFILTER_FLAG_ASYNC});
static_assert(kInterrupt == uint32_t{PERF_DLFILTER_FLAG_INTERRUPT});
static_assert(kTraceBegin == uint32_t{PERF_DLFILTER_FLAG_TRACE_BEGIN});
static_assert(kTraceEnd == uint32_t{PERF_DLFILTER_FLAG_TRACE_END});

constexpr const char* kDefaultOutput = "out.ftf";

Location to_location(const perf_dlfilter_al* al) {
    if (al == nullptr) {
        return {};
    }
    Location out;
    out.has_symbol = al->sym != nullptr;
    if (out.has_symbol) {
        out.sym = al->sym;
    }
    out.addr = al->addr;
    out.sym_start = al->sym_start;
    out.sym_end = al->sym_end;
    out.dso = al->dso;
    return out;
}

// Symbolisation through perf's callbacks, valid only while perf is inside the
// filter_event_early() call this was constructed for.
class PerfResolver final : public Resolver {
public:
    explicit PerfResolver(void* ctx) : ctx_(ctx) {}

    Location resolve_ip() override { return to_location(perf_dlfilter_fns.resolve_ip(ctx_)); }
    Location resolve_addr() override { return to_location(perf_dlfilter_fns.resolve_addr(ctx_)); }

private:
    void* ctx_;
};

// Everything the filter owns between start() and stop(), handed back to perf
// as its opaque `data` pointer.
struct State {
    State(const char* path, TimestampMode mode)
        : out(path, std::ios::binary), converter(out, mode) {}

    std::ofstream out;
    Converter converter;
};

}  // namespace

extern "C" int start(void** data, void* ctx) {
    int argc = 0;
    char** argv = perf_dlfilter_fns.args(ctx, &argc);

    // --dlarg 1: where to write the trace. --dlarg 2: the time axis.
    const char* path = argc >= 1 ? argv[0] : kDefaultOutput;
    const TimestampMode mode =
        argc >= 2 && argv[1][0] != '\0' ? parse_timestamp_mode(argv[1][0])
                                        : TimestampMode::Instructions;

    auto state = std::make_unique<State>(path, mode);
    if (!state->out) {
        std::fprintf(stderr, "perf2perfetto: cannot write %s: %s\n", path, std::strerror(errno));
        return -1;
    }
    *data = state.release();
    return 0;
}

extern "C" int stop(void* data, void* /*ctx*/) {
    std::unique_ptr<State> state(static_cast<State*>(data));
    if (!state) {
        return 0;
    }
    state->converter.finish();
    return 0;
}

extern "C" int filter_event_early(void* data, const perf_dlfilter_sample* sample, void* ctx) {
    State* state = static_cast<State*>(data);

    Sample s;
    s.ip = sample->ip;
    s.addr = sample->addr;
    s.time = sample->time;
    s.insn_cnt = sample->insn_cnt;
    s.cyc_cnt = sample->cyc_cnt;
    s.flags = sample->flags;
    s.pid = static_cast<uint64_t>(sample->pid);
    s.tid = static_cast<uint64_t>(sample->tid);
    s.addr_correlates_sym = sample->addr_correlates_sym != 0;
    // 'branches' or 'instructions'; a sample with no event name at all is
    // treated as a branch, which is the only kind the conversion acts on.
    s.is_branch = sample->event == nullptr || sample->event[0] == 'b';

    PerfResolver resolver(ctx);
    state->converter.handle_sample(s, resolver);
    return 1;
}

extern "C" const char* filter_description(const char** long_description) {
    *long_description =
        "Converts an Intel PT branch trace into a Fuchsia trace (.ftf) for Perfetto.\n"
        "--dlarg 1: output file (default out.ftf). --dlarg 2: time axis, "
        "t(ime)/c(ycles)/i(nstructions).";
    return "Intel PT to Perfetto/Fuchsia trace converter";
}
