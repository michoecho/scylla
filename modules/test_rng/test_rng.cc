// The backends behind TestRng, and the dispatch that picks one.
//
// Everything here is an implementation of a single virtual method -- `raw`,
// "an integer in [0, inclusive_max]" -- plus the loop that drives a test body
// against it. The interface and the reasoning for it are in the header; this
// file is about how each engine answers that one question, and where each one
// stops.

#include "test_rng/test_rng.h"

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <limits>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>

#include <hegel/hegel.h>
#include <hegel/settings.h>

#include "exhaustigen/exhaustigen.h"

// AFL's instrumentation defines these macros in an afl-clang-fast build; the
// guard lets this file build under a plain compiler too, where the AFL backend
// is refused at runtime instead.
//
// __AFL_FUZZ_INIT() defines file-scope globals (__afl_sharedmem_fuzzing, the
// alternate testcase buffer, ...), so it must sit at namespace scope rather
// than inside a function.
#ifdef __AFL_FUZZ_INIT
__AFL_FUZZ_INIT();
#endif

namespace test_rng {

void TestRng::throw_empty_domain() {
    throw std::logic_error(
        "test_rng: empty integer domain (min > max, or index into an empty "
        "container)");
}

std::optional<Backend> parse_backend(std::string_view name) {
    if (name == "exhaustive")
        return Backend::Exhaustive;
    if (name == "random")
        return Backend::Random;
    if (name == "afl")
        return Backend::Afl;
    if (name == "smoke")
        return Backend::Smoke;
    return std::nullopt;
}

std::string_view backend_name(Backend backend) {
    switch (backend) {
        case Backend::Exhaustive: return "exhaustive";
        case Backend::Random:     return "random";
        case Backend::Afl:        return "afl";
        case Backend::Smoke:      return "smoke";
    }
    return "?";
}

namespace {

// ---------------------------------------------------------------------------
// Smoke: no search at all.
// ---------------------------------------------------------------------------

// One representative value per domain, chosen to be the one most likely to
// exercise the body rather than the one most likely to be a bug. That means the
// *low* end of every domain: it is in range by construction, and for the common
// case of a size or an index it is the value the body can least afford to
// mishandle. Picking a midpoint instead would be no more likely to find
// anything and would cost a division.
class SmokeRng final : public TestRng {
    std::uint64_t raw(std::uint64_t, Distribution) override { return 0; }
};

// ---------------------------------------------------------------------------
// Exhaustive: exhaustigen.
// ---------------------------------------------------------------------------

// A thin adapter: exhaustigen::Gen::gen already *is* "a value in
// [0, inclusive_upper_bound]", chosen so that the driving do-while loop visits
// every combination.
//
// The one impedance mismatch is width. Gen counts in size_t, and a domain
// wider than that -- a full 64-bit range on a 32-bit size_t, or a 64-bit range
// whose count is 2^64 -- cannot be enumerated at all, let alone in this
// lifetime. Rather than silently truncate the domain and claim to have covered
// it, the walk refuses: an exhaustive backend that quietly skips most of the
// space is worse than one that says it cannot do the job.
class ExhaustiveRng final : public TestRng {
public:
    explicit ExhaustiveRng(exhaustigen::Gen& gen) : gen_(gen) {}

private:
    std::uint64_t raw(std::uint64_t inclusive_max, Distribution) override {
        if (inclusive_max > std::numeric_limits<std::size_t>::max())
            throw std::runtime_error(
                "test_rng: exhaustive backend cannot enumerate a domain of " +
                std::to_string(inclusive_max) + " + 1 values");
        return gen_.gen(static_cast<std::size_t>(inclusive_max));
    }

    exhaustigen::Gen& gen_;
};

// ---------------------------------------------------------------------------
// Random: Hegel.
// ---------------------------------------------------------------------------

// Draws each integer from the engine, which is what buys the shrinking: Hegel
// records the choice sequence, and on a failure replays it with smaller values
// until the counterexample stops shrinking.
//
// Note what this class does *not* do: it never draws a value itself, not even
// for the Distribution hint. Hegel's own integer generator already biases
// towards the edges of a range and towards zero, and it shrinks towards them
// too. Layering a second distribution on top would hand the engine a choice
// sequence whose shrunk form no longer means what it meant when it was
// generated -- so EdgeBiased is deliberately a no-op here, honoured by the
// engine rather than by us.
class HegelRng final : public TestRng {
public:
    explicit HegelRng(hegel::TestCase& tc) : tc_(tc) {}

private:
    std::uint64_t raw(std::uint64_t inclusive_max, Distribution) override {
        namespace gs = hegel::generators;
        return tc_.draw(gs::integers<std::uint64_t>(
            {.min_value = 0, .max_value = inclusive_max}));
    }

    hegel::TestCase& tc_;
};

// ---------------------------------------------------------------------------
// AFL: parameters carved out of the fuzzer's testcase.
// ---------------------------------------------------------------------------

// The whole point of this backend is that the mapping from bytes to parameters
// is *simple and monotone*: consecutive input bytes become consecutive
// parameters, little-endian, and a byte the body compares against a constant
// appears in the testcase as that same constant. That is what lets AFL's
// coverage feedback and its CmpLog/redqueen instrumentation walk a long magic
// value in one byte at a time, instead of guessing it whole.
//
// So: no hashing, no PRNG seeded from the input, no rejection sampling. Reject
// sampling in particular would destroy the property, since it makes which byte
// feeds which parameter depend on the values of earlier bytes.
//
// Running off the end of the testcase yields zeroes rather than ending the
// case. AFL grows an input that reaches new coverage, so a short input that
// gets partway is exactly the seed the fuzzer needs to extend; refusing to run
// it would hide the coverage that motivates the extension.
//
// One AflRng is constructed per testcase, inside the persistent loop below, so
// each iteration starts drawing from byte zero of a fresh input.
class AflRng final : public TestRng {
public:
    explicit AflRng(std::span<const std::byte> input) : input_(input) {}

private:
    std::uint64_t raw(std::uint64_t inclusive_max, Distribution) override {
        if (inclusive_max == 0)
            return 0;

        // Consume exactly as many bytes as the domain is wide, so a byte-wide
        // domain consumes one byte and a byte-wide comparison in the body is a
        // byte-wide comparison against the testcase.
        int bytes = 0;
        for (std::uint64_t rest = inclusive_max; rest != 0; rest >>= 8)
            ++bytes;

        std::uint64_t value = 0;
        for (int i = 0; i < bytes; ++i) {
            const std::uint64_t byte =
                pos_ < input_.size()
                    ? static_cast<std::uint64_t>(input_[pos_])
                    : 0;
            ++pos_;
            value |= byte << (8 * i);
        }

        // The one place the mapping is not the identity. A modulo folds the
        // domain, so the byte AFL has to discover is no longer literally the
        // byte in the file -- but it only bites when the domain is not a whole
        // number of bytes wide, and the common cases (a byte, a 32-bit word, a
        // full-width integer) are exact.
        if (inclusive_max == std::numeric_limits<std::uint64_t>::max())
            return value;
        return value % (inclusive_max + 1);
    }

    std::span<const std::byte> input_;
    std::size_t pos_ = 0;
};

// Why the `afl` backend cannot run here, or empty if it can.
//
// Two independent conditions, and they fail for different reasons worth
// distinguishing in the message. BUILD_FUZZERS (and __AFL_HAVE_MANUAL_CONTROL,
// which afl-clang-fast++ defines for itself) say the binary is *instrumented*;
// they say nothing about whether afl-fuzz launched it. The shared-memory
// environment variables are what afl-fuzz sets in its child, so that is the
// runtime half of the question.
//
// The runtime half cannot be folded into a compile-time constant, because the
// instrumented binary is also run directly -- by ctest, and for crash replay --
// and it must not claim to be fuzzing then.
std::string afl_unusable_reason() {
#if !defined(BUILD_FUZZERS) || !defined(__AFL_HAVE_MANUAL_CONTROL)
    return "this is not an instrumented build (configure with the Fuzz preset, "
           "which builds with afl-clang-fast++ and BUILD_FUZZERS=ON)";
#else
    if (std::getenv("__AFL_SHM_FUZZ_ID") == nullptr &&
        std::getenv("__AFL_SHM_ID") == nullptr)
        return "the process is not running under afl-fuzz";
    return {};
#endif
}

// Default invocation budgets, used when max_invocations is left at zero. The
// exhaustive walk gets the largest because it stops on its own as soon as the
// space is covered -- the cap is only there so a body with an accidentally huge
// space fails in finite time rather than hanging the suite.
constexpr std::uint64_t kDefaultExhaustiveInvocations = 1'000'000;
constexpr std::uint64_t kDefaultRandomInvocations = 1'000;

// Testcases one forked child serves before AFL recycles it, when
// max_invocations is left at zero. AFL's own conventional value.
constexpr std::uint64_t kDefaultAflIterations = 10'000;

// Hand `body` a tightly-sized heap copy of the testcase.
//
// This copy is load-bearing, not hygiene. AFL's testcase buffer is a ~1 MB
// shared-memory region, so a read one byte past the end of the input still
// lands in valid mapped memory: ASAN sees nothing and the fuzzer runs forever
// finding no bug. A fresh, exactly-sized `new[]` puts an ASAN redzone right
// after the last byte, turning any over-read into an observable crash.
void draw_from_testcase(const std::byte* data,
                        std::size_t size,
                        const std::function<void(TestRng&)>& body) {
    std::unique_ptr<std::byte[]> tight(new std::byte[size]);
    std::memcpy(tight.get(), data, size);
    AflRng rng(std::span<const std::byte>(tight.get(), size));
    body(rng);
}

}  // namespace

}  // namespace test_rng

// The AFL persistent loop, at FILE SCOPE and deliberately outside
// `namespace test_rng`.
//
// __AFL_LOOP expands to a block containing `extern int __afl_connected;` with
// no __asm__ label, so inside a namespace that declaration mangles to
// `test_rng::__afl_connected` and the link fails with an undefined reference.
// The AFL-macro code therefore has to stay at global scope.
//
// It is also a plain non-template function in one .cc, so __AFL_INIT() and the
// loop are emitted exactly once per process no matter how many randomized tests
// exist. That "exactly once" is the property the old FUZZ_TARGET design existed
// to protect, and it survives the move: the provider is what enters the loop,
// and a provider run is what a randomized test already was.
//
// `invoke` returns whether the search should continue; here it is consulted
// only so a stop_on_failure run stops iterating. Under a real fuzzing run the
// body normally kills the process instead, which is what AFL records as a crash.
static void test_rng_afl_loop(
    const std::function<void(std::span<const std::byte>)>& iteration,
    std::uint64_t max_iterations) {
#ifdef __AFL_HAVE_MANUAL_CONTROL
    // Persistent mode + shared memory. __AFL_INIT() starts the fork server
    // here, after doctest startup, so every subsequent testcase forks from a
    // process that has already paid that cost.
    __AFL_INIT();
    const std::uint8_t* buf = __AFL_FUZZ_TESTCASE_BUF;
    while (__AFL_LOOP(max_iterations)) {
        const std::size_t len = __AFL_FUZZ_TESTCASE_LEN;
        iteration(std::span<const std::byte>(
            reinterpret_cast<const std::byte*>(buf), len));
    }
#else
    // Unreachable in practice: the provider refuses the afl backend outside an
    // instrumented build, so this arm exists only to keep the file compiling
    // under a plain compiler.
    (void)iteration;
    (void)max_iterations;
#endif
}

namespace test_rng {

TestRngProvider::TestRngProvider(Backend backend) : backend_(backend) {
    // An afl request that cannot work is a configuration error, and it is
    // rejected here rather than at run() so the error names the mistake at the
    // point it is made. Silently degrading would be the worst outcome
    // available: a fuzzing backend that is not being fuzzed tests nothing, and
    // would look exactly like a passing search.
    if (backend_ == Backend::Afl) {
        const std::string reason = afl_unusable_reason();
        if (!reason.empty())
            throw std::runtime_error(
                "test_rng: the afl backend was requested but cannot run: " +
                reason);
    }
}

TestRngProvider::TestRngProvider()
    : TestRngProvider([] {
          const char* env = std::getenv("TEST_RNG");
          if (env == nullptr || *env == '\0')
              return Backend::Smoke;
          const std::optional<Backend> parsed = parse_backend(env);
          if (!parsed)
              throw std::runtime_error(
                  "test_rng: TEST_RNG=\"" + std::string(env) +
                  "\" is not a known backend (expected one of: exhaustive, "
                  "random, afl, smoke)");
          return *parsed;
      }()) {}

TestRngProvider::~TestRngProvider() = default;

RunReport TestRngProvider::run(const std::function<void(TestRng&)>& body) {
    RunReport report = dispatch(body);
    if (report.found_failure)
        throw std::runtime_error(report.failure_message);
    return report;
}

RunReport TestRngProvider::search(const std::function<void(TestRng&)>& body) {
    return dispatch(body);
}

RunReport TestRngProvider::dispatch(const std::function<void(TestRng&)>& body) {
    RunReport report;
    report.backend = backend_;

    // Shared by the two backends that run the body themselves (the third,
    // Hegel, owns its own loop and reports through its own exception). Records
    // the first failure and says whether the search should keep going.
    const auto invoke = [&](TestRng& rng) {
        ++report.invocations;
        try {
            body(rng);
        } catch (const std::exception& e) {
            if (!report.found_failure) {
                report.found_failure = true;
                report.failure_message = e.what();
            }
            return !stop_on_failure;
        }
        return true;
    };

    switch (backend_) {
        case Backend::Smoke: {
            SmokeRng rng;
            invoke(rng);
            break;
        }

        case Backend::Exhaustive: {
            const std::uint64_t budget = max_invocations != 0
                                             ? max_invocations
                                             : kDefaultExhaustiveInvocations;
            // The do-while from exhaustigen's header. One Gen spans the whole
            // walk; a fresh TestRng per pass, because the header promises the
            // reference is good for one invocation only.
            exhaustigen::Gen gen;
            do {
                ExhaustiveRng rng(gen);
                if (!invoke(rng))
                    break;
                if (report.invocations >= budget)
                    break;
            } while (!gen.is_done());
            break;
        }

        case Backend::Random: {
            hegel::Settings settings;
            settings.test_cases = max_invocations != 0
                                      ? max_invocations
                                      : kDefaultRandomInvocations;
            // Quiet and deterministic for the same reason as everywhere else in
            // this suite: a search that prints a failure report on a *passing*
            // run is noise, and a seed that varies per run is a flake waiting
            // to happen.
            settings.verbosity = hegel::Verbosity::Quiet;
            settings.derandomize = true;
            settings.print_blob = false;

            // Hegel counts invocations itself and does not expose the count, so
            // the wrapper counts them -- including the replays that shrinking
            // performs, which is the honest number: they are body invocations.
            try {
                hegel::test(
                    [&](hegel::TestCase& tc) {
                        HegelRng rng(tc);
                        ++report.invocations;
                        body(rng);
                    },
                    settings);
            } catch (const std::exception& e) {
                report.found_failure = true;
                // Hegel's message is the shrunk counterexample plus its report,
                // which is more useful than the raw what() the body threw.
                report.failure_message = e.what();
            }
            break;
        }

        case Backend::Afl: {
            // The loop belongs to afl-fuzz, which decides how long the run
            // lasts; max_invocations only caps how many testcases one forked
            // child serves before AFL recycles it. Everything else is the same
            // shape as the other backends: one fresh TestRng per pass, handed
            // to the same body.
            const std::uint64_t iterations = max_invocations != 0
                                                 ? max_invocations
                                                 : kDefaultAflIterations;
            // `invoke` records the first failure and honours stop_on_failure,
            // exactly as it does for the smoke and exhaustive walks. Note that
            // it also *catches*: under a live fuzzing run a body that throws
            // would otherwise unwind out of the persistent loop and take the
            // fork server with it, which AFL reports as a broken target rather
            // than as the crash it is. A body that wants AFL to log a crash
            // should terminate the process itself.
            bool keep_going = true;
            test_rng_afl_loop(
                [&](std::span<const std::byte> input) {
                    if (!keep_going)
                        return;
                    draw_from_testcase(input.data(), input.size(),
                                       [&](TestRng& rng) {
                                           keep_going = invoke(rng);
                                       });
                },
                iterations);
            break;
        }
    }

    return report;
}

}  // namespace test_rng
