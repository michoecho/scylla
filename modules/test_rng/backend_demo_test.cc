// Three planted bugs, one per backend, and the checks that each backend finds
// the bug it is suited to and misses the ones it is not.
//
// The point is not that any one of these searches is impressive; each is a toy.
// The point is the *pattern of hits and misses*. If every backend found every
// bug, the interface would be pointless -- one engine would do. What the table
// below shows is that the three engines fail in different directions, so a
// property written against TestRng gets three genuinely different attempts at
// falsifying it for the price of one test body.
//
//                     exhaustive  random   afl   libafl  fuzztest  smoke
//   semiprime factor      yes       no      no     no       no       no
//   magic 5 bytes          no       no     yes    yes      yes       no
//   boundary in a wide     no      yes      no     no       no       no
//   domain
//
// afl, libafl and fuzztest share a column of results because they are one
// strategy -- coverage-guided mutation of a byte string, carved into parameters
// by the same rule -- run by three different engines. What differs is not what
// they find but what it costs to run them: AFL needs a fork server and an
// external afl-fuzz process, so its column is claimed from outside the suite
// (see below), while LibAFL and FuzzTest run in this process and can therefore
// be ordinary test cases. That is the entire reason those two backends exist,
// and the tests below are the evidence: same body, same find, no external
// fuzzer.
//
// That three engines share one column is the honest result, and worth saying
// plainly: this bug does not discriminate between them and was never going to.
// It was built to have a per-byte coverage gradient, which is the one thing
// every coverage-guided engine can see. What the fuzztest column adds is not a
// new capability but a second data point for the interface claim -- that the
// engine really is interchangeable behind TestRng, and that a body tuned years
// ago against AFL is driven unmodified by an engine it never anticipated.
//
// The AFL column is the one that cannot be checked in an ordinary test run:
// that backend needs a live afl-fuzz around the process, which is a thing no
// test case can be while also being the target. So it is split in two. The
// target is here -- "afl guesses the magic bytes", the same
// magic_bytes_are_unguessable body under the afl backend, inert unless
// something is fuzzing it. An external afl-fuzz invocation points directly at
// the Buck2-built binary.
//
// A note on how these tests are written. The bodies below signal a bug by
// throwing, and are run with TestRngProvider::search(), which reports the
// failure rather than rethrowing it. That inversion -- a test that passes when
// the search finds something -- is what lets a *found* bug be an assertion, and
// it is the same trick modules/main/hegel_test.cc uses.
//
// Every search here is capped hard (max_invocations) so the suite's cost is
// bounded and the misses are misses within a stated budget rather than "we got
// bored". The numbers are chosen to be comfortably more than the matching
// backend needs and comfortably less than a mismatched one would.

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <stdexcept>
#include <string>

#include <unistd.h>

#include "doctest/doctest.h"
#include "test_rng/test_rng.h"

namespace {

using test_rng::Backend;
using test_rng::Distribution;
using test_rng::RunReport;
using test_rng::TestRng;

// ---------------------------------------------------------------------------
// Bug 1: factor a semiprime. Suits the exhaustive walk.
// ---------------------------------------------------------------------------

// 61 * 67. Both factors are inside the guess domain below, so the "bug" is
// reachable -- but only by trying the right number.
constexpr std::uint32_t kSemiprime = 61 * 67;

// The largest factor a guess may be. Deliberately just wide enough that the
// exhaustive walk covers it in a few hundred passes and a uniform random draw
// has a ~2-in-256 chance per attempt.
constexpr std::uint32_t kMaxFactor = 255;

// The property: "no guess in [2, 255] divides kSemiprime". It is false, and
// the two guesses that falsify it are 61 and 67.
//
// This is what a *complete* search is uniquely good at. The domain has 254
// points, so exhaustion visits every one of them and cannot miss; more
// interestingly, had the property been true, exhaustion would have *proved* it
// rather than merely failed to refute it. No amount of random sampling gives
// that.
void semiprime_has_no_small_factor(TestRng& rng) {
    const std::uint32_t guess =
        rng.integer<std::uint32_t>({.min = 2, .max = kMaxFactor});
    if (kSemiprime % guess == 0)
        throw std::runtime_error("found factor " + std::to_string(guess));
}

// ---------------------------------------------------------------------------
// Bug 2: guess five specific bytes. Suits AFL.
// ---------------------------------------------------------------------------

// The needle: five specific bytes out of 2^40.
//
// The body below draws all five *unconditionally* and only then compares them.
// That ordering is the whole design of this bug. The obvious alternative --
// return on the first mismatching byte, before drawing the rest -- makes "the
// first i bytes are right" a *shorter choice sequence*, and every incremental
// engine climbs that for free: exhaustigen enumerates byte 0, extends to byte 1
// on the single value that survives, and walks the needle in a few thousand
// passes. A gradient every engine can see demonstrates nothing.
//
// Drawing all five first fixes the parameter space at 2^40 points regardless of
// how many are correct, which puts it out of reach of exhaustion (too many
// points) and of Hegel (one point in a trillion, and nothing about it is near
// an edge of the domain to bias towards). What remains is a gradient only AFL
// can see: with the comparisons arranged as below, a longer correct prefix
// executes a branch no earlier input reached, so AFL records new coverage,
// keeps the input, and mutates onward from it. That turns one 2^40 search into
// five 2^8 ones.
//
// Five was chosen by measurement, and the measurements are worth keeping since
// they are the evidence that this backend is doing what is claimed. From a cold
// "aaaaa" seed AFL saves a crash in ~180-280ms / 17-29k execs across repeated
// runs, with the occasional unlucky one at ~1.8s / 190k.
//
// Length is not what bounds this. Eight bytes ("hegelian", with three more
// unrolled compares) also finds it on every run, in ~0.5-3.7s / 58-440k execs
// -- roughly 4x the cost of five, which is what three more 2^8 steps should
// cost, not a wall. afl-showmap confirms why: across the prefixes "xxxxxxxx",
// "hxxxxxxx", ... "hegeliax", each additional correct byte swaps in exactly one
// distinct new edge, so the gradient holds cleanly to eight.
//
// This paragraph previously claimed the opposite -- that eight bytes did not
// work, 2.4M execs finding nothing, because the chain of per-step gradients
// gets fragile past a few bytes. The observation was real but the diagnosis was
// wrong, and it is worth recording which. That run predated the barriers below,
// and was built at -O2; without either, clang tail-merges the failure paths and
// a correct prefix generates *zero* new coverage, at any length. The failure was
// a missing gradient, not a long one. Five is kept because it is ~4x cheaper in
// a suite that runs this every time, and 2^40 is already comfortably beyond the
// other backends.
constexpr char kMagic[] = "hegel";
constexpr std::size_t kMagicLen = sizeof(kMagic) - 1;

void magic_bytes_are_unguessable(TestRng& rng) {
    std::uint8_t guess[kMagicLen];
    for (std::size_t i = 0; i < kMagicLen; ++i)
        guess[i] = rng.integer<std::uint8_t>();

    // The comparisons are unrolled, and separated by a compiler barrier. Both
    // details are load-bearing, and both were established by measurement after
    // the obvious versions silently failed.
    //
    // Written as a loop -- `for (i) if (guess[i] != kMagic[i]) return;` -- the
    // needle is invisible: a loop is one branch that iterates, so "two bytes
    // correct" and "no bytes correct" light up the same edge.
    //
    // Unrolling alone is not enough either, which is the part worth recording.
    // Optimized, the five compares survive as five `cmp`/`jne` pairs -- but all
    // five jne's target the *same* block, because every failure path here does
    // exactly the same thing (return, no side effects). That is clang's
    // tail-merging, and it is fatal: AFL instruments basic blocks, so five
    // branches sharing one successor bump one map entry. Measured at -O2 with
    // afl-showmap: "xxxxx" through "hegex" produce a byte-identical map -- a
    // correct prefix generates *zero* new coverage, and the search never
    // completes (13.2M execs found nothing).
    //
    // Two independent fixes, and the distinction matters when writing new bugs:
    //
    //   - The Fuzz preset builds at -O0, which does not tail-merge. The five
    //     paths stay five blocks and the gradient is there for free. This is
    //     why the preset is Debug rather than RelWithDebInfo.
    //   - The barrier below makes the optimizer treat `guess` as escaping
    //     between comparisons, which keeps the paths distinct even at -O2. It
    //     is redundant at -O0 and kept only so this body does not silently lose
    //     its gradient if the preset ever moves back.
    //
    // Note what does *not* fix it. cmplog (-c, the FuzzCmplog preset) can read
    // the byte literals straight out of the compare instructions, but it cannot
    // help: an input with one correct byte registers no new coverage, so it is
    // never queued, never becomes the base for the next substitution, and the
    // prefix cannot accumulate. Measured: 2.4M execs, nothing. laf-intel
    // (AFL_LLVM_LAF_ALL) does not help either -- it splits wide comparisons
    // into narrow ones, and these are already byte-sized; the collapse is in
    // the successors, not the compares. Measured: 1.65M execs, nothing.
    //
    // cmplog and laf-intel solve the opposite shape -- one *wide* comparison,
    // where the compare is atomic rather than the paths merged. Replacing the
    // first four bytes here with a single uint32 compare makes plain AFL fail
    // (2M execs) and both of those succeed (6k-68k execs). Coverage defeats
    // depth, cmplog and laf defeat width, and neither substitutes for the
    // other.
    //
    // The general lesson for anyone writing a bug for this backend: coverage
    // feedback rewards *distinct code paths*, not distinct values -- and it is
    // the paths in the optimized binary that count, not the ones in the source.
    // Check with afl-showmap rather than assuming.
    const auto barrier = [&] { asm volatile("" : : "r"(guess) : "memory"); };

    if (guess[0] != static_cast<std::uint8_t>(kMagic[0])) return;
    barrier();
    if (guess[1] != static_cast<std::uint8_t>(kMagic[1])) return;
    barrier();
    if (guess[2] != static_cast<std::uint8_t>(kMagic[2])) return;
    barrier();
    if (guess[3] != static_cast<std::uint8_t>(kMagic[3])) return;
    barrier();
    if (guess[4] != static_cast<std::uint8_t>(kMagic[4])) return;
    barrier();
    static_assert(kMagicLen == 5, "one comparison per byte, unrolled above");

    throw std::runtime_error("guessed the magic bytes");
}

// ---------------------------------------------------------------------------
// Bug 3: a boundary in a domain too wide to enumerate. Suits Hegel.
// ---------------------------------------------------------------------------

// A size computation with the classic off-by-one at the top of the range: it is
// correct for every input except the largest one, where the +1 overflows to
// zero and the "capacity" it returns is nonsense.
//
// This is Hegel's shape. The domain has 2^32 points, so exhaustion cannot cover
// it; the single bad input is one value in four billion, so a uniform draw --
// which is what the AFL backend's byte-carving amounts to without a coverage
// gradient to climb -- essentially never lands on it. But "the extremes of the
// range" is precisely where Hegel's integer generator concentrates its draws,
// so it finds this almost immediately.
//
// The general lesson is that this is the commonest bug shape of the three, not
// the most exotic: off-by-ones at boundaries are what most real integer bugs
// are. That is why Random, not Exhaustive, is the default the header
// recommends.
std::uint32_t buggy_capacity(std::uint32_t size) {
    return size + 1;
}

void capacity_exceeds_size(TestRng& rng) {
    // EdgeBiased is the hint that says "the ends of this range are where the
    // bugs are". The exhaustive and AFL backends ignore it, as the header says
    // they may; Hegel is the one that acts on it.
    const auto size =
        rng.integer<std::uint32_t>({.dist = Distribution::EdgeBiased});
    if (buggy_capacity(size) <= size)
        throw std::runtime_error("capacity(" + std::to_string(size) +
                                 ") did not exceed its size");
}

// Run `body` under `backend` with a fixed budget, reporting rather than
// rethrowing a failure. `search` is what makes "did not find it" observable.
RunReport hunt(Backend backend, void (*body)(TestRng&),
               std::uint64_t budget) {
    test_rng::TestRngProvider provider(backend);
    provider.max_invocations = budget;
    return provider.search(body);
}

// Budgets. Generous enough that a miss is a statement about the backend rather
// than about the cap, and small enough that the whole file is a fraction of a
// second.
constexpr std::uint64_t kBudget = 500;

// Exit status the AFL case below uses to signal "found it", paired with
// AFL_CRASH_EXITCODE in the self-test. Any value AFL will not otherwise see
// works; what matters is that the two agree.
constexpr int kMagicCrashExitCode = 87;

}  // namespace

TEST_SUITE("test_rng") {

// --- Bug 1 -----------------------------------------------------------------

TEST_CASE("exhaustive walk factors the semiprime") {
    const RunReport report =
        hunt(Backend::Exhaustive, semiprime_has_no_small_factor, kBudget);
    REQUIRE(report.found_failure);
    // 61 is the smaller factor and the walk counts upward from 2, so it is
    // always the one reported: a complete search is also a *deterministic*
    // one, which is why this can assert the exact counterexample.
    CHECK(report.failure_message == "found factor 61");
    // And it got there in the number of passes counting from 2 to 61 takes.
    CHECK(report.invocations == 60);
}

TEST_CASE("random and smoke miss the semiprime's factors") {
    // Hegel is not blind here -- 2 in 254 per draw is not long odds -- so this
    // is not a claim that it *cannot* find it. It is pinned to a deliberately
    // small budget to make the contrast visible: with 20 draws from a
    // 254-point domain the odds are against it, and being derandomized (see
    // the Settings in test_rng.cc) makes the outcome stable rather than a
    // coin flip that would flake this suite.
    const RunReport random =
        hunt(Backend::Random, semiprime_has_no_small_factor, 20);
    CHECK_FALSE(random.found_failure);

    // Smoke takes the low end of every domain: guess = 2, which does not
    // divide an odd semiprime. One invocation, no search.
    const RunReport smoke =
        hunt(Backend::Smoke, semiprime_has_no_small_factor, 0);
    CHECK_FALSE(smoke.found_failure);
    CHECK(smoke.invocations == 1);
}

// --- Bug 2 -----------------------------------------------------------------

TEST_CASE("random and exhaustive cannot guess the magic bytes") {
    const RunReport random =
        hunt(Backend::Random, magic_bytes_are_unguessable, kBudget);
    CHECK_FALSE(random.found_failure);

    // Exhaustion does not fail for want of luck -- it fails for want of time.
    // Its walk over 2^64 points is cut off by the budget having covered a
    // vanishing fraction of the space, which is the honest characterisation of
    // an exhaustive search pointed at a domain this wide.
    const RunReport exhaustive =
        hunt(Backend::Exhaustive, magic_bytes_are_unguessable, kBudget);
    CHECK_FALSE(exhaustive.found_failure);
    CHECK(exhaustive.invocations == kBudget);
}

TEST_CASE("the afl backend refuses to run when it cannot fuzz") {
    // The promise is that an unusable request is an error rather than a silent
    // fallback: a fuzzing backend that is not being fuzzed tests nothing, and
    // quietly "passing" would be the worst of the available outcomes.
    //
    // Outside a live fuzzing run -- which is every ordinary buck2 test run,
    // instrumented or not -- constructing the provider therefore throws. Under
    // afl-fuzz it succeeds.
    //
    // The extra parentheses keep this an expression: without them
    // `TestRngProvider(Backend::Afl)` parses as a declaration of a variable
    // named Afl, which is the most-vexing-parse in its least useful form.
    if (std::getenv("__AFL_SHM_FUZZ_ID") == nullptr &&
        std::getenv("__AFL_SHM_ID") == nullptr)
        CHECK_THROWS_AS((test_rng::TestRngProvider(Backend::Afl)),
                        std::runtime_error);

    // Either way it is a legal *name*, so TEST_RNG=afl is a request the parser
    // understands and reports on, rather than an unknown-backend error.
    CHECK(test_rng::parse_backend("afl") == Backend::Afl);
    CHECK(test_rng::backend_name(Backend::Afl) == "afl");
}

// The positive half of bug 2: AFL actually guessing the five bytes.
//
// This is an ordinary test case, and that is the whole point of the change it
// came from. It is the *same* body the three in-process backends were pointed
// at above, driven through the same provider; only the backend differs. Under
// afl-fuzz the provider's persistent loop serves thousands of testcases to it,
// and the body signals a find by killing the process.
//
// It is skipped unless something is actually fuzzing us, since the provider
// refuses the backend otherwise -- and afl-fuzz reaches it by name:
//
//     TEST_RNG=afl afl-fuzz -i in -o out -- <binary> \
//         --test-case='afl guesses the magic bytes'
//
// Buck2 builds the instrumented test binary; the case can then be selected
// directly with the command above.
TEST_CASE("afl guesses the magic bytes") {
    if (std::getenv("__AFL_SHM_FUZZ_ID") == nullptr &&
        std::getenv("__AFL_SHM_ID") == nullptr)
        return;

    test_rng::TestRngProvider provider(Backend::Afl);
    provider.search([](TestRng& rng) {
        try {
            magic_bytes_are_unguessable(rng);
        } catch (const std::runtime_error&) {
            // Signal the find by exiting with a distinguished code rather than
            // abort(). A fatal signal makes the kernel run core_pattern, which
            // on a desktop is a pipe to systemd-coredump -- a journal entry and
            // a KDE crash notification per crash, thousands of times over a
            // run. RLIMIT_CORE=0 does not prevent this: for a piped
            // core_pattern the helper runs regardless of the limit. Exiting
            // normally is the only way to keep the kernel out of it.
            //
            // _exit() rather than exit(): this runs inside the persistent loop
            // and must terminate immediately, without flushing doctest's state.
            // AFL sees the exit code, records a crash, and forks a fresh child.
            ::_exit(kMagicCrashExitCode);
        }
    });
}

// The positive half of bug 2 again, in-process this time.
//
// This is the test the libafl backend was added for. It is the *same* body the
// AFL case above fuzzes and the same one the exhaustive and random backends
// fail on, driven through the same provider -- but it is an ordinary test case
// with no external fuzzer, no fork server and no self-re-exec, because
// LibAFL's executor calls the body as a function in this process.
//
// Skipped unless the binary is instrumented, since the provider refuses the
// backend otherwise. That is the LibAfl preset:
//
//     buck2 test --modifier root//:libafl //modules/test_rng:test_rng_test
//
// The budget is worth explaining, because the obvious small number is wrong and
// the measurements say why. Across seeds the cost of this search varies by more
// than an order of magnitude: 26k, 55k, 63k, 99k, 101k invocations on five
// seeds, but 567k and 753k on two others. Every one of them finds it -- none
// ran out of gradient -- so the spread is how long a mutation-driven search
// takes to stumble onto the first correct byte, not whether it can.
//
// That is the honest character of this backend and the reason the budget is
// 2M rather than the 400k an early version used: at 400k the two unlucky seeds
// above report "not found", which would have made this test a coin flip
// disguised as an assertion. Invocations are cheap enough (~1M/s, no fork per
// testcase) that 2M costs well under a second even in the worst case measured.
//
// The seed itself is fixed in the provider, so this test is deterministic; the
// spread matters only because a *future* change to the body or the engine would
// land somewhere else in that distribution.
// Named "in-process" rather than the obvious "libafl guesses the magic bytes",
// and the reason is a trap worth leaving signposted: keeping the backend name
// in the case makes the two demonstrations easy to distinguish.
TEST_CASE("libafl guesses the magic bytes in-process") {
    if (!test_rng::libafl_available())
        return;

    test_rng::TestRngProvider provider(Backend::LibAfl);
    provider.max_invocations = 2'000'000;

    // search() rather than run(), so the *find* is the assertion -- the same
    // inversion every other demonstration here uses.
    const RunReport report = provider.search(magic_bytes_are_unguessable);
    REQUIRE(report.found_failure);
    CHECK(report.failure_message == "guessed the magic bytes");
}

// The positive half of bug 2 a third time, under a second in-process engine.
//
// Everything said about the libafl case applies here: same body, same provider,
// ordinary test case, no external fuzzer. The only line that differs between the
// two tests is the backend named in the constructor, and that is the claim the
// interface makes -- so this test is worth having precisely because it is
// boring.
//
// Skipped unless the binary is instrumented, since the provider refuses the
// backend otherwise. That is the FuzzTest preset:
//
//     buck2 test --modifier root//:fuzztest //modules/test_rng:test_rng_test
//
// The search is much cheaper than libafl's, and the reason is the one
// interesting difference between the engines. FuzzTest is built with
// -fsanitize-coverage=trace-cmp as well as edge coverage, so its table of recent
// compares sees the operands of `guess[i] != kMagic[i]` directly and can
// substitute the byte it just watched being compared, instead of waiting for a
// mutation to land on it. Where LibAFL's cost ranged from 26k to 753k
// invocations, twelve runs here measured 9.4k, 12k, 17k, 19k, 22k, 23k, 24k,
// 44k, 48k, 51k, 51k, 104k.
//
// That spread is worth a warning, because it is *not* seed noise and cannot be
// removed by pinning things. The provider fixes FUZZTEST_PRNG_SEED for the same
// derandomizing reason the Hegel and libafl backends fix theirs, and the seed
// really is applied -- the engine echoes it back. Runs still vary by more than
// 10x. Disabling ASLR does not settle it either, which rules out the other
// obvious culprit (Abseil's per-process hash salt reordering the cmp table).
// What is left is the engine's own time-dependent scheduling, so this backend
// is reproducible in what it finds but not in how long it takes.
//
// Hence a budget of 1M against a worst case of 104k. It is deliberately ~10x
// the slowest run measured rather than a snug fit, and it is close to free: the
// budget only bounds a search that finds *nothing*, and this one always finds
// something in well under a tenth of it. Sizing it tightly would buy no speed
// and would turn the tail of that distribution into a flaky test -- which is
// the mistake the libafl budget's own history records.
//
// Note what has *not* changed: the body. It still draws all five bytes before
// comparing any, still unrolls the comparisons, still keeps the barriers that
// stop clang tail-merging the failure paths. cmplog-style feedback does not
// rescue a body with no coverage gradient -- the AFL notes above record 2.4M
// execs finding nothing when that was tried -- it only makes an existing
// gradient cheaper to climb. Both are needed, which is why this body works
// unmodified under all three engines.
TEST_CASE("fuzztest guesses the magic bytes in-process") {
    if (!test_rng::fuzztest_available())
        return;

    test_rng::TestRngProvider provider(Backend::FuzzTest);
    provider.max_invocations = 1'000'000;

    // search() rather than run(), so the *find* is the assertion -- the same
    // inversion every other demonstration here uses.
    const RunReport report = provider.search(magic_bytes_are_unguessable);
    REQUIRE(report.found_failure);
    CHECK(report.failure_message == "guessed the magic bytes");
}

TEST_CASE("the fuzztest backend runs at most once per process") {
    if (!test_rng::fuzztest_available())
        return;

    // FuzzTest's runtime is a process-wide singleton whose termination flag can
    // be set but not cleared, and setting it is how the search above stopped
    // early. A second run would see it already set, stop before its first
    // mutation and report a clean pass -- a passing search that searched
    // nothing, which is the exact failure every availability check in this file
    // exists to prevent. So the provider refuses.
    //
    // This test depends on the case above having already run, which doctest's
    // declaration order gives us within a file.
    test_rng::TestRngProvider provider(Backend::FuzzTest);
    CHECK_THROWS_AS(provider.search(magic_bytes_are_unguessable),
                    std::runtime_error);
}

TEST_CASE("the fuzztest backend refuses to run without coverage") {
    // The same promise, and the same reasoning, as the libafl case below: a
    // coverage-guided search with no coverage is a slow random search that
    // reports exactly like a passing one.
    if (!test_rng::fuzztest_available())
        CHECK_THROWS_AS((test_rng::TestRngProvider(Backend::FuzzTest)),
                        std::runtime_error);

    CHECK(test_rng::parse_backend("fuzztest") == Backend::FuzzTest);
    CHECK(test_rng::backend_name(Backend::FuzzTest) == "fuzztest");
}

TEST_CASE("the libafl backend refuses to run without coverage") {
    // The same promise the afl backend makes, and it matters more here. An
    // uninstrumented afl run cannot start at all; an uninstrumented libafl run
    // would happily execute its whole budget against an all-zero coverage map
    // and report a clean pass, which is indistinguishable from a property that
    // is actually true. So the provider checks and throws.
    if (!test_rng::libafl_available())
        CHECK_THROWS_AS((test_rng::TestRngProvider(Backend::LibAfl)),
                        std::runtime_error);

    // And it is a legal name either way, so TEST_RNG=libafl is a request the
    // parser understands rather than an unknown-backend error.
    CHECK(test_rng::parse_backend("libafl") == Backend::LibAfl);
    CHECK(test_rng::backend_name(Backend::LibAfl) == "libafl");
}

// --- Bug 3 -----------------------------------------------------------------

TEST_CASE("random finds the boundary in a 2^32 domain") {
    const RunReport report = hunt(Backend::Random, capacity_exceeds_size, 200);
    REQUIRE(report.found_failure);
    // The counterexample is the top of the range, and it is the *only* one, so
    // whatever Hegel shrinks to has to be it.
    CHECK(report.failure_message.find("4294967295") != std::string::npos);
}

TEST_CASE("exhaustive and smoke miss the boundary") {
    // The walk starts at 0 and counts up. The bad value is 2^32 - 1, so within
    // any budget this suite can afford it never gets remotely close -- and it
    // burns the entire budget failing to.
    const RunReport exhaustive =
        hunt(Backend::Exhaustive, capacity_exceeds_size, kBudget);
    CHECK_FALSE(exhaustive.found_failure);
    CHECK(exhaustive.invocations == kBudget);

    // Smoke's representative value is 0, and capacity(0) == 1 is correct.
    const RunReport smoke = hunt(Backend::Smoke, capacity_exceeds_size, 0);
    CHECK_FALSE(smoke.found_failure);
}

// --- The whole point -------------------------------------------------------

TEST_CASE("TEST_RNG selects the backend") {
    // The default-constructed provider is what an ordinary randomized test uses,
    // so what TEST_RNG does to it is part of the contract rather than an
    // implementation detail. setenv is process-global, so the original value is
    // restored before leaving.
    const char* saved = std::getenv("TEST_RNG");
    const std::string original = saved != nullptr ? saved : "";
    const bool had_original = saved != nullptr;

    const auto set = [](const char* value) {
        if (value != nullptr)
            ::setenv("TEST_RNG", value, 1);
        else
            ::unsetenv("TEST_RNG");
    };

    // Unset means Smoke: an ordinary buck2 test run executes every randomized body
    // once, cheaply, rather than launching a search nobody asked for.
    set(nullptr);
    CHECK(test_rng::TestRngProvider().backend() == Backend::Smoke);

    // As does an empty value, which is what an unset-but-exported shell
    // variable looks like from here.
    set("");
    CHECK(test_rng::TestRngProvider().backend() == Backend::Smoke);

    set("exhaustive");
    CHECK(test_rng::TestRngProvider().backend() == Backend::Exhaustive);

    set("random");
    CHECK(test_rng::TestRngProvider().backend() == Backend::Random);

    // A name that is not a backend is a configuration error, reported rather
    // than quietly ignored -- a typo'd TEST_RNG that silently ran the smoke
    // backend would look exactly like a passing search.
    set("bogus");
    CHECK_THROWS_AS(test_rng::TestRngProvider(), std::runtime_error);

    // And `afl` parses, but in an ordinary test run there is no afl-fuzz around
    // the process, so it throws for that reason instead. Either way the user
    // hears about it rather than getting a silent fallback.
    if (std::getenv("__AFL_SHM_FUZZ_ID") == nullptr &&
        std::getenv("__AFL_SHM_ID") == nullptr) {
        set("afl");
        CHECK_THROWS_AS(test_rng::TestRngProvider(), std::runtime_error);
    }

    // Same for libafl, whose availability is a property of the build rather
    // than of the environment: selectable when the binary is instrumented, and
    // a reported error rather than a silent fallback when it is not.
    set("libafl");
    if (test_rng::libafl_available())
        CHECK(test_rng::TestRngProvider().backend() == Backend::LibAfl);
    else
        CHECK_THROWS_AS(test_rng::TestRngProvider(), std::runtime_error);

    // And fuzztest, on the same terms. Note this only *constructs* a provider --
    // which is the check being made -- and never runs one, so it does not spend
    // the single fuzztest search this process is allowed.
    set("fuzztest");
    if (test_rng::fuzztest_available())
        CHECK(test_rng::TestRngProvider().backend() == Backend::FuzzTest);
    else
        CHECK_THROWS_AS(test_rng::TestRngProvider(), std::runtime_error);

    set(had_original ? original.c_str() : nullptr);
}

TEST_CASE("the same body runs under every available backend") {
    // What the interface actually buys: one property, written once, driven by
    // whichever engine is asked for. This is the usage an ordinary test writes,
    // minus the explicit backend -- a default-constructed provider reads
    // TEST_RNG and picks for itself.
    for (const Backend backend :
         {Backend::Smoke, Backend::Exhaustive, Backend::Random}) {
        test_rng::TestRngProvider provider(backend);
        provider.max_invocations = 50;

        // A property that is actually true, so every backend agrees on it.
        const RunReport report = provider.run([](TestRng& rng) {
            const auto n = rng.integer<int>({.min = -100, .max = 100});
            if (n < -100 || n > 100)
                throw std::runtime_error("draw escaped its domain");
        });

        INFO("backend: ", test_rng::backend_name(backend));
        CHECK_FALSE(report.found_failure);
        CHECK(report.invocations > 0);
    }
}

}  // TEST_SUITE
