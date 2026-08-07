// A single interface for randomized tests, with interchangeable engines behind
// it.
//
// A randomized test is written once, as a function of a TestRng:
//
//     TEST_CASE("my property") {
//         TestRngProvider provider;
//         provider.run([](TestRng& rng) {
//             int x = rng.integer<int>({.min = 0, .max = 100});
//             CHECK(f(x) >= 0);
//         });
//     }
//
// The body says *what* the free parameters are; it does not say where they
// come from. TestRngProvider picks that at runtime from the TEST_RNG
// environment variable, so the same test body can be driven by an exhaustive
// walk of the parameter space, a smart random distribution, or a coverage-
// guided fuzzer -- see Backend below.
//
// That includes AFL. A fuzzing run is not a different kind of test here, it is
// the same test case under a different backend:
//
//     ctest                                          # smoke, one pass
//     TEST_RNG=random ./module_test                  # Hegel, with shrinking
//     afl-fuzz -i in -o out -- \
//         env TEST_RNG=afl ./module_test --test-case='my property'
//
// The provider owns the AFL persistent loop, so a test never writes one. This
// is the whole reason there is no separate "fuzz target" concept: a fuzz target
// was only ever a randomized test whose loop happened to live outside it, and
// splitting the two meant a property had to be written twice to get both.
//
// Why one interface rather than three test-writing styles: the strategies find
// different bugs, and which one suits a given property is rarely obvious in
// advance. A shallow, wide space wants exhaustion; a deep space with a narrow
// target wants coverage feedback; a space with structure worth shrinking wants
// Hegel. Writing to TestRng means a test gets all of them for the cost of one,
// and switching is an environment variable rather than a rewrite.
//
// --- the primitive ---------------------------------------------------------
//
// Every backend here can answer exactly one question: "give me an integer in
// [lo, hi]". That is the whole virtual surface (`raw`), and everything public
// is a non-virtual template on top of it. It is the largest primitive all
// three engines share -- exhaustigen enumerates a bounded choice, Hegel draws a
// bounded integer it knows how to shrink, and AFL consumes bounded bytes from
// its testcase -- so putting anything richer in the vtable would mean emulating
// it for the backends that lack it.
//
// Richer domains (strings, permutations, collections) are deliberately absent
// until something needs them; each should arrive as another non-virtual helper
// over `raw`, not as another virtual.

#pragma once

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <optional>
#include <string>
#include <type_traits>

namespace test_rng {

// How a backend should spend its choices within a domain, for the backends
// that can act on the hint. It is advice, not a contract: an exhaustive walk
// visits every value regardless, and a coverage-guided fuzzer follows its own
// feedback. Only the random backend is really steered by it.
enum class Distribution {
    // Every value in the domain is equally interesting.
    Uniform,
    // Values near the domain's edges (and near zero, when it is in range) are
    // where bugs cluster: off-by-ones, overflow, empty/full boundaries. Ask for
    // this when the domain is a size, a count, or an index.
    EdgeBiased,
};

// The domain of an integer parameter: an inclusive range plus the hint above.
//
// Defaulted to the full range of T so `rng.integer<int>()` is meaningful, and
// aggregate-initialized at the call site with designated initializers:
//
//     rng.integer<int>({.min = 1, .max = 6})
//     rng.integer<std::size_t>({.max = v.size() - 1, .dist = Distribution::EdgeBiased})
template <std::integral T>
struct IntegerDomain {
    T min = std::numeric_limits<T>::min();
    T max = std::numeric_limits<T>::max();
    Distribution dist = Distribution::Uniform;
};

// The interface a randomized test body draws its parameters from.
//
// Obtained from TestRngProvider::run; never constructed directly by a test. The
// reference handed to the body is valid only for that one invocation, so a test
// must not stash it -- the next invocation may be a different object entirely,
// and for the exhaustive backend it certainly is.
class TestRng {
public:
    virtual ~TestRng() = default;

    TestRng(const TestRng&) = delete;
    TestRng& operator=(const TestRng&) = delete;

    // An integer in `domain`, inclusive at both ends.
    //
    // Non-virtual: the range arithmetic and the signed/unsigned bridging are
    // identical for every backend, so they are done once here and the backend
    // only ever sees an unsigned width. An empty domain (min > max) is a bug in
    // the caller, not a case to generate for, so it throws rather than
    // silently picking min.
    //
    // `name` is what the parameter is called at the call site -- see
    // TEST_RNG_DRAW below, which is how a test normally supplies it. Nothing
    // about the search depends on it; it exists so a backend that reports a
    // counterexample can name the values in it rather than list them
    // positionally. A backend with no such report ignores it, which is why this
    // is a plain overload rather than a second virtual.
    template <std::integral T>
    T integer(IntegerDomain<T> domain = {}) {
        return integer<T>(nullptr, domain);
    }

    template <std::integral T>
    T integer(const char* name, IntegerDomain<T> domain) {
        if (domain.min > domain.max)
            throw_empty_domain();

        // Width as an unsigned value, computed in the unsigned counterpart of T
        // so that a full-range domain (min = INT_MIN, max = INT_MAX) does not
        // overflow the way `max - min` in T would. The +1 is applied in
        // std::uint64_t for the same reason: for a 64-bit full range the count
        // of values is 2^64, one past what the type holds, which is why `raw`
        // takes an inclusive bound rather than a count.
        using U = std::make_unsigned_t<T>;
        const U span = static_cast<U>(static_cast<U>(domain.max) -
                                      static_cast<U>(domain.min));

        const std::uint64_t offset =
            raw(name, static_cast<std::uint64_t>(span), domain.dist);

        // Back to T through the unsigned type: adding the offset to min in T
        // itself would be signed overflow for a domain in the upper half of the
        // range. Unsigned wraparound is well defined and the conversion back is
        // value-preserving in C++20 and later, which mandates two's complement.
        return static_cast<T>(static_cast<U>(static_cast<U>(domain.min) +
                                             static_cast<U>(offset)));
    }

    // Convenience for the commonest domain of all: a coin flip.
    bool boolean(const char* name = nullptr) {
        return integer<std::uint8_t>(name, {.min = 0, .max = 1}) != 0;
    }

    // A value in [0, n) -- an index into a container of size `n`.
    //
    // Edge-biased by default: the first and last elements are where indexing
    // bugs live. Requires n > 0; there is no index into an empty container, and
    // returning 0 for one would hand the caller an out-of-bounds subscript.
    std::size_t index(std::size_t n, const char* name = nullptr) {
        if (n == 0)
            throw_empty_domain();
        return integer<std::size_t>(
            name, {.min = 0, .max = n - 1, .dist = Distribution::EdgeBiased});
    }

protected:
    TestRng() = default;

    // The one thing a backend must implement: a value in [0, inclusive_max].
    //
    // Inclusive rather than a count, so that a full 64-bit domain (whose count
    // is 2^64) is expressible. `dist` is the hint from the domain, which a
    // backend is free to ignore, and `name` is the call site's name for this
    // parameter -- null when it was drawn through the unnamed overload, and
    // ignorable by any backend that has nothing to report it in.
    virtual std::uint64_t raw(const char* name,
                              std::uint64_t inclusive_max,
                              Distribution dist) = 0;

private:
    // Out of line, and out of the template above, so that <stdexcept> and the
    // message text are not pulled into every translation unit that draws an
    // integer.
    [[noreturn]] static void throw_empty_domain();
};

// The engine driving a run. Selected by the TEST_RNG environment variable,
// whose value is the lowercase name in the comment beside each enumerator.
enum class Backend {
    // "exhaustive" -- exhaustigen. Walks the entire reachable parameter space,
    // one point per invocation, and stops when it has visited all of them.
    // Complete, and therefore the only backend that can *prove* a property over
    // a small domain rather than fail to refute it. Useless on a large one: the
    // space is a product of every choice point, so it explodes.
    Exhaustive,

    // "random" -- Hegel. Random draws with a distribution that upweights the
    // values bugs cluster at, plus shrinking: a failure is minimized to a small
    // counterexample before it is reported. The general-purpose default.
    Random,

    // "afl" -- AFL++. Parameters are carved out of the fuzzer's testcase, so
    // the mutations are steered by the coverage the test body actually
    // achieves. The only backend that can solve a search whose target is
    // narrow but reachable by increments -- guessing a long magic value byte by
    // byte, say, which the other two would need astronomical luck for.
    //
    // Like every other backend, this one is driven by TestRngProvider::run: the
    // provider owns the persistent loop, and each iteration of it hands the body
    // one testcase's worth of draws. What makes it different is only who decides
    // when the loop ends -- afl-fuzz, from outside the process, rather than a
    // budget from inside it.
    //
    // Requires a BUILD_FUZZERS build *and* an actual afl-fuzz run around the
    // process. The provider errors out rather than silently degrading if either
    // is missing, since a fuzzing backend that is not being fuzzed tests
    // nothing. A test is therefore run under it like this:
    //
    //     afl-fuzz -i corpus -o out -- \
    //         env TEST_RNG=afl ./module_test --test-case='my property'
    Afl,

    // "smoke" -- no search at all. One invocation, every parameter taking a
    // fixed representative value from its domain. This is the default when
    // TEST_RNG is unset, so an ordinary `ctest` run executes every randomized
    // test body once, cheaply, and catches the errors that need no search at
    // all -- a body that does not compile against its domains, a property that
    // is simply false. The real searches are opt-in because they are slow.
    Smoke,
};

// Parses a backend name ("exhaustive", "random", "afl", "smoke").
// Returns nullopt for anything else, so the caller can report the bad value.
std::optional<Backend> parse_backend(std::string_view name);

// The name `parse_backend` accepts for `backend`.
std::string_view backend_name(Backend backend);

// How a run ended. Returned by TestRngProvider::run so a test can assert on the
// search itself -- the demonstration tests below check that a given backend
// does or does not find a planted bug.
struct RunReport {
    // Which engine actually ran.
    Backend backend = Backend::Smoke;
    // How many times the body was invoked.
    std::uint64_t invocations = 0;
    // Whether the body ever signalled a failure by throwing.
    bool found_failure = false;
    // The what() of the first failure, if any.
    std::string failure_message;
};

// Sets up a backend and runs a test body against it, many times over.
//
//     TestRngProvider provider;
//     provider.run([](TestRng& rng) { ... });
//
// Construction reads TEST_RNG and fixes the backend for this provider; run()
// then drives the body. A provider may be run more than once, and each run
// starts a fresh search.
//
// The budget knobs are members rather than run() parameters because they are
// properties of *this* test (how long it may take, how deep its space is), not
// of a particular invocation of it.
class TestRngProvider {
public:
    // Reads TEST_RNG. Throws std::runtime_error if it names an unknown backend,
    // or names `afl` in a build or process where AFL cannot work -- an
    // unusable explicit request is a configuration error worth reporting, not
    // something to paper over with a fallback. The afl check happens here, at
    // construction, so the error names the mistake before any searching starts.
    TestRngProvider();

    // A provider pinned to one backend, ignoring the environment. For the
    // tests below, which assert on how a *specific* engine behaves.
    explicit TestRngProvider(Backend backend);

    ~TestRngProvider();

    TestRngProvider(const TestRngProvider&) = delete;
    TestRngProvider& operator=(const TestRngProvider&) = delete;

    // Which backend this provider will use.
    Backend backend() const { return backend_; }

    // The most times the body may be invoked. What it means depends on the
    // backend: a hard cap on an exhaustive walk (which otherwise stops on its
    // own when the space is exhausted), the test-case budget for the random
    // backend, and a cap on persistent-loop iterations under AFL -- where
    // afl-fuzz, not this, decides when the run is really over, and the cap only
    // bounds how many testcases one forked child serves before it is recycled.
    //
    // Zero means "the backend's own default".
    std::uint64_t max_invocations = 0;

    // Stop the search at the first failure rather than continuing. On by
    // default, which is what a test wants; the demonstration tests turn it off
    // to measure how much of a space a backend covers.
    bool stop_on_failure = true;

    // Drive `body` under this provider's backend.
    //
    // A failure is signalled the way C++ signals everything else: the body
    // throws (a doctest REQUIRE does, as does any exception the code under test
    // lets escape). run() catches it, records it in the report, and -- unless
    // rethrow is false -- rethrows once the search has finished, so an ordinary
    // test fails the way it would without a provider.
    //
    // Note that doctest's CHECK does *not* throw: it records a failure and
    // carries on, which under a search means the failing case is not minimized
    // and the search does not stop. Use REQUIRE inside a body, or throw
    // directly.
    RunReport run(const std::function<void(TestRng&)>& body);

    // As run(), but a failure is reported rather than rethrown. This is what
    // lets a test assert that a search *failed to find* something, which is
    // half of the demonstration below.
    RunReport search(const std::function<void(TestRng&)>& body);

private:
    Backend backend_;

    RunReport dispatch(const std::function<void(TestRng&)>& body);
};

}  // namespace test_rng

// Declare a variable and draw its value in one step, recording the name.
//
//     TEST_RNG_DRAW(rng, exponent_bits,
//                   test_rng::IntegerDomain<unsigned>{.min = 0, .max = 64});
//
// This is the form a randomized test should prefer over a bare
// `rng.integer<T>(...)`. The two draw identically; what the macro adds is that
// the variable's own name travels with the draw, so a backend that reports a
// counterexample can print `exponent_bits = 64` rather than leaving the reader
// to match values against draws by position. Getting that wrong is easy and
// silent -- reordering two draws renames every value after them -- and the
// stringified identifier cannot drift from the variable it names.
//
// Only the random backend acts on it today; the others take the name and
// discard it. That is the same bargain as Distribution: a hint every backend
// accepts and each honours as far as it can.
//
// The domain must be written as a whole object rather than as a braced
// initializer, because a comma inside braces is a comma between macro
// arguments. __VA_ARGS__ collects the pieces back together, so
// `IntegerDomain<T>{.min = 0, .max = 9}` works while a bare `{.min = 0, .max =
// 9}` would not have -- the type is deduced from the object either way.
#define TEST_RNG_DRAW(rng, var, ...) auto var = (rng).integer(#var, __VA_ARGS__)
