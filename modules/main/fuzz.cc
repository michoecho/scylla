#include "main/fuzz.h"

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include <unistd.h>

// AFL's instrumentation defines these macros in an afl-clang-fast build. The
// guards let the same file build under a plain compiler, where it falls back to
// reading a single testcase from stdin (crash replay).
//
// __AFL_FUZZ_INIT() defines file-scope globals (__afl_sharedmem_fuzzing, the
// alternate testcase buffer, ...), so it has to sit at namespace scope here
// rather than inside a function. The other macros expand to calls whose symbols
// are pinned with __asm__ labels, so those are safe anywhere.
#ifdef __AFL_FUZZ_INIT
__AFL_FUZZ_INIT();
#endif

namespace {

// Hand `fn` a tightly-sized heap copy of `size` bytes from `data`.
//
// This copy is load-bearing, not hygiene. AFL's testcase buffer is a ~1 MB
// shared-memory region, so a read one byte past the end of the input still
// lands in valid mapped memory: ASAN sees nothing and the fuzzer runs forever
// finding no bug. A fresh, exactly-sized `new[]` puts an ASAN redzone right
// after the last byte, turning any over-read into an observable crash.
void call_with_tight_buffer(void (*fn)(const std::byte*, std::size_t),
                            const std::byte* data,
                            std::size_t size) {
    std::unique_ptr<std::byte[]> tight(new std::byte[size]);
    std::memcpy(tight.get(), data, size);
    fn(tight.get(), size);
}

}  // namespace

// This driver is at FILE SCOPE, deliberately outside `namespace fuzz`.
// __AFL_LOOP expands to a block containing `extern int __afl_connected;` with
// no __asm__ label, so inside a namespace that declaration mangles to
// `fuzz::__afl_connected` and the link fails with an undefined reference. The
// AFL-macro code therefore has to stay at global scope; fuzz::run below is a
// thin wrapper that just calls in here.
//
// It is also a plain non-template function in one .cc rather than a header
// template, so __AFL_INIT() and the loop are emitted exactly once per process
// no matter how many FUZZ_TARGETs exist.
static void run_driver(void (*fn)(const std::byte*, std::size_t)) {
#ifdef __AFL_HAVE_MANUAL_CONTROL
    // Persistent mode + shared memory. __AFL_INIT() starts the fork server
    // here, after CLI11 parsing and doctest startup, so every subsequent
    // testcase forks from a process that has already paid that cost.
    __AFL_INIT();
    const std::uint8_t* buf = __AFL_FUZZ_TESTCASE_BUF;
    while (__AFL_LOOP(10000)) {
        std::size_t len = __AFL_FUZZ_TESTCASE_LEN;
        call_with_tight_buffer(fn, reinterpret_cast<const std::byte*>(buf), len);
    }
#else
    // Plain build: no fork server, no shared memory. Replay one stdin testcase.
    std::vector<std::byte> input;
    std::byte chunk[4096];
    ssize_t n;
    while ((n = ::read(STDIN_FILENO, chunk, sizeof(chunk))) > 0)
        input.insert(input.end(), chunk, chunk + n);
    call_with_tight_buffer(fn, input.data(), input.size());
#endif
}

namespace fuzz {

void run(void (*fn)(const std::byte*, std::size_t)) {
    run_driver(fn);
}

}  // namespace fuzz

// ---------------------------------------------------------------------------
// Worked example, and the self-test that proves the machinery above works.
// ---------------------------------------------------------------------------

// Exit status the deliberate bug uses to signal "crash", paired with
// AFL_CRASH_EXITCODE in the self-test below.
//
// It deliberately does NOT abort(). A fatal signal makes the kernel run
// core_pattern, and on a desktop that is a pipe to systemd-coredump, which
// journals the death and raises a KDE crash notification — thousands of times
// over a fuzzing run. RLIMIT_CORE=0 does not prevent this: for a *piped*
// core_pattern the helper is invoked regardless of the limit, which only caps
// how much it then writes (hence journal entries whose core is "missing").
// Exiting normally is the only way to keep the kernel from invoking the helper
// at all.
//
// The tradeoff: this exercises AFL's exit-code crash detection rather than its
// signal-based path. A real bug would still die by signal and still be caught;
// what this target proves is the plumbing, not the signal handling.
inline constexpr int kCrashExitCode = 86;

// A deliberately buggy target, here so the fuzzing infrastructure has something
// to find and the test below has something to prove. It "crashes" when the
// input starts with the four bytes FF FF FF FF — a needle AFL has to discover
// by mutating its way to a specific 32-bit value.
FUZZ_TARGET("deliberate_bug", [](std::span<const std::byte> input) {
    if (input.size() < 4)
        return;

    // Read the four bytes through memcpy rather than a cast: the span may not
    // be suitably aligned for std::uint32_t, and the compiler folds this to a
    // single load anyway.
    unsigned char first[4];
    std::memcpy(first, input.data(), sizeof(first));

    // Comment out this exit to check that the test below actually fails (it
    // should then hit the external timeout instead of finding a crash).
    //
    // _exit() rather than exit(): this runs inside __AFL_LOOP in persistent
    // mode, and it must terminate the process immediately without flushing
    // doctest's or the runtime's state. AFL sees the exit code, records a
    // crash, and forks a fresh child for the next testcase.
    if (first[0] == 0xFF && first[1] == 0xFF && first[2] == 0xFF && first[3] == 0xFF)
        ::_exit(kCrashExitCode);
});

namespace {

// Absolute path to this very binary. The test re-executes itself under
// afl-fuzz, so it needs its own path rather than a build-time constant (which
// would go stale if the binary is moved or copied).
std::string self_exe() {
    return std::filesystem::read_symlink("/proc/self/exe").string();
}

// Run `cmd` through the shell, capturing stdout+stderr. Returns the output; the
// exit status is deliberately ignored — afl-fuzz's status is not a reliable
// signal here (it exits non-zero on some clean shutdowns), so the assertions
// below key off its output and the crashes directory instead.
std::string run_capture(const std::string& cmd) {
    std::string out;
    // 2>&1 so AFL's startup diagnostics (which go to stderr) are captured too.
    std::FILE* pipe = ::popen((cmd + " 2>&1").c_str(), "r");
    REQUIRE(pipe != nullptr);
    char chunk[4096];
    while (std::fgets(chunk, sizeof(chunk), pipe) != nullptr)
        out += chunk;
    ::pclose(pipe);
    return out;
}

}  // namespace

// End-to-end check of the fuzzing infrastructure: build a corpus, point AFL++
// at this binary's `deliberate_bug` target, and confirm AFL reports finding a
// crash.
//
// Skipped outside the fuzz build: without afl-clang-fast++ instrumentation
// there is no coverage feedback, so AFL would be doing blind random search for
// a specific 32-bit value and would (almost) never finish.
//
// There is deliberately NO internal timeout. If the bug is not found, this test
// runs until the *external* timeout (ctest's TIMEOUT property) kills it — that
// is the intended failure mode. AFL_BENCH_UNTIL_CRASH makes the success path
// exit as soon as the first crash is saved, so the happy path is fast.
// Skipped in the CmpLog build too (CMPLOG_BUILD, set by the FuzzCmplog preset):
// that binary is afl-fuzz's `-c` input, not a fuzzing target, so it must not
// launch a nested fuzzing run of its own.
TEST_CASE("AFL++ finds the deliberate bug" * doctest::skip(
#if defined(BUILD_FUZZERS) && !defined(CMPLOG_BUILD)
              false
#else
              true
#endif
              )) {
    const std::filesystem::path work =
        std::filesystem::temp_directory_path() /
        ("fuzz_selftest_" + std::to_string(::getpid()));
    // Fresh directories every run: AFL refuses to reuse a non-empty output dir
    // unless explicitly resumed.
    std::filesystem::remove_all(work);
    std::filesystem::create_directories(work / "in");

    // One trivial, non-empty seed. AFL needs somewhere to start; coverage
    // feedback does the rest of the work of reaching FF FF FF FF.
    {
        std::ofstream seed(work / "in" / "seed", std::ios::binary);
        seed << "aaaa";
    }

    const std::string cmd =
        // AFL_BENCH_UNTIL_CRASH: stop as soon as the first crash is saved.
        "AFL_BENCH_UNTIL_CRASH=1 "
        // Line-based output; the TUI's escape codes are unreadable in a log.
        "AFL_NO_UI=1 "
        // Don't refuse to start over the CPU frequency governor.
        "AFL_SKIP_CPUFREQ=1 "
        // core_pattern may pipe cores to an external handler (systemd-coredump)
        // which AFL can't distinguish from a hang. Harmless here: the target
        // signals its crash by exit code, never by a fatal signal.
        "AFL_I_DONT_CARE_ABOUT_MISSING_CRASHES=1 "
        // The deliberate bug _exit()s with this code instead of aborting, so
        // the kernel never invokes core_pattern. This is what tells AFL to
        // count that exit as a crash; without it the bug is invisible and this
        // test would run until the external timeout.
        "AFL_CRASH_EXITCODE=" + std::to_string(kCrashExitCode) + " " +
        "afl-fuzz -i '" + (work / "in").string() + "'" +
        " -o '" + (work / "out").string() + "'" +
        " -- '" + self_exe() + "' fuzz --test-case=deliberate_bug";

    const std::string output = run_capture(cmd);
    INFO("afl-fuzz output:\n", output);

    // The instrumentation and the persistent loop are working. If either is
    // missing AFL says so here, and that is a far more useful failure message
    // than "no crash found".
    CHECK(output.find("Persistent mode binary detected") != std::string::npos);

    // AFL_BENCH_UNTIL_CRASH prints this line when it stops on the first crash.
    CHECK(output.find("Testing aborted programmatically") != std::string::npos);

    // The real assertion: AFL saved at least one crashing input.
    const std::filesystem::path crashes = work / "out" / "default" / "crashes";
    REQUIRE(std::filesystem::exists(crashes));
    int saved = 0;
    for (const auto& entry : std::filesystem::directory_iterator(crashes))
        if (entry.path().filename().string().starts_with("id:"))
            ++saved;
    CHECK(saved > 0);

    if (saved > 0)
        std::filesystem::remove_all(work);
}
