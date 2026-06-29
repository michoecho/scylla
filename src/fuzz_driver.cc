#include "fuzz_driver.h"

#include <cstddef>
#include <cstdint>
#include <print>
#include <string>
#include <vector>

#include <unistd.h>

#include "fuzz_example/fuzz_targets.h"

// AFL's instrumentation provides these in an afl-clang-fast build. The guard
// lets the same code build under a plain compiler, where it falls back to
// reading a single testcase from stdin (for crash replay).
#ifdef __AFL_FUZZ_INIT
__AFL_FUZZ_INIT();
#endif

// Pointer-to-function for a fuzz target. The AFL driver below is a plain
// file-scope function (NOT in a namespace): the __AFL_LOOP / __AFL_INIT macros
// expand to references to AFL's global symbols (e.g. __afl_connected), and
// putting them inside a namespace would mangle those identifiers and break
// linking. So keep the AFL-macro code at global scope.
using TargetFn = void (*)(const std::uint8_t*, std::size_t);

#ifdef __AFL_HAVE_MANUAL_CONTROL
// Run `fn` under AFL's persistent loop: __AFL_INIT() starts the fork server
// after one-time setup, then each __AFL_LOOP iteration hands us a fresh input
// from the shared-memory buffer. The whole loop lives in one process, so the
// process/CLI11 startup is paid once, not per input.
static int run_afl_loop(TargetFn fn) {
    __AFL_INIT();
    const std::uint8_t* buf = __AFL_FUZZ_TESTCASE_BUF;
    while (__AFL_LOOP(10000)) {
        std::size_t len = __AFL_FUZZ_TESTCASE_LEN;
        fn(buf, len);
    }
    return 0;
}
#endif

#ifndef __AFL_HAVE_MANUAL_CONTROL
// Read all of stdin and run `fn` once. Used to reproduce a crash outside AFL.
// Only compiled in a non-AFL build; the AFL build always uses the loop above.
static int run_stdin_once(TargetFn fn) {
    std::vector<std::uint8_t> input;
    std::uint8_t chunk[4096];
    ssize_t n;
    while ((n = ::read(STDIN_FILENO, chunk, sizeof(chunk))) > 0)
        input.insert(input.end(), chunk, chunk + n);
    fn(input.data(), input.size());
    return 0;
}
#endif

namespace fuzz {
namespace {

// Named fuzz targets, for `fuzz <name>` lookup. Add new targets here.
struct Target {
    const char* name;
    TargetFn fn;
};

constexpr Target kTargets[] = {
    {"varint", &fuzz::varint},
};

TargetFn lookup(const std::string& name) {
    for (const Target& t : kTargets)
        if (name == t.name)
            return t.fn;
    return nullptr;
}

}  // namespace

int run(const std::string& target) {
    TargetFn fn = lookup(target);
    if (fn == nullptr) {
        std::println(stderr, "unknown fuzz target: {}", target);
        std::print(stderr, "available:");
        for (const Target& t : kTargets)
            std::print(stderr, " {}", t.name);
        std::println(stderr, "");
        return 2;
    }

#ifdef __AFL_HAVE_MANUAL_CONTROL
    return run_afl_loop(fn);
#else
    // Plain build: no fork server, just replay one stdin testcase.
    return run_stdin_once(fn);
#endif
}

}  // namespace fuzz
