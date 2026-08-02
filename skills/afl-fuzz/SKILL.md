---
name: afl-fuzz
description: Fuzz a function in this project with AFL++ to find crashes (out-of-bounds, asserts, UB), then turn a crash into a doctest regression test. Use when asked to fuzz code, harden a parser/decoder, find memory-safety bugs in a function, or add a fuzz target. The worked example is the `deliberate_bug` target at the bottom of src/fuzz.cc.
---

# Fuzzing a function with AFL++

AFL++ is in the flake devshell (`afl-fuzz`, `afl-clang-fast++`).

Fuzzing lives **inside the one `cpp_template` binary**. A fuzz target is just a
doctest case in the `fuzz` suite, declared with `FUZZ_TARGET`, and the `fuzz`
subcommand lists and runs them next to `test` and `bench`. There is no separate
fuzz executable. The `Fuzz` CMake preset rebuilds `cpp_template` with AFL's
coverage instrumentation + ASAN; you then point `afl-fuzz` at it.

```sh
cpp_template fuzz --list-test-cases      # list the targets
cpp_template fuzz --test-case=<name>     # run one (this is what afl-fuzz execs)
```

## Add a target

One macro, in any `.cc` that's part of the binary:

```cpp
#include "fuzz.h"

FUZZ_TARGET("my_parser", [](std::span<const std::byte> input) {
    parse(input);
});
```

The function may take whichever shape reads best; the bytes are identical:

- `std::span<const std::byte>`
- `std::string_view`
- `(const char*, std::size_t)`

A capture-less lambda or a plain function both work. Anything else fails with a
`static_assert` naming the three accepted forms.

There is **no registration table and no CMake edit** for a new target (as long
as the file is already in the binary) — `FUZZ_TARGET` registers it as a doctest
case and the `fuzz` subcommand picks it up automatically.

Targets must be safe to call repeatedly in one process (persistent mode reuses
the process across inputs) and must not read past the end of the input. Keep
them tiny: whatever a target touches is the surface AFL explores.

### What the macro does

- Registers a doctest case in the `fuzz` suite (`FUZZ_SUITE`), marked `skip()`,
  so a normal test run (`ctest` / `cpp_template test`) never starts a fuzzer and
  the targets stay out of the default test list.
- **Outside the fuzz build** (`BUILD_FUZZERS` off) the body is still *compiled* —
  so it can't rot — but not registered at all, keeping the normal binary's test
  list clean. A signature error surfaces in every build, not just under `Fuzz`.
- Under the fuzz build, the case body calls `fuzz::run`, which drives the target
  in AFL **persistent mode with shared memory** (`src/fuzz.cc`).

## Architecture

- `src/fuzz.h` — the `FUZZ_TARGET` macro and the `fuzz::invoke` adapter that
  converts AFL's bytes to whichever parameter shape the target takes.
- `src/fuzz.cc` — the driver: `__AFL_INIT()` + the `__AFL_LOOP` persistent loop
  in an AFL build, single-testcase stdin replay otherwise. The AFL macros sit at
  **file scope, not inside a namespace** — see the gotcha below. The worked
  example (`deliberate_bug`, a target that aborts on `FF FF FF FF`) and the
  self-test that fuzzes it live at the bottom of the same file.
- `src/main.cc` — the `fuzz` subcommand: scopes doctest to the fuzz suite with
  `--no-skip` and forwards the rest of the args, so all of doctest's listing and
  filtering works unchanged.

Because AFL only mutates the bytes handed to the target (not `argv`), the fixed
subcommand args are parsed once per process and the fuzzed bytes only ever reach
the target. AFL stays in the target; it does not wander into CLI11 or doctest.

## Build

```sh
cmake --preset Fuzz          # afl-clang-fast++, BUILD_FUZZERS=ON, ASAN on
cmake --build --preset Fuzz
```

Binary lands at `out/build/Fuzz/cpp_template`. Sanity-check instrumentation:
`afl-fuzz` prints **"Persistent mode binary detected"** at startup, and
`nm out/build/Fuzz/cpp_template | grep -c __afl` is non-zero.

## Run

```sh
mkdir -p fuzz/in
printf 'aaaa' > fuzz/in/seed                         # one or two *valid* inputs

export AFL_SKIP_CPUFREQ=1                            # don't fail on cpufreq governor
export AFL_I_DONT_CARE_ABOUT_MISSING_CRASHES=1       # see "core_pattern" below
afl-fuzz -i fuzz/in -o fuzz/out \
    -- ./out/build/Fuzz/cpp_template fuzz --test-case=my_parser
```

A good seed corpus (a few *valid* inputs) helps AFL reach interesting code fast.
Watch the TUI: "saved crashes" climbs when it finds something. Useful env vars:

- `AFL_BENCH_UNTIL_CRASH=1` — exit as soon as the first crash is saved (handy
  for a scripted "does this still crash?" check).
- `AFL_NO_UI=1` — line-based output instead of the TUI (for logs / non-tty).

### core_pattern

AFL refuses to start if `/proc/sys/kernel/core_pattern` pipes cores to an
external handler (e.g. systemd-coredump): a slow external handler makes a
crashing child look like a *hang*, so AFL can misclassify or drop crashes. Two
fixes:

- Proper (needs root, system-wide): `sudo sh -c 'echo core > /proc/sys/kernel/core_pattern'`
- No root: export `AFL_I_DONT_CARE_ABOUT_MISSING_CRASHES=1`. Fine for the ASAN
  targets here — ASAN reports the crash to AFL directly.

## The self-test

The bottom of `src/fuzz.cc` holds an end-to-end check of this whole pipeline: it
re-executes **its own binary** (`/proc/self/exe`) under `afl-fuzz` against the
`deliberate_bug` target and asserts AFL reports persistent mode, stops on a
crash, and saves a crashing input. It is `skip()`'d outside the fuzz build
(without coverage feedback AFL would be searching blindly for a specific 32-bit
value) and normally passes in well under a second.

It has **no internal timeout, deliberately**. If the infrastructure breaks, AFL
searches forever and the *external* `TIMEOUT 10` ctest property (set in
`CMakeLists.txt` under `BUILD_FUZZERS`) turns that into a failure. To confirm
the test can actually fail, comment out the `std::abort()` in the
`deliberate_bug` target and rerun — it should time out at 10s rather than pass.

## Triage a crash

Crashing inputs land in `fuzz/out/default/crashes/id:*`. A persistent binary run
directly (outside afl-fuzz) executes one loop iteration reading stdin, so replay
is just:

```sh
CRASH=$(ls fuzz/out/default/crashes/id:* | head -1)
xxd "$CRASH"                                        # see the bytes
ASAN_OPTIONS=abort_on_error=0 \
    ./out/build/Fuzz/cpp_template fuzz --test-case=my_parser < "$CRASH"
```

The ASAN summary names the file:line of the bug. Minimise a large input:

```sh
afl-tmin -i "$CRASH" -o min_crash \
    -- ./out/build/Fuzz/cpp_template fuzz --test-case=my_parser
```

## Freeze it into a regression test

Turn the (minimised) crashing bytes into an ordinary doctest case next to the
code, so the bug stays fixed:

```cpp
TEST_CASE("my_parser does not read past a truncated buffer") {
    std::vector<std::uint8_t> input = {0x80};   // the minimised crash
    auto r = my_parser(input);
    CHECK(r.bytes_read <= input.size());        // an invariant the bug broke
}
```

Run it under the **Sanitize** preset (ASAN + libstdc++ hardening) so the test
actually trips on memory bugs, not just wrong return values:

```sh
cmake --preset Sanitize && cmake --build --preset Sanitize
ASAN_OPTIONS=detect_leaks=0 ./out/build/Sanitize/cpp_template test \
    --test-case='my_parser*'
```

The regression test runs in the normal suite (`ctest`); fuzzing itself does not
— it is a deliberate, out-of-band activity under the Fuzz preset.

## The one pitfall that wastes hours

**The input must be copied into a tightly-sized buffer before the target sees
it.** `__AFL_FUZZ_TESTCASE_BUF` points into a ~1 MB shared-memory region, so a
read one byte past `len` lands in still-valid memory — ASAN sees nothing and the
fuzzer runs forever finding no crash, even though the bug is real. A fresh
`new std::byte[size]` puts an ASAN redzone right after the last byte.

`fuzz::run` in `src/fuzz.cc` does this copy centrally, for every target, so
individual targets no longer have to remember it. Don't bypass it by capturing
AFL's raw pointer.

Other gotchas:

- **AFL macros must be at file scope, not in a namespace.** `__AFL_LOOP` expands
  to a block containing a bare `extern int __afl_connected;` with no `__asm__`
  label, so inside a `namespace fuzz` it mangles to `fuzz::__afl_connected` and
  the link fails with an undefined reference. `src/fuzz.cc` keeps the driver in
  a file-scope function for exactly this reason; `fuzz::run` is a thin wrapper.
- `__AFL_LOOP` uses a GNU statement expression, hence `-Wno-gnu-statement-expression`.
- No ASAN → memory-safety bugs read garbage silently instead of crashing. The
  Fuzz preset adds `-fsanitize=address`; keep it.
- Built without afl-clang-fast (wrong compiler) → no coverage feedback, AFL
  degrades to blind random testing. The `Fuzz` preset sets the compiler; don't
  override it.
