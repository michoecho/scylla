---
name: afl-fuzz
description: Fuzz a function in this project with AFL++ to find crashes (out-of-bounds, asserts, UB), then turn a crash into a doctest regression test. Use when asked to fuzz code, harden a parser/decoder, find memory-safety bugs in a function, or add a fuzz target. The worked example is the `varint` target under src/fuzz_example/.
---

# Fuzzing a function with AFL++

AFL++ is in the flake devshell (`afl-fuzz`, `afl-clang-fast++`).

Fuzzing lives **inside the one `cpp_template` binary**, as a `fuzz <target>`
subcommand next to `test` and `bench`. There is no separate fuzz executable.
The `Fuzz` CMake preset rebuilds `cpp_template` with AFL's coverage
instrumentation + ASAN and the example bug compiled in; you then point
`afl-fuzz` at `cpp_template fuzz <target>`.

```
afl-fuzz -i corpus -o out -- ./out/build/Fuzz/cpp_template fuzz varint
```

`varint` is a complete worked example — copy its shape for a new target.

## Architecture (why it's one binary)

- `src/fuzz_example/fuzz_targets.h` — declares each fuzz target: a
  `void name(const uint8_t* data, size_t size)` function.
- `src/fuzz_example/fuzz_varint.cc` — the `varint` target: copies the bytes into
  a tight buffer (see pitfall) and calls `varint::decode`.
- `src/fuzz_driver.cc` — the `fuzz <name>` subcommand. Looks up the target by
  name and drives it under AFL's **persistent loop** (`__AFL_LOOP`), or, in a
  non-AFL build, runs it once on stdin (crash replay). The AFL macros sit at
  **file scope, not inside a namespace** — see the gotcha below.
- `src/main.cc` — registers the `fuzz` subcommand and calls `fuzz::run(target)`.

Because AFL only mutates the bytes handed to the target (not `argv`), the fixed
`fuzz varint` args are parsed by CLI11 once per process and the fuzzed bytes
only ever reach `decode()`. AFL stays in the target; it does not wander into
CLI11 or doctest.

## Add a new target

1. **Code under test** lives wherever it normally would. The example's
   `src/fuzz_example/varint.{h,cc}` hides an intentional bug behind
   `DELIBERATE_BUGS_FOR_FUZZING` so the demo reproduces; real code needs no flag.

2. **Declare + define the target.** In `fuzz_targets.h`:

   ```cpp
   void mytarget(const std::uint8_t* data, std::size_t size);
   ```

   In a `.cc` (mirror `fuzz_varint.cc`):

   ```cpp
   void fuzz::mytarget(const std::uint8_t* data, std::size_t size) {
       // COPY into a tight, exactly-sized heap buffer first (see pitfall).
       std::unique_ptr<std::uint8_t[]> tight(new std::uint8_t[size]);
       std::memcpy(tight.get(), data, size);
       my_function(std::span(tight.get(), size));
   }
   ```

3. **Register it** in `src/fuzz_driver.cc`'s `kTargets` table:

   ```cpp
   constexpr Target kTargets[] = {
       {"varint", &fuzz::varint},
       {"mytarget", &fuzz::mytarget},
   };
   ```

4. **Add the `.cc` to `cpp_template`** in `CMakeLists.txt`.

That's it — `cpp_template fuzz mytarget` now works under the Fuzz preset.

## Build

```sh
cmake --preset Fuzz          # afl-clang-fast++, BUILD_FUZZERS=ON, bug + ASAN on
cmake --build --preset Fuzz
```

Binary lands at `out/build/Fuzz/cpp_template`. Sanity-check instrumentation:
`afl-fuzz` prints **"Persistent mode binary detected"** at startup, and
`nm out/build/Fuzz/cpp_template | grep -c __afl` is non-zero.

The cost of the one-binary approach is only that this preset recompiles the
whole program with instrumentation + ASAN. At runtime, persistent mode pays the
process/CLI11 startup once per process (not per input), and unexecuted
instrumented code (CLI11, doctest) costs nothing.

## Run

```sh
mkdir -p fuzz/varint/in fuzz/varint/out
printf '\xac\x02' > fuzz/varint/in/seed             # one or two *valid* inputs

export AFL_SKIP_CPUFREQ=1                            # don't fail on cpufreq governor
export AFL_I_DONT_CARE_ABOUT_MISSING_CRASHES=1       # see "core_pattern" below
afl-fuzz -i fuzz/varint/in -o fuzz/varint/out \
    -- ./out/build/Fuzz/cpp_template fuzz varint
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

## Triage a crash

Crashing inputs land in `fuzz/varint/out/default/crashes/id:*`. A persistent
binary run directly (outside afl-fuzz) executes one loop iteration reading
stdin, so replay is just:

```sh
CRASH=$(ls fuzz/varint/out/default/crashes/id:* | head -1)
xxd "$CRASH"                                        # see the bytes
ASAN_OPTIONS=abort_on_error=0 \
    ./out/build/Fuzz/cpp_template fuzz varint < "$CRASH"
```

The ASAN summary names the file:line of the bug. Minimise a large input:

```sh
afl-tmin -i "$CRASH" -o min_crash \
    -- ./out/build/Fuzz/cpp_template fuzz varint
```

## Freeze it into a regression test

Turn the (minimised) crashing bytes into a doctest case next to the code, so the
bug stays fixed. See `src/fuzz_example/varint_test.cc`:

```cpp
TEST_CASE("my_function does not read past a truncated buffer") {
    std::vector<std::uint8_t> input = {0x80};   // the minimised crash
    auto r = my_function(input);
    CHECK(r.bytes_read <= input.size());        // an invariant the bug broke
}
```

Run it under the **Sanitize** preset (ASAN + libstdc++ hardening) so the test
actually trips on memory bugs, not just wrong return values:

```sh
cmake --preset Sanitize && cmake --build --preset Sanitize
ASAN_OPTIONS=detect_leaks=0 ./out/build/Sanitize/cpp_template test \
    --test-case='my_function*'
```

Then fix the code and confirm the test goes green. The regression test runs in
the normal suite (`ctest`); the fuzzer does not — fuzzing is a deliberate,
out-of-band activity under the Fuzz preset.

## The one pitfall that wastes hours

**Copy AFL's input into a tightly-sized buffer before calling the target.**
`__AFL_FUZZ_TESTCASE_BUF` points into a ~1 MB shared-memory region. If you pass
that pointer straight in, a read one byte past `len` lands in still-valid shared
memory — ASAN sees nothing and the fuzzer runs forever finding no crash, even
though the bug is real. A fresh `new uint8_t[size]` puts an ASAN redzone right
after the last byte. Every target's body must do this copy; `fuzz_varint.cc`
shows it.

Other gotchas:

- **AFL macros must be at file scope, not in a namespace.** `__AFL_LOOP` /
  `__AFL_INIT` expand to references to AFL's global symbols (`__afl_connected`,
  …). Inside a `namespace`, those identifiers get mangled and linking fails with
  `undefined reference to ...::__afl_connected`. `fuzz_driver.cc` keeps the loop
  in a plain file-scope function for this reason.
- No ASAN → memory-safety bugs read garbage silently instead of crashing. The
  Fuzz preset adds `-fsanitize=address`; keep it.
- Built without afl-clang-fast (wrong compiler) → no coverage feedback, AFL
  degrades to blind random testing. The `Fuzz` preset sets the compiler; don't
  override it.
- `fuzz/*/out` is throwaway (gitignored). The seed corpus `fuzz/*/in` is worth
  keeping. Delete `out/` between unrelated runs to start clean.
```
