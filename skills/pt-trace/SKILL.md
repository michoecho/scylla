---
name: pt-trace
description: Capture an Intel PT execution trace of a single doctest test case and view it in Perfetto. Use when asked to trace, profile, or inspect the control flow / call hierarchy of a test, or to produce a .ftf / perf.data for a test.
---

# Tracing a test with Intel PT

`tools/pt-trace` runs the test binary under `perf record` with Intel PT, but only
records the region a test wraps in `pt::Trace` (see `src/pt_control.h`). It can
then decode the trace to text (`perf script`) or to a Fuchsia trace (`.ftf`) for
Perfetto, using the vendored `perf2perfetto` dlfilter.

## Prerequisites

- An Intel PT capable host: `/sys/devices/intel_pt` must exist and `perf list`
  must show `intel_pt//`. Without it, `perf record -e intel_pt//u` fails.
- The test binary built: `cmake --build --preset Debug` (binary at
  `out/build/Debug/cpp_template`).
- For `--ftf` / `--perfetto`: the dlfilter must be built once:

  ```sh
  cargo build --release --manifest-path vendor/perf2perfetto/Cargo.toml
  ```

  This needs `libclang` for bindgen. Inside the flake devshell `LIBCLANG_PATH`
  is set automatically; outside it, export it to your clang's lib dir.

## 1. Make the test traceable

The trace only covers code between `pt::enable()` and `pt::disable()`. Use the
RAII scope to bound the region of interest:

```cpp
TEST_CASE("my hot path") {
    {
        pt::Trace _;          // enable() here; disable() at end of scope
        hot_function();
    }
}
```

When the test runs untraced (normal `ctest`), the helpers no-op, so this is safe
to leave in place. See `src/pt_control_test.cc` for worked examples.

## 2. Capture the trace

Pass the test binary and a doctest filter after `--`:

```sh
# Plain text decode to stdout (good for eyeballing control flow):
tools/pt-trace --script -- ./out/build/Debug/cpp_template test \
    --test-case='my hot path'

# Decode to a .ftf for Perfetto (writes perf.ftf):
tools/pt-trace --ftf -- ./out/build/Debug/cpp_template test \
    --test-case='my hot path'

# Decode and open it in Perfetto in the default browser:
tools/pt-trace --perfetto -- ./out/build/Debug/cpp_template test \
    --test-case='my hot path'
```

Notes:
- `test` is the doctest subcommand (the binary parses args with CLI11 and runs
  the suite under `test`); always include it before the doctest flags.
- `--test-case=...` selects which test runs; quote patterns with spaces. Use a
  glob like `--test-case=pt_control*` to match by prefix.
- `--perfetto` implies `--ftf`. It serves the `.ftf` on `127.0.0.1:9001` with the
  CORS header Perfetto needs, opens `ui.perfetto.dev/#!/?url=...`, and blocks
  serving until the UI fetches the file once (then exits). Ctrl-C to stop early.

## Useful options

- `-o, --output FILE` — perf.data path (default `perf.data`).
- `--ftf [FILE]` — .ftf output path (default `perf.ftf`).
- `--ftf-mode {t,c,i}` — time axis in the .ftf: timestamp / cpu cycles /
  instructions (default `c`).
- `--itrace SPEC` — `perf script` itrace spec for `--script` (default `be`;
  use `i0ns` for full per-instruction decode). The `--ftf` path always uses
  `bei0ns` because the dlfilter needs branch records.
- `--dlfilter PATH` — override the `libperf2perfetto.so` location.
- `-e, --event SPEC` — perf event (default `intel_pt//u`, user space only). For
  more exact cycle/instruction counts try `intel_pt/cyc=1,noretcomp=1/u`.
- `-v, --verbose` — print the perf command lines and keep perf's own output.

## Verifying the result

A real `.ftf` is a Fuchsia trace; confirm the traced functions are present:

```sh
strings perf.ftf | grep my_function
```

In Perfetto the calls render as a flamegraph nested by call depth. The `.ftf`
and `perf.data` are throwaway artifacts — delete them when done.

## Troubleshooting

- `cannot find perf binary` — install `perf` or pass `--perf /path/to/perf`.
- `dlfilter not found` — build it (see Prerequisites).
- `perf record` fails with an event error — the host has no Intel PT
  (`/sys/devices/intel_pt` missing); tracing is not possible there.
- Empty / tiny trace — the test didn't enter a `pt::Trace` scope, or the
  scoped region did almost nothing.
- `cannot serve on port 9001` — a previous `--perfetto` run is still serving;
  stop it (or another process holds the port).
