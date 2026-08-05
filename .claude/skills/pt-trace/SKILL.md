---
name: pt-trace
description: Capture an Intel PT execution trace of a single doctest test case and view it in Perfetto. Use when asked to trace, profile, or inspect the control flow / call hierarchy of a test, or to produce a .ftf / perf.data for a test.
---

# Tracing a test with Intel PT

`tools/pt-trace` has two subcommands, and one is always required:

| | |
|---|---|
| `pt-trace run [flags] -- PROGRAM ARGS...` | record a program under Intel PT, then optionally decode |
| `pt-trace view FILE.ftf` | open a `.ftf` captured earlier in the Perfetto UI |

`run` executes **any program** under `perf record` with Intel PT, but only
records the region that program wraps in `pt::Trace` (the `pt` module — see
`modules/pt/include/pt/pt_control.h`). It can
then decode the trace to text (`perf script`) or to a Fuchsia trace (`.ftf`) for
Perfetto, using the `perf2perfetto` dlfilter.

Whatever follows `--` is exec'd as-is; pt-trace neither knows nor cares what it
is. It is not tied to `cpp_template`, to doctest, or to tests at all.

All the recording flags below belong to `run` and must come after it —
`pt-trace --ftf -- ...` is an error, `pt-trace run --ftf -- ...` is correct.

## Which binary to trace

**Trace the binary that already contains the code you care about.** Almost
always that is the owning module's own test binary:

```
out/build/Debug/modules/<module>/<module>_test
```

Do **not** link a module into `cpp_template` just to make it traceable — that
puts scratch or experimental code into the shipping executable to serve a
tooling habit, which is backwards. `<module>_test` is a complete doctest
executable and is the default choice.

Reach for `out/build/Debug/cpp_template` only when the thing you want to trace
genuinely lives in the program's own path (`src/main.cc`, startup, CLI
handling), or when you specifically need the whole executable linked together.

The two take **different command lines**:

| Binary | Invocation |
|---|---|
| `<module>_test` | `<module>_test --test-case='...'` — doctest flags directly |
| `cpp_template` | `cpp_template test --test-case='...'` — doctest lives under a `test` subcommand |

The `test` word is a `cpp_template`-only quirk: it parses argv with CLI11 and
runs the suite under a subcommand. Module test binaries have no subcommands.
Passing a stray `test` to one is silently ignored rather than rejected, so this
mistake does not announce itself — see the empty-trace note in Troubleshooting.

## Prerequisites

- An Intel PT capable host: `/sys/devices/intel_pt` must exist and `perf list`
  must show `intel_pt//`. Without it, `perf record -e intel_pt//u` fails.
- The binary you intend to trace, built: `cmake --build --preset Debug`, or
  `--target <module>_test` for a single module's test binary.
- For `run --ftf` / `run --perfetto`: the `perf2perfetto` dlfilter. Nothing to build —
  the devshell provides it prebuilt and points `$PERF2PERFETTO_DLFILTER` at it.
  Outside `nix develop` the variable is unset and you must pass `--dlfilter`.

## 1. Make the test traceable

The trace only covers code between `pt::enable()` and `pt::disable()`. Use the
RAII scope to bound the region of interest:

```cpp
#include "pt/pt_control.h"

TEST_CASE("my hot path") {
    {
        pt::Trace _;          // enable() here; disable() at end of scope
        hot_function();
    }
}
```

The helpers live in the `pt` module, so link it from the module whose test you
are tracing (see skills/modules):

```cmake
target_link_module(module_x PRIVATE pt)
```

Linking `pt` into the module is the whole wiring job — it makes that module's
own `<module>_test` traceable, which is the binary you will point pt-trace at.
No further linking is needed, and in particular nothing has to be added to
`cpp_template`.

When the test runs untraced (normal `ctest`), the helpers no-op, so this is safe
to leave in place. Worked examples, each traced via its own module's test binary:
`modules/pt/pt_control_test.cc` (a nested call tree, via `pt_test`) and
`modules/playground/hegel_trace_test.cc` (a `pt::Trace` around `hegel::test`,
via `playground_test`).

## 2. Capture the trace

Pass the program and its arguments after `--`. Tracing a module's test case —
the usual case:

```sh
# Plain text decode to stdout (good for eyeballing control flow):
tools/pt-trace run --script -- ./out/build/Debug/modules/module_x/module_x_test \
    --test-case='my hot path'

# Decode to a .ftf for Perfetto (writes perf.ftf):
tools/pt-trace run --ftf -- ./out/build/Debug/modules/module_x/module_x_test \
    --test-case='my hot path'

# Decode and open it in Perfetto in the default browser:
tools/pt-trace run --perfetto -- ./out/build/Debug/modules/module_x/module_x_test \
    --test-case='my hot path'
```

Tracing something in the program itself — note the extra `test`:

```sh
tools/pt-trace run --ftf -- ./out/build/Debug/cpp_template test \
    --test-case='some case in the main module'
```

Check first that the case you name is actually in there — `cpp_template` links
only `main` and what `main` depends on, so most modules' cases are absent:

```sh
./out/build/Debug/cpp_template test --list-test-cases
```

As of writing, nothing reachable from `cpp_template` opens a `pt::Trace` scope,
so this form has nothing to record. It is the right shape for tracing the
program's own startup or CLI path; for a module's test, use the form above.

Any other program works too; it need not be a test, or even ours:

```sh
tools/pt-trace run --ftf -- ./some/other/binary --whatever-flags
```

Notes:
- `--test-case=...` selects which test runs; quote patterns with spaces. Use a
  glob like `--test-case='intel pt*'` to match by prefix. A pattern matching
  nothing is **not** an error — see Troubleshooting.
- `run --perfetto` implies `--ftf`. It serves the `.ftf` on `127.0.0.1:9001` with the
  CORS header Perfetto needs, opens `ui.perfetto.dev/#!/?url=...`, and blocks
  serving until the UI fetches the file once (then exits). Ctrl-C to stop early.

## Viewing a .ftf you already have

`view` is `run --perfetto`'s last step on its own — no recording, no perf, no
dlfilter needed:

```sh
tools/pt-trace view perf.ftf
```

Same one-shot server and teardown as `run --perfetto`. The browser is whatever
`webbrowser.open` picks, so `$BROWSER` selects it:

```sh
BROWSER=firefox tools/pt-trace view perf.ftf
```

Use it to re-open a trace after the original `run --perfetto` server exited, or
to view a `.ftf` captured with plain `run --ftf`. A missing or zero-byte file is
rejected up front rather than loaded as an empty trace.

## Useful options for `run`

- `-o, --output FILE` — perf.data path (default `perf.data`).
- `--ftf [FILE]` — .ftf output path (default `perf.ftf`).
- `--ftf-mode {t,c,i}` — time axis in the .ftf: timestamp / cpu cycles /
  instructions (default `c`).
- `--itrace SPEC` — `perf script` itrace spec for `--script` (default `be`;
  use `i0ns` for full per-instruction decode). The `--ftf` path always uses
  `bei0ns` because the dlfilter needs branch records.
- `--dlfilter PATH` — override the `libperf2perfetto.so` location (defaults to
  `$PERF2PERFETTO_DLFILTER`).
- `-e, --event SPEC` — perf event (default `intel_pt/cyc=1/u`, user space only).
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
- `no dlfilter` / `dlfilter not found` — run inside `nix develop` so
  `$PERF2PERFETTO_DLFILTER` is set, or pass `--dlfilter PATH`.
- `perf record` fails with an event error — the host has no Intel PT
  (`/sys/devices/intel_pt` missing); tracing is not possible there.
- Empty / tiny trace — the program never entered a `pt::Trace` scope, or the
  scoped region did almost nothing. The common cause is that **no test
  actually ran**: doctest exits 0 having run 0 cases when `--test-case=...`
  matches nothing, so pt-trace still prints `wrote perf.ftf` and you get a
  valid, empty trace. Nothing fails loudly. Check by running the binary alone
  first and reading the case count:

  ```sh
  ./out/build/Debug/modules/module_x/module_x_test --test-case='my hot path'
  # [doctest] test cases: 0 | ...   <- wrong binary, or typo in the pattern
  ```

  `--list-test-cases` shows what that binary actually has. Remember a module
  test binary defaults to its own module's cases only (see skills/modules).
- `cannot serve on port 9001` — a previous `run --perfetto` or `view` is still serving;
  stop it (or another process holds the port).
