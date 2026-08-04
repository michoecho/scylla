---
name: snapshot-tests
description: Write or update snapshot ("expect") tests — assertions whose expected value is rewritten into the source by the test runner, via SNAPSHOT_UPDATE=1 or a per-snapshot .update(). Use when asked to add a snapshot/expect test, accept new expected output, update snapshots after a behaviour change, characterise existing output, or fix a "no `snapshot` identifier at ..." / "no longer holds the recorded value" / "still has .update() on it" / "not valid UTF-8" / unsupported-literal error.
---

# Snapshot (expect) tests

An assertion whose expected value is written back into the source file by the
runner. You write the test empty, run once in update mode, and review the diff.

Provided by the `snapshot` module (`modules/snapshot/`), in namespace
`snapshot_testing`. Link it and include one header.

## Add one

`modules/module_x/CMakeLists.txt`:

```cmake
target_link_module(module_x PRIVATE snapshot)
```

`modules/module_x/x_test.cc`:

```cpp
#include <doctest/doctest.h>

#include "snapshot/check.h"

using snapshot_testing::check_snapshot;
using snapshot_testing::snapshot;
using snapshot_testing::operator""_snap;   // for multi-line values; see below

TEST_CASE("render_table lays out aligned columns") {
    check_snapshot(render_table(items), snapshot());
}
```

Then fill it in:

```sh
SNAPSHOT_UPDATE=1 out/build/Debug/modules/module_x/module_x_test
cmake --build --preset Debug --target module_x_test
out/build/Debug/modules/module_x/module_x_test        # now green
git diff                                              # review what it wrote
```

The update run **still fails** — by design. A run that rewrote sources must not
come back green.

## No macros — and why it matters

`snapshot()` and `check_snapshot()` are ordinary functions. This is load-bearing,
not a style choice.

`snapshot()` captures its call site through a defaulted
`std::source_location::current()` argument, and that location is the *only* thing
the updater uses to find the literal. For a plain function call the location is
exact and lands on the callee's name — clang reports its first character, gcc the
`(` just past it — so the updater can demand the identifier `snapshot` at one of
those two spots and refuse anything else.

Inside a macro expansion that guarantee evaporates: the reported location is
wherever the expansion is anchored, not the macro's name, and it moves with the
surrounding expression.

**So: never wrap `snapshot()` in a macro.** Write helpers as functions
(see "Snapshots are values" below) and everything keeps working.

## Update after a behaviour change

```sh
SNAPSHOT_UPDATE=1 out/build/Debug/modules/module_x/module_x_test   # rewrites all
cmake --build --preset Debug --target module_x_test
git diff                                                          # this is the review
```

One run fixes **every** stale snapshot in the file, not just the first. Line
numbers shifting as values grow or shrink is handled: all locations are resolved
against the original text before any edit, then applied bottom-up.

## Update just one — `.update()`

For the inner loop on a single value. No env var; every other snapshot in the
suite keeps asserting normally and can still catch an unintended change.

```cpp
check_snapshot(render(x), snapshot("stale").update());
```

```sh
out/build/Debug/modules/module_x/module_x_test    # rewrites only this one
```

Then **delete the `.update()`**. Once the value matches, a leftover marker fails
the test with `matches but still has .update() on it` — it would otherwise
silently accept every future change to that snapshot.

## Rules for the expected value

Two spellings, and the updater picks whichever suits the value.

**Single line** — one ordinary literal, inline:

```cpp
check_snapshot(f(), snapshot("total 0\n"));
```

**Spanning lines** — a block literal: a `snap`-delimited raw string through the
`_snap` suffix, with a `|` margin. No escaped newlines; the file holds the text
the program printed.

```cpp
check_snapshot(f(), snapshot(R"snap(
                             |apple    3
                             |total  115
                             )snap"_snap));
```

`_snap` strips, at compile time, the newline after the opening delimiter and
each line's leading spaces plus one `|`. Everything *after* the `|` is content,
so the block's indentation never leaks into the value and a value's own leading
whitespace survives. A value line starting with `|` is written `||`.

**`_snap` must be in scope** — a literal operator is found by unqualified lookup,
not ADL. Add `using snapshot_testing::operator""_snap;` to any test file, or the
block the updater writes won't compile.

| Do | Don't |
|---|---|
| `snapshot("a\n")` — one single-line literal | `snapshot(R"(...)")` — any raw string but the `_snap` block |
| `snapshot(R"snap(...)snap"_snap)` — block literal | mixing a block with an ordinary literal in one call |
| `\n \t \r \" \\` in quoted form | `\x41`, `\0`, `ሴ` — variable-length escapes are refused |
| plain `"..."` | `L"..."`, `u8"..."` — prefixes are refused |

Values holding a tab, a carriage return, or the literal text `)snap"_snap` fall
back to the escaped one-literal-per-line form automatically.

Values must be UTF-8. Both the source file and the new value are validated
before anything is written.

## Snapshots are values

`Snapshot` is an ordinary struct — pass it to helpers. The location is captured
where `snapshot()` is *called*, so the helper can live anywhere:

```cpp
void check_item(const Item& item, const snapshot_testing::Snapshot& expected) {
    check_snapshot(render(item), expected);        // failure blamed on the line below
}

TEST_CASE("...") {
    check_item(kiwi, snapshot("kiwi  7\n"));       // location captured here
}
```

Make these helpers **functions, not macros** — see above.

## Make output snapshot-worthy

Serialize to text that reads well and is **deterministic**. The formatting
function is what makes the snapshot useful:

- No addresses, timestamps, hash values, or unordered-container iteration order.
- One record per line — the updater emits one source line per value line, so a
  diff of the snapshot is a line diff of the value.
- Include the fields you'd want to see when it breaks; a snapshot is free to be
  large.

## Layout the updater produces

Single line (including one trailing `\n`) stays inline; anything spanning lines
becomes a block literal aligned under the opening paren:

```cpp
check_snapshot(f(), snapshot("total 0\n"));

check_snapshot(f(), snapshot(R"snap(
                             |a
                             |b
                             )snap"_snap));
```

A value with no trailing newline keeps `)snap"_snap` on the last content line —
a break before it would be inside the raw string and add a newline the value
never had.

## Run

```sh
out/build/Debug/modules/module_x/module_x_test --test-case="render*"
SNAPSHOT_UPDATE=1 out/build/Debug/modules/module_x/module_x_test --test-case="render*"
```

`SNAPSHOT_UPDATE=1` scopes to whatever the filter selects — narrow it to update
one test.

## Architecture

| File | What |
|---|---|
| `include/snapshot/snapshot.h` | `snapshot()`, `Snapshot`, `compare` → `Comparison`, `render_mismatch` — no doctest dependency |
| `include/snapshot/check.h` | `check_snapshot()` — the assertion; include this from tests |
| `include/snapshot/updater.h` | `apply_updates` — pure `(source, diffs) -> text \| error` |
| `updater.cc` | the rewriter: anchoring, literal parsing, UTF-8 checks, bottom-up edits |
| `flush_reporter.cc` | doctest listener; applies updates after the run |
| `example_test.cc` | worked example — copy from here |
| `updater_test.cc`, `compare_test.cc` | plain unit tests (**not** snapshot tests — see gotchas) |

The updater does not parse C++. It goes to the reported
source location, verifies that it points to the `snapshot` identifier (or just past it),
verifies the call's shape (`( <single-line literals> )`), checks the literals
still decode to the value the test saw, and rewrites only that span.

## Troubleshooting

| Symptom | Fix |
|---|---|
| ``no `snapshot` identifier at line N column C`` | file edited since the test ran, or `snapshot()` got wrapped in a macro — rebuild, re-run, then update |
| `no longer holds the recorded value` | file edited since the test ran — rebuild, re-run, then update |
| ``` `snapshot` at ... is not followed by '(' ``` | problematic syntax, fix on case-by-case basis |
| `expected a string literal or ')' ... found 'R'` | a raw string that isn't the `R"snap(...)snap"_snap` block form |
| `mixes a block literal with another literal` | one spelling per call — a block *or* quoted literals, not both |
| `unterminated block literal ... no )snap"_snap` | the block's closing delimiter is missing or misspelled |
| `no matching literal operator for ... _snap` | add `using snapshot_testing::operator""_snap;` to the test file |
| `unsupported escape '\x'` | only `\n \t \r \" \\` are decoded; rewrite the value |
| `unterminated string literal` | a literal spans a line break; keep each on one line |
| `refusing to update ...: reported a relative path` | the TU was compiled with a relative source path; build via the CMake presets, which pass absolute ones |
| `source file is not valid UTF-8` | fix the file's encoding; nothing was written |
| `new snapshot value ... is not valid UTF-8` | the code under test emitted invalid UTF-8 — that's the bug |
| Nothing rewritten, test still fails | `SNAPSHOT_UPDATE=1` not set and no `.update()`, or a filter excluded the case |
| `matches but still has .update() on it` | the marker did its job — delete `.update()` |
| Rewrote, but still fails on re-run | you didn't rebuild between the update run and the re-run |
| Update aborts mid-way | a partial rewrite is refused outright; fix the reported cause and re-run |

## Gotchas

- **Never wrap `snapshot()` in a macro.** It destroys the location the updater
  anchors on; see above. Helpers over snapshots are plain functions.
- **Rebuild between the update run and the verifying run** — the new values are
  in the source, not the binary.
- **Don't edit the file between the run and the update.** The recorded locations
  describe the file the compiler saw; the updater will refuse rather than rewrite
  the wrong span, but you'll have to re-run.
- **A passing snapshot writes nothing.** The file system is touched only when
  the test would otherwise fail, so a green suite runs fine without sources.
- **Review the diff.** Update mode accepts whatever the code currently does;
  it does not know whether that is correct. This is the whole risk of the
  technique.
- Don't set `SNAPSHOT_UPDATE=1` in CI, and don't commit a `.update()`. A rewrite
  there fixes nothing and hides a real behaviour change. The leftover-marker
  check catches a committed `.update()` only once its value matches, so review
  the diff rather than relying on it.
