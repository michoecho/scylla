---
name: snapshot-tests
description: Write or update snapshot ("expect") tests — _snap literals whose expected value is rewritten into the source by the test runner, via SNAPSHOT_UPDATE=1 or a per-snapshot .update(). Use when asked to add a snapshot/expect test, accept new expected output, update snapshots after a behaviour change, characterise existing output, or fix a "no snapshot literal at ..." / "no longer holds the recorded value" / "still has .update() on it" / "not valid UTF-8" / unsupported-literal error.
---

# Snapshot (expect) tests

An assertion whose expected value is written back into the source file by the
runner. You write the test empty, run once in update mode, and review the diff.

Provided by the `snapshot` module (`modules/snapshot/`), in namespace
`snapshot_testing`. Link it and include one header.

## Add one

`modules/module_x/BUCK`:

```python
add_module(
    name = "module_x",
    srcs = ["x.cc", "x_test.cc"],
    deps = ["//modules/snapshot:snapshot"],
)
```

`modules/module_x/x_test.cc`:

```cpp
#include <doctest/doctest.h>

#include "snapshot/check.h"

using snapshot_testing::check_snapshot;
using snapshot_testing::operator""_snap;

TEST_CASE("render_table lays out aligned columns") {
    check_snapshot(render_table(items), ""_snap);
}
```

Then fill it in:

```sh
SNAPSHOT_UPDATE=1 buck2 run //modules/module_x:module_x_test
buck2 test //modules/module_x:module_x_test
git diff                                              # review what it wrote
```

The update run **still fails** — by design. A run that rewrote sources must not
come back green.

## File-backed snapshots

Use `_filesnap` when the expected value is large, binary, or should remain
stable while the test source moves. Start with an empty ID:

```cpp
using snapshot_testing::operator""_filesnap;
check_snapshot(render_document(), ""_filesnap);
```

An explicit update generates a UUID in the source and stores the exact bytes
under `.snapshots/<first-two-hex-digits>/<uuid>.snap`. `_filesnap.update()` and
update mode behave exactly like their inline counterparts, including the
deliberately failing update run and stale-marker check.

Reads and writes use different roots, and both are project-relative. Reads come
from `SNAPSHOT_ROOT`, the build's copy of the store, so a test that compares
against file snapshots stays hermetic and can run remotely. Writes go to
`SNAPSHOT_SOURCE_ROOT`, the store's path in the source tree, so a recorded
snapshot reaches the repository rather than buck-out. Inline `_snap` updates use
neither — they need no store at all.

File snapshots compare bytes exactly. Do not copy an initialized `_filesnap`
literal: the UUID has one owning assertion. To audit the whole repository run:

```sh
tools/snapshot-files                 # dry-run validation (default)
tools/snapshot-files --cleanup       # remove orphans, but only if nothing else is wrong
```

The validator reports duplicate or malformed literals, missing snapshot files,
malformed storage paths, and orphans. Buck2 test runs the dry-run validator too.

## Regex snapshots — for values with unreproducible parts

When part of the value cannot be reproduced — a buck-out path, an address, a
line number that moves whenever the file above it is edited — serialize to a
`RegexText` instead of a string. It carries the text and a pattern describing
its general form side by side:

```cpp
#include "snapshot/regex_text.h"

using snapshot_testing::RegexText;

RegexText serialize_offset(std::uintptr_t offset) {
    RegexText out;
    out.variable(std::format("{:#x}", offset), "0x[0-9a-f]+");   // varies
    return out;
}

RegexText serialize_row(const Row& row) {
    RegexText out;
    out.literal("row ").literal(row.name).literal(" at ");       // asserted
    out.append(serialize_offset(row.offset));
    out.literal("\n");
    return out;
}

TEST_CASE("...") {
    check_snapshot(serialize_row(row), ""_snap);   // same call, same _snap
}
```

The snapshot is **recorded from `text()`** — the file holds a full, readable
sample, real paths and all — but **compared against `pattern()`**, by full
match. So a rebuild that moves every offset stays green, while a row appearing,
disappearing, or being renamed fails and is re-recorded.

- `literal(s)` — contributes `s` to both halves, escaped (`RE2::QuoteMeta`) on
  the pattern side, so a `.` in a path stays a `.`.
- `variable(text, pattern)` — the sample and its form. `pattern` is wrapped in
  `(?:...)`, so an alternation stays local.
- `append(other)` — concatenate both halves, which is what lets a per-field
  serializer be a function and the record serializer compose them.

Patterns are **RE2**, not `std::regex`: matching is linear in the value, and a
malformed pattern comes back as a failed assertion naming it rather than as an
exception. A module using this needs `//modules/snapshot:snapshot` and nothing
else — re2 arrives with it.

Rules that follow from the design:

- **A variable field is a sample, never an assertion.** It is not re-checked and
  goes stale silently. Declare one only for a field that genuinely varies, and
  give it the tightest pattern that admits every value it can take.
- **Don't align columns across a variable field.** Padding is literal text, so
  padding computed from a variable field's width is asserted against a width
  that changes. One field per line instead.
- **Keep record order independent of the variable fields.** Sorting a listing by
  a path or an address orders it by something the snapshot does not assert.

`modules/static_keys/static_key_list_test.cc` is the worked example.

## No macros — and why it matters

`check_snapshot()` is an ordinary function, and `_snap` first produces an
unlocated token. Its implicit conversion to `Snapshot` has a defaulted
`std::source_location::current()` argument, so conversion captures the literal's
opening character. That exact location is the only thing the updater uses to
find the expression.

Inside a macro expansion that guarantee evaporates: the reported location is
wherever the expansion is anchored, not the macro's name, and it moves with the
surrounding expression.

**So: never wrap a `_snap` literal in a macro.** Write helpers as functions
(see "Snapshots are values" below) and everything keeps working.

## Update after a behaviour change

```sh
SNAPSHOT_UPDATE=1 buck2 run //modules/module_x:module_x_test   # rewrites all
buck2 test //modules/module_x:module_x_test
git diff                                                          # this is the review
```

One run fixes **every** stale snapshot in the file, not just the first. Line
numbers shifting as values grow or shrink is handled: all locations are resolved
against the original text before any edit, then applied bottom-up.

## Update just one — `.update()`

For the inner loop on a single value. No env var; every other snapshot in the
suite keeps asserting normally and can still catch an unintended change.

```cpp
check_snapshot(render(x), "stale"_snap.update());
```

```sh
buck2 run //modules/module_x:module_x_test    # rewrites only this one
```

Then **delete the `.update()`**. Once the value matches, a leftover marker fails
the test with `matches but still has .update() on it` — it would otherwise
silently accept every future change to that snapshot.

## Rules for the expected value

Two spellings, and the updater picks whichever suits the value.

**Single line** — one ordinary literal, inline:

```cpp
check_snapshot(f(), "total 0\n"_snap);
```

**Spanning lines** — a block literal: a `snap`-delimited raw string through the
`_snap` suffix, with a `|` margin. No escaped newlines; the file holds the text
the program printed.

```cpp
check_snapshot(f(), R"snap(
    |apple    3
    |total  115
    )snap"_snap);
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
| `"a\n"_snap` — one single-line literal | `R"(...)"_snap` — any raw string but the supported block |
| `R"snap(...)snap"_snap` — block literal | mixing a block with adjacent ordinary literals |
| `\n \t \r \" \\` in quoted form | `\x41`, `\0`, `ሴ` — variable-length escapes are refused |
| plain `"..."` | `L"..."`, `u8"..."` — prefixes are refused |

Values holding a tab, a carriage return, or the literal text `)snap"_snap` fall
back to the escaped one-literal-per-line form automatically.

Values must be UTF-8. Both the source file and the new value are validated
before anything is written.

## Snapshots are values

`Snapshot` is an ordinary struct — pass it to helpers. The location is captured
where a `_snap` token converts to `Snapshot`, so the helper can live anywhere:

```cpp
void check_item(const Item& item, const snapshot_testing::Snapshot& expected) {
    check_snapshot(render(item), expected);        // failure blamed on the line below
}

TEST_CASE("...") {
    check_item(kiwi, "kiwi  7\n"_snap);       // location captured here
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
becomes a block literal indented **4 spaces from the line holding the literal** —
relative to the line, not to the literal's column, so an expression nested deep in
an expression doesn't fling its value off to the right:

```cpp
check_snapshot(f(), "total 0\n"_snap);

check_snapshot(f(), R"snap(
    |a
    |b
    )snap"_snap);
```

A value with no trailing newline keeps `)snap"_snap` on the last content line —
a break before it would be inside the raw string and add a newline the value
never had.

## Run

```sh
buck2 run //modules/module_x:module_x_test -- --test-case="render*"
SNAPSHOT_UPDATE=1 buck2 run //modules/module_x:module_x_test -- --test-case="render*"
```

Under `buck2 test`, update mode is a buckconfig rather than an environment
variable — `buck2 test` does not forward the caller's environment:

```sh
buck2 test -c snapshot.update=1 //modules/module_x:module_x_test
```

The config also switches the test to a local-only executor, because a rewrite
needs the real source tree and a remotely executed test runs in a sandbox
holding only the action's inputs. No `--local-only` flag: normal runs stay
remote so that their results keep populating the action cache.

`snapshot.update` is repo-wide, so `-c snapshot.update=1 //...` puts every
module into update mode at once. A module can opt out by pinning the variable
in its own `env`, which `add_module` will not overwrite:

```python
add_module(
    name = "snapshot",
    env = {"SNAPSHOT_UPDATE": "0"},
)
```

`modules/snapshot` does exactly that, because its tests are the updater's own
fixtures. Updating such a module means going through `buck2 run`, which does
not apply the test's environment.

`SNAPSHOT_UPDATE=1` scopes to whatever the filter selects — narrow it to update
one test.

## Architecture

| File | What |
|---|---|
| `include/snapshot/snapshot.h` | `_snap` / `_filesnap`, their value types, comparison and diagnostics — no doctest dependency |
| `include/snapshot/regex_text.h`, `regex_text.cc` | `RegexText` — text plus the pattern describing its general form |
| `include/snapshot/check.h` | `check_snapshot()` — the assertion; include this from tests |
| `include/snapshot/updater.h` | `apply_updates` — pure `(source, diffs) -> text \| error` |
| `updater.cc` | the rewriter: anchoring, literal parsing, UTF-8 checks, bottom-up edits |
| `flush_reporter.cc` | doctest listener; applies updates after the run |
| `example_test.cc` | worked example — copy from here |
| `updater_test.cc`, `compare_test.cc`, `regex_text_test.cc` | plain unit tests (**not** snapshot tests — see gotchas) |

The updater does not parse C++. It goes to the reported
source location, verifies that it points to a supported `_snap` expression, checks the literals
still decode to the value the test saw, and rewrites only that span.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `no snapshot literal at line N column C` | file edited since the test ran, or the literal got wrapped in a macro — rebuild, re-run, then update |
| `no longer holds the recorded value` | file edited since the test ran — rebuild, re-run, then update |
| `expected a string literal or _snap` | malformed adjacent quoted-literal form |
| `unterminated block literal ... no )snap"_snap` | the block's closing delimiter is missing or misspelled |
| `no matching literal operator for ... _snap` | add `using snapshot_testing::operator""_snap;` to the test file |
| `unsupported escape '\x'` | only `\n \t \r \" \\` are decoded; rewrite the value |
| `unterminated string literal` | a literal spans a line break; keep each on one line |
| `SNAPSHOT_SOURCE_ROOT must be set to update file snapshots` | only `_filesnap` needs a store; inline `_snap` updates need none |
| `cannot read .../x_test.cc` during an update | the test ran remotely, so there was no source tree to rewrite — update mode must come from `-c snapshot.update=1`, which selects a local executor |
| `source file is not valid UTF-8` | fix the file's encoding; nothing was written |
| `new snapshot value ... is not valid UTF-8` | the code under test emitted invalid UTF-8 — that's the bug |
| `malformed snapshot pattern at ...` | a `variable()` pattern is not valid RE2; the message quotes the compound pattern |
| A regex snapshot fails on every rebuild | a field that varies is being contributed with `literal()`, or column padding was computed from one |
| Nothing rewritten, test still fails | `SNAPSHOT_UPDATE=1` not set and no `.update()`, or a filter excluded the case |
| `matches but still has .update() on it` | the marker did its job — delete `.update()` |
| Rewrote, but still fails on re-run | you didn't rebuild between the update run and the re-run |
| Update aborts mid-way | a partial rewrite is refused outright; fix the reported cause and re-run |

## Gotchas

- **Never wrap a `_snap` literal in a macro.** It destroys the location the updater
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
