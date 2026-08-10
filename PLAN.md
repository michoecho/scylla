# Per-test coverage in the VS Code test explorer

> **Status.** Steps 1 and 2 are done: a `Coverage`-preset run now writes one
> LCOV per test plus a manifest naming them, under
> `<build>/coverage/per-test/`. Steps 3 and 4 — the extension work — have not
> been started. See "Suggested order" at the end for how to reproduce the
> current state.

## The goal

Right-click a line of C++ and find out **which tests in the suite covered it**.

Today this project produces one merged LCOV file for the whole suite, and VS Code
shows covered/uncovered markers from it. That answers "is this line covered",
not "by what". This plan closes that gap.

The end state has two halves:

1. **Native per-test coverage.** The Test Explorer gains a per-test filter: pick
   a test, and the gutter markers narrow to just the lines that test executed.
   This is a built-in VS Code feature that nothing currently feeds.
2. **A per-line reverse lookup.** A context-menu command on a source line that
   lists the tests covering it. This one has no built-in equivalent and must be
   written.

Half 1 is mostly plumbing into an API that already exists. Half 2 is a small
amount of new UI over the same index. Do them in that order — half 1 forces the
data model into shape, and half 2 is cheap once it exists.

---

## Background: what VS Code already supports

The stable `vscode.d.ts` API supports per-test coverage. No proposed API needed.
Reference a real checkout of `microsoft/vscode` (clone with
`--filter=blob:none`); the symbols below are stable enough to grep for by name.

- **`FileCoverage.includesTests?: TestItem[]`** — the list of tests that
  generated coverage in a file. This is the fifth constructor parameter, and it
  is the switch that turns the whole feature on.
- **`TestRunProfile.loadDetailedCoverageForTest(testRun, fileCoverage, fromTestItem, token)`**
  — called when the user drills into one test; return only the statements that
  test executed. The per-test sibling of `loadDetailedCoverage`.
- **`TestRunProfileKind.Coverage`** — the profile kind to register.

The editor-side machinery is fully built and waiting for data:

- `src/vs/workbench/contrib/testing/common/testingContextKeys.ts` — the context
  key `testing.hasPerTestCoverage`. Not something an extension sets; the editor
  derives it.
- `src/vs/workbench/contrib/testing/common/testCoverageService.ts` — where that
  key is computed: true iff the selected coverage report contains at least one
  test ID.
- `src/vs/workbench/contrib/testing/common/testCoverage.ts` — the model. Note
  `perTestData` is a `Set<string>` **per file**, and `filterTreeForTest`. This is
  the reason half 2 exists: the built-in filter is per-file, not per-line.
- `src/vs/workbench/contrib/testing/browser/testCoverageView.ts` — the
  "Filter Coverage by Test" quick-pick.
- `src/vs/workbench/contrib/testing/browser/codeCoverageDecorations.ts` — the
  in-editor toolbar ("N test(s) ran code in this file"). **Note the assertion
  here**: every test named in `includesTests` must be a `TestItem` that actually
  exists in the controller, or the editor throws.

### Why CMake Tools doesn't do this

Clone `microsoft/vscode-cmake-tools` for reference. Two files matter:

- `src/coverage.ts` — parses LCOV into `FileCoverage`. It passes at most four
  constructor arguments, so `includesTests` is never set.
- `src/ctest.ts` — registers the Coverage run profile and wires
  `loadDetailedCoverage`. `loadDetailedCoverageForTest` appears nowhere in the
  repo.

The structural reason: coverage is collected **once per run, after all tests
finish**, via the `preRunCoverageTarget` / `postRunCoverageTarget` settings, and
merged into a flat per-file picture. Which test produced which line is discarded
before the extension ever sees it.

Also relevant — CMake Tools has both a serial path (one `ctest` invocation per
test) and a parallel path (`ctest -j`, one invocation for many tests). Per-test
attribution is easy in the former and needs filename stamping in the latter.
**This project sidesteps that entirely** — see below.

---

## What this project already has

This is the good news: most of the hard infrastructure exists. Read these before
changing anything.

- **`CMakeLists.txt`**, the `ENABLE_TEST_COVERAGE` block — sets
  `-fprofile-instr-generate=/dev/null -fcoverage-mapping`, `-Wl,--build-id` and
  `-no-pie`, and defines the `coverage-clean` / `coverage-export` targets that
  CMake Tools invokes before and after a coverage run.
- **`cmake/Coverage.cmake`** — the CLEAN and EXPORT actions. EXPORT calls
  `tools/merge-coverage`, then `llvm-cov export --format=lcov` into
  `coverage/total.lcov`.
- **`tools/merge-coverage`** — maps `.profraw` files back to binaries by ELF
  build-id, groups them, and already generates **per-group** reports as well as
  the total.
- **`cmake/Module.cmake`**, the `doctest_discover_tests` call at the end of
  `add_module()` — sets `LLVM_PROFILE_FILE` per test via
  `ENVIRONMENT_MODIFICATION`.
- **`nix/patches/doctest-discover-tests.patch`** — the existing custom patch to
  doctest's discovery scripts. It already adds a `TEST_SUBCOMMAND` parameter and
  a `DEF_SOURCE_LINE` lookup via the project's `test-locations` reporter.

### The decisive fact

Every doctest case is registered with CTest as its **own test**, running the
binary with `--test-case=<name>` in its own process. So test-level process
isolation was already there; the profile filename identified the *binary* and
the *PID*, not the test.

That was the one thing missing, and closing it was the smallest change in this
plan — step 1 below, now done. The filename now identifies the test.

---

## What needs to change

### 1. Make the profile filename identify the test — **DONE**

**What:** each `.profraw` must be attributable to the CTest test that produced
it, without relying on PID.

Done in **`nix/patches/doctest-discover-tests.patch`**, alongside the existing
`DEF_SOURCE_LINE` injection. `doctest_discover_tests` gained a `PROFILE_DIR`
parameter; when set, each discovered case gets its own `LLVM_PROFILE_FILE`:

```
<PROFILE_DIR>/<md5 of "${prefix}${test}${suffix}">.%p%m.profraw
```

**The id is a hash, and a sidecar manifest carries the real name.** Of the two
options weighed here, the manifest won — but the filename is not a sanitized
name, it is an MD5 of the full CTest name. That makes the encoding question
disappear rather than be answered: no escaping scheme for spaces, slashes,
quotes and the module `:::` prefix that both writer and reader must agree on,
and no way for two names to collide once sanitized. The name is recoverable
only through the manifest, which is the point.

`%p%m` is kept after the id: the id identifies the *test*, `%p%m` keeps the
individual writers *within* that test unique (a test spawning subprocesses has
several).

The manifest is one `<id> <ctest name>` record per line — name last, so it may
contain spaces unquoted; id fixed-width, so the reader splits on the first
space. Written per discovered runner as `<ctest-script-name>.manifest` in
`PROFILE_DIR`, so two runners cannot overwrite each other; the reader
concatenates every `*.manifest` it finds.

Where the files land: `<build>/coverage-profraw/`, set by
`COVERAGE_PROFILE_DIR` in the root `CMakeLists.txt`. One directory rather than
"next to the test binary", because the filename now names a test and not a
binary — and because the manifests need to be somewhere findable without
walking the build tree.

Two things worth knowing before touching this again:

- **`cmake/Module.cmake` must not also set `LLVM_PROFILE_FILE`.** CTest keeps
  only the *last* `ENVIRONMENT_MODIFICATION` for a given variable, so the old
  per-module line would silently replace the per-test path. It was removed, and
  a module must likewise not set it via `TEST_PROPERTIES`.
- **`PROFILE_DIR` must precede `ADD_LABELS` in the call.** `ADD_LABELS` is a
  multi-value keyword, so anything following it is eaten as one of its values.
  This fails loudly at build time, but the message names `if()`, not the call.

**The Python tests participate.** The root `CMakeLists.txt` computes the same
MD5 over `python.<name>` and writes `python.manifest` in the same format. A
python test spawning several binaries is fine and is why its profile path was
never anchored to one target: all its profiles share its id and merge into one
per-test report spanning both executables.

Non-coverage builds are unaffected: `COVERAGE_PROFILE_DIR` is only set under
`ENABLE_TEST_COVERAGE`, and without instrumentation nothing writes the file.

### 2. Emit per-test LCOV, not just a merged one — **DONE**

**What:** `coverage-export` must produce one LCOV per test, in addition to the
existing `total.lcov`.

`tools/merge-coverage` gained `--per-test` and `--profile-dir`. It groups the
per-test `.profraw` files back onto their id (by stripping the `%p%m` suffix),
merges each group, and exports `coverage/per-test/<id>.lcov`.

**The output the extension reads** is `coverage/per-test/manifest.json`:

```json
{ "tests": [ { "id": "<md5>", "test": "<ctest name>", "lcov": "<md5>.lcov" } ] }
```

JSON here, unlike the flat `*.manifest` of step 1, because its consumer is the
extension rather than CMake. It lists only tests that actually have an `.lcov`
beside them, so a consumer never has to handle a named test with no report.

- **Binary resolution** reuses the `.build-id` symlink farm the total pass
  already builds, via `--debug-file-directory`. That is why per-test export runs
  *after* the total report and needs no build-id lookup of its own — and it is
  what lets one invocation per test cover whichever binaries that test ran,
  including a python test that spawned several.
- **`total.lcov` is unchanged** — verified byte-identical with the option ON and
  OFF.
- **Cost** is handled by making export opt-in: `ENABLE_PER_TEST_COVERAGE`,
  default OFF, ON in the `Coverage` preset. It gates only the *export*; the
  per-test `.profraw` files are always collected under `ENABLE_TEST_COVERAGE`,
  since the profiles are written either way. So turning the option on does not
  require re-running the suite. Measured cost on the current suite: 88 tests,
  a few seconds — not enough to justify anything cleverer.
- **`coverage-clean` removes `coverage/per-test/`.** Stale reports here are
  worse than untidy: a renamed or deleted test would keep a report and the
  extension would attribute coverage to a test that no longer exists.

One incidental fix: `merge-coverage`'s existing HTML report loop grouped by
filename stem, which under hashed profile names would have produced one
directory per test named by an opaque hash. It now resolves the name through
the manifest and sanitizes it for use as a directory component.

### 3. The per-test coverage cmake tools extension — **DONE**

**What:** Extend the CMake Tools extension so that it is able to match lcov
files to test cases, and so that it uses this to feed the native testing APIs.

Upstream v1.23.52 — the version nixpkgs packages — is forked into
**`tools/vscode-cmake-tools/`**; see its `README.md` for the full account. The
three changes that matter:

- **`src/perTestCoverage.ts`** (new) indexes
  `coverage/per-test/manifest.json` and its LCOVs into *file → test →
  executed statements*. It keeps only lines with `hit > 0`: an LCOV section
  lists every instrumented line in the file, so keeping the zeroes would make
  every test look like it covered every file it was merely linked against.
- **`src/coverage.ts`** now passes the fifth `FileCoverage` argument,
  `includesTests`.
- **`src/ctest.ts`** registers `loadDetailedCoverageForTest`.

**The name matching resolved cleanly.** CMake Tools gives each discovered test
a `TestItem` whose *id* is the full CTest name — only the *label* is shortened
to the last `:::` segment — so matching the manifest against it is an exact
string comparison, with nothing to parse or escape. Verified against a real
run: all 88 manifest names resolve.

**The one real obstacle was the type definitions.** `@types/vscode` was pinned
at 1.88, and both APIs are absent from the published `index.d.ts` through
**1.95**; 1.96 is the first release containing them. The pin and the
`engines.vscode` floor moved to 1.96 (the installed editor is 1.130). That bump
also broke upstream's test fakes, which needed a
`languageModelAccessInformation` stub.

Three of the four `python.*` tests run no instrumented binary, so they have no
manifest entry; the extension treats a reportless test as "no coverage" rather
than an error, and a test asserts that this case still occurs in real data.

**Verification.** Non-interactive: `tsc --noEmit` is clean across `src/`, and
`ctest -R per_test_coverage_manifest` (new,
`tools/tests/test_per_test_coverage_manifest.py`) checks every manifest name
against `ctest --show-only=json-v1` — the invariant that matters, since
`codeCoverageDecorations.ts` *asserts* on unknown names rather than ignoring
them. The suite is 93/93. What remains is interactive and needs a human: that
"Test: Filter Coverage by Test" appears and the gutter actually narrows.

Build and install with `tools/vscode-cmake-tools/build-and-install`. The fork
shares upstream's identity, so it shadows the nixpkgs copy in the
project-local editor. Note that `--extensions-dir` *replaces* the extension set
rather than adding to it, so the script seeds the writable directory from the
Nix set first — otherwise clangd, Python and LLDB silently vanish.

### 4. The line -> tests query command — **NOT STARTED**

- **The per-line command**: contributed to `editor/context`, reading the cursor
  line, querying the index, and showing the covering tests in a quick-pick.
  Selecting one should reveal it in the Test Explorer.

## Suggested order

1. ~~Per-test `LLVM_PROFILE_FILE` + manifest (patch + `Module.cmake`).~~ **Done.**
2. ~~Per-test LCOV export (`tools/merge-coverage`, `cmake/Coverage.cmake`).~~
   **Done.** Both verified from a clean tree: 90 tests across the discovery
   manifests, 88 per-test `.lcov` files, attribution spot-checked (an
   `exponential_histogram` case's executed lines concentrate in
   `exponential_histogram.cc`), and `total.lcov` byte-identical with the option
   ON and OFF.
3. ~~Extension: the native coverage API surface (`includesTests` +
   `loadDetailedCoverageForTest`).~~ **Done**, as a fork of upstream v1.23.52
   in `tools/vscode-cmake-tools/`. Typechecks clean, name matching verified
   against a real run by a new ctest test, suite 93/93. The remaining check is
   interactive: that `testing.hasPerTestCoverage` flips, observable as
   "Filter Coverage by Test" appearing in the command palette.
4. **Next.** The line->tests query extension. It can build directly on
   `PerTestCoverageIndex`, which already holds *file → test → lines*; a
   reverse lookup is a scan of that index at one line.

Steps 3 and 4 are independent of each other.

**Reproducing the current state:**

```sh
cmake --preset Coverage
cmake --build --preset Coverage
cmake --build --preset Coverage --target coverage-clean
(cd out/build/Coverage && ctest -j8)
cmake --build --preset Coverage --target coverage-export
# -> out/build/Coverage/coverage/per-test/{manifest.json,<id>.lcov}
```

The `coverage-clean` matters: without it, profiles from an earlier run merge
into this one and execution counts accumulate.

---

## Open questions — resolved by steps 1 and 2

- ~~**Name encoding vs. manifest.**~~ Manifest, with an MD5 id rather than a
  sanitized name, so there is no encoding to agree on. Applied in both the patch
  and `tools/merge-coverage`.
- ~~**Cost of per-test export.**~~ Measured: seconds for 88 tests. Made opt-in
  anyway (`ENABLE_PER_TEST_COVERAGE`), since the cost is linear in the suite
  size and nothing but the extension needs the output.
- ~~**The `python.*` CTest tests.**~~ They participate, and the build-id mapping
  handles the multi-binary case as expected — one invocation per test resolves
  every binary it ran through the `.build-id` farm. Caveat found while
  verifying: only `python.test_pt_trace` actually runs an instrumented binary,
  so the other three have no per-test report at all.
- ~~**`ctest -j`.**~~ Confirmed: the whole suite was verified under `ctest -j8`.
  The filename carries both the test id and `%p%m`, so parallel runs do not
  collide.

## Open questions — resolved by step 3

- ~~**What the extension keys on.**~~ The `TestItem` *id* is the full CTest
  name (the *label* is the shortened last segment), so it is an exact string
  match with nothing to parse. Checked against a real run by
  `tools/tests/test_per_test_coverage_manifest.py`, which exists precisely
  because `codeCoverageDecorations.ts` throws on an unknown name.
- ~~**Where the extension finds the manifest.**~~ Derived from the directory of
  the configured coverage info file rather than from the build directory:
  `per-test/` beside whatever `cmake.coverageInfoFiles` points at. No second
  setting to keep in sync, and nothing new to surface.

## Still open

- **Fork maintenance.** The fork pins upstream v1.23.52 to match nixpkgs, and
  its dependencies come from `yarn` over the network rather than from Nix —
  the one place this repo departs from reproducible builds. Both are revisited
  whenever the nixpkgs extension version moves.
