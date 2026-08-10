# Per-test coverage in the VS Code test explorer

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
binary with `--test-case=<name>` in its own process, and
`LLVM_PROFILE_FILE` is **already set per test**:

```cmake
LLVM_PROFILE_FILE=set:$<TARGET_FILE:${name}_test>.%p%m.profraw
```

So test-level process isolation is already there. The profile filename just
identifies the *binary* and the *PID* — not the test. That is the one thing
missing, and it is the smallest change in this plan.

---

## What needs to change

### 1. Make the profile filename identify the test

**What:** each `.profraw` must be attributable to the CTest test that produced
it, without relying on PID.

The filename is set in `cmake/Module.cmake` in the `doctest_discover_tests`
`PROPERTIES` block. The problem: that one line is shared by every discovered
test in the module, and at that point CMake has no per-test name to interpolate.

The natural place to fix it is **`nix/patches/doctest-discover-tests.patch`**,
which is already the mechanism for per-test property injection — it is where
`DEF_SOURCE_LINE` is computed per case and appended to
`add_command(set_tests_properties ...)`. The test name is in scope there as
`${test}` / `${test_name}`, and the full CTest name as `${prefix}${test}${suffix}`.

Add a per-test `LLVM_PROFILE_FILE` (or a new opt-in parameter carrying a
directory) so each case writes to a path derived from its own CTest name.

Design constraints to respect:

- **Sanitize the name for the filesystem.** doctest case names are arbitrary user
  strings — spaces, slashes, quotes, `:::` from the module `TEST_PREFIX`. Pick an
  encoding that round-trips, or emit a **sidecar manifest** mapping filename →
  CTest test name. A manifest is the more robust option and avoids a lossy
  escaping scheme; it also survives names that collide after sanitization.
- **Keep `%p%m`** in addition to the test name. A single test may spawn
  subprocesses, and `%m` is what keeps concurrent writers apart. The name
  identifies the test; `%p%m` keeps files unique.
- **Don't break the non-coverage build.** Nothing writes the file without
  instrumentation, which is why the current line is harmless — preserve that.
- **The Python tests** in the root `CMakeLists.txt` set their own
  `LLVM_PROFILE_FILE` anchored to the build dir. Decide whether they participate;
  they can reasonably be left as-is at first.

### 2. Emit per-test LCOV, not just a merged one

**What:** `coverage-export` must produce one LCOV per test, in addition to the
existing `total.lcov`.

`tools/merge-coverage` already groups `.profraw` files by filename stem and runs
`llvm-profdata merge` per group — the grouping loop is most of the work. What it
currently emits per group is an HTML report; it needs to also emit LCOV
(`llvm-cov export --format=lcov`, as `cmake/Coverage.cmake` already does for the
total).

Requirements:

- Output to a predictable layout, e.g. `coverage/per-test/<id>.lcov`, plus the
  manifest from step 1 mapping each file to its CTest test name.
- Keep `total.lcov` exactly as-is. The existing whole-suite view must not
  regress.
- Note the existing build-id resolution and `.build-id` symlink farm — per-test
  export needs the same `--debug-file-directory` treatment to resolve binaries.
- Watch the cost. This is N invocations of `llvm-profdata` + `llvm-cov` instead
  of one. Consider making per-test export opt-in via a CMake option or an env
  var so ordinary coverage runs stay fast.

### 3. The extension

**What:** a VS Code extension that reads the artifacts from step 2 and feeds the
API from the background section.

Decide first between two hosting options:

- **Extend CMake Tools upstream.** Correct long-term, but requires negotiating
  the design of a per-test mode with maintainers (their post-run-target model
  assumes a single merged dataset), and doesn't help this repo this week.
- **A separate local extension.** Recommended to start. Register your own
  `TestController` with a single `TestRunProfileKind.Coverage` profile.

Note the cost of the separate-extension route: a run profile can only be created
via `controller.createRunProfile`, so it always belongs to a controller you
created — there is no way to attach a profile to CMake Tools' controller. That
means a **duplicate test tree** in the explorer. This is cosmetic; mitigate with
a distinct controller label.

The extension needs:

- **Test discovery**, to build `TestItem`s whose IDs match the CTest test names
  in the manifest. Use `ctest --show-only=json-v1` against the configured build
  directory — this is what CMake Tools does, and it reads `CTestTestfile.cmake`
  without needing the binaries to have been run. IDs must match the manifest
  exactly, or the `codeCoverageDecorations.ts` assertion fires.
- **When to discover:** on activation from a cached tree (don't shell out on the
  activation path); a `refreshHandler` for the manual, always-correct path; and a
  debounced `FileSystemWatcher` on the build directory as a build signal. The
  build directory is `out/build/<presetName>` per `CMakePresets.json` — the
  `Coverage` preset is the relevant one.
- **An index**: `file → line → set<test id>`, built from the per-test LCOV files.
  This single structure serves both halves of the goal.
- **Coverage reporting**: one `FileCoverage` per file with `includesTests`
  populated (merge across tests — do not emit one `FileCoverage` per file *per
  test*), plus `loadDetailedCoverage` and `loadDetailedCoverageForTest`.
- **The per-line command**: contributed to `editor/context`, reading the cursor
  line, querying the index, and showing the covering tests in a quick-pick.
  Selecting one should reveal it in the Test Explorer.

An important simplification available here: the extension does **not** need to
run the build or the tests to be useful. It can ingest LCOV files produced by an
ordinary `cmake --build --target coverage-export`. Consider making ingestion the
first milestone and test *execution* a later one — it gets the interesting
feature working with far less machinery. When creating a run for
already-existing files, note `createTestRun`'s `persist` parameter, which the API
docs specifically describe for coverage data read from disk.

---

## Suggested order

1. Per-test `LLVM_PROFILE_FILE` + manifest (patch + `Module.cmake`). Verify by
   inspecting the `.profraw` files after a `Coverage`-preset ctest run.
2. Per-test LCOV export (`tools/merge-coverage`, `cmake/Coverage.cmake`). Verify
   the files exist and that `total.lcov` is unchanged.
3. Extension: ingest + index + the per-line command. This is the payoff and is
   independently useful.
4. Extension: the native coverage API surface (`includesTests` +
   `loadDetailedCoverageForTest`). Verify `testing.hasPerTestCoverage` flips —
   the "Filter Coverage by Test" entry appearing in the command palette is the
   observable signal.
5. Only then consider test discovery/execution ownership, or upstreaming.

Steps 1 and 2 are testable from the command line with no extension at all, and
steps 3 and 4 are independent of each other. Don't build the extension first.

---

## Open questions for whoever picks this up

- **Name encoding vs. manifest.** Recommended above as a manifest, but if a
  reversible encoding is preferred, decide it once and apply it in both the patch
  and `tools/merge-coverage`.
- **Cost of per-test export** on the full suite is unmeasured. Measure before
  deciding whether it is on by default.
- **The `python.*` CTest tests** spawn multiple binaries under one test. They fit
  the model (one CTest test, one profile path) but their coverage spans several
  executables; confirm the build-id mapping handles that as expected — it
  currently anchors their profile to the build dir for exactly this reason.
- **`ctest -j`.** Because every doctest case is its own CTest test and its own
  process, parallel runs are fine as long as the filename carries the test name
  *and* `%p%m`. This project does not have the batching problem that would
  otherwise make parallel per-test coverage hard.
