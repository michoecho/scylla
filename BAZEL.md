# The Bazel build

A second build, alongside CMake, not generated from it. Both describe the same
sources; only `modules/snapshot` is ported so far.

```sh
nix develop
bazel test //...
```

## What comes from where

Everything C++ comes from Nix, through
[rules_nixpkgs](https://github.com/tweag/rules_nixpkgs): the compiler and every
third-party library. The Bazel Central Registry supplies only Bazel rulesets
(`rules_cc`, `rules_nixpkgs_core`, …), which are Starlark, not artifacts.

The nixpkgs revision is **not** pinned twice. `nix/bazel/nixpkgs.nix` reads it
out of the project's own `flake.lock`, so `nix develop` and `bazel build`
resolve to the same nixpkgs by construction — bumping the flake bumps both, and
there is no second lock file to forget.

That file uses `flake.lock`'s `narHash` as `fetchTarball`'s `sha256`, which
works because they are hashes of the same tree.

## Why nothing depends on `@nixpkgs_*` directly

Targets depend on `//third_party:doctest`, never on `@nixpkgs_doctest`. One
line of indirection per dependency, and the reason is worth stating.

rules_nixpkgs shells out to `nix-build` while Bazel fetches repositories. Fine
on a dev machine and in CI; impossible inside a **nixpkgs derivation**, which
builds in a sandbox with no network and no access to the Nix daemon. A project
wired straight to `@nixpkgs_*` can therefore never be packaged *in* nixpkgs.

Keeping the references behind aliases means packaging this project for nixpkgs
is a change to one `BUILD` file — repoint the aliases at `cc_library` targets
describing what Nix already placed in the sandbox via `buildInputs` — instead of
a rewrite. Nothing under `modules/` knows which provider it got.

Not implemented; there is one provider today. The point is that adding the
second one stays local, and stops being local the moment a `BUILD` file
elsewhere spells out `@nixpkgs_doctest`.

## Modules

`build/module.bzl` is the counterpart of `cmake/Module.cmake`. It keeps that
design's two load-bearing ideas:

- **A module publishes only `include/<module>/`.** So a declared dependency is
  what grants access to a header. Bazel enforces this harder than CMake: an
  undeclared include fails at the `#include`, naming the file. Verified in both
  directions.
- **Test sources live in the module library**, not the test binary, so a
  dependee's runner can run the whole transitive suite (`--all`). That needs
  `alwayslink = True` — Bazel's spelling of `--whole-archive` — for the same
  reason CMake needs it: test cases register through static initialisers that
  nothing references.

Each module gets a generated runner that filters to the cases defined in its own
directory, via doctest's `--source-file`. `MODULE_SOURCE_DIR` is the *package
path* here rather than CMake's absolute directory, because Bazel compiles with
execroot-relative paths and that is what `__FILE__` holds.

### The stamps are deliberately not ported

`cmake/Module.cmake` gives each module a stamp file that exists iff its tests
passed, and makes a module's test command take its dependencies' stamps as file
inputs. Two things come out of that: not re-running unchanged tests, and
ordering.

Bazel's action cache already provides the first — an unchanged `cc_test` reports
`(cached)` and does not run. Confirmed:

```
//modules/snapshot:snapshot_test               PASSED in 0.0s     # first run
//modules/snapshot:snapshot_test      (cached) PASSED in 0.0s     # unchanged
Executed 0 out of 1 test: 1 test passes.
```

The ordering is genuinely lost. Bazel runs tests as independent leaf actions by
design, so a broken low layer produces failures from every module above it
rather than stopping at the break. That is the accepted trade. It *is*
reconstructible out of ordinary build actions if it turns out to matter.

## Two things that needed working around

**rules_nixpkgs does not run on NixOS out of the box.** Its CC toolchain scripts
start with `#!/bin/bash`, which does not exist there. The error misdirects:
`execvp` reports ENOENT and Bazel prints "No such file or directory" naming the
*script*, which is present and executable — it is the interpreter that is
missing. Fixed by a `sed` in `patch_cmds`; worth upstreaming.

**`patch_cmds` is silently ignored on `single_version_override`.** On Bazel
7.6.0 the commands simply never run against a registry-sourced module — no
error, no warning, shebang unchanged. `archive_override` pointing at the same
tarball the registry itself references does work, so that is what `MODULE.bazel`
uses. Same bytes, same version, different fetch path.

## Snapshot tests under a sandbox

`SNAPSHOT_ROOT` is a deliberately *relative* path here, against CMake's absolute
`${PROJECT_SOURCE_DIR}/.snapshots`. Bazel has no compile-time equivalent of
`PROJECT_SOURCE_DIR`, by design — baking a checkout location into an object file
makes it non-relocatable and poisons a shared cache.

A runfiles-relative path splits the two uses exactly where they should split:

- **reading works** — paths resolve against the runfiles directory, so the
  tests assert against the recorded values normally;
- **writing refuses** — `flush_updates()` checks `is_absolute()` first and
  returns an error rather than writing.

Refusing to record under `bazel test` is correct, not a limitation: a sandboxed
action must not write into the source tree. Recording new values stays the CMake
build's job (`SNAPSHOT_UPDATE=1`) until there is a `bazel run` updater target,
which is where it belongs — `bazel run` has `BUILD_WORKSPACE_DIRECTORY` and can
honestly produce the source path.

Tests that read files at run time declare them in `test_data`, including the
tests that read *their own source*. The CMake build never had to: CTest runs in
the source tree, so everything is simply there.

## Not ported

`src/`, every module other than `snapshot`, and the tooling around the CMake
build: coverage (`tools/merge-coverage`), Intel PT tracing, AFL/LibAFL fuzzing,
benchmarks, and CTest-style per-case IDE discovery. The `test`/`bench`
subcommand dispatch in `cmake/module_run.cc` is not here either — nothing ported
has benchmarks yet, and when the first one arrives that dispatch should become a
shared library target rather than be duplicated.
