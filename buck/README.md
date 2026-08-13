# buck2 + nativelink + Nix

A minimal C++ example, built by [buck2](https://buck2.build/), with the
compiler and its one third-party dependency (zstd) coming from the same
nixpkgs the rest of this repository is pinned to, and
[nativelink](https://github.com/TraceMachina/nativelink) acting as the remote
cache.

This directory is self-contained and unrelated to the CMake build in the
repository root.

## Layout

| Path | What it is |
| --- | --- |
| `src/zstd_hello.cc` | The example: a zstd compress/decompress round-trip. |
| `src/BUCK` | `cxx_binary` for the above. |
| `BUCK` | zstd, taken from Nix and wrapped as a `prebuilt_cxx_library`. |
| `toolchains/BUCK` | The C++ and python-bootstrap toolchain definitions. |
| `toolchains/cxx.bzl` | Vendored from buck2.nix; see "Deviations" below. |
| `toolchains/nix/flake.nix` | The flake that provides the compiler and zstd. |
| `platforms/` | Execution platform with remote caching enabled. |
| `basic_cas.json5` | nativelink's configuration. |

## Running it

Two terminals; nativelink runs in the foreground.

**1. Start nativelink** — from the *repository root*, so that `--inputs-from .`
finds the flake that pins it:

```console
$ nix run nativelink ./buck/basic_cas.json5 --inputs-from .
```

It is ready once it logs `Ready, listening on 0.0.0.0:50051`.

**2. Build and run**, from this directory:

```console
$ buck2 run root//src:zstd_hello
```

Expected output:

```
zstd version:  1.5.7
original:      181 bytes
compressed:    91 bytes
round-tripped: ok
```

`buck2` itself is not in the devShell; it comes from the same pinned nixpkgs:

```console
$ nix shell "github:NixOS/nixpkgs/6b316287bae2ee04c9b93c8c858d930fd07d7338#buck2"
```

## Checking that the cache is actually used

`buck2 clean` throws away all local state, so a build after it can only be fast
if something remote served it:

```console
$ buck2 build root//src:zstd_hello    # cold: Cache hits: 0%
$ buck2 clean
$ buck2 build root//src:zstd_hello    # Cache hits: 14%, cached: 1
```

A line reading `RE Session: ...` and non-zero `Network: Up/Down` in the build
output mean buck2 is talking to nativelink. Server-side, the entries are
visible on disk:

```console
$ find /tmp/nativelink/data-worker-test/content_path-ac -type f | wc -l   # action results
$ find /tmp/nativelink/data-worker-test/content_path-cas -type f | wc -l  # blobs
```

Only some of the seven actions are cacheable, hence 14% rather than 100%: the
`nix_flake` actions are declared `local_only` by buck2.nix (they shell out to
`nix build`), and genrules are not cached by default. The C++ compile and link
steps are the ones that round-trip through the cache.

Note that this is remote *caching*, not remote *execution* — actions still run
locally. Turning on remote execution would mean uploading each action's full
input closure, and these actions reference absolute `/nix/store` paths that a
worker would need to already have; that happens to hold for this
single-machine setup but is not a property worth relying on.

## How the Nix parts fit together

`toolchains/nix/flake.nix` is a standalone flake, pinned by hand to the exact
nixpkgs revision that `nixpkgs-stable` resolves to in the repository root's
`flake.lock`. It cannot use `follows` to reach the root flake, because
buck2.nix's `flake.package()` rule shells out to `nix build path:...` from
inside a build action and so evaluates it on its own. Keeping the revision
identical is what makes the compiler here the same store path the outer
devShell uses; if the root flake's nixpkgs is bumped, update the `inputs.nixpkgs`
URL here and re-run `nix flake lock` in that directory to match.

It exposes two things: `zstd`, and a `cxx` package that gathers
`ar`/`cc`/`c++`/`nm`/`objcopy`/`ranlib`/`strip` into one `bin` directory.
`cc` and `c++` have to be *wrappers* rather than plain symlinks — nixpkgs'
compiler wrapper reads `NIX_CFLAGS_COMPILE`, `NIX_LDFLAGS` and friends from the
environment to locate libc, and buck2 scrubs the environment of its actions, so
those values are captured into the wrapper at build time instead.

zstd is linked **dynamically**, which takes two things beyond copying the
library out of the store. Getting either wrong produces a binary that compiles
and links cleanly but dies at startup with
`libzstd.so.1: cannot open shared object file`:

- **The name.** The ELF carries `SONAME=libzstd.so.1`, and that is what the
  loader looks for -- not whatever the artifact happens to be called. The
  `soname` attribute on the `prebuilt_cxx_library` in `BUCK` declares it, and
  the genrule copies the versioned `libzstd.so.1` rather than the `libzstd.so`
  symlink.
- **The search path.** `runtime_dependency_handling = "symlink"` in
  `toolchains/cxx.bzl` makes the prelude assemble a symlink tree of the
  executable's shared library dependencies and link with
  `-Wl,-rpath,$ORIGIN/<tree>`. The default (`"no_symlink"`) builds no tree and
  adds no RPATH, so nothing points at libzstd at runtime.

Because the RPATH is `$ORIGIN`-relative, the result stays relocatable -- no Nix
store path is baked into the binary for zstd. Copying `zstd_hello` together
with its `__zstd_hello__shared_libs_symlink_tree` to any other directory gives
a binary that still runs. `ldd` on the built binary shows `libzstd.so.1`
resolving through that tree.

## Deviations from upstream buck2.nix

buck2.nix is pinned in `.buckconfig` at commit `038b031b`. Two changes were
needed against the prelude bundled with this buck2
(`2026-04-14-7600cb80`), both confined to `toolchains/cxx.bzl`:

- `runtime_dependency_handling = "symlink"` is passed to `CxxToolchainInfo`.
  This prelude declares the field with no default, so omitting it fails
  analysis outright; `"symlink"` specifically is what makes the dynamically
  linked zstd runnable, as described above.
- `shlib_interfaces` is `"disabled"` instead of `"stub_from_library"`. This
  prelude demands a `shared_library_interface_producer` tool whenever shared
  library interfaces are enabled, and buck2.nix defines none.

That file is a full copy of upstream's rather than a wrapper around it because a
Starlark `rule` object exposes neither `impl` nor `attrs`, so the upstream rule
cannot be re-used and its provider patched from outside. Re-vendoring is a plain
copy plus those two edits.

Separately, `platforms/` exists because the prelude's own
`prelude//platforms:default` hardcodes `remote_enabled = False`, which silently
disables all cache traffic no matter what `[buck2_re_client]` says. The
platform here is that rule with the flag flipped.
