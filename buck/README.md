# buck/

Buck2 rules for building nix packages and defining nix-backed toolchains.

Vendored from [tweag/buck2.nix](https://github.com/tweag/buck2.nix). These files
used to be consumed through a `nix` cell pointing at a checkout of that repo;
they now live here so this repository builds on its own.

| file         | upstream path           |
| ------------ | ----------------------- |
| `flake.bzl`  | `flake.bzl`             |
| `cxx.bzl`    | `buck/cxx.bzl`          |
| `python.bzl` | `buck/python.bzl`       |
| `rust.bzl`   | `buck/rust.bzl`         |

Vendored at upstream `038b031` ("Run check workflow weekly"), plus two local
changes that are not upstream:

* `flake.bzl`: a `files` attribute, exposing paths inside a nix package as
  sub-targets so that packages which are not just a bag of executables
  (headers, libraries, data) can be consumed by other rules.
* `flake.bzl`: `flake.prebuilt_pkgconfig_library()`, which discovers a
  package's include directories and libraries with pkg-config instead of having
  the BUCK file name them. This is what `//:doctest` is.

  The query runs inside `nix develop .#pkgconfig-<package>`, one of the shells
  flake.nix derives from the flake's package set. That is the whole trick: in
  nix every package is its own prefix, so `PKG_CONFIG_PATH` has to be assembled
  from the package and everything it propagates, and entering the shell has
  nixpkgs' own setup hooks do that -- rather than this file reimplementing
  stdenv's walk over `nix-support/propagated-build-inputs` and the env hooks
  packages are free to ship. A package that is already a flake output therefore
  needs nothing further to be consumable from buck2.

  The flags reach `prebuilt_cxx_library` as response files, the same shape as
  `@prelude//third-party:pkgconfig.bzl` -- which shells out to whatever
  `pkg-config` finds on the ambient `PKG_CONFIG_PATH`, where this builds the
  environment from the package itself.

* `cxx.bzl`: `shlib_interfaces` set to `"disabled"`. Upstream sets
  `"stub_from_library"` without defining a `shared_library_interface_producer`,
  which makes analysis fail for any target with a shared library dependency.

`rust.bzl` is vendored verbatim and is currently unused -- this project defines
no Rust toolchain target. Wire up `nix_rust_toolchain` in `buck/toolchains/BUCK` if
that changes.

## Usage

```starlark
load("//buck:flake.bzl", "flake")
```

From a non-root cell, spell it `@root//buck:flake.bzl` (see `buck/toolchains/BUCK`).
