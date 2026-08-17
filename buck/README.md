# buck/

Buck2 rules for building nix packages and defining nix-backed toolchains.

Vendored from [tweag/buck2.nix](https://github.com/tweag/buck2.nix). These files
used to be consumed through a `nix` cell pointing at a checkout of that repo;
they now live here so this repository builds on its own.

| file         | upstream path           |
| ------------ | ----------------------- |
| `flake.bzl`  | `flake.bzl`             |
| `cxx.bzl`    | `toolchains/cxx.bzl`    |
| `python.bzl` | `toolchains/python.bzl` |
| `rust.bzl`   | `toolchains/rust.bzl`   |

Vendored at upstream `038b031` ("Run check workflow weekly"), plus two local
changes that are not upstream:

* `flake.bzl`: a `files` attribute, exposing paths inside a nix package as
  sub-targets so that packages which are not just a bag of executables
  (headers, libraries, data) can be consumed by other rules.
* `flake.bzl`: `flake.store()` and `flake.prebuilt_cmake_library()`, which
  discover a package's include directories and libraries with cmake instead of
  having the BUCK file name them. `flake.store()` names the store path of a nix
  package, and `prebuilt_cmake_library` configures a throwaway CMake project
  against it, walks the imported targets `find_package()` defines, and feeds the
  resulting flags to `prebuilt_cxx_library` as response files -- the same shape
  as `@prelude//third-party:pkgconfig.bzl`. This is what `//:doctest` is.
  It needs a `cmake` binary target, which `toolchains//:cmake` provides.
* `cxx.bzl`: `shlib_interfaces` set to `"disabled"`. Upstream sets
  `"stub_from_library"` without defining a `shared_library_interface_producer`,
  which makes analysis fail for any target with a shared library dependency.

`rust.bzl` is vendored verbatim and is currently unused -- this project defines
no Rust toolchain target. Wire up `nix_rust_toolchain` in `toolchains/BUCK` if
that changes.

## Usage

```starlark
load("//buck:flake.bzl", "flake")
```

From a non-root cell, spell it `@root//buck:flake.bzl` (see `toolchains/BUCK`).
