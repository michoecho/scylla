{ lib
, rustPlatform
, fetchFromGitHub
}:

# libhegel — Hegel's native engine, exposed as a C ABI shared library.
#
# This is the `hegel-c` crate (package `hegeltest-c`, lib stem `hegel_c`) from
# the hegel-rust workspace. hegel-cpp is a thin C++ layer that drives this
# engine in-process through the `hegel_*` extern "C" functions declared in
# include/hegel.h.
#
# Upstream's own CMake and flake both *download* a prebuilt .so from a GitHub
# release and check it against a published SHA-256. We deliberately don't:
# everything here is built from source, so the engine is compiled by the same
# toolchain as the rest of the closure and the build needs no network beyond
# Nix's own fixed-output source fetches.
#
# The version is pinned to what hegel-cpp expects (HEGEL_LIBHEGEL_VERSION in
# its cmake/libhegel.cmake). See nix/hegel-cpp.nix, which asserts the two agree.
rustPlatform.buildRustPackage rec {
  pname = "libhegel";
  version = "0.29.0";

  src = fetchFromGitHub {
    owner = "hegeldev";
    repo = "hegel-rust";
    tag = "v${version}";
    hash = "sha256-Co0GWb1mytelf1edkLQxsKT4/LkIv751Tfch4zZcDJc=";
  };

  # Upstream commits a Cargo.lock and every dependency comes from crates.io, so
  # Nix vendors straight from the lockfile with no extra hash to maintain.
  cargoLock.lockFile = "${src}/Cargo.lock";

  # Debug info, so a profile of the engine names its Rust frames instead of
  # showing bare addresses (tools/pt-trace decodes traces through the symbol
  # table; see skills/pt-trace).
  #
  # This overrides the *release* profile rather than switching to the debug
  # one: cargoBuildType stays "release", so the engine keeps opt-level=3 and
  # the timings stay representative. Debug info only adds DWARF sections; it
  # does not change codegen.
  CARGO_PROFILE_RELEASE_DEBUG = "full";

  # Cargo emits the DWARF, but nixpkgs' fixup phase would strip it straight
  # back out of the .so. Both halves are needed to actually ship it.
  dontStrip = true;

  # Record source paths that outlive the build, so a debugger can list the
  # engine's Rust source. See the same treatment (and the reasoning for doing
  # it here rather than via the debugger's substitute-path) in nix/hegel-cpp.nix.
  #
  # Two prefixes, because a Rust build has two source roots: the crate itself,
  # unpacked at /build/source, and the dependencies Nix vendors from the
  # lockfile into /build/cargo-vendor-dir. Only the first is remapped to a path
  # that persists; the vendor dir is a build-time artifact with no store
  # equivalent, so its frames stay unlisted -- they are third-party crates, not
  # the engine, and mapping them would need every dependency's source in the
  # closure.
  RUSTFLAGS = "--remap-path-prefix=/build/source=${src}";

  # Only the C ABI crate is wanted; the workspace root (`hegeltest`, the Rust
  # binding) and the proc-macro crate are not part of this closure.
  buildAndTestSubdir = "hegel-c";

  # The crate is cdylib + staticlib + rlib. We consume the cdylib: hegel-cpp is
  # a static archive whose hegel_* symbols are resolved against this shared
  # library at the consumer's final link step.
  #
  # The workspace's test suite spawns `cargo` subprocesses to compile temporary
  # crates, which cannot work in the offline build sandbox.
  doCheck = false;

  postInstall = ''
    # buildRustPackage installs binaries but not cdylibs, so take the .so from
    # the build tree directly. The name matches the Rust output stem
    # (libhegel_c.so), which is also the SONAME, so consumers' RPATH lookups and
    # the recorded SONAME agree.
    install -Dm555 \
      "$(find target -name 'libhegel_c.so' -print -quit)" \
      "$out/lib/libhegel_c.so"

    # The C header describing the ABI above. hegel-cpp compiles against this
    # (its own bundled copy is byte-different only in cbindgen formatting).
    install -Dm444 hegel-c/include/hegel.h "$out/include/hegel.h"
  '';

  meta = {
    description = "Native engine for Hegel property-based testing, as a C ABI shared library";
    homepage = "https://hegel.dev";
    license = lib.licenses.mit;
    platforms = lib.platforms.linux;
  };
}
