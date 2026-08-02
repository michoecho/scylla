{ lib
, rustPlatform
, fetchFromGitHub
, rustfmt
, llvmPackages
, linuxHeaders
}:

# perf2perfetto is a `perf script` dlfilter: a dynamic library perf dlopen()s to
# convert an Intel PT trace into a Fuchsia trace (.ftf) for Perfetto. It is not
# a Rust dependency of anything here — the only artifact we want is the .so, so
# this package installs it into $out/lib and nothing else.
#
# Upstream has no Nix packaging and we deliberately don't add any; this wraps
# the plain cargo project from the outside.
rustPlatform.buildRustPackage rec {
  pname = "perf2perfetto";
  version = "0.1.0-unstable-2025-04-04";

  src = fetchFromGitHub {
    owner = "michoecho";
    repo = "perf2perfetto";
    rev = "b1e82573b1daa9e46e288334566459f42110e108";
    hash = "sha256-r1V+Ii3NbW5v90ss9HJt0RGZCMavUUJ5q5vepxtq9e8=";
  };

  # Upstream commits a Cargo.lock and every dependency comes from crates.io, so
  # Nix can vendor straight from the lockfile with no extra hash to maintain.
  cargoLock.lockFile = "${src}/Cargo.lock";

  # build.rs runs bindgen over include/wrapper.h to generate bindings for
  # perf's dlfilter ABI. bindgen needs libclang at build time, and rustfmt to
  # format what it emits.
  nativeBuildInputs = [ rustfmt ];
  LIBCLANG_PATH = "${llvmPackages.libclang.lib}/lib";

  # perf_dlfilter.h includes <linux/perf_event.h>. The stdenv gives clang libc
  # headers but not the kernel UAPI ones, and bindgen invokes clang directly
  # (so it doesn't pick these up from buildInputs) — point it at them.
  BINDGEN_EXTRA_CLANG_ARGS = "-isystem ${linuxHeaders}/include";

  # crate-type = ["dylib"], so cargo emits a .so rather than a binary. That is
  # a Rust-ABI dylib, not a cdylib, but perf only needs to dlopen it and call
  # the C-ABI entry points the crate exports, which works either way.
  #
  # There are no tests, and `cargo test` would rebuild for the test harness.
  doCheck = false;

  postInstall = ''
    mkdir -p "$out/lib"
    # buildRustPackage installs binaries but not dylibs, so take the .so from
    # the build tree directly.
    install -Dm444 \
      "$(find target -name 'libperf2perfetto.so' -print -quit)" \
      "$out/lib/libperf2perfetto.so"
  '';

  meta = {
    description = "perf script dlfilter converting Intel PT traces to Perfetto/Fuchsia traces";
    homepage = "https://github.com/michoecho/perf2perfetto";
    license = lib.licenses.gpl2Only;
    platforms = lib.platforms.linux;
  };
}
