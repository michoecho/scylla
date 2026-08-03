{ lib
, stdenv
, fetchFromGitHub
, cmake
, ninja
, libhegel
, reflectcpp
}:

# hegel-cpp — the C++ binding for Hegel, a property-based testing library.
#
# Two things upstream's build does over the network, both replaced here:
#
#   * cmake/libhegel.cmake downloads a prebuilt engine .so from a GitHub
#     release. HEGEL_LIBHEGEL_LIBRARY short-circuits that with a local file, so
#     we point it at the engine we built from source (nix/libhegel.nix).
#   * FetchContent pulls reflect-cpp. FETCHCONTENT_SOURCE_DIR_REFLECTCPP would
#     redirect it to a source tree, but then it would be rebuilt (and
#     re-installed) as part of this derivation; instead we turn FetchContent
#     fully disconnected and hand it our already-built package, which
#     hegelConfig.cmake then find_dependency()s like any other system library.
#
# The result is a normal installed CMake package: consumers just do
# find_package(hegel) and link hegel::hegel.
let
  # Versions hegel-cpp v0.10.0 pins internally. If a bump changes either, the
  # asserts below fail rather than silently building against a mismatched ABI —
  # the engine's C header is generated from the Rust source, so a version skew
  # shows up as link or runtime breakage, not a compile error.
  expectedLibhegelVersion = "0.29.0";
  expectedReflectcppVersion = "0.22.0";
in
assert lib.assertMsg (libhegel.version == expectedLibhegelVersion)
  ("hegel-cpp expects libhegel ${expectedLibhegelVersion} "
    + "(HEGEL_LIBHEGEL_VERSION in cmake/libhegel.cmake), got ${libhegel.version}");
assert lib.assertMsg (reflectcpp.version == expectedReflectcppVersion)
  ("hegel-cpp expects reflect-cpp ${expectedReflectcppVersion} "
    + "(FetchContent_Declare in CMakeLists.txt), got ${reflectcpp.version}");

stdenv.mkDerivation rec {
  pname = "hegel-cpp";
  version = "0.10.0";

  src = fetchFromGitHub {
    owner = "hegeldev";
    repo = "hegel-cpp";
    tag = "v${version}";
    hash = "sha256-KazoL8A8eoF6jsI9Ag74LqQmtWAJVyNo5ZP4rDxGPPQ=";
  };

  nativeBuildInputs = [ cmake ninja ];

  # Debug info for the binding, matching the engine (nix/libhegel.nix), so
  # profiles name hegel's own frames rather than bare addresses.
  #
  # RelWithDebInfo rather than Debug: the optimisation level stays where a
  # release build has it, so timings remain representative and only DWARF is
  # added. nixpkgs' cmake hook defaults this to Release, hence setting it
  # explicitly here.
  cmakeBuildType = "RelWithDebInfo";

  # cmake emits the DWARF; without this, nixpkgs' fixup phase strips it again.
  dontStrip = true;

  # Record source paths that still exist after the build.
  #
  # DWARF stores file *paths*, never the source text, and the debugger reads
  # them off disk when you ask for a listing. Left alone, those paths point at
  # the build sandbox (/build/source/...), which is gone by then -- so gdb
  # knows the line number but cannot show the line.
  #
  # Rewriting the prefix at compile time points them at the fixed-output source
  # in the store, which does persist. This is preferred over fixing it up in
  # the debugger with `set substitute-path`, for two reasons:
  #
  #   * Every nixpkgs stdenv build uses /build/source, so a substitution rule
  #     for one package silently matches every other package's frames too --
  #     showing the wrong file rather than no file.
  #   * The mapping lives with the package, so it cannot go stale against a
  #     hand-maintained .gdbinit after a version bump.
  #
  # No debugger configuration is needed as a result.
  env.NIX_CFLAGS_COMPILE = "-fdebug-prefix-map=/build/source=${src}";

  # reflectcpp is propagated: hegel's public headers include <rfl.hpp>, and
  # hegelConfig.cmake does find_dependency(reflectcpp), so every consumer needs
  # both on the search path.
  propagatedBuildInputs = [ reflectcpp ];
  buildInputs = [ libhegel ];

  cmakeFlags = [
    # Use the engine built from source instead of downloading a release asset.
    # This also makes install() ship it beside the static archive, which is
    # what hegelConfig.cmake's imported target then points at.
    (lib.cmakeFeature "HEGEL_LIBHEGEL_LIBRARY" "${libhegel}/lib/libhegel_c.so")
    # Belt and braces: fail loudly if any FetchContent_Declare we haven't
    # accounted for tries to reach the network.
    (lib.cmakeBool "FETCHCONTENT_FULLY_DISCONNECTED" true)
    # Upstream's tests pull in googletest and ApprovalTests via FetchContent,
    # which the line above forbids. The example test in this repo's own
    # src/hegel_test.cc is what exercises the package.
    (lib.cmakeBool "HEGEL_BUILD_TESTS" false)
    (lib.cmakeBool "HEGEL_BUILD_DOCS" false)
  ];

  meta = {
    description = "Property-based testing for C++, based on Hypothesis";
    homepage = "https://hegel.dev/cpp";
    license = lib.licenses.mit;
    platforms = lib.platforms.linux;
  };
}
