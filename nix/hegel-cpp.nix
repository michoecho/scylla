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
