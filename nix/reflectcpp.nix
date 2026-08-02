{ lib
, stdenv
, fetchFromGitHub
, cmake
, ninja
}:

# reflect-cpp — compile-time reflection for C++20, used by hegel-cpp for
# type-directed generation (`default_generator`) and struct printing. Not in
# nixpkgs, hence this wrapper.
#
# hegel-cpp pulls this in via FetchContent; we build it once here and let
# find_package(reflectcpp) pick it up instead, so nothing downloads at build
# time.
#
# Version is pinned to the tag hegel-cpp's FetchContent_Declare names; see
# nix/hegel-cpp.nix, which asserts the two agree.
stdenv.mkDerivation rec {
  pname = "reflectcpp";
  version = "0.22.0";

  src = fetchFromGitHub {
    owner = "getml";
    repo = "reflect-cpp";
    tag = "v${version}";
    hash = "sha256-5Og3+dM3QuCX6sT+6Rz8vwvyzQb+8qz10ROk9yOMPgE=";
  };

  nativeBuildInputs = [ cmake ninja ];

  cmakeFlags = [
    # ctre and yyjson ship vendored under include/rfl/thirdparty, so the
    # bundled path is the one that needs no downloads and no extra packages.
    # (The alternative, USE_BUNDLED_DEPENDENCIES=OFF, does find_package on
    # both, neither of which is in nixpkgs either.)
    (lib.cmakeBool "REFLECTCPP_USE_BUNDLED_DEPENDENCIES" true)
    (lib.cmakeBool "REFLECTCPP_INSTALL" true)
    # hegel only uses rfl::to_view (see include/hegel/repr.h and
    # generators/default.h) — none of the serialization formats. Dropping JSON
    # also drops the bundled yyjson translation unit from the build.
    (lib.cmakeBool "REFLECTCPP_JSON" false)
    (lib.cmakeBool "REFLECTCPP_BUILD_TESTS" false)
    (lib.cmakeBool "REFLECTCPP_BUILD_BENCHMARKS" false)
    # Never let it try to bootstrap vcpkg (which would fetch).
    (lib.cmakeBool "REFLECTCPP_USE_VCPKG" false)
  ];

  meta = {
    description = "Compile-time reflection and serialization library for C++20";
    homepage = "https://github.com/getml/reflect-cpp";
    license = lib.licenses.mit;
    platforms = lib.platforms.all;
  };
}
