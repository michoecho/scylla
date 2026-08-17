filegroup(
    name = "flake",
    srcs = [
        "flake.lock",
        "flake.nix",
        "nix",
    ],
    visibility = ["PUBLIC"],
)

load("//buck:flake.bzl", "flake")

flake.package(
    name = "doctest_pkg",
    files = {"include": "include"},
    package = "doctest",
    path = "root//:flake",
)

# doctest is header-only, so it only needs to contribute its include directory
prebuilt_cxx_library(
    name = "doctest",
    header_dirs = [":doctest_pkg[include]"],
    header_only = True,
    visibility = ["PUBLIC"],
)

cxx_binary(
  name = 'hello',
  srcs = [
    'src/a.cc',
  ],
  headers = [
  ],
  deps = [
    ':doctest',
  ],
)
