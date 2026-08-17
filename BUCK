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

flake.prebuilt_cmake_library(
    name = "doctest",
    path = flake.store(
        package = "doctest",
        path = "root//:flake",
    ),
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
