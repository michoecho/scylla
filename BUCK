filegroup(
    name = "flake",
    srcs = [
        "flake.lock",
        "flake.nix",
        "nix",
    ],
    visibility = ["PUBLIC"],
)

load("@nix//flake.bzl", "flake")

flake.package(
    name = "doctest",
    path = "root//:flake",
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
