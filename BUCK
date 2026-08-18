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

flake.prebuilt_pkgconfig_library(
    name = "doctest",
    path = "root//:flake",
)

cxx_library(
  name = 'tests',
  srcs = [
    'src/b.cc',
  ],
  deps = [
    ':doctest',
  ],
  preferred_linkage = 'static',
  link_whole = True,
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
    ':tests',
  ],
)
