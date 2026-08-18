filegroup(
    name = "flake",
    srcs = [
        "flake.lock",
        "flake.nix",
        "nix",
    ],
    visibility = ["PUBLIC"],
)

constraint_setting(
    name = "build_mode",
)

constraint_value(
    name = "libafl",
    constraint_setting = ":build_mode",
)

load("//buck:flake.bzl", "flake")

flake.prebuilt_pkgconfig_library(
    name = "doctest",
    path = "root//:flake",
)

flake.package(
    name = "boost_package",
    package = "boost",
    files = {
        "include": "include",
        "lib": "lib",
        "stacktrace": "lib/libboost_stacktrace_from_exception.so",
    },
    path = "root//:flake",
)

flake.package(
    name = "libbacktrace_package",
    package = "libbacktrace",
    files = {
        "include": "include",
        "lib": "lib",
        "backtrace": "lib/libbacktrace.so",
    },
    path = "root//:flake",
)

prebuilt_cxx_library(
    name = "boost_stacktrace",
    header_dirs = [":boost_package[include]"],
    shared_lib = ":boost_package[stacktrace]",
    exported_linker_flags = [
        "-Wl,-rpath,$(location :boost_package[lib])",
    ],
    preferred_linkage = "shared",
    visibility = ["PUBLIC"],
)

prebuilt_cxx_library(
    name = "backtrace",
    header_dirs = [":libbacktrace_package[include]"],
    shared_lib = ":libbacktrace_package[backtrace]",
    exported_linker_flags = [
        "-Wl,-rpath,$(location :libbacktrace_package[lib])",
    ],
    preferred_linkage = "shared",
    visibility = ["PUBLIC"],
)

flake.prebuilt_pkgconfig_library(
  name = 'hegel',
  package = 'hegel-cpp',
  path = 'root//:flake',
  static = True,
)

flake.package(
  name = "vk_bootstrap_package",
  package = "vk-bootstrap",
  files = {
    "include": "include",
    "vk_bootstrap_lib": "lib/libvk-bootstrap.a",
  },
  path = "root//:flake",
)

flake.package(
  name = "vma_package",
  package = "vulkan-memory-allocator",
  files = {
    "include": "include",
  },
  path = "root//:flake",
)

flake.package(
  name = "slangc",
  binary = "slangc",
  package = "shader-slang",
  path = "root//:flake",
  visibility = ["PUBLIC"],
)

flake.prebuilt_pkgconfig_library(
  name = "sdl3",
  package = "sdl3",
  module = "sdl3",
  path = "root//:flake",
)

flake.prebuilt_pkgconfig_library(
  name = "vulkan_loader",
  package = "vulkan-loader",
  module = "vulkan",
  path = "root//:flake",
)

prebuilt_cxx_library(
  name = "vk_bootstrap",
  header_dirs = [":vk_bootstrap_package[include]"],
  static_lib = ":vk_bootstrap_package[vk_bootstrap_lib]",
  exported_linker_flags = ["-ldl"],
  visibility = ["PUBLIC"],
)

prebuilt_cxx_library(
  name = "vma",
  header_dirs = [":vma_package[include]"],
  header_only = True,
  visibility = ["PUBLIC"],
)

cxx_library(
  name = 'tests',
  srcs = [
    'src/b.cc',
  ],
  deps = [
    ':doctest',
    ':hegel',
  ],
  preferred_linkage = 'static',
  link_whole = True,
)

cxx_test(
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
