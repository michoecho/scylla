filegroup(
    name = "flake",
    srcs = [
        "flake.lock",
        "flake.nix",
        "nix",
    ],
    visibility = ["PUBLIC"],
)

constraint(
    name = "build_mode",
    default = "debug",
    values = [
        "debug",
        "release",
    ],
)

constraint(
    name = "instrumentation",
    default = "none",
    values = [
        "coverage",
        "none",
        "libafl",
        "fuzztest",
    ],
)

constraint(
    name = "precompiled_headers",
    default = "enabled",
    values = [
        "enabled",
        "disabled",
    ],
)

configuration_alias(
    name = "libafl",
    actual = ":instrumentation[libafl]",
)

configuration_alias(
    name = "fuzztest",
    actual = ":instrumentation[fuzztest]",
)

configuration_alias(
    name = "coverage",
    actual = ":instrumentation[coverage]",
)

configuration_alias(
    name = "pch",
    actual = ":precompiled_headers[enabled]",
)

configuration_alias(
    name = "no_pch",
    actual = ":precompiled_headers[disabled]",
)

load("//buck:flake.bzl", "flake")

export_file(
    name = "module_test_main",
    src = "buck/module_test_main.cc",
    out = "module_test_main.cc",
    visibility = ["PUBLIC"],
)

cxx_library(
    name = "module_runner",
    srcs = ["buck/module_run.cc"],
    exported_headers = {
        "module_run.h": "buck/module_run.h",
    },
    exported_deps = [":doctest"],
    visibility = ["PUBLIC"],
)

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
    exported_preprocessor_flags = ["-DBOOST_STACKTRACE_USE_BACKTRACE"],
    shared_lib = ":boost_package[stacktrace]",
    extract_soname = True,
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
    extract_soname = True,
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

cxx_precompiled_header(
    name = "project_pch",
    compile_pch_file = True,
    pch_clanguage = ".cc",
    preferred_linkage = "static",
    src = "buck/pch.h",
    deps = [
        ":backtrace",
        ":boost_stacktrace",
        ":doctest",
        ":hegel",
        ":sdl3",
        ":vk_bootstrap",
        ":vma",
        ":vulkan_loader",
    ],
    visibility = ["PUBLIC"],
)

cxx_library(
    name = "vscode_results_reporter",
    srcs = [
        "modules/main/test_locations_reporter.cc",
        "modules/main/vscode_results_reporter.cc",
    ],
    deps = [
        ":doctest",
    ],
    link_whole = True,
    preferred_linkage = "static",
    visibility = ["PUBLIC"],
)
