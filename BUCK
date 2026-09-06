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
        "centipede",
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
    name = "centipede",
    actual = ":instrumentation[centipede]",
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

# The doctest runtime for a non-test binary that links a module. See the source
# for why one is needed at all.
cxx_library(
    name = "doctest_impl",
    srcs = ["buck/doctest_impl.cc"],
    exported_deps = [":doctest"],
    preferred_linkage = "static",
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
    compiler_flags = ["-O2"],
)

flake.prebuilt_pkgconfig_library(
    name = "doctest",
    path = "root//:flake",
)

flake.prebuilt_pkgconfig_library(
    name = "fmt",
    path = "root//:flake",
)

flake.prebuilt_pkgconfig_library(
    name = "sqlite",
    package = "sqlite",
    module = "sqlite3",
    path = "root//:flake",
)

flake.package(
    name = "glaze_package",
    package = "glaze",
    files = {
        "include": "include",
    },
    path = "root//:flake",
)

prebuilt_cxx_library(
    name = "glaze",
    header_dirs = [":glaze_package[include]"],
    header_only = True,
    visibility = ["PUBLIC"],
)

# SDL3 creates the window/context, while Dear ImGui's OpenGL3 renderer calls
# the OpenGL entry points. Both are provided by the flake rather than the host.
flake.prebuilt_pkgconfig_library(
    name = "opengl",
    package = "libGL",
    module = "gl",
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

genrule(
    name = "boost_stacktrace_rpath",
    out = "boost_stacktrace_rpath.args",
    cmd = "store=`readlink -f $(location :boost_package[stacktrace])`; " +
          "printf -- '-Wl,-rpath,%s' \"`dirname \"$store\"`\" > $OUT",
)

prebuilt_cxx_library(
    name = "boost_stacktrace",
    header_dirs = [":boost_package[include]"],
    exported_preprocessor_flags = ["-DBOOST_STACKTRACE_USE_BACKTRACE"],
    shared_lib = ":boost_package[stacktrace]",
    extract_soname = True,
    exported_linker_flags = [
        "@$(location :boost_stacktrace_rpath)",
    ],
    preferred_linkage = "shared",
    visibility = ["PUBLIC"],
)

# The rpath these two need is the *store* path, not the buck-out tree that
# mirrors it.
#
# `$(location ...)` expands to a project-relative path, and a relative RUNPATH
# entry is resolved by the loader against the process's working directory. That
# is invisible in most configurations, because a shared-link-style binary also
# gets an `$ORIGIN/...shared_libs_symlink_tree` entry ahead of it, which is
# CWD-independent and always wins. Under `root//:fuzztest` the test binary links
# statically (see buck/module.bzl), no symlink tree is produced, and the relative
# entry is all that is left -- so the binary runs from the project root and
# nowhere else. `buck2 run` happens to chdir there; `buck2 test` does not, and
# every fuzztest-modifier test died with "libbacktrace.so.0: cannot open shared
# object file".
#
# These genrules resolve the symlink to the store path it points at and write an
# absolute `-Wl,-rpath` into a linker argsfile. Store paths are absolute and
# immutable, so a binary linked this way runs from any directory -- which is the
# same property flake.prebuilt_pkgconfig_library already gives every pkg-config
# dependency by rewriting `-L` into a matching rpath. This just extends it to the
# two libraries that are wired up by hand.
#
# Backticks rather than `$(...)` for the shell substitutions: buck2 parses
# `$(name ...)` as a macro and would fail on `$(readlink ...)`.
genrule(
    name = "backtrace_rpath",
    out = "backtrace_rpath.args",
    cmd = "store=`readlink -f $(location :libbacktrace_package[backtrace])`; " +
          "printf -- '-Wl,-rpath,%s' \"`dirname \"$store\"`\" > $OUT",
)

prebuilt_cxx_library(
    name = "backtrace",
    header_dirs = [":libbacktrace_package[include]"],
    shared_lib = ":libbacktrace_package[backtrace]",
    extract_soname = True,
    exported_linker_flags = [
        "@$(location :backtrace_rpath)",
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
        "modules/main/pt_vscode_results_reporter.cc",
        "modules/main/vscode_results_reporter.cc",
    ],
    headers = [
        "modules/main/vscode_results_reporter.h",
    ],
    deps = [
        ":doctest",
        "//modules/pt:pt",
    ],
    link_whole = True,
    preferred_linkage = "static",
    visibility = ["PUBLIC"],
    compiler_flags = ["-O2"],
)

# The original Rust `perf script` dlfilter that turns an Intel PT trace into a
# Fuchsia trace for Perfetto; see tools/pt-trace.
#
# It is no longer what pt-trace uses -- modules/perf2perfetto is a C++ port that
# writes byte-identical traces, and pt-trace builds that instead -- but it is
# kept buildable as the reference the port is checked against, and as the
# fallback for `--dlfilter`. The crate is vendored as a submodule under
# third-party/rust/perf2perfetto and built by the reindeer-generated rules, which
# name their output after the Buck target rather than the crate. perf loads the
# filter by the path it is given, so the name is cosmetic -- but everything that
# refers to this file, upstream's README included, calls it
# libperf2perfetto.so, so hand it out under that name.
genrule(
    name = "perf2perfetto",
    out = "libperf2perfetto.so",
    cmd = "cp -- $(location rust_third_party//:perf2perfetto[cdylib]) $OUT",
    visibility = ["PUBLIC"],
)
