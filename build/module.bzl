"""Modules: a library that owns its tests.

The Bazel counterpart of cmake/Module.cmake. It keeps that design's two
load-bearing ideas and drops the third:

  * a module publishes exactly the headers under its own include/<module>/, so
    a declared dependency is what grants access to them;
  * a module's tests live in the module's own library and are run by a
    generated runner that filters to the cases defined in that module;
  * the dependency-ordered test *stamps* are not ported. They exist in the
    CMake build to stop tests re-running when nothing changed, and Bazel's
    action cache already does that -- a cc_test whose inputs are unchanged
    reports (cached) and does not run. What the stamps additionally give is
    ordering (a broken low layer stops the run there instead of producing a
    wall of failures from everything above it), which Bazel deliberately does
    not do for tests; that is a trade this build accepts.

--- the include boundary ---------------------------------------------------

The CMake build gets this from per-module include directories: only
<module>/include is PUBLIC, so a dependee sees the module's headers under a
<module>/ prefix and nothing else. `includes = ["include"]` reproduces it
exactly, and the reason for it is the same -- an include path entry is
all-or-nothing, so one shared root for modules/ would let any module include
any other's headers and only the subset of violations that leave an undefined
symbol would ever be caught. Header-only use would compile, link and pass.

Bazel enforces more than CMake does here: strict deps means including a header
of a module you did not declare a dependency on is an error naming the file,
rather than something that happens to work if the include path allows it.

--- test sources are part of the library -----------------------------------

Test sources go into the module library, not into the test binary, exactly as
in the CMake build. That is what lets a dependee's runner see its dependencies'
cases and run the whole transitive suite on request. Whether they run by
*default* is the runner's source-file filter, not what is linked.

The consequence, which is the same one CMake has: a static library contributes
its test registrations only if the linker keeps object files nothing
references. alwayslink = True is Bazel's spelling of --whole-archive, and it is
set on the module library for that reason.
"""

load("@rules_cc//cc:defs.bzl", "cc_library", "cc_test")

def cc_module(
        name,
        srcs = [],
        hdrs = None,
        test_srcs = [],
        deps = [],
        test_deps = [],
        test_data = [],
        test_size = "small",
        defines = [],
        local_defines = [],
        copts = [],
        visibility = ["//visibility:public"]):
    """Declare a module: a library, its tests, and a runner for them.

    Args:
      name: module name. Must match the directory it is declared in, because
        the published header prefix and the runner's filter are both derived
        from the package path.
      srcs: production sources.
      hdrs: public headers. Defaults to everything under include/<name>/, which
        is the layout every module follows; pass explicitly only to deviate.
      test_srcs: test sources. Compiled into the library rather than into the
        test binary -- see the module docstring.
      deps: dependencies of the library, and so of its tests.
      test_deps: dependencies needed only by the test sources. They still land
        on the library, since that is where the test sources are compiled; the
        separate argument is documentation of intent, and it keeps a dependee
        from being told it needs them.
      test_data: files the tests read at run time, placed in the runner's
        runfiles. A sandboxed test sees only what is declared here -- including,
        for a test that reads its own source, that source file. The CMake build
        needs no equivalent because CTest runs tests in the source tree.
      test_size: Bazel test size, which is a timeout and a resource estimate.
        "small" by default because a module suite that is not fast is a problem
        worth being told about; a module with a genuinely slow suite (fuzzing,
        property-based tests) should say so explicitly.
      defines: preprocessor definitions propagated to dependees.
      local_defines: preprocessor definitions for this module's own sources.
      copts: extra compiler options for this module's own sources.
      visibility: visibility of the library target.
    """
    if hdrs == None:
        hdrs = native.glob(["include/%s/**/*.h" % name], allow_empty = True)

    # Private headers sit directly in the module directory, public ones under
    # include/<module>/. Both are compiled against here; only the latter is on
    # a dependee's include path.
    private_hdrs = native.glob(
        ["*.h"],
        exclude = ["include/**"],
        allow_empty = True,
    )

    cc_library(
        name = name,
        srcs = srcs + test_srcs + private_hdrs,
        hdrs = hdrs,
        copts = copts,
        defines = defines + [
            # Much of doctest's API is declared in the header but defined only
            # in the TU that sets DOCTEST_CONFIG_IMPLEMENT -- the runner. This
            # makes those references resolvable across a library boundary by
            # giving them default visibility instead of nothing. The
            # export/import halves must agree, so it is propagated to dependees
            # rather than kept local.
            "DOCTEST_CONFIG_IMPLEMENTATION_IN_DLL",
        ],
        local_defines = local_defines,
        # The module's own directory, so its sources can include their private
        # headers unprefixed, the way the CMake build's PRIVATE include
        # directory allows.
        includes = ["include", "."],
        # Test cases register through static initialisers that nothing
        # references, so a linker that drops unreferenced objects drops the
        # tests with them. See the module docstring.
        alwayslink = True,
        deps = deps + test_deps + ["//third_party:doctest"],
        visibility = visibility,
    )

    if not test_srcs:
        return

    # The runner. Generated per module rather than shared, because the filter
    # that scopes a run to this module's own cases is baked in at compile time
    # -- so the binary behaves the same however and from wherever it is
    # invoked, which is what the CMake build uses MODULE_SOURCE_DIR for.
    cc_test(
        name = name + "_test",
        size = test_size,
        srcs = ["//build:module_test_main.cc"],
        local_defines = [
            "DOCTEST_CONFIG_IMPLEMENTATION_IN_DLL",
            'MODULE_NAME=\\"%s\\"' % name,
            # The package path, not an absolute directory: Bazel compiles with
            # paths relative to the execution root, so __FILE__ for a source in
            # this module reads modules/<name>/<file>.cc. That is what the
            # filter has to match.
            'MODULE_SOURCE_DIR=\\"%s\\"' % native.package_name(),
        ],
        data = test_data,
        deps = [name, "//third_party:doctest"],
    )
