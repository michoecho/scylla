// The main() linked into every module test binary in the Bazel build.
//
// Counterpart of cmake/module_test_main.cc, and it exists separately for one
// reason: the module filter. A module's library carries the tests of
// everything it links, deliberately -- that is what lets a runner run the
// whole transitive suite on request -- so a runner has to filter the registry
// down to the cases defined in its own module, and the two builds spell the
// path in that filter differently.
//
// CMake bakes in an absolute source directory. Bazel compiles with paths
// relative to the execution root, so __FILE__ for a source in this module
// reads `modules/<name>/<file>.cc`; MODULE_SOURCE_DIR is correspondingly the
// package path. Sharing one file between the builds would mean one of them
// carrying the other's path convention.
//
// The `test`/`bench` subcommand dispatch that cmake/module_run.cc adds is not
// here yet: nothing in the ported subset has benchmarks. When the first one is
// ported, that dispatch should move into a shared library target rather than
// be duplicated here -- the CMake build learned that lesson already (see the
// header comment in cmake/module_run.h).

#include <cstring>
#include <string>
#include <vector>

// DOCTEST_CONFIG_IMPLEMENTATION_IN_DLL is supplied by the build, so this TU and
// every module library agree on it. See build/module.bzl.
#define DOCTEST_CONFIG_IMPLEMENT
#include <doctest/doctest.h>

#ifndef MODULE_NAME
#error "MODULE_NAME must be defined by the build (see build/module.bzl)"
#endif
#ifndef MODULE_SOURCE_DIR
#error "MODULE_SOURCE_DIR must be defined by the build (see build/module.bzl)"
#endif

int main(int argc, char** argv) {
    // --all opts out of the module filter and runs every linked case,
    // including dependencies'. Consumed here rather than forwarded, since
    // doctest would reject it as unknown.
    bool run_all = false;
    std::vector<const char*> forwarded;
    forwarded.push_back(argv[0]);
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--all") == 0)
            run_all = true;
        else
            forwarded.push_back(argv[i]);
    }

    doctest::Context context;
    context.setAsDefaultForAssertsOutOfTestCases();

    // Applied before the user's arguments so an explicit --source-file replaces
    // this default rather than being overridden by it.
    //
    // A prefix match on the directory: doctest compares against the path as the
    // compiler saw it in __FILE__. The leading wildcard tolerates a build that
    // produced an absolute path; the trailing one covers the separator and the
    // file name. doctest parses this as a single --source-file=<value> token --
    // passing flag and value as two argv entries is silently ignored.
    std::string filter = std::string("--source-file=*") + MODULE_SOURCE_DIR + "/*";

    std::vector<const char*> args;
    args.push_back(forwarded[0]);
    if (!run_all)
        args.push_back(filter.c_str());
    for (size_t i = 1; i < forwarded.size(); ++i)
        args.push_back(forwarded[i]);

    context.applyCommandLine(static_cast<int>(args.size()), args.data());

    return context.run();
}
