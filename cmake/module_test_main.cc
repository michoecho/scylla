// The main() linked into every module test executable.
//
// A module's library carries the tests of everything it links, because test
// sources are compiled into the module library and dependencies are linked in.
// That is deliberate: it lets `<module>_test --all` run the full transitive
// suite from one binary. But those dependency tests do not belong to this
// module, and re-running them here would defeat the point of the per-module
// stamps -- so by default the runner filters the registry down to the cases
// defined in this module's own directory.
//
// The filter is doctest's --source-file, matched against the __FILE__ of each
// registration. MODULE_SOURCE_DIR is the module's directory, baked in at
// compile time, so the binary filters correctly regardless of the working
// directory it is run from.

#include <cstring>
#include <string>
#include <vector>

// DOCTEST_CONFIG_IMPLEMENTATION_IN_DLL is supplied by the build, so that this
// TU and every module library agree on it (see cmake/Module.cmake).
#define DOCTEST_CONFIG_IMPLEMENT
#include <doctest/doctest.h>

#ifndef MODULE_NAME
#error "MODULE_NAME must be defined by the build (see cmake/Module.cmake)"
#endif
#ifndef MODULE_SOURCE_DIR
#error "MODULE_SOURCE_DIR must be defined by the build (see cmake/Module.cmake)"
#endif

int main(int argc, char** argv) {
    // --all opts out of the module filter and runs every linked test,
    // including those of dependencies. Consumed here rather than passed to
    // doctest, which would reject it as unknown.
    bool run_all = false;
    std::vector<char*> forwarded;
    forwarded.push_back(argv[0]);
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--all") == 0)
            run_all = true;
        else
            forwarded.push_back(argv[i]);
    }

    doctest::Context context;
    context.setAsDefaultForAssertsOutOfTestCases();

    // Applied before the command line, so an explicit --source-file from the
    // user replaces this default rather than being overridden by it.
    //
    // The pattern is a prefix match on the directory. doctest compares against
    // the path as the compiler saw it in __FILE__; the trailing wildcard covers
    // both that and the separator, and a leading wildcard tolerates a relative
    // __FILE__ from a build directory elsewhere in the tree.
    // doctest parses this as a single `--source-file=<value>` token; passing
    // the flag and its value as two argv entries is silently ignored.
    std::string module_filter;
    if (!run_all) {
        module_filter = std::string("--source-file=*") + MODULE_SOURCE_DIR + "/*";
        const char* preset[] = {argv[0], module_filter.c_str()};
        context.applyCommandLine(2, preset);
    }

    context.applyCommandLine(static_cast<int>(forwarded.size()), forwarded.data());

    int res = context.run();
    if (context.shouldExit()) // query flags (--list-test-cases, --exit) rely on this
        return res;
    return res;
}
