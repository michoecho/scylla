// The doctest runtime, for a binary that links a module but is not a test.
//
// A module's library carries its test translation units -- that is how
// add_module() works, and it is what makes `link_whole` register every case --
// and the project PCH puts doctest's headers in front of every TU besides. So
// any executable that links a module pulls in doctest's *declarations*, and
// exactly one TU in that executable has to define its implementation. For a
// module test that is buck/module_test_main.cc, which also owns main().
//
// This is the same TU for everything else: the implementation and nothing more,
// no main() and no runner. The cases registered by the modules it links are
// simply never run.
#define DOCTEST_CONFIG_IMPLEMENT
#include <doctest/doctest.h>
