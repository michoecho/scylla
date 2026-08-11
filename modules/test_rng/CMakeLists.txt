# One interface for randomized tests, with exhaustigen / Hegel / AFL++ behind
# it. See include/test_rng/test_rng.h for what the interface is and why it is
# shaped this way.
add_module(test_rng
    SOURCES
        test_rng.cc
        # The demonstration: three planted bugs, each in a shape that suits a
        # different backend, and the checks that the matching backend finds its
        # bug where the others do not.
        backend_demo_test.cc
    # The demonstration tests run real searches. Every one of them is capped
    # from inside (max_invocations), but a backend that regressed into an
    # unbounded loop should fail the suite rather than hang it.
    #
    # The AFL self-test deliberately has no internal cap -- if the backend is
    # broken it searches forever -- so in a fuzzing build this timeout is what
    # turns that into a failure. 60s is ample: the search is measured at well
    # under a second.
    TEST_PROPERTIES TIMEOUT 60)

# exhaustigen backs the exhaustive walk. Nothing else is needed: this module
# owns the AFL persistent loop itself, so it no longer depends on `main` for a
# fuzzing entry point.
target_link_module(test_rng PRIVATE exhaustigen)

# Not a module, so plain CMake.
target_link_libraries(test_rng PRIVATE hegel::hegel)

target_compile_options(test_rng PRIVATE -Wall -Wextra)

# The AFL backend branches on both of these: BUILD_FUZZERS to know the binary is
# instrumented at all, and __AFL_HAVE_MANUAL_CONTROL (defined by
# afl-clang-fast++ itself) to know the persistent-mode macros are available.
# Without the definition the backend refuses to run rather than silently
# degrading to something that tests nothing.
#
# Only the definition belongs here. ASAN is a whole-binary property -- mixing
# sanitized and unsanitized objects does not link -- so it is set once, on the
# Fuzz preset, rather than per module.
if(BUILD_FUZZERS)
    target_compile_definitions(test_rng PRIVATE BUILD_FUZZERS)
endif()

# The libafl backend. The header comes from the modules/libafl imported target;
# the *library* is not linked here, because SanCov instruments every target in
# the build and its runtime therefore has to reach all of them -- the top-level
# CMakeLists does that with link_libraries(). See the block there.
#
# The dependency on libafl_rust is what orders cargo's build before this
# module's compile. Linking alone would not: an imported target carries no build
# rule, so without this Ninja is free to compile test_rng.cc (and link
# test_rng_test against a .so that does not exist yet) before cargo has run.
if(BUILD_LIBAFL)
    target_compile_definitions(test_rng PRIVATE BUILD_LIBAFL)
    target_link_libraries(test_rng PRIVATE libafl)
    add_dependencies(test_rng libafl_rust)
endif()
