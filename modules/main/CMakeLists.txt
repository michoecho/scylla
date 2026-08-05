# The program itself, as a module: everything the executable is built from
# except main() -- which cannot live here, because add_module puts sources in a
# library and the runner TU that defines DOCTEST_CONFIG_IMPLEMENT has to be
# linked into an executable exactly once.
#
# Being a module rather than a pile of sources on the executable is what gives
# this code the same treatment as everything under modules/: its tests are
# discovered by the per-module facilities, run from main_test, and gated behind
# the main.passed stamp.
add_module(main
    SOURCES
        bench.cc
        # The AFL driver behind FUZZ_TARGET (persistent loop in a fuzz build,
        # stdin replay otherwise), plus the worked example target and the
        # self-test that runs AFL++ against it. Both of the latter are inert
        # outside the Fuzz preset: the target is compiled but not registered,
        # the test is skip()'d.
        fuzz.cc
        # The `test-locations` reporter that CMake test discovery lists source
        # locations with, so the IDE can jump to a test. Inert unless selected.
        test_locations_reporter.cc
        # Property-based tests: the worked example that proves Hegel finds a
        # planted bug in a toy function.
        hegel_test.cc
    # The AFL self-test's 10s cap, set by the top-level BUILD_FUZZERS block.
    # Empty in a non-fuzzing build, where nothing here needs a timeout.
    TEST_PROPERTIES ${FUZZ_TEST_PROPERTIES})

# None of these are modules, so they are linked with plain CMake.
target_link_libraries(main PUBLIC
    CLI11::CLI11
    nanobench_header
    hegel::hegel)

# Stacktraces off exceptions. Nothing in `main` calls it today; it is linked so
# the executable keeps carrying the from_exception library that captures traces
# at throw time, and so its tests are gated behind main's.
target_link_module(main PUBLIC exception_hacks)

target_compile_options(main PRIVATE
    -Wall
    -Wextra
    -Wno-gnu-statement-expression
)

# See the BUILD_FUZZERS block in the top-level CMakeLists for why this is a
# compile definition and not just a CMake variable. Applied here because the
# sources that key off it (include/main/fuzz.h, the AFL self-test in fuzz.cc)
# now live in this module.
if(BUILD_FUZZERS)
    target_compile_definitions(main PRIVATE BUILD_FUZZERS)
    target_compile_options(main PRIVATE -fsanitize=address)
    target_link_options(main PUBLIC -fsanitize=address)
endif()
