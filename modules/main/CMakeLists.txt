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
        # The `test-locations` reporter that CMake test discovery lists source
        # locations with, so the IDE can jump to a test. Inert unless selected.
        test_locations_reporter.cc
        # Property-based tests: the worked example that proves Hegel finds a
        # planted bug in a toy function.
        hegel_test.cc)

# None of these are modules, so they are linked with plain CMake.
target_link_libraries(main PUBLIC
    CLI11::CLI11
    nanobench_header
    hegel::hegel)

# Stacktraces off exceptions. Nothing in `main` calls it today; it is linked so
# the executable keeps carrying the from_exception library that captures traces
# at throw time, and so its tests are gated behind main's.
target_link_module(main PUBLIC exception_hacks)
target_link_module(main PUBLIC utils)

target_compile_options(main PRIVATE
    -Wall
    -Wextra
    -Wno-gnu-statement-expression
)
