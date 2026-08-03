cmake_minimum_required(VERSION 4.1)
project(cpp_template LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_EXTENSIONS ON)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_SCAN_FOR_MODULES OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(ENABLE_TEST_COVERAGE)
    # Directory-wide so every target below (including vendored ones) is
    # instrumented. Optimization/debug level is left to the build type, so
    # coverage layers additively on top of any preset. tools/merge-coverage
    # maps .profraw build-ids back to binaries, and -no-pie lets llvm-cov
    # resolve addresses without a load-bias guess.
    add_compile_options(-fprofile-instr-generate=/dev/null -fcoverage-mapping)
    add_link_options(-fprofile-instr-generate=/dev/null -fcoverage-mapping -Wl,--build-id -no-pie)
endif()

find_package(doctest REQUIRED)

# nanobench is a single header, and src/bench.cc compiles the implementation
# itself via ANKERL_NANOBENCH_IMPLEMENT. The nixpkgs package also ships a
# prebuilt libnanobench.a holding those same symbols, so link against the
# header alone (an INTERFACE target over the package's include dir) rather than
# nanobench::nanobench, which would drag in the archive and duplicate them.
find_path(NANOBENCH_INCLUDE_DIR nanobench.h REQUIRED)
add_library(nanobench_header INTERFACE)
target_include_directories(nanobench_header INTERFACE ${NANOBENCH_INCLUDE_DIR})

# Coverage trick: Clang's -fcoverage-mapping drops regions for headers reached
# via a system include path (-isystem), so SYSTEM-include deps (doctest, most
# vendored libs) never show up in coverage reports. Clearing the dep's
# INTERFACE_SYSTEM_INCLUDE_DIRECTORIES turns its -isystem into a plain -I, so
# its headers appear in the covmap. (Note: the SYSTEM target property and an
# extra -I do NOT override an explicit SYSTEM keyword; you must clear this
# property. NO_SYSTEM_FROM_IMPORTED only applies to IMPORTED targets.)
#
# if(ENABLE_TEST_COVERAGE)
#     set_target_properties(doctest::doctest PROPERTIES INTERFACE_SYSTEM_INCLUDE_DIRECTORIES "")
# endif()

find_package(CLI11 REQUIRED)

# Hegel, a property-based testing library (see src/hegel_test.cc). The nix
# packages under nix/ build it and its Rust engine from source; hegelConfig
# re-creates the imported target for the engine shared library it ships beside
# its archive, so linking hegel::hegel is all a consumer needs.
find_package(hegel REQUIRED)

# Stacktraces from exceptions (src/exception_hacks.cc). Boost splits this in
# two: the from_exception library does the capture at throw time, and it only
# records instruction addresses. Turning those into names is a separate
# backend, chosen by a BOOST_STACKTRACE_USE_* macro at compile time. The
# default backend resolves names with dladdr, which sees only dynamic symbols
# and so leaves ordinary function names blank in this executable; libbacktrace
# reads DWARF instead and names every frame (it is also what std::stacktrace
# uses under the hood). The macro has to match in every TU that includes
# boost/stacktrace, hence a target-wide definition below.
find_package(Boost REQUIRED CONFIG COMPONENTS stacktrace_from_exception)
find_path(BACKTRACE_INCLUDE_DIR backtrace.h REQUIRED)
find_library(BACKTRACE_LIBRARY backtrace REQUIRED)

add_executable(cpp_template
    src/main.cc
    src/exception_hacks.cc
    src/bench.cc
    src/pt_control.cc
    src/pt_control_test.cc
    # The AFL driver behind FUZZ_TARGET (persistent loop in a fuzz build, stdin
    # replay otherwise), plus the worked example target and the self-test that
    # runs AFL++ against it. Both of the latter are inert outside the Fuzz
    # preset: the target is compiled but not registered, the test is skip()'d.
    src/fuzz.cc
    # Property-based tests: the worked example that proves Hegel finds a
    # planted bug in a toy function.
    src/hegel_test.cc
    # The `test-locations` reporter that CMake test discovery lists source
    # locations with, so the IDE can jump to a test. Inert unless selected.
    src/test_locations_reporter.cc
)
target_link_libraries(cpp_template PRIVATE
    doctest::doctest
    CLI11::CLI11
    nanobench_header
    # Trace capture at throw time, plus the libbacktrace name resolution its
    # frames are printed with. ${CMAKE_DL_LIBS} is dladdr, which Boost uses to
    # attribute each frame to its shared object.
    Boost::stacktrace_from_exception
    ${BACKTRACE_LIBRARY}
    ${CMAKE_DL_LIBS}
    hegel::hegel
)

target_include_directories(cpp_template SYSTEM PRIVATE ${BACKTRACE_INCLUDE_DIR})

target_compile_definitions(cpp_template PRIVATE BOOST_STACKTRACE_USE_BACKTRACE)

target_compile_options(cpp_template PRIVATE
    -Wall
    -Wextra
    -Wno-gnu-statement-expression
)

# For stable stacktraces.
target_link_options(cpp_template PRIVATE -no-pie)

# Fuzzing build. Off by default. The `Fuzz` preset turns this on AND sets the
# compiler to afl-clang-fast++, so the whole cpp_template binary gets AFL's
# coverage instrumentation. Fuzz targets are doctest cases in the `fuzz` suite
# (see src/fuzz.h), so you fuzz one with:
#
#     afl-fuzz -i corpus -o out -- ./cpp_template fuzz --test-case=<name>
#
# Because the fuzz entry point lives in the one binary (the `fuzz` subcommand),
# there is no separate target — the cost is just that this preset rebuilds the
# whole program with instrumentation + ASAN. See skills/afl-fuzz.
#
# The `FuzzCmplog` preset builds these same sources a second time as the CmpLog
# (redqueen) binary. It differs from `Fuzz` in two ways, both set in the preset
# itself: AFL_LLVM_CMPLOG=1 in the compiler's environment (an env-only knob, with
# no compile flag of its own) swaps in the CmpLog instrumentation, and
# -DCMPLOG_BUILD tells the source it is that binary, so the AFL self-test
# compiles out of it — see src/fuzz.cc.
#
# afl-fuzz never fuzzes the CmpLog binary. It runs the normal Fuzz binary and
# consults this one to learn the operands of each comparison, then writes those
# values straight into the input at the right offset (input-to-state) rather than
# mutating its way to them:
#
#     cmake --preset Fuzz       && cmake --build --preset Fuzz
#     cmake --preset FuzzCmplog && cmake --build --preset FuzzCmplog
#     afl-fuzz -i corpus -o out \
#         -c out/build/FuzzCmplog/cpp_template \
#         -- out/build/Fuzz/cpp_template fuzz --test-case=<name>
#
# Deliberately not wired into the AFL self-test: that test proves the fuzzing
# machinery works, and it does so from the Fuzz preset alone. Making it depend on
# a second build directory would mean two build modes had to be in sync before a
# single test could run.
option(BUILD_FUZZERS "Instrument cpp_template for AFL++ fuzzing (needs afl-clang-fast++)" OFF)
if(BUILD_FUZZERS)
    # BUILD_FUZZERS is a compile definition, not just a CMake variable: FUZZ_TARGET
    # (src/fuzz.h) keys off it to decide whether to register a target as a doctest
    # case, and the AFL self-test keys off it to un-skip itself.
    #
    # ASAN turns a memory-safety bug into a hard crash AFL can observe, instead
    # of a silent read of adjacent memory; afl-clang-fast++ adds the coverage
    # instrumentation on top.
    target_compile_definitions(cpp_template PRIVATE BUILD_FUZZERS)
    target_compile_options(cpp_template PRIVATE -fsanitize=address)
    target_link_options(cpp_template PRIVATE -fsanitize=address)

    # The AFL self-test runs afl-fuzz until it finds the planted bug, with no
    # internal timeout by design: if the infrastructure is broken (no
    # instrumentation, target never invoked) AFL searches forever, and this
    # external timeout is what turns that into a test failure. Applied to every
    # discovered test rather than by name, because doctest_discover_tests
    # creates them at build time — they don't exist yet for set_tests_properties.
    # The other tests here are microseconds long, so a shared 10s cap is free.
    set(FUZZ_TEST_PROPERTIES TIMEOUT 10)
endif()

enable_testing()
# doctestConfig.cmake puts its lib/cmake/doctest on CMAKE_MODULE_PATH, so this
# picks up the doctest_discover_tests helper from the same (patched) package.
include(doctest)

# Modules: libraries that own their tests, with per-module "tests passed" stamps
# ordering each module's tests after its dependencies'. See cmake/Module.cmake.
include(${PROJECT_SOURCE_DIR}/cmake/Module.cmake)
add_subdirectory(modules)

# The executable parses args with CLI11 and runs the suite under a `test`
# subcommand; TEST_SUBCOMMAND (our doctest patch) prepends it to every
# discovery and per-test invocation.
doctest_discover_tests(cpp_template
    TEST_SUBCOMMAND test
    WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}"
    EXTRA_ARGS --exit
    ADD_LABELS ON
    PROPERTIES
        ENVIRONMENT_MODIFICATION LLVM_PROFILE_FILE=set:$<TARGET_FILE:cpp_template>.%p%m.profraw
        ${FUZZ_TEST_PROPERTIES}
)

# Tests for the Python tools under tools/, which drive the built binary rather
# than link against it (tools/pt-trace orchestrates perf around cpp_template).
# They run as pytest, registered one CTest test per file: coarse enough to stay
# cheap, fine enough that `ctest -j` parallelizes them and a failure names a
# file.
#
# Listed explicitly rather than globbed, so that adding a test file is a
# deliberate edit here and CMake never has to re-glob to stay correct.
#
# The binary under test is passed in as CPP_TEMPLATE_BIN via
# $<TARGET_FILE:cpp_template>, which resolves per-preset -- so `ctest --preset
# SanitizeTest` tests the sanitized binary with no path duplicated in Python.
# The generator expression also makes each test depend on the built binary.
#
# pytest itself comes from the Nix devshell, not from CMake: having the build
# system create venvs or install packages is what puts network and mutable
# state into a dependency graph CMake cannot model.
#
# The interpreter is deliberately spelled `python3` (resolved from PATH at test
# time) rather than a configure-time absolute path from find_package(Python3).
# Those differ here: the devshell's python3 is the one carrying pytest, and a
# build directory may well have been configured outside it. Resolving late
# keeps the build dir independent of which shell ran cmake.
set(PYTHON_TOOL_TESTS
    tools/tests/test_pt_trace.py
)

foreach(test_file IN LISTS PYTHON_TOOL_TESTS)
    cmake_path(GET test_file STEM test_name)
    add_test(
        NAME "python.${test_name}"
        COMMAND python3 -m pytest
                "${PROJECT_SOURCE_DIR}/${test_file}"
                -q --no-header
        WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}"
    )
    # pytest reports "no tests ran" as exit code 5. That happens when every test
    # skipped for lack of hardware (no intel_pt PMU, say), which is not a
    # failure of the code under test.
    set_tests_properties("python.${test_name}" PROPERTIES
        LABELS "python"
        SKIP_RETURN_CODE 5
        TIMEOUT 300
        # Mirror the coverage variable used for the doctest tests, so binaries
        # these spawn contribute their .profraw to the report.
        ENVIRONMENT_MODIFICATION
            "CPP_TEMPLATE_BIN=set:$<TARGET_FILE:cpp_template>;LLVM_PROFILE_FILE=set:$<TARGET_FILE:cpp_template>.%p%m.profraw"
    )
endforeach()
