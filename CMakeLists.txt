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
    # The `fuzz <target>` subcommand: AFL's entry point, compiled into this one
    # binary alongside test/bench (see src/fuzz_driver.cc).
    src/fuzz_driver.cc
    # Worked example fuzz target. The varint decoder isn't used by the program;
    # it's here so the fuzz subcommand has something to drive. The bug it can
    # trip is behind DELIBERATE_BUGS_FOR_FUZZING (off unless BUILD_FUZZERS), so
    # the normal test suite stays green.
    src/fuzz_example/varint.cc
    src/fuzz_example/fuzz_varint.cc
    src/fuzz_example/varint_test.cc
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
)

target_include_directories(cpp_template SYSTEM PRIVATE ${BACKTRACE_INCLUDE_DIR})

target_compile_definitions(cpp_template PRIVATE BOOST_STACKTRACE_USE_BACKTRACE)

target_compile_options(cpp_template PRIVATE
    -Wall
    -Wextra
    -Wno-gnu-statement-expression
)

# So src/fuzz_example/*.cc can include "varint.h" by name. (main.cc and
# fuzz_driver.cc reach their headers via src/ being each file's own directory.)
target_include_directories(cpp_template PRIVATE src/fuzz_example)

# For stable stacktraces.
target_link_options(cpp_template PRIVATE -no-pie)

enable_testing()
# doctestConfig.cmake puts its lib/cmake/doctest on CMAKE_MODULE_PATH, so this
# picks up the doctest_discover_tests helper from the same (patched) package.
include(doctest)

# The executable parses args with CLI11 and runs the suite under a `test`
# subcommand; TEST_SUBCOMMAND (our doctest patch) prepends it to every
# discovery and per-test invocation.
doctest_discover_tests(cpp_template
    TEST_SUBCOMMAND test
    WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}"
    EXTRA_ARGS --exit
    PROPERTIES ENVIRONMENT_MODIFICATION LLVM_PROFILE_FILE=set:$<TARGET_FILE:cpp_template>.%p%m.profraw
)

# Fuzzing build. Off by default. The `Fuzz` preset turns this on AND sets the
# compiler to afl-clang-fast++, so the whole cpp_template binary gets AFL's
# coverage instrumentation. You then fuzz a target with:
#
#     afl-fuzz -i corpus -o out -- ./cpp_template fuzz varint
#
# Because the fuzz entry point lives in the one binary (the `fuzz` subcommand),
# there is no separate target — the cost is just that this preset rebuilds the
# whole program with instrumentation + ASAN. See skills/afl-fuzz.
option(BUILD_FUZZERS "Instrument cpp_template for AFL++ fuzzing (needs afl-clang-fast++)" OFF)
if(BUILD_FUZZERS)
    # DELIBERATE_BUGS_FOR_FUZZING compiles in the example decoder's intentional
    # out-of-bounds read for the fuzzer to find. ASAN turns that read into a
    # hard crash AFL can observe instead of a silent read of adjacent memory;
    # afl-clang-fast++ adds the coverage instrumentation on top.
    target_compile_definitions(cpp_template PRIVATE DELIBERATE_BUGS_FOR_FUZZING)
    target_compile_options(cpp_template PRIVATE -fsanitize=address)
    target_link_options(cpp_template PRIVATE -fsanitize=address)
endif()
