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

add_subdirectory(vendor/doctest)

# nanobench is a single header. Its top-level CMake builds the standalone test
# binaries, so wire up just the header as an INTERFACE target instead.
add_library(nanobench INTERFACE)
target_include_directories(nanobench INTERFACE vendor/nanobench/src/include)

# Coverage trick: Clang's -fcoverage-mapping drops regions for headers reached
# via a system include path (-isystem), so SYSTEM-include deps (doctest, most
# vendored libs) never show up in coverage reports. Clearing the dep's
# INTERFACE_SYSTEM_INCLUDE_DIRECTORIES turns its -isystem into a plain -I, so
# its headers appear in the covmap. (Note: the SYSTEM target property and an
# extra -I do NOT override an explicit SYSTEM keyword; you must clear this
# property. NO_SYSTEM_FROM_IMPORTED only applies to IMPORTED targets.)
#
# if(ENABLE_TEST_COVERAGE)
#     set_target_properties(doctest PROPERTIES INTERFACE_SYSTEM_INCLUDE_DIRECTORIES "")
# endif()

find_package(CLI11 REQUIRED)

add_executable(cpp_template
    src/main.cc
    src/exception_hacks.cc
    src/bench.cc
)
target_link_libraries(cpp_template PRIVATE
    doctest::doctest
    CLI11::CLI11
    nanobench
    # stdc++exp: GNU libstdc++ backend for std::stacktrace, used by the
    # exception hack to record a trace at throw time. ${CMAKE_DL_LIBS}: dlsym,
    # used to chain to the real __cxa_allocate_exception/__cxa_free_exception.
    stdc++exp
    ${CMAKE_DL_LIBS}
)

target_compile_options(cpp_template PRIVATE
    -Wall
    -Wextra
    -Wno-gnu-statement-expression
)

# For stable stacktraces.
target_link_options(cpp_template PRIVATE -no-pie)

enable_testing()
if (DEFINED DOCTEST_CMAKE_HELPER)
    include(${DOCTEST_CMAKE_HELPER})
else()
    include(doctest)
endif()

# The executable parses args with CLI11 and runs the suite under a `test`
# subcommand; TEST_SUBCOMMAND (our doctest patch) prepends it to every
# discovery and per-test invocation.
doctest_discover_tests(cpp_template
    TEST_SUBCOMMAND test
    WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}"
    EXTRA_ARGS --exit
    PROPERTIES ENVIRONMENT_MODIFICATION LLVM_PROFILE_FILE=set:$<TARGET_FILE:cpp_template>.%p%m.profraw
)
