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

    find_program(LLVM_COV llvm-cov REQUIRED)

    # VS Code's CMake Tools coverage profile builds these targets immediately
    # before and after running the selected CTest tests. Keep the orchestration
    # in a CMake script so cleanup is recursive and llvm-cov's stdout can be
    # written directly to an LCOV file without relying on shell redirection.
    add_custom_target(coverage-clean
        COMMAND ${CMAKE_COMMAND}
            -DACTION=CLEAN
            -DBINARY_DIR=${CMAKE_BINARY_DIR}
            -P ${PROJECT_SOURCE_DIR}/cmake/Coverage.cmake
        COMMENT "Removing old LLVM coverage profiles"
        VERBATIM
    )

    add_custom_target(coverage-export
        COMMAND ${CMAKE_COMMAND}
            -DACTION=EXPORT
            -DBINARY_DIR=${CMAKE_BINARY_DIR}
            -DMERGE_COVERAGE=${PROJECT_SOURCE_DIR}/tools/merge-coverage
            -DLLVM_COV=${LLVM_COV}
            -P ${PROJECT_SOURCE_DIR}/cmake/Coverage.cmake
        COMMENT "Merging LLVM profiles and exporting LCOV"
        VERBATIM
    )
endif()

find_package(doctest REQUIRED)

# nanobench is a single header, and modules/main/bench.cc compiles the implementation
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

# The Vulkan stack behind the `vulkan` module (modules/vulkan/): the loader and
# headers, SDL3 for windowing and surface creation, vk-bootstrap for
# instance/device selection and swapchain building, and VMA for allocation.
# All four come from the devShell; none is vendored or a submodule.
#
# Vulkan::Vulkan links the loader and pulls in the C headers. The module uses
# the C API only, so nothing here needs vulkan.hpp.
find_package(Vulkan REQUIRED)
find_package(SDL3 REQUIRED CONFIG)
find_package(vk-bootstrap REQUIRED CONFIG)
find_package(VulkanMemoryAllocator REQUIRED CONFIG)

# Shader compilation for the vulkan module. slangc emits SPIR-V straight from
# .slang, and the compiled shaders land next to the test binaries so a test can
# find them relative to its own executable path.
find_program(SLANGC slangc REQUIRED)

# Hegel, a property-based testing library (see modules/main/hegel_test.cc). The nix
# packages under nix/ build it and its Rust engine from source; hegelConfig
# re-creates the imported target for the engine shared library it ships beside
# its archive, so linking hegel::hegel is all a consumer needs.
find_package(hegel REQUIRED)

# Stacktraces from exceptions (modules/exception_hacks/). Boost splits this in
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

# Everything the program is made of lives in the `main` module
# (modules/main/), which is defined after this file includes Module.cmake --
# so the executable itself is created down there, below add_subdirectory.

# Fuzzing build. Off by default. The `Fuzz` preset turns this on AND sets the
# compiler to afl-clang-fast++, so the whole binary gets AFL's coverage
# instrumentation.
#
# There is no such thing as a "fuzz target" here. A fuzzed test is an ordinary
# randomized test -- a TEST_CASE driving a TestRngProvider -- and AFL is simply
# the backend TEST_RNG selects, with the persistent loop owned by the provider
# (see modules/test_rng). So you fuzz one by name:
#
#     TEST_RNG=afl afl-fuzz -i corpus -o out -- \
#         ./test_rng_test --test-case='<name>'
#
# There is no separate build target — the cost is just that this preset rebuilds
# the program with instrumentation + ASAN.
#
# The `FuzzCmplog` preset builds these same sources a second time as the CmpLog
# (redqueen) binary. It differs from `Fuzz` in two ways, both set in the preset
# itself: AFL_LLVM_CMPLOG=1 in the compiler's environment (an env-only knob, with
# no compile flag of its own) swaps in the CmpLog instrumentation, and
# -DCMPLOG_BUILD tells the source it is that binary, so the AFL self-test
# compiles out of it — see modules/test_rng/backend_demo_test.cc.
#
# afl-fuzz never fuzzes the CmpLog binary. It runs the normal Fuzz binary and
# consults this one to learn the operands of each comparison, then writes those
# values straight into the input at the right offset (input-to-state) rather than
# mutating its way to them:
#
#     cmake --preset Fuzz       && cmake --build --preset Fuzz
#     cmake --preset FuzzCmplog && cmake --build --preset FuzzCmplog
#     TEST_RNG=afl afl-fuzz -i corpus -o out \
#         -c out/build/FuzzCmplog/<binary> \
#         -- out/build/Fuzz/<binary> --test-case='<name>'
#
# TEST_RNG is set on afl-fuzz rather than as an `env TEST_RNG=afl ...` prefix on
# the target: AFL inspects argv[0] for instrumentation, so a wrapper makes it
# examine `env` and abort with "No instrumentation detected".
#
# tools/fuzz automates exactly the above, taking a ctest filter instead of a
# binary path and a test name. tools/tests/test_fuzz.py drives that tool against
# the planted magic-bytes case, and is what proves the machinery works; it skips
# itself unless both presets are configured, so the two build directories only
# have to be in sync for the fuzzing presets rather than for every ctest run.
#
# Both halves of the instrumentation are set by the preset, not here and not per
# module: afl-clang-fast++ supplies the coverage instrumentation, and the
# preset's build-type flags carry ASAN. ASAN has to be uniform across the whole
# binary -- sanitized and unsanitized objects do not link together -- so a
# per-module opt-in would be a link error waiting for the first module that
# forgot it. This option only tells the sources they are in a fuzzing build;
# test_rng is the one that reads it.
option(BUILD_FUZZERS "Instrument cpp_template for AFL++ fuzzing (needs afl-clang-fast++)" OFF)

enable_testing()
# doctestConfig.cmake puts its lib/cmake/doctest on CMAKE_MODULE_PATH, so this
# picks up the doctest_discover_tests helper from the same (patched) package.
include(doctest)

# Modules: libraries that own their tests, with per-module "tests passed" stamps
# ordering each module's tests after its dependencies'. See cmake/Module.cmake.
include(${PROJECT_SOURCE_DIR}/cmake/Module.cmake)
add_subdirectory(modules)

# Repository-wide integrity check for file-backed snapshots. This complements
# the per-process duplicate guard in the C++ runtime.
add_test(NAME snapshot_files.validate
    COMMAND python3 "${PROJECT_SOURCE_DIR}/tools/snapshot-files"
            --root "${PROJECT_SOURCE_DIR}")
set_tests_properties(snapshot_files.validate PROPERTIES LABELS "snapshot")

# The program: main() and nothing else. Every source it is built from lives in
# the `main` module, so this target exists only to turn that library into a
# runnable binary -- and to hold the one TU that may define
# DOCTEST_CONFIG_IMPLEMENT, which a library cannot.
#
# No doctest_discover_tests here. The tests inside the main module are
# discovered from main_test like every other module's, so registering them a
# second time through this binary would run each one twice under a second name.
add_executable(cpp_template src/main.cc ${MODULE_RUN})
target_include_directories(cpp_template PRIVATE ${MODULE_RUN_INCLUDE_DIR})

# Whole-archived for the same reason a module test executable is: a doctest
# case (and a bench) is registered by a static initializer that nothing in the
# program references, so a plain link drops the archive members holding them and
# the `test` and `bench` subcommands both come up empty. MODULE_WHOLE_ARCHIVE_LIBS is the closure add_module computed for
# exactly this; it is empty for a SHARED module, whose initializers run at load
# time, so the ordinary link below covers that case.
get_target_property(main_whole_archive main MODULE_WHOLE_ARCHIVE_LIBS)
if(main_whole_archive)
    target_link_libraries(cpp_template PRIVATE
        "$<LINK_LIBRARY:WHOLE_ARCHIVE,${main_whole_archive}>")
else()
    target_link_libraries(cpp_template PRIVATE main)
endif()

# For stable stacktraces.
target_link_options(cpp_template PRIVATE -no-pie)

# Tests for the Python tools under tools/, which drive built binaries rather
# than link against them (tools/pt-trace orchestrates perf around pt_test).
# They run as pytest, registered one CTest test per file: coarse enough to stay
# cheap, fine enough that `ctest -j` parallelizes them and a failure names a
# file.
#
# Listed explicitly rather than globbed, so that adding a test file is a
# deliberate edit here and CMake never has to re-glob to stay correct.
#
# The binaries under test are passed in as CPP_TEMPLATE_BIN and PT_TEST_BIN via
# $<TARGET_FILE:...>, which resolves per-preset -- so `ctest --preset
# SanitizeTest` tests the sanitized binaries with no path duplicated in Python.
# The PT tracing test uses pt_test, the `pt` module's own runner, so that it
# tests the module's control protocol without also depending on the program
# linking that module.
#
# Those generator expressions resolve paths; they do not build anything. ctest
# is not the build system and will happily run a test whose binary was never
# compiled, which fails in the fixture that checks the path exists -- an
# unhelpful way to learn you only built some other target. The
# `build_python_test_bins` fixture below closes that: a setup test that invokes
# the build, which every python test REQUIRES, so `ctest` after a bare configure
# builds the binaries rather than failing on their absence.
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
# test_fuzz.py is the exception to the "drive built binaries" description above:
# it drives tools/fuzz, which does its own building via the Fuzz and FuzzCmplog
# presets rather than through this build directory. So it needs neither the
# fixture below nor a $<TARGET_FILE:...>, and it skips itself wholesale in a
# tree where those presets are not configured -- which is every non-fuzzing
# preset, including the Debug one most runs use.
set(PYTHON_TOOL_TESTS
    tools/tests/test_fuzz.py
    tools/tests/test_pt_trace.py
    tools/tests/test_snapshot_files.py
)

# The setup half of the fixture: build every binary the python tests drive, in
# this build directory, with whatever generator and configuration configured it.
# Spelled as a cmake --build invocation rather than a bare ninja/make call so it
# stays generator-agnostic; --config matters only for multi-config generators and
# is ignored elsewhere.
#
# Note that building pt_test also *runs* nothing -- the module stamp targets are
# what run a suite, and this names the executable itself.
add_test(
    NAME "python.build_test_binaries"
    COMMAND ${CMAKE_COMMAND} --build "${CMAKE_BINARY_DIR}"
            --target cpp_template pt_test --config $<CONFIG>
)
set_tests_properties("python.build_test_binaries" PROPERTIES
    LABELS "python"
    FIXTURES_SETUP build_python_test_bins
    # A build is not parallel-safe against itself: two ctest workers running it
    # at once would race in the same build directory.
    RUN_SERIAL TRUE
    TIMEOUT 600
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
        # Pulls in the setup test above, so selecting a python test by name or
        # label builds the binaries first even from a freshly configured tree.
        FIXTURES_REQUIRED build_python_test_bins
        # Mirror the coverage variable used for the doctest tests, so binaries
        # these spawn contribute their .profraw to the report. A python test may
        # spawn more than one of them (cpp_template, pt_test), so this is
        # anchored to the build directory rather than to one target's path --
        # naming it after cpp_template would file pt_test's profile under the
        # wrong binary. %p%m still keeps concurrent writers apart, and
        # tools/merge-coverage maps each .profraw back by build-id regardless of
        # where it sits.
        ENVIRONMENT_MODIFICATION
            "CPP_TEMPLATE_BIN=set:$<TARGET_FILE:cpp_template>;PT_TEST_BIN=set:$<TARGET_FILE:pt_test>;LLVM_PROFILE_FILE=set:${CMAKE_BINARY_DIR}/python-${test_name}.%p%m.profraw"
    )
endforeach()
