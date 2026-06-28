cmake_minimum_required(VERSION 4.1)
project(cpp_template LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_EXTENSIONS ON)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_SCAN_FOR_MODULES OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_subdirectory(vendor/doctest)

find_package(CLI11 REQUIRED)

add_executable(cpp_template
    src/main.cc
)
target_link_libraries(cpp_template PRIVATE
    doctest::doctest
    CLI11::CLI11
)

target_compile_options(cpp_template PRIVATE
    -Wall
    -Wextra
    -Wno-gnu-statement-expression
)

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
