cmake_minimum_required(VERSION 3.29)
project(TrieIndex VERSION 1.0 LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 23)

add_subdirectory(src)
include(CTest)

option(PACKAGE_TESTS "Build the tests" ON)
if(PACKAGE_TESTS)
    enable_testing()
    include(GoogleTest)
    add_subdirectory(tests)
endif()

