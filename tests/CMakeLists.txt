find_package(Fmt REQUIRED)
find_package(CapnProto REQUIRED)
add_subdirectory("${PROJECT_SOURCE_DIR}/extern/googletest" "extern/googletest")
add_library(TestUtils test_utils.cc)

mark_as_advanced(
    BUILD_GMOCK BUILD_GTEST BUILD_SHARED_LIBS
    gmock_build_tests gtest_build_samples gtest_build_tests
    gtest_disable_pthreads gtest_force_shared_crt gtest_hide_internal_symbols
)
set_target_properties(gtest PROPERTIES FOLDER extern)
set_target_properties(gtest_main PROPERTIES FOLDER extern)
set_target_properties(gmock PROPERTIES FOLDER extern)
set_target_properties(gmock_main PROPERTIES FOLDER extern)
macro(package_add_test TESTNAME)
    # create an executable in which the tests will be stored
    add_executable(${TESTNAME} ${ARGN})
    # link the Google test infrastructure, mocking library, and a default main function to
    # the test executable.  Remove g_test_main if writing your own main function.
    target_link_libraries(${TESTNAME} gtest gmock gtest_main Lib CapnProto::capnp CapnProto::kj fmt::fmt TestUtils)
    # gtest_discover_tests replaces gtest_add_tests,
    # see https://cmake.org/cmake/help/v3.10/module/GoogleTest.html for more options to pass to it
    gtest_discover_tests(${TESTNAME}
        # set a working directory so your project root so that you can find test data via paths relative to the project root
        WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
        PROPERTIES VS_DEBUGGER_WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}"
    )
    set_target_properties(${TESTNAME} PROPERTIES FOLDER tests)
    target_compile_options(${TESTNAME} PRIVATE -coverage)
    target_link_options(${TESTNAME} PRIVATE -coverage)
endmacro()

package_add_test(test_page_aware_writer test_page_aware_writer.cc)
package_add_test(test_trivial_reader_writer test_trivial_reader_writer.cc node.capnp.c++)
package_add_test(test_partition_index_writer test_partition_index_writer.cc node.capnp.c++)
