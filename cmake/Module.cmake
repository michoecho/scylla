# Modules: a unit of code that owns its tests, and whose tests are gated on its
# dependencies' tests having passed.
#
# add_module(<name> ...) creates, for a module living in its own directory:
#
#   <name>              the library, holding both production and test code
#   <name>_test         an executable linking that library plus a doctest main()
#   <name>_tested       a marker target: <name>.passed exists iff the module's
#                       own tests passed
#   <name>_tested_deep  a marker target: <name>.deep.passed, produced by the
#                       same test invocation, but ordered after every
#                       dependency's *_tested_deep marker
#
# The markers are stamp files produced by add_custom_command, so Ninja's
# incrementality decides when a test re-runs: a module whose sources and
# dependency stamps are all older than its stamp is not re-tested. That is what
# lets expensive suites (fuzzing, property-based tests) live on a leaf module
# without being paid for on every build of something above it.
#
# The deep stamp is what enforces topological order. It DEPENDS on the deep
# stamps of the module's direct dependencies, and dependency stamps are
# themselves deep, so the ordering is transitive without expanding the closure
# here. A failing test leaves no stamp, so nothing above it can be built --
# a broken low layer stops the run at that layer instead of producing a wall of
# failures from every module that transitively uses it.
#
# Both stamps come from one test invocation rather than two: running the suite
# twice per build would double the cost of exactly the expensive tests this
# design exists to avoid re-running.

include_guard(GLOBAL)

# Where a module's stamps live. Kept in one directory rather than beside each
# module so that the aggregate targets below can find them without walking the
# source tree.
set(MODULE_STAMP_DIR "${CMAKE_BINARY_DIR}/module-stamps")
file(MAKE_DIRECTORY "${MODULE_STAMP_DIR}")

# Whether module libraries are STATIC or SHARED. This is per-module (the TYPE
# argument), defaulting to CMake's own BUILD_SHARED_LIBS convention, so a build
# can flip wholesale without editing module definitions.
option(BUILD_SHARED_LIBS "Build module libraries as shared libraries by default" OFF)

# The doctest main() every module test executable links. One shared source, so
# the "only my own tests by default" policy is defined in exactly one place.
set(MODULE_TEST_MAIN "${CMAKE_CURRENT_LIST_DIR}/module_test_main.cc"
    CACHE INTERNAL "Test runner main() shared by all module test executables")

# The test/bench/fuzz subcommand dispatch, shared by the module test runners
# above and by the shipping executable (src/main.cc). One implementation, so a
# fuzz target behaves the same whichever binary afl-fuzz is pointed at -- see
# the AFL self-test, which fuzzes its own executable.
set(MODULE_RUN "${CMAKE_CURRENT_LIST_DIR}/module_run.cc"
    CACHE INTERNAL "Subcommand dispatch shared by every runner")

# Where module_run.h lives, for the runners and for any module whose headers
# use the BENCH_SUITE / FUZZ_SUITE names it defines.
set(MODULE_RUN_INCLUDE_DIR "${CMAKE_CURRENT_LIST_DIR}"
    CACHE INTERNAL "Include directory holding module_run.h")

# The `test-locations` reporter that CMake test discovery lists source
# locations with, so the IDE can jump to a test. Linked into every module test
# executable, because discovery runs against each of them separately.
#
# It lives in the main module (which compiles it into its library as well) but
# is named by path rather than inherited through a link dependency: a module
# test executable must not have to depend on the main module to be discoverable.
set(MODULE_TEST_REPORTER "${PROJECT_SOURCE_DIR}/modules/main/test_locations_reporter.cc"
    CACHE INTERNAL "Discovery reporter shared by all module test executables")

function(add_module name)
    cmake_parse_arguments(M "" "TYPE" "SOURCES;TEST_SOURCES;DEPS;LINK_LIBRARIES;TEST_PROPERTIES" ${ARGN})

    if(NOT M_TYPE)
        if(BUILD_SHARED_LIBS)
            set(M_TYPE SHARED)
        else()
            set(M_TYPE STATIC)
        endif()
    endif()
    if(NOT M_TYPE STREQUAL "STATIC" AND NOT M_TYPE STREQUAL "SHARED")
        message(FATAL_ERROR "add_module(${name}): TYPE must be STATIC or SHARED, got '${M_TYPE}'")
    endif()

    # --- the library -----------------------------------------------------
    #
    # Test sources go into the library rather than into the executable. That is
    # what makes a dependency's tests available to a dependee's test binary:
    # linking the library brings its tests along, so `<dependee>_test` can run
    # the whole transitive suite on request. Whether they run *by default* is
    # decided by the runner's source-file filter, not by what is linked.
    add_library(${name} ${M_TYPE} ${M_SOURCES} ${M_TEST_SOURCES})

    # Public headers live in <module>/include/<module>/, private ones directly
    # in <module>/. Only the include/ directory is PUBLIC, so a dependee sees
    # exactly the headers this module chose to publish, spelled with the module
    # prefix: #include "module_b/b.h".
    #
    # Publishing that directory per-module, rather than putting the shared
    # modules/ root on everyone's include path, is what makes DEPS actually
    # enforce the module boundary. An include path entry is all-or-nothing: one
    # shared root would let any module include any other module's headers, and
    # nothing would object -- linking catches only the subset of violations that
    # leave an undefined symbol, so a header-only use (inline functions,
    # templates, constants) would compile, link, and test green. With per-module
    # include dirs the path is assembled from the link graph, so including a
    # non-dependency fails at the #include, naming the file.
    #
    # PRIVATE on the module's own directory keeps its internal headers off that
    # public path while its own sources reach them unprefixed.
    # MODULE_RUN_INCLUDE_DIR is PUBLIC because a module's own public headers may
    # use the suite names from module_run.h (bench.h and fuzz.h do), so anything
    # including them needs it on its path too.
    target_include_directories(${name}
        PUBLIC  ${CMAKE_CURRENT_SOURCE_DIR}/include
                ${MODULE_RUN_INCLUDE_DIR}
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

    target_link_libraries(${name} PUBLIC ${M_DEPS} ${M_LINK_LIBRARIES})

    # Much of doctest's API (String, Context, detail::regTest, ...) is declared
    # in the header but defined only in the TU that sets DOCTEST_CONFIG_IMPLEMENT
    # -- the runner. So the program holds exactly one definition of each, in the
    # executable, and every module library carries an undefined reference to it.
    #
    # DOCTEST_CONFIG_IMPLEMENTATION_IN_DLL is what makes that reference
    # resolvable across a shared library boundary: it expands DOCTEST_INTERFACE
    # to visibility("default") (export in the implementing TU, import elsewhere)
    # instead of to nothing. Without it, a build with -fvisibility=hidden hides
    # the executable's definitions, and linking a module .so against them fails
    # outright -- "hidden symbol ... is referenced by DSO".
    #
    # The export/import halves must agree, so this is PUBLIC and the runner gets
    # the matching definition below. No visibility properties are needed on top:
    # the attribute the macro applies already overrides -fvisibility=hidden.
    target_link_libraries(${name} PUBLIC doctest::doctest)
    target_compile_definitions(${name} PUBLIC DOCTEST_CONFIG_IMPLEMENTATION_IN_DLL)

    # Record the module's source directory. The runner needs it at runtime to
    # filter to this module's own tests, and it is read back from the target
    # below rather than passed around, so it cannot fall out of sync.
    set_target_properties(${name} PROPERTIES MODULE_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}")

    # --- the test executable ---------------------------------------------
    add_executable(${name}_test
        ${MODULE_TEST_MAIN} ${MODULE_RUN} ${MODULE_TEST_REPORTER})
    target_include_directories(${name}_test PRIVATE ${MODULE_RUN_INCLUDE_DIR})
    target_compile_definitions(${name}_test PRIVATE
        DOCTEST_CONFIG_IMPLEMENTATION_IN_DLL
        # Baked in at compile time so the binary is self-contained: running it
        # by hand from any directory still filters to its own module.
        MODULE_NAME="${name}"
        MODULE_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}")

    # A test case is registered by a static initializer that nothing in the
    # program references. From a static library the linker therefore has no
    # reason to pull in the object file holding it, and those tests silently
    # vanish. --whole-archive forces every member in.
    #
    # This has to cover the module's static *dependencies* too, not just its
    # own archive, or `<module>_test --all` would be missing their cases. A
    # shared dependency needs nothing: its initializers all run at load time.
    #
    # $<LINK_LIBRARY:WHOLE_ARCHIVE,...> rather than a raw
    # -Wl,--whole-archive <path> pair: naming an archive by file path takes it
    # out of CMake's link-order computation, so a whole-archived module could be
    # placed ahead of a dependency it needs and fail to resolve against it. The
    # generator expression keeps the targets in the graph -- correct ordering,
    # transitive usage requirements, and the right spelling per linker
    # (--whole-archive, -force_load, /WHOLEARCHIVE) -- while still scoping the
    # flag to the module archives, so doctest's own archive and any third-party
    # library are linked on demand as usual.
    set(whole_archive_libs "")
    if(M_TYPE STREQUAL "STATIC")
        list(APPEND whole_archive_libs ${name})
    endif()
    foreach(dep IN LISTS M_DEPS)
        get_target_property(dep_type ${dep} TYPE)
        if(dep_type STREQUAL "STATIC_LIBRARY")
            # Transitive: a static dep's own static deps are already in this
            # property, because add_module records it as the closure below.
            get_target_property(dep_whole ${dep} MODULE_WHOLE_ARCHIVE_LIBS)
            if(dep_whole)
                list(APPEND whole_archive_libs ${dep_whole})
            endif()
        endif()
    endforeach()
    list(REMOVE_DUPLICATES whole_archive_libs)

    if(whole_archive_libs)
        # The library list is a genex-list -- semicolon-separated, as a CMake
        # list already is. Joining it with commas instead would make the whole
        # thing one unrecognized library name, and the feature's closing
        # --no-whole-archive would never be emitted.
        target_link_libraries(${name}_test PRIVATE
            "$<LINK_LIBRARY:WHOLE_ARCHIVE,${whole_archive_libs}>")
    endif()
    # A shared module is not in the list above, so link it the ordinary way.
    if(NOT M_TYPE STREQUAL "STATIC")
        target_link_libraries(${name}_test PRIVATE ${name})
    endif()

    # The closure a dependee must whole-archive to inherit this module's tests:
    # this archive if it is one, plus whatever its own deps contributed.
    set_target_properties(${name} PROPERTIES
        MODULE_WHOLE_ARCHIVE_LIBS "${whole_archive_libs}")

    # --- stamps -----------------------------------------------------------
    set(shallow_stamp "${MODULE_STAMP_DIR}/${name}.passed")
    set(deep_stamp    "${MODULE_STAMP_DIR}/${name}.deep.passed")

    # Deep stamps of direct dependencies. Read back off the dependency targets,
    # so this graph is derived from DEPS and has no second copy to maintain.
    # Non-module link targets (doctest, third-party) simply carry no such
    # property and drop out.
    set(dep_deep_stamps "")
    foreach(dep IN LISTS M_DEPS)
        if(NOT TARGET ${dep})
            message(FATAL_ERROR
                "add_module(${name}): dependency '${dep}' is not a target. "
                "Modules must be added in dependency order.")
        endif()
        get_target_property(dep_stamp ${dep} MODULE_DEEP_STAMP)
        if(dep_stamp)
            list(APPEND dep_deep_stamps "${dep_stamp}")
        endif()
    endforeach()

    # One invocation, two stamps. The commands of a custom command run in
    # sequence and stop at the first failure, so a failing test run touches
    # neither file and the build fails; the next build re-runs it.
    #
    # DEPENDS lists the dependencies' *deep* stamps, which is what orders this
    # module's tests after theirs. Ninja will not start this command until each
    # of those files exists and is up to date.
    add_custom_command(
        OUTPUT "${shallow_stamp}" "${deep_stamp}"
        COMMAND "$<TARGET_FILE:${name}_test>"
        COMMAND ${CMAKE_COMMAND} -E touch "${shallow_stamp}"
        COMMAND ${CMAKE_COMMAND} -E touch "${deep_stamp}"
        DEPENDS ${name}_test ${dep_deep_stamps}
        COMMENT "Testing module ${name}"
        VERBATIM)

    # <name>_tested builds only the shallow stamp, but the shallow stamp is an
    # output of a command whose inputs include the dependency deep stamps -- so
    # asking for it still tests dependencies first. The distinction between the
    # two targets is which file downstream consumers key on, not whether
    # ordering applies.
    add_custom_target(${name}_tested      DEPENDS "${shallow_stamp}")
    add_custom_target(${name}_tested_deep DEPENDS "${deep_stamp}")

    set_target_properties(${name} PROPERTIES
        MODULE_SHALLOW_STAMP "${shallow_stamp}"
        MODULE_DEEP_STAMP    "${deep_stamp}")

    # --- CTest ------------------------------------------------------------
    #
    # Registered in addition to the stamps, not instead of them: discovery gives
    # the IDE's test panel one entry per case, while the stamps are what the
    # build depends on. Only the module's own cases are discovered, because the
    # runner applies its source-file filter to --list-test-cases too.
    #
    # TEST_PROPERTIES is appended after the label, so a module can set CTest
    # properties on its own discovered tests (a timeout, an environment) without
    # this function having to know what they are.
    #
    # LLVM_PROFILE_FILE names a per-process .profraw next to the test binary, so
    # a coverage build (ENABLE_TEST_COVERAGE) collects the module suites too --
    # %p%m keeps concurrent `ctest -j` runs from overwriting each other's file.
    # Harmless without coverage instrumentation: nothing writes the file.
    doctest_discover_tests(${name}_test
        TEST_PREFIX "${name}:::"
        WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
        ADD_LABELS ON
        PROPERTIES
            LABELS "module.${name}"
            ENVIRONMENT_MODIFICATION
                LLVM_PROFILE_FILE=set:$<TARGET_FILE:${name}_test>.%p%m.profraw
            ${M_TEST_PROPERTIES})
endfunction()
