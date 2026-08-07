# Modules: a library with a standard set of extra targets -- it owns its tests,
# and its tests are gated on its dependencies' tests having passed.
#
# add_module(<name> ...) is add_library() plus those extras. It takes SOURCES,
# TYPE and TEST_PROPERTIES, and deliberately takes no dependencies: those are
# declared afterwards, with the ordinary target_* commands.
#
#   add_module(module_c SOURCES c.cc c_test.cc)
#   target_link_module(module_c PRIVATE module_a)
#   target_link_libraries(module_c PRIVATE CLI11::CLI11)
#
# It creates, for a module living in its own directory:
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
# dependency stamps are all older than its stamp is not re-tested. So a slow
# suite (fuzzing, property-based tests) is paid for when its module actually
# changes, not on every build of something above it.
#
# The deep stamp is what enforces topological order. The test command takes the
# dependencies' deep stamp *files* as inputs, and dependency stamps are
# themselves deep, so the ordering is transitive without expanding the closure
# here. A failing test leaves no stamp, so nothing above it can be built -- a
# broken low layer stops the run at that layer instead of producing a wall of
# failures from every module that transitively uses it.
#
# Both stamps come from one test invocation rather than two: running the suite
# twice per build would double the cost of exactly the expensive tests this
# design exists to avoid re-running.
#
# --- linking modules --------------------------------------------------------
#
# target_link_module() is target_link_libraries() plus the two module extras:
# the deep-stamp ordering edge, and the whole-archive closure that keeps a
# static dependency's test registrations alive under `<module>_test --all`.
#
# Linking a module with plain target_link_libraries() is allowed, and during
# development it is often what you want. It links correctly; it just does not
# add those two extras, so that dependency's tests are neither ordered before
# this module's nor visible to `--all`. Nothing detects or forbids it -- the
# rigidity of enforcing target_link_module() everywhere is not worth catching a
# mistake this unlikely.
#
# Because dependencies arrive after add_module() returns, nothing here may read
# them at declaration time. Both extras are therefore generator expressions over
# target properties that target_link_module() appends to -- read at generate
# time, once every call has contributed -- so modules can be declared in any
# order.

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

# The test/bench subcommand dispatch, shared by the module test runners above
# and by the shipping executable (src/main.cc). One implementation, so a test
# behaves the same whichever binary it is run from -- including under afl-fuzz,
# which the AFL self-test points at its own executable.
set(MODULE_RUN "${CMAKE_CURRENT_LIST_DIR}/module_run.cc"
    CACHE INTERNAL "Subcommand dispatch shared by every runner")

# Where module_run.h lives, for the runners and for any module whose headers
# use the BENCH_SUITE name it defines.
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
    cmake_parse_arguments(M "" "TYPE" "SOURCES;TEST_PROPERTIES" ${ARGN})

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
    # Test sources are ordinary SOURCES: they go into the library rather than
    # into the executable, which is what makes a dependency's tests available to
    # a dependee's test binary -- linking the library brings its tests along, so
    # `<dependee>_test` can run the whole transitive suite on request. Whether
    # they run *by default* is decided by the runner's source-file filter (which
    # keys on the defining file's directory, not on how it was listed here), not
    # by what is linked.
    add_library(${name} ${M_TYPE} ${M_SOURCES})

    # Public headers live in <module>/include/<module>/, private ones directly
    # in <module>/. Only the include/ directory is PUBLIC, so a dependee sees
    # exactly the headers this module chose to publish, spelled with the module
    # prefix: #include "module_b/b.h".
    #
    # Publishing that directory per-module, rather than putting the shared
    # modules/ root on everyone's include path, is what makes a declared link
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
    # use the suite name from module_run.h (bench.h does), so anything including
    # them needs it on its path too.
    target_include_directories(${name}
        PUBLIC  ${CMAKE_CURRENT_SOURCE_DIR}/include
                ${MODULE_RUN_INCLUDE_DIR}
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

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
    # The closure a dependee must whole-archive to inherit this module's tests:
    # this archive if it is one, plus whatever target_link_module() appends for
    # each static dependency. Seeded here, appended to later, and read only
    # inside the generator expression below -- which is what lets dependencies
    # be declared after this function has returned.
    set(own_archive "")
    if(M_TYPE STREQUAL "STATIC")
        set(own_archive ${name})
    endif()
    set_target_properties(${name} PROPERTIES
        MODULE_WHOLE_ARCHIVE_LIBS "${own_archive}")

    # $<TARGET_PROPERTY:...> defers the list to generate time, by which point
    # every target_link_module() call has contributed. The $<BOOL:...> guard
    # matters: with an empty property the feature would wrap nothing, and some
    # generators emit an unbalanced --whole-archive for that.
    #
    # The property is a genex-list -- semicolon-separated, as a CMake list
    # already is. Joining it with commas instead would make the whole thing one
    # unrecognized library name, and the feature's closing --no-whole-archive
    # would never be emitted.
    set(whole_archive_prop "$<TARGET_PROPERTY:${name},MODULE_WHOLE_ARCHIVE_LIBS>")
    target_link_libraries(${name}_test PRIVATE
        "$<$<BOOL:${whole_archive_prop}>:$<LINK_LIBRARY:WHOLE_ARCHIVE,${whole_archive_prop}>>")

    # A shared module is not in the closure above, so link it the ordinary way.
    if(NOT M_TYPE STREQUAL "STATIC")
        target_link_libraries(${name}_test PRIVATE ${name})
    endif()

    # --- stamps -----------------------------------------------------------
    set(shallow_stamp "${MODULE_STAMP_DIR}/${name}.passed")
    set(deep_stamp    "${MODULE_STAMP_DIR}/${name}.deep.passed")

    # The dependencies' deep stamps, as a file-level input to the test command.
    # target_link_module() appends to this property; $<TARGET_PROPERTY:...>
    # defers reading it to generate time, so dependencies may be declared after
    # this function returns.
    #
    # It has to be a genex in DEPENDS rather than an add_dependencies() edge
    # between the *_tested_deep targets. An add_dependencies() edge only orders
    # two commands, so a failing dependency does not stop this one: Ninja runs
    # this module's tests anyway and the build ends in a wall of failures from
    # every module above the break. Naming the stamp *file* is what makes a
    # missing stamp block the command outright.
    set_property(TARGET ${name} PROPERTY MODULE_DEP_DEEP_STAMPS "")

    # One invocation, two stamps. The commands of a custom command run in
    # sequence and stop at the first failure, so a failing test run touches
    # neither file and the build fails; the next build re-runs it.
    add_custom_command(
        OUTPUT "${shallow_stamp}" "${deep_stamp}"
        COMMAND "$<TARGET_FILE:${name}_test>"
        COMMAND ${CMAKE_COMMAND} -E touch "${shallow_stamp}"
        COMMAND ${CMAKE_COMMAND} -E touch "${deep_stamp}"
        DEPENDS ${name}_test
                "$<TARGET_PROPERTY:${name},MODULE_DEP_DEEP_STAMPS>"
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

# target_link_module(<target> <PUBLIC|PRIVATE|INTERFACE> <module>...)
#
# target_link_libraries() for module dependencies, plus the two extras that a
# plain link does not give you:
#
#   * an ordering edge from <target>_tested_deep onto each <module>_tested_deep,
#     so this module's tests run after its dependencies' have passed
#   * for a static dependency, its whole-archive closure, so its test cases
#     survive into `<target>_test --all` instead of being dropped by the linker
#
# Both are applied here rather than in add_module() so that dependencies can be
# declared after the module, in any order.
function(target_link_module target visibility)
    if(NOT visibility MATCHES "^(PUBLIC|PRIVATE|INTERFACE)$")
        message(FATAL_ERROR
            "target_link_module(${target}): expected PUBLIC, PRIVATE or "
            "INTERFACE, got '${visibility}'")
    endif()

    foreach(dep IN LISTS ARGN)
        # A typo is caught here rather than at link time, where the message
        # would be an unresolved symbol instead of a name. This does not reject
        # forward references to modules defined later -- there is nothing to
        # forward-reference, since every extra below tolerates late binding.
        if(NOT TARGET ${dep})
            message(FATAL_ERROR
                "target_link_module(${target}): '${dep}' is not a target.")
        endif()

        target_link_libraries(${target} ${visibility} ${dep})

        # Ordering: the dependency's deep stamp becomes a file-level input of
        # this module's test command, so a failing dependency leaves no stamp
        # and this module's tests never run. Only between modules -- a
        # non-module target carries no such property.
        get_target_property(dep_deep ${dep} MODULE_DEEP_STAMP)
        if(dep_deep AND TARGET ${target}_tested_deep)
            set_property(TARGET ${target} APPEND
                PROPERTY MODULE_DEP_DEEP_STAMPS "${dep_deep}")
        endif()

        # Whole-archive closure. Appended, and transitive because the
        # dependency's own property already holds the closure its deps
        # contributed. A shared dependency needs nothing: its initializers all
        # run at load time.
        get_target_property(dep_type ${dep} TYPE)
        if(dep_type STREQUAL "STATIC_LIBRARY" AND TARGET ${target}_test)
            get_target_property(dep_whole ${dep} MODULE_WHOLE_ARCHIVE_LIBS)
            if(dep_whole)
                set_property(TARGET ${target} APPEND
                    PROPERTY MODULE_WHOLE_ARCHIVE_LIBS ${dep_whole})
            endif()
        endif()
    endforeach()
endfunction()
