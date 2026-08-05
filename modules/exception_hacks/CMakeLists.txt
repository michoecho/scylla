# Stacktrace of the throw-expression behind an arbitrary exception_ptr, via
# Boost's from_exception library (see the find_package block in the top-level
# CMakeLists for how the capture and the name resolution split in two).
#
# A module of its own rather than part of `main` because nothing in the program
# uses it -- it is an opt-in diagnostic any module can link when it wants
# traces off its exceptions, and it depends on nothing but Boost.
add_module(exception_hacks
    SOURCES exception_hacks.cc)

target_link_libraries(exception_hacks PUBLIC
    # Trace capture at throw time, plus the libbacktrace name resolution its
    # frames are printed with. ${CMAKE_DL_LIBS} is dladdr, which Boost uses
    # to attribute each frame to its shared object.
    Boost::stacktrace_from_exception
    ${BACKTRACE_LIBRARY}
    ${CMAKE_DL_LIBS})

# exception_hacks.cc is the only TU here that includes boost/stacktrace, but
# the macro selecting the libbacktrace backend has to match in every TU that
# does, and PUBLIC keeps that true for anything linking this module later.
target_compile_definitions(exception_hacks PUBLIC BOOST_STACKTRACE_USE_BACKTRACE)
target_include_directories(exception_hacks SYSTEM PUBLIC ${BACKTRACE_INCLUDE_DIR})

target_compile_options(exception_hacks PRIVATE
    -Wall
    -Wextra
)
