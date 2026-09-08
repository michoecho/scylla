include(add_whole_archive)

add_library(scylla_tracing STATIC)
target_sources(scylla_tracing
  PRIVATE
    tracing.cc
    trace_keyspace_helper.cc
    trace_state.cc
    traced_file.cc
    tracer.cc)
target_include_directories(scylla_tracing
  PUBLIC
    ${CMAKE_SOURCE_DIR})
target_link_libraries(scylla_tracing
  PUBLIC
    Seastar::seastar
    xxHash::xxhash
  PRIVATE
    service
    scylla-main)
if (Scylla_USE_PRECOMPILED_HEADER_USE)
  target_precompile_headers(scylla_tracing REUSE_FROM scylla-precompiled-header)
endif()

# The binary tracepoints of tracer.cc are call sites, not the tracer: that lives
# in libseastar.so, and Seastar::seastar carries the include directories for it.
# -w because the tracer's headers are not written to Scylla's warning set.
set_source_files_properties (tracer.cc PROPERTIES COMPILE_OPTIONS "-w")

add_whole_archive(tracing scylla_tracing)

check_headers(check-headers scylla_tracing
  GLOB_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/*.hh)
