if(NOT DEFINED SOURCE_ROOT OR NOT DEFINED DEST_DIR)
    message(FATAL_ERROR "SOURCE_ROOT and DEST_DIR are required")
endif()

file(GLOB_RECURSE sancov_cmp_archives
    "${SOURCE_ROOT}/rust_*/**/release/build/libafl_targets-*/out/libsancov_cmp.a")
if(NOT sancov_cmp_archives)
    message(FATAL_ERROR "Could not find LibAFL's libsancov_cmp.a after Cargo built")
endif()

list(GET sancov_cmp_archives 0 sancov_cmp_archive)
get_filename_component(sancov_out_dir "${sancov_cmp_archive}" DIRECTORY)
file(MAKE_DIRECTORY "${DEST_DIR}")

foreach(archive IN ITEMS libsancov_cmp.a libcommon.a libcoverage.a libcmplog.a)
    if(NOT EXISTS "${sancov_out_dir}/${archive}")
        message(FATAL_ERROR "Missing LibAFL SanCov archive: ${sancov_out_dir}/${archive}")
    endif()
    file(COPY_FILE "${sancov_out_dir}/${archive}" "${DEST_DIR}/${archive}" ONLY_IF_DIFFERENT)
endforeach()
