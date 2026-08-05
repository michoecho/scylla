if(NOT DEFINED ACTION OR NOT DEFINED BINARY_DIR)
    message(FATAL_ERROR "ACTION and BINARY_DIR are required")
endif()

if(ACTION STREQUAL "CLEAN")
    file(GLOB_RECURSE coverage_profiles
        LIST_DIRECTORIES FALSE
        "${BINARY_DIR}/*.profraw"
        "${BINARY_DIR}/*.profdata"
    )
    if(coverage_profiles)
        file(REMOVE ${coverage_profiles})
    endif()
elseif(ACTION STREQUAL "EXPORT")
    if(NOT DEFINED MERGE_COVERAGE OR NOT DEFINED LLVM_COV)
        message(FATAL_ERROR "MERGE_COVERAGE and LLVM_COV are required for export")
    endif()

    execute_process(
        COMMAND "${MERGE_COVERAGE}" "${BINARY_DIR}"
        COMMAND_ERROR_IS_FATAL ANY
    )

    set(total_profdata "${BINARY_DIR}/total.profdata")
    set(lcov_output "${BINARY_DIR}/coverage/total.lcov")
    if(NOT EXISTS "${total_profdata}")
        message(FATAL_ERROR "merge-coverage did not produce ${total_profdata}")
    endif()

    file(MAKE_DIRECTORY "${BINARY_DIR}/coverage")
    execute_process(
        COMMAND "${LLVM_COV}" export
            --format=lcov
            --check-binary-ids
            --debug-file-directory "${BINARY_DIR}/coverage"
            "-instr-profile=${total_profdata}"
        OUTPUT_FILE "${lcov_output}"
        COMMAND_ERROR_IS_FATAL ANY
    )
else()
    message(FATAL_ERROR "Unknown coverage action: ${ACTION}")
endif()
