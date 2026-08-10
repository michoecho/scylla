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

    # The per-test LCOV directory, which the glob above does not cover: its
    # contents are .lcov, not profiles. Stale files here are worse than merely
    # untidy -- a test deleted or renamed since the last run would otherwise
    # keep a report, and the extension would show coverage attributed to a test
    # that no longer exists. The manifests are left alone: they are written by
    # discovery at configure/build time, not by a run.
    if(DEFINED PROFILE_DIR)
        file(REMOVE_RECURSE "${BINARY_DIR}/coverage/per-test")
    endif()
elseif(ACTION STREQUAL "EXPORT")
    if(NOT DEFINED MERGE_COVERAGE OR NOT DEFINED LLVM_COV)
        message(FATAL_ERROR "MERGE_COVERAGE and LLVM_COV are required for export")
    endif()

    # --per-test is what makes merge-coverage additionally emit
    # coverage/per-test/<id>.lcov plus the manifest the extension reads. Off by
    # default (ENABLE_PER_TEST_COVERAGE): it costs an llvm-profdata and an
    # llvm-cov invocation per test rather than one for the suite.
    set(per_test_args "")
    if(PER_TEST AND DEFINED PROFILE_DIR)
        set(per_test_args --per-test --profile-dir "${PROFILE_DIR}")
    endif()

    execute_process(
        COMMAND "${MERGE_COVERAGE}" "${BINARY_DIR}" ${per_test_args}
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
