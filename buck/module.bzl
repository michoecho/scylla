def add_module(
        name,
        srcs,
        headers = [],
        exported_headers = {},
        deps = [],
        compiler_flags = [],
        exported_needs_coverage_instrumentation = False,
        exported_preprocessor_flags = [],
        exported_linker_flags = [],
        resources = [],
        env = {},
        module_source_dir = None,
        labels = ["startup_shared"],
        visibility = ["PUBLIC"]):
    """Declare a module library and its matching doctest executable.

    Test translation units live in the library. The test executable links the
    library whole so static initializers register every doctest case.
    """
    test_name = name + "_test"
    source_dir = module_source_dir or name
    snapshot_sources = glob([".snapshots/**"])
    snapshot_target = name + "_snapshots"

    # Snapshot stores belong to the module whose tests consume them. Keep the
    # filegroup harmless for modules without a store: glob() is empty, and no
    # location macro is emitted into the test environment in that case.
    native.filegroup(
        name = snapshot_target,
        srcs = snapshot_sources,
        visibility = visibility,
    )

    test_env = dict(env)
    if snapshot_sources:
        test_env["SNAPSHOT_ROOT"] = "$(location :{})/.snapshots".format(snapshot_target)

    native.cxx_library(
        name = name,
        srcs = srcs,
        headers = headers,
        compiler_flags = compiler_flags,
        exported_deps = deps,
        exported_headers = exported_headers,
        exported_linker_flags = exported_linker_flags,
        exported_preprocessor_flags = exported_preprocessor_flags,
        exported_needs_coverage_instrumentation = exported_needs_coverage_instrumentation,
        precompiled_header = select({
            "//:no_pch": None,
            "DEFAULT": "//:project_pch",
        }),
        header_namespace = "",
        link_whole = True,
        preferred_linkage = "static",
        tests = [":" + test_name],
        visibility = visibility,
    )

    native.cxx_test(
        name = test_name,
        srcs = ["//:module_test_main"],
        deps = [":" + name, "//:vscode_results_reporter", "//:module_runner"] + select({
            "//:no_pch": [],
            "DEFAULT": ["//:project_pch"],
        }),
        resources = resources,
        env = test_env,
        labels = labels,
        supports_test_execution_caching = True,
        # Buck2 only caches test executions through the RE action cache. Keep
        # these tests on the same local RE worker used by the build platform.
        remote_execution = {
            "capabilities": {
                "OSFamily": "",
                "container-image": "",
                "ISA": "x86-64",
            },
            "remote_cache_enabled": True,
            "use_case": "buck2-default",
        },
        header_namespace = "",
        compiler_flags = compiler_flags + [
            "-DMODULE_NAME=\"{}\"".format(name),
            "-DMODULE_SOURCE_DIR=\"{}\"".format(source_dir),
        ],
        precompiled_header = select({
            "//:no_pch": None,
            "DEFAULT": "//:project_pch",
        }),
        # Shared normally, because it links faster and is what every other
        # configuration wants. Static under the fuzztest modifier, and that is
        # not a preference but a correctness requirement of the backend.
        #
        # FuzzTest reads SanitizerCoverage's 8-bit counters, and each shared
        # object registers its own counter map through
        # __sanitizer_cov_8bit_counters_init. FuzzTest keeps only the *first*
        # map it is handed and warns about the rest, so with a shared link the
        # counters it steers on belong to whichever DSO happened to register
        # first -- not the one holding the code under test. The search then runs
        # with edge feedback permanently reading zero: it still works, because
        # the cmp table is fed separately, but the coverage half of a
        # coverage-guided search is silently gone. Linking statically leaves one
        # counter map, which is the one that matters.
        link_style = select({
            "//:fuzztest": "static",
            "//:centipede": "static",
            "DEFAULT": "shared",
        }),
        visibility = visibility,
    )
