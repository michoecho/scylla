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

    snapshot_env = {}
    if snapshot_sources:
        snapshot_env["SNAPSHOT_ROOT"] = "$(location :{})/.snapshots".format(snapshot_target)

    # File snapshots are read through SNAPSHOT_ROOT above -- the build's copy,
    # which is hermetic and travels into a remote sandbox -- and written through
    # this one, the store's path in the source tree. A write has to reach the
    # repository, and buck-out is not the repository.
    #
    # Set unconditionally, because a module recording its first file snapshot
    # has no store for the glob above to find.
    snapshot_env["SNAPSHOT_SOURCE_ROOT"] = "{}/.snapshots".format(source_dir)

    # Snapshot update mode arrives as a buckconfig, not as an ambient
    # environment variable.
    #
    # `buck2 test` does not forward the caller's environment to the test, so
    # `SNAPSHOT_UPDATE=1 buck2 test ...` silently does nothing -- the run just
    # fails again with "re-run with SNAPSHOT_UPDATE=1". Routing it through `-c`
    # also makes update mode part of the action key, so a cached green result
    # cannot stand in for a run that was asked to rewrite sources.
    #
    #     buck2 test -c snapshot.update=1 //modules/x:x_test
    #
    # It also selects the executor; see remote_execution below.
    snapshot_update = read_config("snapshot", "update", "0")
    if snapshot_update != "0":
        snapshot_env["SNAPSHOT_UPDATE"] = snapshot_update

    # The module's own `env` wins over everything computed above. The config is
    # repo-wide, so `-c snapshot.update=1 //...` puts every module into update
    # mode at once, and a module whose tests are fixtures *about* snapshotting
    # cannot survive that -- rewriting them replaces the expectations that would
    # have caught a bad rewrite with that rewrite's own output. Pinning
    # SNAPSHOT_UPDATE in `env` opts such a module out, and the same precedence
    # applies to the two roots for anything that needs to point them elsewhere.
    test_env = dict(snapshot_env)
    test_env.update(env)

    # What the test will actually see, which is what the executor below has to
    # agree with -- not the config, which the env may have just overridden.
    snapshot_update = test_env.get("SNAPSHOT_UPDATE", "0")

    # How the test executes, which update mode has to change.
    #
    # Normally: remote, on the same local RE worker the build platform uses.
    # Buck2 only caches test executions through the RE action cache, and only a
    # remotely executed action populates it -- the test executor config leaves
    # `allow_cache_uploads` false and the `remote_execution` attr exposes no way
    # to set it. So a locally executed test uploads nothing, and any action key
    # that starts cold stays cold.
    #
    # Under update mode: local-only, via the attr's "disabled" spelling. A
    # rewrite needs the real source tree, and a remotely executed test runs in a
    # sandbox holding only the action's inputs.
    #
    # "disabled" rather than adding `local_enabled` to the dict below, because
    # `local_enabled` merely makes the executor *hybrid*. The prelude builds the
    # test executor without `use_limited_hybrid` (the platform's setting does not
    # reach it), so hybrid there means `HybridExecutionLevel::Full` -- local and
    # remote race, and remote can win. That would put the rewrite back in a
    # sandbox, intermittently. `--local-only` overrides the race, but relying on
    # a flag to make the build correct is the wrong shape.
    #
    # "disabled" instead yields a genuinely local-only executor, and is the one
    # path in the prelude that also sets `run_from_project_root`, which is what
    # makes the project-relative paths a rewrite works with resolve. See
    # prelude/tests/re_utils.bzl.
    if snapshot_update != "0":
        test_remote_execution = "disabled"
    else:
        test_remote_execution = {
            "capabilities": {
                "OSFamily": "",
                "container-image": "",
                "ISA": "x86-64",
            },
            "remote_cache_enabled": True,
            "use_case": "buck2-default",
        }

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
        remote_execution = test_remote_execution,
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
