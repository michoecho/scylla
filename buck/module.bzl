def add_module(
        name,
        srcs,
        exported_headers = {},
        deps = [],
        compiler_flags = [],
        exported_preprocessor_flags = [],
        exported_linker_flags = [],
        resources = [],
        env = {},
        module_source_dir = None,
        visibility = ["PUBLIC"]):
    """Declare a module library and its matching doctest executable.

    Test translation units live in the library, just like they do for the
    CMake add_module() convention.  The test executable links the library
    whole so static initializers register every doctest case.
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
        compiler_flags = compiler_flags,
        exported_deps = deps,
        exported_headers = exported_headers,
        exported_linker_flags = exported_linker_flags,
        exported_preprocessor_flags = exported_preprocessor_flags,
        header_namespace = "",
        link_whole = True,
        preferred_linkage = "static",
        tests = [":" + test_name],
        visibility = visibility,
    )

    native.cxx_test(
        name = test_name,
        srcs = ["//cmake:module_test_main"],
        deps = [":" + name, "//cmake:module_runner"],
        resources = resources,
        env = test_env,
        header_namespace = "",
        compiler_flags = compiler_flags + [
            "-Icmake",
            "-DMODULE_NAME=\"{}\"".format(name),
            "-DMODULE_SOURCE_DIR=\"{}\"".format(source_dir),
        ],
        link_style = "shared",
        visibility = visibility,
    )
