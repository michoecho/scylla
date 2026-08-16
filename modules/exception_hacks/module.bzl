def add_module(
        name,
        srcs,
        exported_headers,
        deps = [],
        compiler_flags = [],
        exported_preprocessor_flags = [],
        exported_linker_flags = [],
        visibility = ["PUBLIC"]):
    """Declare a module library and its matching local doctest executable.

    Like the CMake add_module(), test registrations live in the module library.
    link_whole keeps their static initializers in the test executable.
    """
    test_name = name + "_test"

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
        srcs = ["buck_test_main.cc"],
        deps = [":" + name],
        link_style = "shared",
    )
