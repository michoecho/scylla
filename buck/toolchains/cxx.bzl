# Vendored from buck2.nix (commit 038b031b84846101030b9d081445003e82e3be5c),
# toolchains/cxx.bzl, with one change: `runtime_dependency_handling` is passed
# to CxxToolchainInfo. See the comment at that line.
#
# It is copied rather than loaded from @nix//toolchains:cxx.bzl because a
# Starlark `rule` object exposes neither `impl` nor `attrs`, so the upstream
# rule cannot be wrapped and its provider patched from outside -- the whole
# file has to be present to change one argument. Everything else is upstream's;
# keep it that way so re-vendoring is a plain copy plus that one line.

load(
    "@prelude//cxx:cxx_toolchain_types.bzl",
    "BinaryUtilitiesInfo",
    "CCompilerInfo",
    "CxxCompilerInfo",
    "CxxInternalTools",
    "CxxPlatformInfo",
    "CxxToolchainInfo",
    "LinkerInfo",
    "LinkerType",
    "PicBehavior",
    "RuntimeDependencyHandling",
    "ShlibInterfacesMode",
)
load("@prelude//cxx:headers.bzl", "HeaderMode")
load("@prelude//cxx:linker.bzl", "is_pdb_generated")
load("@prelude//linking:link_info.bzl", "LinkOrdering", "LinkStyle")
load("@prelude//linking:lto.bzl", "LtoMode")

def _nix_cxx_toolchain(ctx: AnalysisContext) -> list[Provider]:
    nix_cc = ctx.attrs.nix_cc[DefaultInfo].sub_targets

    compiler = nix_cc["cc"][RunInfo]
    cxx_compiler = nix_cc["c++"][RunInfo]

    compiler_type = "clang" if host_info().os.is_macos else "g++"
    archiver = nix_cc["ar"][RunInfo]
    archiver_type = "gnu"
    archiver_supports_argfiles = True
    asm_compiler = compiler
    asm_compiler_type = compiler_type
    compiler = compiler
    cxx_compiler = cxx_compiler
    linker = cxx_compiler
    linker_type = LinkerType("gnu")
    pic_behavior = PicBehavior("supported")
    binary_extension = ""
    object_file_extension = "o"
    static_library_extension = "a"
    shared_library_name_default_prefix = "lib"
    shared_library_name_format = "{}.so"
    shared_library_versioned_name_format = "{}.so.{}"
    additional_linker_flags = []
    if host_info().os.is_macos:
        archiver_supports_argfiles = False
        linker_type = LinkerType("darwin")
        pic_behavior = PicBehavior("always_enabled")
    elif host_info().os.is_windows:
        fail("not supported")
    elif host_info().os.is_linux:
        pass
    else:
        additional_linker_flags = ["-fuse-ld=lld"]

    if compiler_type == "clang":
        llvm_link = RunInfo(args = ["llvm-link"])
    else:
        llvm_link = None

    return [
        DefaultInfo(),
        CxxToolchainInfo(
            internal_tools = ctx.attrs._internal_tools[CxxInternalTools],
            linker_info = LinkerInfo(
                linker = RunInfo(args = linker),
                linker_flags = additional_linker_flags + ctx.attrs.link_flags,
                archiver = archiver,
                archiver_type = archiver_type,
                archiver_supports_argfiles = archiver_supports_argfiles,
                generate_linker_maps = False,
                lto_mode = LtoMode("none"),
                type = linker_type,
                link_binaries_locally = True,
                archive_objects_locally = True,
                use_archiver_flags = True,
                static_dep_runtime_ld_flags = [],
                static_pic_dep_runtime_ld_flags = [],
                shared_dep_runtime_ld_flags = [],
                independent_shlib_interface_linker_flags = [],
                # Upstream buck2.nix uses "stub_from_library". This prelude
                # requires a `shared_library_interface_producer` tool in the
                # toolchain whenever interfaces are enabled, and buck2.nix
                # defines none, so generation is turned off instead. Shared
                # library interfaces are only a link-time optimisation; the one
                # shared library here (zstd) arrives prebuilt from Nix.
                shlib_interfaces = ShlibInterfacesMode("disabled"),
                link_style = LinkStyle(ctx.attrs.link_style),
                link_weight = 1,
                binary_extension = binary_extension,
                object_file_extension = object_file_extension,
                shared_library_name_default_prefix = shared_library_name_default_prefix,
                shared_library_name_format = shared_library_name_format,
                shared_library_versioned_name_format = shared_library_versioned_name_format,
                static_library_extension = static_library_extension,
                force_full_hybrid_if_capable = False,
                is_pdb_generated = is_pdb_generated(linker_type, ctx.attrs.link_flags),
                link_ordering = ctx.attrs.link_ordering,
            ),
            bolt_enabled = False,
            binary_utilities_info = BinaryUtilitiesInfo(
                nm = nix_cc["nm"][RunInfo],
                objcopy = nix_cc["objcopy"][RunInfo],
                ranlib = nix_cc["ranlib"][RunInfo],
                strip = nix_cc["strip"][RunInfo],
                dwp = None,
                bolt_msdk = None,
            ),
            cxx_compiler_info = CxxCompilerInfo(
                compiler = RunInfo(args = [cxx_compiler]),
                preprocessor_flags = [],
                compiler_flags = ctx.attrs.cxx_flags,
                compiler_type = compiler_type,
            ),
            c_compiler_info = CCompilerInfo(
                compiler = RunInfo(args = [compiler]),
                preprocessor_flags = [],
                compiler_flags = ctx.attrs.c_flags,
                compiler_type = compiler_type,
            ),
            as_compiler_info = CCompilerInfo(
                compiler = RunInfo(args = [compiler]),
                compiler_type = compiler_type,
            ),
            asm_compiler_info = CCompilerInfo(
                compiler = RunInfo(args = [asm_compiler]),
                compiler_type = asm_compiler_type,
            ),
            header_mode = HeaderMode("symlink_tree_only"),
            cpp_dep_tracking_mode = ctx.attrs.cpp_dep_tracking_mode,
            pic_behavior = pic_behavior,
            llvm_link = llvm_link,
            # A deviation from upstream buck2.nix, which does not set this
            # field at all -- this prelude declares it with no default, so
            # omitting it fails analysis outright.
            #
            # "symlink" rather than the prelude's usual "no_symlink" default,
            # because it is what makes dynamically linked prebuilt libraries
            # runnable. With it, the prelude builds a symlink tree of the
            # executable's transitive shared library deps and links the binary
            # with `-Wl,-rpath,$ORIGIN/<tree>` (see
            # executable_shared_lib_arguments_template in the prelude's
            # cxx_link_utility.bzl). Without it, zstd_hello links fine but dies
            # at startup with "libzstd.so.1: cannot open shared object file",
            # since nothing would put libzstd's directory on the RPATH.
            #
            # The resulting RPATH is $ORIGIN-relative, so the binary stays
            # relocatable rather than pinned to a /nix/store path.
            runtime_dependency_handling = RuntimeDependencyHandling("symlink"),
        ),
        CxxPlatformInfo(name = "aarch64" if host_info().arch.is_aarch64 else "x86_64"),
    ]

nix_cxx_toolchain = rule(
    impl = _nix_cxx_toolchain,
    attrs = {
        "_internal_tools": attrs.default_only(attrs.exec_dep(providers = [CxxInternalTools], default = "prelude//cxx/tools:internal_tools")),
        "c_flags": attrs.list(attrs.string(), default = []),
        "cpp_dep_tracking_mode": attrs.string(default = "makefile"),
        "cxx_flags": attrs.list(attrs.string(), default = []),
        "link_ordering": attrs.option(attrs.enum(LinkOrdering.values()), default = None),
        "link_flags": attrs.list(attrs.string(), default = []),
        "link_style": attrs.string(default = "shared"),
        "nix_cc": attrs.exec_dep(),
    },
    doc = """
    Creates a cxx toolchain that is required by all C/C++ rules.

    ## Examples

    ```starlark
    # use the `cxx` flake package output from `./nix` to provide the compiler tools
    flake.package(
        name = "nix_cc",
        binaries = [
            "ar",
            "cc",
            "c++",
            "nm",
            "objcopy",
            "ranlib",
            "strip",
        ],
        package = "cxx",
        path = "nix",
    )

    # provide the `cxx` toolchain using the `:nix_cc` target
    nix_cxx_toolchain(
        name = "cxx",
        nix_cc = ":nix_cc",
        visibility = ["PUBLIC"],
    )
    ```

    _Note_: The `nixpkgs` cc infrastructure depends on environment variables to be set during execution. You might
            need to wrap the C/C++ compiler tools capturing the environment. Take a look at the example project.
    """,
    is_toolchain_rule = True,
)
