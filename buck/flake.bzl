# HOW TO USE THIS MODULE:
#
#    load("//buck:flake.bzl", "flake")
#
#    flake.package(name = "pkg", path = "path/to/flake/dir", ...)
#    flake.prebuilt_cmake_library(name = "lib", path = flake.store(package = "lib", path = "path/to/flake/dir"))

load("@prelude//:prelude.bzl", "native")
load("@prelude//cxx:cxx_toolchain_types.bzl", "CxxToolchainInfo")
load("@prelude//decls/common.bzl", "buck")
load("@prelude//os_lookup:defs.bzl", "Os", "OsLookup")

## ---------------------------------------------------------------------------------------------------------------------
def __nix_build(
        ctx: AnalysisContext,
        path: Artifact,
        package_set: str,
        package: str,
        output: str,
        target_os_info: OsLookup) -> Artifact:
    """Run `nix build` for a flake attribute and return the resulting out-link artifact."""

    # calls nix build path:<path>#package.<arch-os>.<package>

    if target_os_info.os == Os("linux"):
        os = "linux"
    elif target_os_info.os == Os("macos"):
        os = "darwin"
    else:
        fail("host os not supported: {}".format(target_os_info.os))

    if target_os_info.cpu == "x86_64":
        cpu = "x86_64"
    elif target_os_info.cpu == "arm64":
        cpu = "aarch64"
    else:
        fail("host arch is not supported: {}".format(target_os_info.cpu))

    system = "{cpu}-{os}".format(os = os, cpu = cpu)

    attribute = package_set + "." + system + "." + package + "." + output

    # nix will build the first output by default, but we do not know what the first output is called.
    # That's why we build the "out" output by default.
    # Note, nix does not append a suffix to the out-link for the "out" output.
    out_link = ctx.actions.declare_output("out.link" if output == "out" else "out.link-" + output)

    nix_build = cmd_args([
        "env",
        "--",  # this is needed to avoid "Spawning executable `nix` failed: Failed to spawn a process"
        "nix",
        "--extra-experimental-features",
        "nix-command flakes",
        "build",
        #"--show-trace",         # for debugging
        cmd_args("--out-link", cmd_args(out_link.as_output(), parent = 1, absolute_suffix = "/out.link")),
        cmd_args(cmd_args(path, format = "path:{}"), attribute, delimiter = "#"),
    ])
    ctx.actions.run(nix_build, category = "nix_flake", local_only = True)

    return out_link

def __nix_package_tree(ctx: AnalysisContext, out_link: Artifact) -> Artifact:
    """Expose the package as a directory tree buck2 can `project()` into.

    `out_link` is itself a symlink into the nix store, and buck2 will not `project()` through a
    symlink -- it treats it as a leaf. So we build a tree of *real* directories next to it, whose
    leaves are symlinks into the store. The directories make the paths projectable; the symlinked
    leaves keep this cheap, since the package contents are never duplicated.
    """
    tree = ctx.actions.declare_output("tree", dir = True)

    # `cp -Rs` recreates directories and symlinks the files within them. It requires an absolute
    # source, hence `readlink -f` to resolve the out-link to its store path. The trailing `/.`
    # copies the *contents* of the package into `tree` rather than nesting it one level deeper.
    script = cmd_args(
        "set -eu",
        cmd_args("mkdir", "-p", "--", tree.as_output(), delimiter = " "),
        cmd_args("cp", "-Rs", "--", cmd_args(out_link, format = "$(readlink -f {})/."), tree.as_output(), delimiter = " "),
        delimiter = "\n",
    )
    wrapper, _ = ctx.actions.write(
        "nix_package_tree.sh",
        script,
        allow_args = True,
        is_executable = True,
    )
    ctx.actions.run(
        cmd_args("/bin/sh", wrapper, hidden = [out_link, tree.as_output()]),
        category = "nix_package_tree",
        local_only = True,
    )
    return tree

def __flake_package_impl(
        ctx: AnalysisContext,
        path: Artifact,
        package_set: str,
        package: str,
        output: str,
        binary: str | None,
        binaries: list[str],
        target_os_info: OsLookup) -> list[Provider]:
    out_link = __nix_build(ctx, path, package_set, package, output, target_os_info)

    run_info = []
    if binary:
        run_info.append(
            RunInfo(
                args = cmd_args(out_link, "bin", binary, delimiter = "/"),
            ),
        )

    sub_targets = {
        bin: [DefaultInfo(default_output = out_link), RunInfo(args = cmd_args(out_link, "bin", bin, delimiter = "/"))]
        for bin in binaries
    }

    # `files` exposes arbitrary paths inside the package as sub-targets, so that packages which are
    # not just a bag of executables (headers, libraries, data files, ...) can be consumed by other
    # rules. This is the counterpart to `build_file`/`build_file_content` in `rules_nixpkgs`.
    if ctx.attrs.files:
        tree = __nix_package_tree(ctx, out_link)
        for name, sub_path in ctx.attrs.files.items():
            if name in sub_targets:
                fail("`files` entry {} collides with a binary of the same name".format(repr(name)))
            sub_targets[name] = [DefaultInfo(default_output = tree.project(sub_path))]

    return [
        DefaultInfo(
            default_output = out_link,
            sub_targets = sub_targets,
        ),
    ] + run_info

__common_attrs = {
    "binary": attrs.option(attrs.string(), default = None, doc = """
      specify the default binary of this package

      This provides `RunInfo` for a binary in the `bin` directory of the package.
    """),
    "binaries": attrs.list(attrs.string(), default = [], doc = """
      add auxiliary binaries for this package

      These can be accessed as sub-targets with the given name in dependent rules.
    """),
    "files": attrs.dict(attrs.string(), attrs.string(), default = {}, doc = """
      expose paths inside the package as sub-targets

      Maps a sub-target name to a path relative to the root of the built package, e.g.
      `{"include": "include", "libz.a": "lib/libz.a"}`. Each entry becomes a sub-target
      providing `DefaultInfo` for that file or directory.

      This is the counterpart to `build_file`/`build_file_content` in `rules_nixpkgs`: it is how a
      package that is not simply a set of executables gets consumed by other rules.
    """),
    "path": attrs.source(allow_directory = True, doc = "the path to the flake"),
    "output": attrs.string(default = "out", doc = """
      specify the output to build instead of the default

      (optional, default: `"out"`)
    """),
    "package": attrs.option(attrs.string(), doc = """
      name of the flake output

      (optional, default: same as `name`)
    """, default = None),
    "_target_os_type": buck.target_os_type_arg(),
}

__flake_package = rule(
    impl = lambda ctx: __flake_package_impl(
        ctx,
        ctx.attrs.path,
        "packages",
        ctx.attrs.package or ctx.label.name,
        ctx.attrs.output,
        ctx.attrs.binary,
        ctx.attrs.binaries,
        ctx.attrs._target_os_type[OsLookup],
    ),
    attrs = __common_attrs,
    doc = """
    A `flake.package()` rule builds a nix package of a given flake.

    ## Examples

    ```starlark
    flake.package(
        name = "curl",
        path = "nix",
        output = "bin",
        binary = "curl",
    )
    ```

    This creates a target called `curl` from the nix flake in `./nix`, building `path:nix#packages.<system>.curl.bin`.

    Packages that are not simply a set of executables are exposed with `files`, which makes paths
    inside the package available as sub-targets. Those sub-targets can then be consumed by any rule
    taking a source, such as `prebuilt_cxx_library`:

    ```starlark
    # zlib headers live in the `dev` output, the libraries in the default `out` output
    flake.package(
        name = "zlib_dev",
        files = {"include": "include"},
        output = "dev",
        package = "zlib",
        path = "nix",
    )

    flake.package(
        name = "zlib_out",
        files = {"libz.so.1": "lib/libz.so.1"},
        package = "zlib",
        path = "nix",
    )

    prebuilt_cxx_library(
        name = "zlib",
        header_dirs = [":zlib_dev[include]"],
        shared_lib = ":zlib_out[libz.so.1]",
        preferred_linkage = "shared",
        extract_soname = True,
    )
    ```
    """,
)

## ---------------------------------------------------------------------------------------------------------------------
## cmake package discovery

# The CMake project that `flake.prebuilt_cmake_library()` configures. Nothing is ever built from it:
# configuring is enough, because `find_package()` is what knows where the headers and the libraries
# of a package are, and the imported targets it defines carry that in their properties. We walk
# those targets and write the flags out as two response files, one for the compiler and one for the
# linker.
#
# This is the same shape as `@prelude//third-party:pkgconfig.bzl`, which shells out to `pkg-config`
# and feeds `@<file>` to `prebuilt_cxx_library`. CMake needs a configure step rather than one query
# command, but the result is the same: the flags nobody has to write out by hand.
__CMAKE_QUERY = """
cmake_minimum_required(VERSION 3.21)

# Generated by //buck:flake.bzl -- see `flake.prebuilt_cmake_library`.
project(flake_cmake_query LANGUAGES ${FLAKE_LANGUAGES})

find_package(${FLAKE_PACKAGE} ${FLAKE_VERSION} REQUIRED)

# Accumulators live in global properties so that the recursive walk below can append to them without
# threading PARENT_SCOPE through every frame.
function(flake_add prop value)
  if(value STREQUAL "")
    return()
  endif()
  get_property(cur GLOBAL PROPERTY ${prop})
  if(NOT "${value}" IN_LIST cur)
    set_property(GLOBAL APPEND PROPERTY ${prop} "${value}")
  endif()
endfunction()

# Property values may be generator expressions, which are only resolved at generate time and so are
# opaque here. The two that carry a usable value are unwrapped; `BUILD_INTERFACE` describes the
# package's own build tree and never applies to an installed prefix, and anything else is dropped
# with a note rather than passed through as a literal.
function(flake_strip_genex value out)
  set(v "${value}")
  while(v MATCHES "^[$]<(LINK_ONLY|INSTALL_INTERFACE):(.*)>$")
    set(v "${CMAKE_MATCH_2}")
  endwhile()
  if(v MATCHES "^[$]<BUILD_INTERFACE:")
    set(v "")
  elseif(v MATCHES "[$]<")
    message(STATUS "flake: ignoring generator expression: ${v}")
    set(v "")
  endif()
  set(${out} "${v}" PARENT_SCOPE)
endfunction()

# The location of an imported target, which for a multi-config package hangs off the configuration
# rather than off `IMPORTED_LOCATION` itself.
function(flake_location target out)
  set(loc "")
  get_target_property(loc ${target} IMPORTED_LOCATION)
  if(NOT loc)
    get_target_property(cfgs ${target} IMPORTED_CONFIGURATIONS)
    if(cfgs)
      list(REMOVE_DUPLICATES cfgs)
      # Prefer an optimised build, then a package that was installed without any configuration at
      # all, then whatever else is on offer.
      foreach(cfg RELEASE RELWITHDEBINFO NOCONFIG ${cfgs})
        if(cfg IN_LIST cfgs)
          get_target_property(loc ${target} IMPORTED_LOCATION_${cfg})
          if(loc)
            break()
          endif()
        endif()
      endforeach()
    endif()
  endif()
  if(NOT loc)
    set(loc "")
  endif()
  set(${out} "${loc}" PARENT_SCOPE)
endfunction()

# A library we link by absolute path is found again at runtime through its own directory, so a
# shared one contributes an rpath entry. Store paths are absolute and immutable, so this is stable.
function(flake_add_library path)
  flake_add(FLAKE_LIBS "${path}")
  if(path MATCHES "[.](so|dylib)([.][0-9]+)*$")
    get_filename_component(dir "${path}" DIRECTORY)
    flake_add(FLAKE_LIBS "-Wl,-rpath,${dir}")
  endif()
endfunction()

# Depth-first over the link interface, emitting each target's own library before the libraries it
# depends on -- which is the order a single-pass linker needs.
function(flake_visit item)
  flake_strip_genex("${item}" item)
  if(item STREQUAL "")
    return()
  endif()

  if(NOT TARGET "${item}")
    # Not a target: an absolute path to a library, a linker flag, or a bare name for the linker to
    # resolve on its own.
    if(IS_ABSOLUTE "${item}")
      flake_add_library("${item}")
    elseif(item MATCHES "^-")
      flake_add(FLAKE_LIBS "${item}")
    else()
      flake_add(FLAKE_LIBS "-l${item}")
    endif()
    return()
  endif()

  get_property(seen GLOBAL PROPERTY FLAKE_SEEN)
  if("${item}" IN_LIST seen)
    return()
  endif()
  set_property(GLOBAL APPEND PROPERTY FLAKE_SEEN "${item}")

  get_target_property(alias ${item} ALIASED_TARGET)
  if(alias)
    flake_visit("${alias}")
    return()
  endif()

  get_target_property(type ${item} TYPE)
  if(NOT type STREQUAL "INTERFACE_LIBRARY")
    flake_location(${item} loc)
    if(loc)
      flake_add_library("${loc}")
    endif()
  endif()

  foreach(prop INTERFACE_INCLUDE_DIRECTORIES INTERFACE_SYSTEM_INCLUDE_DIRECTORIES)
    get_target_property(dirs ${item} ${prop})
    if(dirs)
      foreach(dir IN LISTS dirs)
        flake_strip_genex("${dir}" dir)
        if(NOT dir STREQUAL "")
          flake_add(FLAKE_CFLAGS "-isystem${dir}")
        endif()
      endforeach()
    endif()
  endforeach()

  get_target_property(defs ${item} INTERFACE_COMPILE_DEFINITIONS)
  if(defs)
    foreach(def IN LISTS defs)
      flake_strip_genex("${def}" def)
      if(NOT def STREQUAL "")
        flake_add(FLAKE_CFLAGS "-D${def}")
      endif()
    endforeach()
  endif()

  get_target_property(opts ${item} INTERFACE_COMPILE_OPTIONS)
  if(opts)
    foreach(opt IN LISTS opts)
      flake_strip_genex("${opt}" opt)
      flake_add(FLAKE_CFLAGS "${opt}")
    endforeach()
  endif()

  get_target_property(lopts ${item} INTERFACE_LINK_OPTIONS)
  if(lopts)
    foreach(lopt IN LISTS lopts)
      flake_strip_genex("${lopt}" lopt)
      flake_add(FLAKE_LIBS "${lopt}")
    endforeach()
  endif()

  # `IMPORTED_LINK_INTERFACE_LIBRARIES` is how packages predating target_link_libraries(INTERFACE)
  # spell the same thing, and find modules still produce it.
  set(deps "")
  foreach(prop INTERFACE_LINK_LIBRARIES IMPORTED_LINK_INTERFACE_LIBRARIES)
    get_target_property(value ${item} ${prop})
    if(value)
      list(APPEND deps ${value})
    endif()
  endforeach()
  foreach(dep IN LISTS deps)
    flake_visit("${dep}")
  endforeach()
endfunction()

# Which targets to start from. An explicit list wins; otherwise the imported targets the
# `find_package()` above defined, preferring the package's own `pkg::pkg` and falling back to
# everything in its namespace. Anything outside the namespace is a dependency of the package and is
# reached through the link interface anyway.
#
# The convention is worth leaning on: a package that offers several targets in its namespace usually
# offers *variants* of itself -- fmt::fmt against fmt::fmt-header-only, say -- whose flags
# contradict each other, and `pkg::pkg` is the one meant by default. Use `targets` to pick another.
if(FLAKE_TARGETS)
  set(roots ${FLAKE_TARGETS})
else()
  get_property(imported DIRECTORY PROPERTY IMPORTED_TARGETS)
  string(TOLOWER "${FLAKE_PACKAGE}" pkg)
  set(roots "")
  set(canonical "")
  foreach(target IN LISTS imported)
    string(TOLOWER "${target}" lowered)
    if(lowered STREQUAL "${pkg}::${pkg}")
      set(canonical ${target})
    elseif(lowered MATCHES "^${pkg}::")
      list(APPEND roots ${target})
    endif()
  endforeach()
  if(canonical)
    set(roots ${canonical})
  elseif(NOT roots)
    set(roots ${imported})
  endif()
endif()

foreach(root IN LISTS roots)
  flake_visit("${root}")
endforeach()

# Packages that define no imported targets at all -- older find modules, mostly -- only report
# themselves through these variables.
if(NOT roots)
  foreach(var ${FLAKE_PACKAGE}_INCLUDE_DIRS ${FLAKE_PACKAGE}_INCLUDE_DIR)
    foreach(dir IN LISTS ${var})
      flake_add(FLAKE_CFLAGS "-isystem${dir}")
    endforeach()
  endforeach()
  foreach(var ${FLAKE_PACKAGE}_LIBRARIES ${FLAKE_PACKAGE}_LIBRARY)
    foreach(lib IN LISTS ${var})
      flake_visit("${lib}")
    endforeach()
  endforeach()
endif()

get_property(cflags GLOBAL PROPERTY FLAKE_CFLAGS)
get_property(libs GLOBAL PROPERTY FLAKE_LIBS)
list(JOIN cflags "\\n" cflags_text)
list(JOIN libs "\\n" libs_text)
file(WRITE "${FLAKE_CFLAGS_OUT}" "${cflags_text}\\n")
file(WRITE "${FLAKE_LIBS_OUT}" "${libs_text}\\n")

message(STATUS "flake: ${FLAKE_PACKAGE} cflags: ${cflags}")
message(STATUS "flake: ${FLAKE_PACKAGE} libs: ${libs}")
"""

def __cmake_query_impl(ctx: AnalysisContext) -> list[Provider]:
    cmakelists = ctx.actions.write("cmake_query/CMakeLists.txt", __CMAKE_QUERY)
    cflags = ctx.actions.declare_output("cflags")
    libs = ctx.actions.declare_output("libs")
    build_dir = ctx.actions.declare_output("cmake-build", dir = True)

    # cmake is invoked through a shell so the paths it needs as absolute can be made so with `$PWD`:
    # `file(WRITE)` resolves a relative path against cmake's build directory rather than against the
    # directory buck2 runs the action in, and `CMAKE_PREFIX_PATH` has to be absolute to be searched.
    # The prefix is additionally resolved through `readlink`, since it is an out-link into the store
    # and a package's config file locates its own siblings relative to itself.
    prefixes = cmd_args(
        [dep[DefaultInfo].default_outputs[0] for dep in [ctx.attrs.path] + ctx.attrs.prefixes],
        format = "$(readlink -f {})",
        delimiter = ";",
    )

    argv = cmd_args(
        ctx.attrs._cmake[RunInfo],
        "-S",
        cmd_args(cmakelists, parent = 1),
        "-B",
        build_dir.as_output(),
        # Quoted as a whole so the `;` separating prefixes reaches cmake instead of the shell.
        cmd_args("\"-DCMAKE_PREFIX_PATH=", prefixes, "\"", delimiter = ""),
        "'-DFLAKE_PACKAGE={}'".format(ctx.attrs.package or ctx.label.name),
        "'-DFLAKE_VERSION={}'".format(ctx.attrs.version),
        "'-DFLAKE_TARGETS={}'".format(";".join(ctx.attrs.targets)),
        "'-DFLAKE_LANGUAGES={}'".format(";".join(ctx.attrs.languages)),
        cmd_args(cflags.as_output(), format = "-DFLAKE_CFLAGS_OUT=$PWD/{}"),
        cmd_args(libs.as_output(), format = "-DFLAKE_LIBS_OUT=$PWD/{}"),
        delimiter = " ",
    )

    # A package configured for a language expects the compiler that will consume it, so it comes
    # from the same cxx toolchain the dependents are built with.
    if [lang for lang in ctx.attrs.languages if lang != "NONE"]:
        toolchain = ctx.attrs._cxx_toolchain[CxxToolchainInfo]
        argv.add(cmd_args(toolchain.c_compiler_info.compiler, format = "-DCMAKE_C_COMPILER=$PWD/{}"))
        argv.add(cmd_args(toolchain.cxx_compiler_info.compiler, format = "-DCMAKE_CXX_COMPILER=$PWD/{}"))

    wrapper, _ = ctx.actions.write(
        "cmake_query.sh",
        cmd_args("set -eu", argv, delimiter = "\n"),
        allow_args = True,
        is_executable = True,
    )
    ctx.actions.run(
        cmd_args("/bin/sh", wrapper, hidden = [argv]),
        category = "cmake_query",
        local_only = True,
    )

    return [
        DefaultInfo(
            default_outputs = [cflags, libs],
            sub_targets = {
                "cflags": [DefaultInfo(default_output = cflags)],
                "libs": [DefaultInfo(default_output = libs)],
            },
            other_outputs = [build_dir],
        ),
    ]

__cmake_query = rule(
    impl = __cmake_query_impl,
    attrs = {
        "languages": attrs.list(attrs.string(), default = ["NONE"], doc = """
          languages to enable in the query project

          `["NONE"]` (the default) needs no compiler, which is all most packages require of the
          project that consumes them. Packages whose config file probes the compiler -- with
          `check_cxx_source_compiles()`, or `find_package(Threads)` -- need the real thing, e.g.
          `["CXX"]`, which is taken from the cxx toolchain.
        """),
        "package": attrs.option(attrs.string(), default = None, doc = """
          name to pass to `find_package()`

          (optional, default: same as `name`)
        """),
        "path": attrs.dep(doc = "a target whose output is the installed prefix to search, see `flake.store()`"),
        "prefixes": attrs.list(attrs.dep(), default = [], doc = """
          further prefixes to search

          A package whose config file calls `find_dependency()` needs its dependencies findable too,
          and in nix each of those is its own store path.
        """),
        "targets": attrs.list(attrs.string(), default = [], doc = """
          imported targets to take the flags from

          (optional, default: the targets `find_package()` defined in the package's own namespace)
        """),
        "version": attrs.string(default = "", doc = "version constraint to pass to `find_package()`"),
        "_cmake": attrs.exec_dep(providers = [RunInfo], default = "toolchains//:cmake"),
        "_cxx_toolchain": attrs.toolchain_dep(default = "toolchains//:cxx", providers = [CxxToolchainInfo]),
    },
    doc = "Configures a CMake project against a prefix and writes out the flags `find_package()` reports.",
)

def __flake_store(package: str, path: str, output: str = "out", name: str | None = None) -> str:
    """Declare (once) a target whose output is the store path of a nix package, and return its label.

    Unlike `flake.package()`, nothing inside the package is named: the whole prefix is the output,
    which is what a consumer that discovers the layout for itself -- `flake.prebuilt_cmake_library()`
    -- needs. Repeated calls for the same package reuse the one target, so the same store path can be
    handed to several rules without the caller having to name and share it.
    """
    if name == None:
        slug = "{}_{}_{}".format(path, package, output)
        for char in ["/", ":", ".", "-", "@", "#"]:
            slug = slug.replace(char, "_")
        name = "_flake_store__" + slug

    if not rule_exists(name):
        __flake_package(name = name, package = package, path = path, output = output)

    return ":" + name

def __prebuilt_cmake_library(
        name,
        path,
        package = None,
        version = "",
        targets = [],
        prefixes = [],
        languages = ["NONE"],
        exported_deps = [],
        visibility = ["PUBLIC"],
        **kwargs):
    """A `prebuilt_cxx_library` whose flags come from `find_package()` rather than from the caller.

    ## Examples

    ```starlark
    flake.prebuilt_cmake_library(
        name = "doctest",
        path = flake.store(package = "doctest", path = "root//:flake"),
    )
    ```

    This asks cmake where doctest's headers and libraries are, instead of the caller naming them
    with `files` and wiring them into a `prebuilt_cxx_library` by hand. The package name defaults to
    the target name; `package` overrides it, which is needed whenever the nix package and the cmake
    package are spelled differently (nix `hegel-cpp` provides cmake `hegel`).

    The flags land in two response files handed to the compiler and the linker, so they are
    discovered when the package is built rather than when the BUCK file is parsed.

    Details a package can force:

    * `targets` -- which imported targets to take the flags from, when the default (`pkg::pkg`, else
      everything in the `pkg::` namespace) picks the wrong variant.
    * `prefixes` -- extra `flake.store()` targets to search, for a config file that calls
      `find_dependency()` on a package living in its own store path.
    * `languages` -- languages to enable, for a config file that probes the compiler. Defaults to
      `["NONE"]`, which needs no compiler at all.
    """
    flags = name + "__cmake_flags"
    __cmake_query(
        name = flags,
        path = path,
        package = package or name,
        version = version,
        targets = targets,
        prefixes = prefixes,
        languages = languages,
    )

    native.prebuilt_cxx_library(
        name = name,
        exported_preprocessor_flags = ["@$(location :{}[cflags])".format(flags)],
        exported_linker_flags = ["@$(location :{}[libs])".format(flags)],
        exported_deps = exported_deps,
        visibility = visibility,
        **kwargs
    )

flake = struct(
    package = __flake_package,
    prebuilt_cmake_library = __prebuilt_cmake_library,
    store = __flake_store,
)
