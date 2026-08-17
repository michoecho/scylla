# HOW TO USE THIS MODULE:
#
#    load("//buck:flake.bzl", "flake")
#
#    flake.package(name = "pkg", path = "path/to/flake/dir", ...)
#    flake.prebuilt_pkgconfig_library(name = "lib", path = flake.store(package = "lib", path = "path/to/flake/dir"))

load("@prelude//:prelude.bzl", "native")
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
## pkg-config package discovery

# A `.pc` file's `Requires:` line names further modules, and pkg-config resolves them only among the
# `.pc` files on its search path. On an ordinary system one prefix holds them all; in nix every
# package is its own store path, so the path has to be assembled -- and the packages a prefix needs
# are exactly the ones nix recorded in `nix-support/propagated-build-inputs` when it was built.
#
# That is the same file nixpkgs' own setup hooks read to decide what a dependent gets to see, so
# following it here reproduces the search path a nix build of a dependent would have had, with
# nothing for the caller to restate.
__PKG_CONFIG_PATH = """
pkg_config_path() {
    todo=$*
    seen=
    while [ -n "$todo" ]; do
        set -- $todo
        prefix=$1
        shift
        todo=$*

        case " $seen " in
            *" $prefix "*) continue ;;
        esac
        seen="$seen $prefix"

        for file in propagated-build-inputs propagated-native-build-inputs; do
            if [ -f "$prefix/nix-support/$file" ]; then
                todo="$todo $(cat "$prefix/nix-support/$file")"
            fi
        done
    done

    path=
    for prefix in $seen; do
        for dir in lib/pkgconfig share/pkgconfig lib64/pkgconfig; do
            if [ -d "$prefix/$dir" ]; then
                path="$path:$prefix/$dir"
            fi
        done
    done
    printf '%s' "${path#:}"
}
"""

def __pkg_config_impl(ctx: AnalysisContext) -> list[Provider]:
    cflags = ctx.actions.declare_output("cflags")
    libs = ctx.actions.declare_output("libs")

    # The search path is computed at build time from the packages themselves, see
    # `__PKG_CONFIG_PATH`. `readlink` resolves the out-links, since a package names its own prefix
    # and has to be read from where it really lives.
    roots = cmd_args(
        [
            cmd_args(dep[DefaultInfo].default_outputs[0], format = "$(readlink -f {})")
            for dep in [ctx.attrs.path] + ctx.attrs.prefixes
        ],
        delimiter = " ",
    )

    modules = ctx.attrs.package or ctx.label.name
    if ctx.attrs.version:
        modules += " " + ctx.attrs.version

    # `--libs` reports where the libraries are, not that the loader should look there, so every `-L`
    # grows a matching rpath entry. Store paths are absolute and immutable, so a binary linked this
    # way runs anywhere on the machine. `-I` becomes `-isystem` so that warnings in third-party
    # headers are not the consumer's problem.
    rpaths = "sed -E 's#(^| )-L([^ ]+)#\\1-L\\2 -Wl,-rpath,\\2#g'"
    system = "sed -E 's#(^| )-I#\\1-isystem#g'" if ctx.attrs.system_includes else "cat"

    pkg_config = ctx.attrs._pkg_config[RunInfo]
    options = " --print-errors --static" if ctx.attrs.static else " --print-errors"

    # pkg-config is captured into a variable rather than piped straight into `sed`: a pipeline
    # reports the status of its *last* command, so a missing module would leave `set -e` none the
    # wiser and write an empty file that only fails much later, at the compile. An assignment from a
    # command substitution does fail the script.
    script = cmd_args(
        "set -eu",
        __PKG_CONFIG_PATH,
        cmd_args("export PKG_CONFIG_PATH=\"$(pkg_config_path ", roots, ")\"", delimiter = ""),
        cmd_args("cflags=$(", pkg_config, "{} --cflags '{}')".format(options, modules), delimiter = ""),
        cmd_args("libs=$(", pkg_config, "{} --libs '{}')".format(options, modules), delimiter = ""),
        cmd_args(
            "printf '%s\\n' \"$cflags\" |",
            system,
            ">",
            cmd_args(cflags.as_output(), format = "$PWD/{}"),
            delimiter = " ",
        ),
        cmd_args(
            "printf '%s\\n' \"$libs\" |",
            rpaths,
            ">",
            cmd_args(libs.as_output(), format = "$PWD/{}"),
            delimiter = " ",
        ),
        delimiter = "\n",
    )

    wrapper, _ = ctx.actions.write(
        "pkg_config.sh",
        script,
        allow_args = True,
        is_executable = True,
    )
    ctx.actions.run(
        cmd_args("/bin/sh", wrapper, hidden = [script]),
        category = "pkg_config",
        local_only = True,
    )

    return [
        DefaultInfo(
            default_outputs = [cflags, libs],
            sub_targets = {
                "cflags": [DefaultInfo(default_output = cflags)],
                "libs": [DefaultInfo(default_output = libs)],
            },
        ),
    ]

__pkg_config = rule(
    impl = __pkg_config_impl,
    attrs = {
        "package": attrs.option(attrs.string(), default = None, doc = """
          name of the pkg-config module

          (optional, default: same as `name`)
        """),
        "path": attrs.dep(doc = "a target whose output is the installed prefix to search, see `flake.store()`"),
        "prefixes": attrs.list(attrs.dep(), default = [], doc = """
          further prefixes to search

          Rarely needed: the packages `path` propagates are followed on their own. This is for a
          module that is required but not propagated, which is a packaging bug on the nix side more
          often than not.
        """),
        "static": attrs.bool(default = False, doc = """
          ask for the flags of a static link

          This adds the `Libs.private` of every module, which are the libraries only a static link
          has to name for itself.
        """),
        "system_includes": attrs.bool(default = True, doc = "report include directories as `-isystem` rather than `-I`"),
        "version": attrs.string(default = "", doc = """
          version constraint on the module, e.g. `">= 1.2"`

          (optional, default: any version)
        """),
        "_pkg_config": attrs.exec_dep(providers = [RunInfo], default = "toolchains//:pkg_config"),
    },
    doc = "Asks pkg-config for the flags of a module inside a prefix, and writes them out as response files.",
)

def __flake_store(package: str, path: str, output: str = "out", name: str | None = None) -> str:
    """Declare (once) a target whose output is the store path of a nix package, and return its label.

    Unlike `flake.package()`, nothing inside the package is named: the whole prefix is the output,
    which is what a consumer that discovers the layout for itself --
    `flake.prebuilt_pkgconfig_library()` -- needs. Repeated calls for the same package reuse the one
    target, so the same store path can be handed to several rules without the caller having to name
    and share it.
    """
    if name == None:
        slug = "{}_{}_{}".format(path, package, output)
        for char in ["/", ":", ".", "-", "@", "#"]:
            slug = slug.replace(char, "_")
        name = "_flake_store__" + slug

    if not rule_exists(name):
        __flake_package(name = name, package = package, path = path, output = output)

    return ":" + name

def __prebuilt_pkgconfig_library(
        name,
        path,
        package = None,
        version = "",
        prefixes = [],
        static = False,
        system_includes = True,
        exported_deps = [],
        visibility = ["PUBLIC"],
        **kwargs):
    """A `prebuilt_cxx_library` whose flags come from pkg-config rather than from the caller.

    ## Examples

    ```starlark
    flake.prebuilt_pkgconfig_library(
        name = "doctest",
        path = flake.store(package = "doctest", path = "root//:flake"),
    )
    ```

    This asks the package's own `.pc` file where its headers and libraries are, instead of the
    caller naming them with `files` and wiring them into a `prebuilt_cxx_library` by hand. The
    module name defaults to the target name; `package` overrides it, for when the nix package and
    the pkg-config module are spelled differently.

    The flags land in two response files handed to the compiler and the linker, so they are
    discovered when the package is built rather than when the BUCK file is parsed. This is the same
    shape as `@prelude//third-party:pkgconfig.bzl`, except that the prefix to search is an input of
    the rule rather than whatever `PKG_CONFIG_PATH` happens to hold.

    Details a package can force:

    * `prefixes` -- extra `flake.store()` targets to search, for a required module that the package
      does not propagate. The propagated ones are found without being named.
    * `static` -- take `Libs.private` into account, for a static link.
    * `version` -- a constraint such as `">= 1.2"`, which fails the build if unmet.
    """
    flags = name + "__pkg_config_flags"
    __pkg_config(
        name = flags,
        path = path,
        package = package or name,
        version = version,
        prefixes = prefixes,
        static = static,
        system_includes = system_includes,
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
    prebuilt_pkgconfig_library = __prebuilt_pkgconfig_library,
    store = __flake_store,
)
