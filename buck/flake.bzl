# HOW TO USE THIS MODULE:
#
#    load("//buck:flake.bzl", "flake")
#
#    flake.package(name = "pkg", path = "path/to/flake/dir", ...)
#    flake.prebuilt_pkgconfig_library(name = "lib", path = "path/to/flake/dir")

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

# The query runs inside `nix develop`, in a shell whose `buildInputs` hold the package (see the
# `pkgconfig-<name>` shells in flake.nix). Entering that shell runs nixpkgs' own setup hooks, which
# is what assembles `PKG_CONFIG_PATH`.
#
# That indirection is the point. A `.pc` file's `Requires:` line names further modules, and
# pkg-config resolves them only among the `.pc` files on its search path; on an ordinary system one
# prefix holds them all, while in nix every package is its own store path. Reconstructing that path
# here would mean reimplementing stdenv's offset-parameterised walk over the propagated inputs, plus
# the env hooks any package is free to ship -- an approximation that agrees until it doesn't. Asking
# nix for the environment instead leaves that to the code that defines it.
def __pkg_config_impl(ctx: AnalysisContext) -> list[Provider]:
    cflags = ctx.actions.declare_output("cflags")
    libs = ctx.actions.declare_output("libs")

    package = ctx.attrs.package or ctx.label.name
    modules = ctx.attrs.module or package
    if ctx.attrs.version:
        modules += " " + ctx.attrs.version

    # `--libs` reports where the libraries are, not that the loader should look there, so every `-L`
    # grows a matching rpath entry. Store paths are absolute and immutable, so a binary linked this
    # way runs anywhere on the machine. `-I` becomes `-isystem` so that warnings in third-party
    # headers are not the consumer's problem.
    rpaths = "sed -E 's#(^| )-L([^ ]+)#\\1-L\\2 -Wl,-rpath,\\2#g'"
    system = "sed -E 's#(^| )-I#\\1-isystem#g'" if ctx.attrs.system_includes else "cat"

    options = "--print-errors --static" if ctx.attrs.static else "--print-errors"

    # pkg-config is captured into a variable rather than piped straight into `sed`: a pipeline
    # reports the status of its *last* command, so a missing module would leave `set -e` none the
    # wiser and write an empty file that only fails much later, at the compile. An assignment from a
    # command substitution does fail the script.
    script = cmd_args(
        "set -eu",
        "cflags=$(pkg-config {} --cflags '{}')".format(options, modules),
        "libs=$(pkg-config {} --libs '{}')".format(options, modules),
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

    # Written out as a file rather than passed as `--command sh -c '...'`, which would mean quoting
    # the sed expressions inside an already quoted argument.
    query, _ = ctx.actions.write(
        "pkg_config.sh",
        script,
        allow_args = True,
        is_executable = True,
    )

    shell = ctx.attrs.shell or "pkgconfig-" + package
    ctx.actions.run(
        cmd_args(
            "env",
            "--",  # see `__nix_build`
            "nix",
            "--extra-experimental-features",
            "nix-command flakes",
            "develop",
            cmd_args(ctx.attrs.path, format = "path:{}#" + shell),
            "--command",
            "/bin/sh",
            query,
            hidden = [script],
        ),
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
        "module": attrs.option(attrs.string(), default = None, doc = """
          name of the pkg-config module

          (optional, default: same as `package`, i.e. the nix package's own name)
        """),
        "package": attrs.option(attrs.string(), default = None, doc = """
          name of the flake output holding the package

          It selects the `pkgconfig-<package>` shell to ask, so it is the nix name rather than the
          pkg-config one; use `module` when the two differ.

          (optional, default: same as `name`)
        """),
        "path": attrs.source(allow_directory = True, doc = "the path to the flake"),
        "shell": attrs.option(attrs.string(), default = None, doc = """
          name of the devShell to ask instead of `pkgconfig-<package>`

          For a module that needs more in scope than its own package -- one whose `Requires:` names
          something the package fails to propagate, say -- declare a shell with both and name it
          here.
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
    },
    doc = "Asks pkg-config, from inside the package's own nix shell, for the flags of a module.",
)

def __prebuilt_pkgconfig_library(
        name,
        path,
        package = None,
        module = None,
        version = "",
        shell = None,
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
        path = "root//:flake",
    )
    ```

    This asks the package's own `.pc` file where its headers and libraries are, instead of the
    caller naming them with `files` and wiring them into a `prebuilt_cxx_library` by hand. The
    package name defaults to the target name, and the question is put to the package's
    `pkgconfig-<package>` shell, which flake.nix derives from the flake's package set -- so a
    package that is already a flake output needs nothing further to be consumable here.

    The flags land in two response files handed to the compiler and the linker, so they are
    discovered when the package is built rather than when the BUCK file is parsed. This is the same
    shape as `@prelude//third-party:pkgconfig.bzl`, except that the environment pkg-config runs in
    is built by nix from the package itself rather than being whatever the ambient
    `PKG_CONFIG_PATH` happens to hold.

    Details a package can force:

    * `module` -- the pkg-config module name, when it differs from the nix package name.
    * `shell` -- a devShell to ask instead of `pkgconfig-<package>`, for a module needing more than
      its own package in scope.
    * `static` -- take `Libs.private` into account, for a static link.
    * `version` -- a constraint such as `">= 1.2"`, which fails the build if unmet.
    """
    flags = name + "__pkg_config_flags"
    __pkg_config(
        name = flags,
        path = path,
        package = package or name,
        module = module,
        version = version,
        shell = shell,
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
)
