# HOW TO USE THIS MODULE:
#
#    load("//buck:flake.bzl", "flake")
#
#    flake.package(name = "pkg", path = "path/to/flake/dir", ...)

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

flake = struct(
    package = __flake_package,
)
