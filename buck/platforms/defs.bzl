# An execution platform that permits remote cache access.
#
# The prelude's own `prelude//platforms:default` hardcodes
# `remote_enabled = False` in its CommandExecutorConfig. That flag, not the
# `[buck2_re_client]` section of .buckconfig, is what decides whether buck2
# talks to a remote service at all: with it false, buck2 connects to nativelink
# and then never issues a FindMissingBlobs or GetActionResult, so every build
# is purely local and the cache stays empty no matter how it is configured.
#
# This is a copy of the prelude's execution_platform rule with that one flag
# flipped, plus the properties nativelink's scheduler needs to match an action
# to its worker.

load("@prelude//cfg/exec_platform:marker.bzl", "get_exec_platform_marker")

def _execution_platform_impl(ctx: AnalysisContext) -> list[Provider]:
    constraints = dict()
    constraints.update(ctx.attrs.cpu_configuration[ConfigurationInfo].constraints)
    constraints.update(ctx.attrs.os_configuration[ConfigurationInfo].constraints)
    cfg = ConfigurationInfo(constraints = constraints, values = {})

    name = ctx.label.raw_target()
    platform = ExecutionPlatformInfo(
        label = name,
        configuration = cfg,
        executor_config = CommandExecutorConfig(
            local_enabled = True,
            remote_enabled = True,
            # Actions still run locally; only the cache is remote. Remote
            # *execution* would additionally require the action's whole input
            # closure to be uploaded, and these actions reference absolute
            # /nix/store paths that a worker would have to already have -- true
            # for this single-machine setup, but not something to imply here.
            # `remote_cache_enabled` is what makes the AC/CAS lookups happen.
            remote_cache_enabled = True,
            remote_execution_use_case = "buck2-default",
            # Matched against the `platform_properties` the worker advertises
            # in basic_cas.json5; the scheduler rejects actions whose
            # properties it cannot satisfy.
            remote_execution_properties = {
                "OSFamily": "Linux",
                "container-image": "",
            },
            use_windows_path_separators = ctx.attrs.use_windows_path_separators,
        ),
    )

    return [
        DefaultInfo(),
        platform,
        PlatformInfo(label = str(name), configuration = cfg),
        ExecutionPlatformRegistrationInfo(
            platforms = [platform],
            exec_marker_constraint = get_exec_platform_marker(),
        ),
    ]

execution_platform = rule(
    impl = _execution_platform_impl,
    attrs = {
        "cpu_configuration": attrs.dep(providers = [ConfigurationInfo]),
        "os_configuration": attrs.dep(providers = [ConfigurationInfo]),
        "use_windows_path_separators": attrs.bool(),
    },
)
