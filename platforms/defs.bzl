def _platforms(ctx):
    # Keep the host OS/CPU constraints from Buck's default platform. The
    # Nix-backed toolchains use those constraints to select their flake
    # package system (for example, x86_64-linux).
    configuration = ctx.attrs.base[ExecutionPlatformInfo].configuration

    platform = ExecutionPlatformInfo(
        label = ctx.label.raw_target(),
        configuration = configuration,
        executor_config = CommandExecutorConfig(
            local_enabled = True,
            remote_enabled = True,
            remote_cache_enabled = True,
            use_limited_hybrid = True,
            allow_cache_uploads = True,
            # These match the local worker in buck/basic_cas.json5.  In
            # particular, this worker does not run a container image.
            remote_execution_properties = {
                "OSFamily": "",
                "container-image": "",
                "ISA": "x86-64",
            },
            remote_execution_use_case = "buck2-default",
            remote_output_paths = "output_paths",
        ),
    )

    return [
        DefaultInfo(),
        ExecutionPlatformRegistrationInfo(platforms = [platform]),
    ]


platforms = rule(
    attrs = {
        "base": attrs.dep(providers = [ExecutionPlatformInfo]),
    },
    impl = _platforms,
)
