# Project command runner. `just` on its own lists what's here.
#
# Recipes always run from the repo root, whatever directory you invoke them
# from, so relative paths below are repo-relative.

# Show the available recipes.
default:
    @just --list

# Regenerate compile_commands.json and link it into the repo root.
compdb targets="//...":
    #!/usr/bin/env bash
    set -euo pipefail
    # Captured before linking so a failed BXL stops here, rather than pointing
    # the symlink at nothing and leaving the stale database in place.
    db="$(buck2 bxl prelude//cxx/tools/compilation_database.bxl:generate -- --targets {{targets}})"
    ln -sfn "$db" {{justfile_directory()}}/compile_commands.json

# Sources are listed explicitly in add_module (no globs), so the build files
# filtered below are the only inputs that can change the compilation database.
#
# Rebuild compile_commands.json whenever a build file changes.
watch-compdb targets="//...":
    watchexec --debounce 500ms --project-origin {{justfile_directory()}} \
        --filter 'BUCK' --filter '**/*.bzl' --filter 'PACKAGE' --filter '.buckconfig' \
        -- just compdb {{targets}}

# The socket below needs its parent to exist; process-compose makes the log
# directory itself.
_cache:
    mkdir -p {{justfile_directory()}}/.cache

# Run process-compose on the project's unix socket
process-compose +cmd:
    env PC_LOG_LEVEL=error process-compose --use-uds --unix-socket {{justfile_directory()}}/.cache/process-compose.sock {{cmd}}

# Long-running dev processes -- local CAS and the compdb watcher. F10 (or
# Ctrl-C) stops the lot.
#
# Talks over a unix socket at a fixed path rather than process-compose's
# default TCP :8080, which is a port worth not squatting on. A second terminal
# can drive the same session with that path:
#
#   process-compose attach -U -u .cache/process-compose.sock
#   process-compose process logs compdb -U -u .cache/process-compose.sock
#
# Start a dev environment, in a sandbox, using process-compose
dev: _cache
    just sandbox just process-compose --config {{justfile_directory()}}/process-compose.yaml

# Regenerate buck2 targets for Rust dependencies (in third-party/rust) from Cargo.toml
reindeer:
    reindeer buckify

# Build with buck2
build targets="//...":
    buck2 build {{targets}}

# Test with buck2
test targets="//...":
    buck2 test {{targets}}

# Run the given targets (snapshot tests), letting them update outdated snapshots
snapshot-update targets="//...":
    buck2 test -c snapshot.update=1 {{targets}}

# Run a build server (for caching) for buck2
nativelink:
    nativelink buck/basic_cas.json5

# Run the command in a sandbox, (mainly to limit LLM blast radius).
sandbox +cmd:
    tools/sandbox --whole-sys {{cmd}}

extension:
    tools/vscode-buck2/build-and-install
