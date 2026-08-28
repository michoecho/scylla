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
        -- just --justfile {{justfile_directory()}}/justfile compdb {{targets}}

# The socket below needs its parent to exist; process-compose makes the log
# directory itself.
_cache:
    mkdir -p {{justfile_directory()}}/.cache

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
# Bring up everything in ./process-compose.yaml.
dev: _cache
    tools/sandbox --whole-sys env PC_LOG_LEVEL=error process-compose up \
        --config {{justfile_directory()}}/process-compose.yaml \
        --use-uds --unix-socket {{justfile_directory()}}/.cache/process-compose.sock

reindeer:
    reindeer buckify

build targets="//...":
    buck2 build {{targets}}

test targets="//...":
    buck2 test {{targets}}

snapshot-update targets="//...":
    buck2 test -c snapshot.update=1 {{targets}}

nativelink:
    nativelink buck/basic_cas.json5