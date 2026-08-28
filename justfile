# Project command runner. `just` on its own lists what's here.
#
# Recipes always run from the repo root, whatever directory you invoke them
# from, so relative paths below are repo-relative.

# Show the available recipes.
default:
    @just --list

# Regenerate compile_commands.json and link it into the repo root.
compdb targets="//...":
    ln -sf "$(buck2 bxl prelude//cxx/tools/compilation_database.bxl:generate -- --targets {{targets}})" {{justfile_directory()}}

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