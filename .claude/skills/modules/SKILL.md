---
name: modules
description: Add, split, or wire up a module under modules/ — a library that owns its tests, with dependency-ordered test stamps. Use when asked to add a new module, add sources/tests/dependencies to an existing one, publish or hide a header, run one module's tests, or fix "not a target" / missing-test-case / include-not-found errors from the module build.
---

# Working with modules

A module is a directory under `modules/` holding a library, its tests, and its
public headers. `add_module()` (`cmake/Module.cmake`) generates from it:

| Target | What it is |
|---|---|
| `<name>` | the library (production **and** test sources) |
| `<name>_test` | executable: that library + doctest `main()` |
| `<name>_tested` | stamp `<name>.passed` — this module's tests passed |
| `<name>_tested_deep` | stamp `<name>.deep.passed` — same run, ordered after every dependency's deep stamp |

Stamps are files, so Ninja skips re-running a suite whose sources and dependency
stamps are unchanged. A failing test leaves no stamp and stops everything above it.

## Add a module

```sh
mkdir -p modules/module_x/include/module_x
```

```
modules/module_x/
  CMakeLists.txt
  x.cc                      # sources
  x_test.cc                 # tests — just SOURCES, no separate list
  detail.h                  # private header — this module only
  include/module_x/x.h      # public header — dependees #include "module_x/x.h"
```

`modules/module_x/CMakeLists.txt`:

```cmake
add_module(module_x
    SOURCES x.cc x_test.cc)

target_link_module(module_x PUBLIC module_a)      # module deps
target_link_libraries(module_x PRIVATE CLI11::CLI11)  # everything else
```

Then in `modules/CMakeLists.txt` — order doesn't matter:

```cmake
add_subdirectory(module_x)
```

### `add_module` arguments

`add_module` is `add_library` plus extras. It takes no dependencies; declare
those afterwards with `target_link_module` / `target_link_libraries`.

| Argument | Use |
|---|---|
| `SOURCES` | all `.cc`, production and test alike |
| `TYPE` | `STATIC` or `SHARED`; defaults to `BUILD_SHARED_LIBS` |
| `TEST_PROPERTIES` | extra CTest properties on discovered cases (e.g. `TIMEOUT 10`) |

### Linking

```cmake
target_link_module(module_x PRIVATE module_a)   # PUBLIC / PRIVATE / INTERFACE
```

Use it for **module** deps. On top of the plain link it adds:
- test ordering — `module_x`'s tests run only after `module_a`'s have passed
- whole-archive, so a static dep's cases survive into `module_x_test --all`

Plain `target_link_libraries(module_x PRIVATE module_a)` on a module is
allowed and links correctly — it just skips those two extras. Fine while
developing; use `target_link_module` once it settles.

Per-target flags go after the call, on `<name>`:

```cmake
target_compile_options(module_x PRIVATE -Wall -Wextra)
target_compile_definitions(module_x PUBLIC SOME_MACRO)
```

## Headers

```cpp
#include "module_a/a.h"   // dependency's public header — must be linked first
#include "detail.h"       // own private header, unprefixed
```

- Publish: put it in `include/<module>/`.
- Hide: put it directly in the module directory.
- Including a non-dependency fails at the `#include`, not at link.

## Test file shape

```cpp
#include <doctest/doctest.h>

#include "module_a/a.h"

TEST_CASE("module_a::twice doubles") {
    CHECK(module_a::twice(3) == 6);
}
```

## Run

```sh
cmake --build --preset Debug --target module_x_tested       # own tests only
cmake --build --preset Debug --target module_x_tested_deep  # deps' tests first

out/build/Debug/modules/module_x/module_x_test              # own cases
out/build/Debug/modules/module_x/module_x_test --all        # + all linked deps' cases
out/build/Debug/modules/module_x/module_x_test --test-case="module_a::twice*"
out/build/Debug/modules/module_x/module_x_test --list-test-cases

ctest --preset DebugTest -L module.module_x                 # by label
ctest --preset DebugTest -R "module_x:::"                   # by name prefix
```

A test binary defaults to only its own module's cases — the runner filters on
`--source-file=*<module dir>/*`. `--all` opts out. Benchmarks and fuzz targets
run from it too: `module_x_test bench`, `module_x_test fuzz`.

## Force a re-run

Stamps are cached by mtime:

```sh
rm out/build/Debug/module-stamps/module_x.*.passed
```

## Troubleshooting

| Symptom | Fix |
|---|---|
| `'<x>' is not a target` from `target_link_module` | typo, or `add_subdirectory(<x>)` hasn't run yet — the dep must exist by the time it's linked |
| `#include "module_a/a.h"` not found | `module_a` not linked into this module |
| Test case never runs / missing from `--list-test-cases` | file not in `SOURCES`, or case is outside the module dir (it is filtered out — check with `--all`) |
| Dep's cases missing from `--all` | dep linked with plain `target_link_libraries`; use `target_link_module` |
| Dep's tests don't run first | same — plain link carries no ordering |
| Dependency tests re-run every build | you named `<dep>_tested_deep` where `<dep>_tested` was meant |
| Suite doesn't re-run after editing | touched file isn't in `SOURCES`; delete the stamp |
| `hidden symbol ... referenced by DSO` | a target links doctest without `DOCTEST_CONFIG_IMPLEMENTATION_IN_DLL` — build it via `add_module` |

## Gotchas

- Never put `main()` in a module — `add_module` compiles sources into a library.
  The shipping `main()` lives in `src/main.cc`.
- Test sources go in the **library**, so `--whole-archive` handling for static
  modules is what keeps registrations alive. Handled by `add_module`; don't
  hand-roll link flags for module archives.
