---
name: modules
description: Add, split, or wire up a module under modules/ — a library that owns its tests. Use when asked to add a module, sources, dependencies, headers, or to fix missing Buck2 targets and test cases.
---

# Working with modules

A module is a directory under `modules/` holding a library, its tests, and its
public headers. `add_module()` in `buck/module.bzl` creates the library and its
matching doctest executable:

| Target | What it is |
|---|---|
| `<name>` | the library, including production and test sources |
| `<name>_test` | executable: that library plus the shared doctest runner |

Test sources live in the library. The test executable links the library whole
so static initializers register every doctest case. By default it filters to
the module's own source directory; `--all` includes linked dependencies.

## Add a module

```sh
mkdir -p modules/module_x/include/module_x
```

Add `modules/module_x/BUCK`:

```python
load("//buck:module.bzl", "add_module")

add_module(
    name = "module_x",
    srcs = ["x.cc", "x_test.cc"],
    exported_headers = {
        "module_x/x.h": "include/module_x/x.h",
    },
    deps = ["//modules/module_a:module_a"],
    compiler_flags = ["-Wall", "-Wextra"],
    module_source_dir = "modules/module_x",
)
```

Use `deps` for other modules and libraries. Add a module's own test sources to
`srcs`; there is no separate test target declaration. Publish headers through
`exported_headers`, and keep private headers in the module directory.

## Test file shape

```cpp
#include <doctest/doctest.h>

#include "module_a/a.h"

TEST_CASE("module_a::twice doubles") {
    CHECK(module_a::twice(3) == 6);
}
```

## Build and run

```sh
buck2 build //modules/module_x:module_x_test
buck2 test //modules/module_x:module_x_test
buck2 run //modules/module_x:module_x_test -- --all
buck2 run //modules/module_x:module_x_test -- --test-case="module_x*"
buck2 run //modules/module_x:module_x_test -- --list-test-cases
```

The `test` subcommand is optional for module runners: bare doctest flags are
treated as a test run. Benchmarks are ordinary doctest cases in the `bench`
suite and can be scoped with `buck2 run ... -- bench`.

Randomized tests are ordinary cases whose engine is selected by `TEST_RNG`; see
`modules/test_rng`.

## Benchmarks

Include `main/bench.h` and declare a benchmark with `BENCHMARK()`:

```cpp
#include "main/bench.h"

BENCHMARK("module_x operation") {
    benchmark::Bench bench;
    bench.title("module_x operation");
    bench.run("operation", [&] { /* exercise the operation */ });
}
```

Normal tests exercise the operation as a smoke test. Set `BENCHMARK=1` when
running the `bench` subcommand to enable full nanobench measurements.
