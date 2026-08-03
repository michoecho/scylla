#pragma once

#include "doctest/doctest.h"

// BENCH_SUITE, the doctest suite every benchmark lives in, comes from the
// shared runner (cmake/module_run.h): it is that dispatcher which scopes a
// `bench` run to the suite, and BENCHMARK() below is what puts cases into it.
#include "module_run.h"

// Define a benchmark. It registers as a doctest test case so listing and
// filtering work, but lives in the BENCH_SUITE suite and is marked skip() so a
// normal test run (ctest / `cpp_template test`) never executes it. Run them
// with `cpp_template bench`, which flips --no-skip and scopes to the suite.
#define BENCHMARK(name) \
    TEST_CASE(name * doctest::test_suite(BENCH_SUITE) * doctest::skip())
