#pragma once

#include "doctest/doctest.h"

// Name of the doctest test suite that all benchmarks live in. The `bench`
// subcommand (see main.cc) targets this suite, and BENCHMARK() puts every
// benchmark into it.
#define BENCH_SUITE "bench"

// Define a benchmark. It registers as a doctest test case so listing and
// filtering work, but lives in the BENCH_SUITE suite and is marked skip() so a
// normal test run (ctest / `cpp_template test`) never executes it. Run them
// with `cpp_template bench`, which flips --no-skip and scopes to the suite.
#define BENCHMARK(name) \
    TEST_CASE(name * doctest::test_suite(BENCH_SUITE) * doctest::skip())
