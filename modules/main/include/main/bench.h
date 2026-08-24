#pragma once

#include <cstdlib>
#include <memory>
#include <string>
#include <utility>

#include <nanobench.h>

#include "doctest/doctest.h"

// BENCH_SUITE, the doctest suite every benchmark lives in, comes from the
// shared runner (buck/module_run.h): it is that dispatcher which scopes a
// `bench` run to the suite, and BENCHMARK() below is what puts cases into it.
#include "module_run.h"

namespace benchmark {

// Avoid constructing nanobench in ordinary test runs: its setup consults the
// operating system and is unnecessary when all we need is a functionality
// smoke test. Add forwarding methods here only as benchmarks need them.
class Bench {
   public:
    Bench() {
        if (std::getenv("BENCHMARK") != nullptr)
            bench_ = std::make_unique<ankerl::nanobench::Bench>();
    }

    Bench& title(const std::string& title) {
        if (bench_)
            bench_->title(title);
        return *this;
    }

    template <typename Op>
    Bench& run(const std::string& name, Op&& op) {
        if (bench_)
            bench_->run(name, std::forward<Op>(op));
        else
            std::forward<Op>(op)();
        return *this;
    }

   private:
    std::unique_ptr<ankerl::nanobench::Bench> bench_;
};

}  // namespace benchmark

// Define a benchmark. It is an ordinary doctest test case in BENCH_SUITE, so a
// normal test run exercises its functionality through benchmark::Bench without
// constructing nanobench. BENCHMARK=1 enables full measurement. The `bench`
// subcommand scopes a run to this suite; the caller supplies the BENCHMARK
// environment variable when full measurements are wanted.
#define BENCHMARK(name) \
    TEST_CASE(name * doctest::test_suite(BENCH_SUITE))
