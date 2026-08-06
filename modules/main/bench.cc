#include <cstdint>
#include <map>
#include <unordered_map>

#define ANKERL_NANOBENCH_IMPLEMENT
#include <nanobench.h>

#include "main/bench.h"

// Placeholder benchmark: lookup cost of std::map (balanced tree) vs
// std::unordered_map (hash table) over the same set of keys. A normal test run
// performs one iteration to catch bitrot; the Benchmark preset performs the
// full measurement.
BENCHMARK("map vs unordered_map lookup") {
    constexpr std::uint64_t n = 1000;

    std::map<std::uint64_t, std::uint64_t> ordered;
    std::unordered_map<std::uint64_t, std::uint64_t> unordered;
    for (std::uint64_t i = 0; i < n; ++i) {
        ordered[i] = i;
        unordered[i] = i;
    }

    benchmark::Bench bench;
    bench.title("map vs unordered_map lookup");

    std::uint64_t key = 0;
    bench.run("std::map", [&] {
        key = (key + 1) % n;
        ankerl::nanobench::doNotOptimizeAway(ordered.find(key));
    });
    bench.run("std::unordered_map", [&] {
        key = (key + 1) % n;
        ankerl::nanobench::doNotOptimizeAway(unordered.find(key));
    });

    CHECK(ordered.size() == unordered.size());
}
