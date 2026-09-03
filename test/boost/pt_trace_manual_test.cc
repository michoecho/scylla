/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <cstdint>

#include <boost/test/unit_test.hpp>
#include <seastar/testing/test_case.hh>

namespace {

__attribute__((noinline)) std::uint64_t trace_work() {
    volatile std::uint64_t value = 0x123456789abcdef0ULL;
    for (std::uint64_t i = 0; i < 20'000'000; ++i) {
        value = value * 6364136223846793005ULL + i;
    }
    return value;
}

} // namespace

SEASTAR_TEST_CASE(pt_trace_manual_workload) {
    BOOST_REQUIRE_NE(trace_work(), 0);
    return seastar::make_ready_future<>();
}
