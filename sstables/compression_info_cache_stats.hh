/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cstdint>

struct compression_info_cache_stats {
    uint64_t hits = 0; // Number of times a bucket was found ready
    uint64_t misses = 0; // Number of times a bucket was not found
    uint64_t blocks = 0; // Number of times a bucket was not ready (>= misses)
    uint64_t evictions = 0; // Number of times a bucket was evicted
    uint64_t populations = 0; // Number of times a bucket was inserted
    uint64_t used_bytes = 0; // Number of bytes the cached buckets occupy in memory
};
