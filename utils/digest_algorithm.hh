/*
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cstdint>

namespace query {

enum class digest_algorithm : uint8_t {
    none = 0,  // digest not required
    xxHash = 3, // default algorithm
    // Like xxHash, but the digest of a partition also covers whether the
    // partition has a live static row, even when the query selects no static
    // column. Without it, a replica which returns a static-only row and one
    // which returns nothing from the partition have the same digest, so the
    // coordinator does not notice that they disagree. The READ_FRONTIERS
    // cluster feature gates it, because a digest is only comparable with
    // digests of the same algorithm.
    xxHash_with_static_row = 4,
};

// Whether `algo` covers a partition's static-row liveness even when the query
// selects no static column.
inline bool digests_static_row_liveness(digest_algorithm algo) {
    return algo == digest_algorithm::xxHash_with_static_row;
}

}
