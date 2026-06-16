/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>

#include <seastar/core/future.hh>
#include <seastar/core/temporary_buffer.hh>

#include "seastarx.hh"
#include "reader_permit.hh"
#include "sstables/shared_sstable.hh"
#include "sstables/sstable_datafile_position.hh"
#include "tracing/trace_state.hh"

namespace sstables {

class sstable_datafile_cursor {
public:
    class impl;
private:
    std::unique_ptr<impl> _impl;
public:
    explicit sstable_datafile_cursor(shared_sstable, reader_permit, tracing::trace_state_ptr,
            std::optional<uint32_t> digest = std::nullopt);
    ~sstable_datafile_cursor();

    void seek(sstable_datafile_position);
    future<temporary_buffer<char>> read_forwards(size_t n);
    future<temporary_buffer<char>> read(sstable_datafile_position start, sstable_datafile_position end);
    sstable_datafile_position compute_relative_position(ssize_t offset_from_current);
    // The number of decompressed bytes between `a` and `b` (i.e. b - a). Both
    // positions must already be reachable through the cursor's metadata cache
    // (the same precondition as compute_relative_position); this is synchronous
    // and never reads from the file.
    int64_t subtract_positions(sstable_datafile_position b, sstable_datafile_position a);
    // Advance `from` forward by `n` bytes, returning the resulting position.
    // Unlike compute_relative_position, this may read from the file to discover
    // the chunks it crosses, so it can move past chunks the cursor has not seen
    // yet; it also primes the cursor's caches for those chunks.
    future<sstable_datafile_position> skip_forwards(sstable_datafile_position from, size_t n);
    void drop_caches_after(sstable_datafile_position);
    void drop_caches_before(sstable_datafile_position);
    future<> close();
};

} // namespace sstables
