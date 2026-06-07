/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cstddef>
#include <memory>

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
    explicit sstable_datafile_cursor(shared_sstable, reader_permit, tracing::trace_state_ptr);
    ~sstable_datafile_cursor();

    void seek(sstable_datafile_position);
    future<temporary_buffer<char>> read_forwards(size_t n);
    future<temporary_buffer<char>> read_backwards(size_t n);
    sstable_datafile_position compute_relative_position(ssize_t offset_from_current);
    void drop_caches_after(sstable_datafile_position);
    void drop_caches_before(sstable_datafile_position);
    future<> close();
};

} // namespace sstables
