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

#include <seastar/core/future.hh>
#include <seastar/core/temporary_buffer.hh>

#include "seastarx.hh"
#include "sstables/consumer.hh"
#include "sstables/sstable_position.hh"

namespace sstables {

// The facade through which the partition reversing data source does all IO into
// an sstable data file, unifying its two backing implementations:
//
//   * the physical-position compressed format, backed by a shared
//     compressed_file_cursor (see compressed_reversing_cursor), and
//   * the logical-position format, backed by sst->data_stream / sst->data_read
//     (see logical_reversing_cursor).
//
// It is deliberately higher-level than an input stream: rather than *being* the
// stream the parsers drive, it *hands out* a forward stream the parser owns and
// closes (make_forward_input_stream). This lets the logical implementation give
// each parser its own data_stream -- with no long-lived stream state to manage --
// while the compressed implementation hands out a borrowed view of its one shared
// cursor, which is what that format actually requires (forward parsing and the
// backward row reads must share a single physical cursor position).
class reversing_source_cursor {
public:
    virtual ~reversing_source_cursor() = default;

    // Opens a forward input stream over [start, end) for a parser to own and
    // drive; the parser closes it when it is done.
    virtual future<std::unique_ptr<data_consumer::continuous_data_consumer_input_stream>>
    make_forward_input_stream(sstable_position start, sstable_position end) = 0;

    // Reads [start, end) forwards into a freshly-owned buffer. `size` is the
    // segment's decompressed length (== end - start). Used for the partition
    // header, which is handed back to the sstable reader in file order.
    virtual future<temporary_buffer<char>>
    read_forwards(sstable_position start, sstable_position end, size_t size) = 0;

    // Reads [start, end) backwards into a freshly-owned buffer. `size` is the
    // segment's decompressed length (== end - start). Used for the rows handed
    // back in reverse file order; the returned buffer may be mutated in place.
    virtual future<temporary_buffer<char>>
    read_backwards(sstable_position start, sstable_position end, size_t size) = 0;

    // Given the start position of a row and the length of the row preceding it
    // (row_body_skipping_context::prev_len()), returns the start position of that
    // preceding row.
    virtual future<sstable_position>
    prev_row_start(sstable_position row_start, uint64_t prev_len) = 0;

    // Discard any read-ahead retained from a backward walk that is being
    // abandoned because the read range shrank. Cheap; safe to call between
    // backward reads.
    virtual void drop_read_ahead() = 0;

    virtual future<> close() = 0;
};

} // namespace sstables
