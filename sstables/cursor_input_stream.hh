/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "seastarx.hh"
#include "reader_permit.hh"
#include "sstables/compressed_file_cursor.hh"
#include "sstables/sstable_datafile_input_stream.hh"
#include "sstables/sstable_datafile_position.hh"
#include "sstables/shared_sstable.hh"
#include "tracing/trace_state.hh"

namespace sstables {

// A `sstable_datafile_input_stream` that reads forwards from a
// `sstable_datafile_cursor`, starting at a given position.
//
// The cursor may be either borrowed (shared with other readers, e.g. the
// backward row reads in the partition reversing data source) or owned by the
// stream itself (see the factories below). When borrowed, `close()` is a no-op
// and the cursor is closed by its owner; when owned, `close()` closes it.
//
// The parsers (`continuous_data_consumer`s) want to own an input stream and
// drive it via the seastar consume protocol. This adapter bridges them to the
// cursor: because the cursor's internal position may be shared, the stream
// tracks its own logical position `_pos` - the next byte it will return - and
// seeks the cursor to `_pos` before every read. This also makes repeated
// `consume()` calls on the same parser resume exactly where the previous one
// stopped, mirroring how a seastar input_stream retains its leftover buffer.
//
// `detach()` is unsupported - there is no underlying `data_source` to hand out.

// Creates a stream that reads forwards from a cursor it does not own, starting
// at `start`. The cursor must outlive the stream and is closed by its owner.
sstable_datafile_input_stream make_cursor_input_stream(sstable_datafile_cursor& cursor, sstable_datafile_position start);

// Creates a stream that owns a freshly-opened cursor over `sst`'s data file and
// reads forwards over `range`. The stream's `close()` closes the owned cursor.
// When `digest` is set, the cursor verifies the whole-file digest while it
// streams the file forwards from the start (see sstable_datafile_cursor).
sstable_datafile_input_stream make_owning_cursor_input_stream(shared_sstable sst, disk_read_range range,
        reader_permit permit, tracing::trace_state_ptr trace_state, std::optional<uint32_t> digest = std::nullopt);

} // namespace sstables
