/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cstdint>
#include <memory>
#include <optional>

#include <seastar/core/iostream.hh>
#include <seastar/util/noncopyable_function.hh>

#include "reader_permit.hh"
#include "sstables/consumer.hh"
#include "sstables/sstable_position.hh"
#include "sstables/shared_sstable.hh"
#include "tracing/trace_state.hh"

class compressor;

namespace sstables {

using decompressing_input_stream_source_opener = noncopyable_function<future<data_source>()>;

std::unique_ptr<data_consumer::continuous_data_consumer_input_stream>
make_decompressing_input_stream(decompressing_input_stream_source_opener open_source,
        sstable_position start, const compressor& compressor,
        uint32_t uncompressed_chunk_length, uint64_t compressed_file_length,
        std::optional<uint32_t> digest = std::nullopt);

std::unique_ptr<data_consumer::continuous_data_consumer_input_stream>
make_decompressing_input_stream(shared_sstable sst, disk_read_range range,
        reader_permit permit, tracing::trace_state_ptr trace_state,
        std::optional<uint32_t> digest = std::nullopt);

} // namespace sstables
