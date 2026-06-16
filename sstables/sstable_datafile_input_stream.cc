/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "sstable_datafile_input_stream.hh"

namespace sstables {

namespace {

class input_stream_impl final : public sstable_datafile_input_stream::impl {
    using tmp_buf = sstable_datafile_input_stream::tmp_buf;
    using consumer_fn = sstable_datafile_input_stream::consumer_fn;
    input_stream<char> _stream;
public:
    explicit input_stream_impl(input_stream<char> stream) noexcept
        : _stream(std::move(stream)) {}

    future<tmp_buf> read_exactly(size_t n) noexcept override {
        return _stream.read_exactly(n);
    }

    future<> consume(consumer_fn consumer) noexcept override {
        return _stream.consume(std::move(consumer));
    }

    bool eof() const noexcept override {
        return _stream.eof();
    }

    future<tmp_buf> read() noexcept override {
        return _stream.read();
    }

    future<> close() noexcept override {
        return _stream.close();
    }

    future<> skip(uint64_t n) noexcept override {
        return _stream.skip(n);
    }
    
    sstable_datafile_position compute_relative_position(sstable_datafile_position pos, ssize_t offset) override {
        return pos + sstable_datafile_offset::from_logical_approved(offset);
    }

    int64_t subtract_positions(sstable_datafile_position b, sstable_datafile_position a) override {
        return b.to_logical_approved() - a.to_logical_approved();
    }

    data_source detach() && override {
        return std::move(_stream).detach();
    }
};

} // anonymous namespace

sstable_datafile_input_stream::sstable_datafile_input_stream(input_stream<char> stream)
    : _impl(std::make_unique<input_stream_impl>(std::move(stream))) {}

} // namespace sstables
