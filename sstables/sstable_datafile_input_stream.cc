/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "sstable_datafile_input_stream.hh"
#include "utils/log.hh"

namespace sstables {

extern logging::logger sstable_cursor_log;

namespace {

class input_stream_impl final : public sstable_datafile_input_stream::impl {
    using tmp_buf = sstable_datafile_input_stream::tmp_buf;
    using consumer_fn = sstable_datafile_input_stream::consumer_fn;
    input_stream<char> _stream;
public:
    explicit input_stream_impl(input_stream<char> stream) noexcept
        : _stream(std::move(stream)) {}

    future<tmp_buf> read_exactly(size_t n) noexcept override {
        sstable_cursor_log.trace("[stream@{}] read_exactly: n={}", fmt::ptr(this), n);
        return _stream.read_exactly(n);
    }

    future<> consume(consumer_fn consumer) noexcept override {
        sstable_cursor_log.trace("[stream@{}] consume", fmt::ptr(this));
        return _stream.consume(std::move(consumer));
    }

    bool eof() const noexcept override {
        return _stream.eof();
    }

    future<tmp_buf> read() noexcept override {
        sstable_cursor_log.trace("[stream@{}] read", fmt::ptr(this));
        return _stream.read();
    }

    future<> close() noexcept override {
        sstable_cursor_log.trace("[stream@{}] close", fmt::ptr(this));
        return _stream.close();
    }

    future<> skip(uint64_t n) noexcept override {
        sstable_cursor_log.trace("[stream@{}] skip: n={}", fmt::ptr(this), n);
        return _stream.skip(n);
    }

    future<> skip_to(sstable_datafile_position target, sstable_datafile_position current) noexcept override {
        sstable_cursor_log.trace("[stream@{}] skip_to: target={} current={}", fmt::ptr(this), target, current);
        // The underlying input_stream supports only relative skips, so we rely
        // on the caller-supplied current position to compute how far to skip.
        return _stream.skip(subtract_positions(target, current));
    }

    sstable_datafile_position compute_relative_position(sstable_datafile_position pos, ssize_t offset) override {
        auto result = pos + sstable_datafile_offset::from_logical_approved(offset);
        sstable_cursor_log.trace("[stream@{}] compute_relative_position: pos={} offset={} result={}", fmt::ptr(this), pos, offset, result);
        return result;
    }

    int64_t subtract_positions(sstable_datafile_position b, sstable_datafile_position a) override {
        auto result = b.to_logical_approved() - a.to_logical_approved();
        sstable_cursor_log.trace("[stream@{}] subtract_positions: b={} a={} result={}", fmt::ptr(this), b, a, result);
        return result;
    }

    data_source detach() && override {
        sstable_cursor_log.trace("[stream@{}] detach", fmt::ptr(this));
        return std::move(_stream).detach();
    }
};

} // anonymous namespace

sstable_datafile_input_stream::sstable_datafile_input_stream(input_stream<char> stream)
    : _impl(std::make_unique<input_stream_impl>(std::move(stream))) {}

} // namespace sstables
