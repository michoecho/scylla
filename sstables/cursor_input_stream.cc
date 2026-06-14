/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <memory>

#include <seastar/core/coroutine.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/util/log.hh>

#include "sstables/cursor_input_stream.hh"

namespace sstables {

extern logging::logger sstlog;

namespace {

// See cursor_input_stream.hh for the rationale behind this adapter.
//
// The cursor is reached through `_cursor`. If `_owned_cursor` is set, this
// stream owns that cursor and `close()` closes it; otherwise the cursor is
// borrowed, outlives the stream, and `close()` is a no-op.
class cursor_input_stream_impl final : public sstable_datafile_input_stream::impl {
    using tmp_buf = sstable_datafile_input_stream::tmp_buf;
    using consumer_fn = sstable_datafile_input_stream::consumer_fn;

    // Set only when this stream owns the cursor; `_cursor` then refers to it.
    std::unique_ptr<sstable_datafile_cursor> _owned_cursor;
    sstable_datafile_cursor& _cursor;
    // The file position of the next byte this stream will produce.
    sstable_datafile_position _pos;

    // Default size of a forward read issued to the cursor. The cursor itself
    // caches and reads ahead, so this only bounds how much we ask for at once.
    static constexpr size_t read_size = 8 * 1024;

    // Advance _pos by `delta` bytes (which may be negative) through the cursor,
    // so position arithmetic stays in terms of sstable_datafile_position.
    void advance_pos(ssize_t delta) {
        _cursor.seek(_pos);
        _pos = _cursor.compute_relative_position(delta);
    }

    future<tmp_buf> read_at_pos(size_t n) {
        _cursor.seek(_pos);
        auto buf = co_await _cursor.read_forwards(n);
        advance_pos(buf.size());
        co_return buf;
    }
public:
    cursor_input_stream_impl(sstable_datafile_cursor& cursor, sstable_datafile_position start) noexcept
        : _cursor(cursor), _pos(start) {}

    cursor_input_stream_impl(std::unique_ptr<sstable_datafile_cursor> cursor, sstable_datafile_position start) noexcept
        : _owned_cursor(std::move(cursor)), _cursor(*_owned_cursor), _pos(start) {}

    future<tmp_buf> read_exactly(size_t n) noexcept override {
        return read_at_pos(n);
    }

    future<> consume(consumer_fn consumer) noexcept override {
        while (true) {
            auto buf = co_await read_at_pos(read_size);
            bool eof = buf.empty();
            auto result = co_await consumer(std::move(buf));
            bool stop = seastar::visit(result.get(),
                [eof] (const continue_consuming&) {
                    // Whole buffer consumed; stop only at end of file.
                    return eof;
                },
                [this] (stop_consuming<char>& stop) {
                    // The unconsumed tail must be produced again by the next
                    // read, so rewind our position over it.
                    advance_pos(-static_cast<ssize_t>(stop.get_buffer().size()));
                    return true;
                },
                [this] (const skip_bytes& skip) {
                    advance_pos(skip.get_value());
                    return false;
                });
            if (stop) {
                co_return;
            }
        }
    }

    bool eof() const noexcept override {
        // The cursor has no standalone eof flag; the consumers bound their
        // reads by length and never query this.
        return false;
    }

    future<tmp_buf> read() noexcept override {
        return read_at_pos(read_size);
    }

    future<> close() noexcept override {
        if (_owned_cursor) {
            return _owned_cursor->close();
        }
        // The cursor is owned elsewhere, not by this stream.
        return make_ready_future<>();
    }

    future<> skip(uint64_t n) noexcept override {
        advance_pos(n);
        co_return;
    }

    sstable_datafile_position compute_relative_position(sstable_datafile_position pos, ssize_t offset) override {
        auto prev = _cursor.compute_relative_position(0);
        _cursor.seek(pos);
        auto result = _cursor.compute_relative_position(offset);
        _cursor.seek(prev);
        return result;
    }

    int64_t subtract_positions(sstable_datafile_position b, sstable_datafile_position a) override {
        return b.to_logical_fixme() - a.to_logical_fixme();
    }

    data_source detach() && override {
        on_internal_error(sstlog, "cursor_input_stream_impl does not support detach()");
    }
};

} // anonymous namespace

sstable_datafile_input_stream make_cursor_input_stream(sstable_datafile_cursor& cursor, sstable_datafile_position start) {
    return sstable_datafile_input_stream(std::make_unique<cursor_input_stream_impl>(cursor, start));
}

sstable_datafile_input_stream make_owning_cursor_input_stream(shared_sstable sst, disk_read_range range,
        reader_permit permit, tracing::trace_state_ptr trace_state) {
    auto cursor = std::make_unique<sstable_datafile_cursor>(std::move(sst), std::move(permit), std::move(trace_state));
    return sstable_datafile_input_stream(std::make_unique<cursor_input_stream_impl>(std::move(cursor), range.start));
}

} // namespace sstables
