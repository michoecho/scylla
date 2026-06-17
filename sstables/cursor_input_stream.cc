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
extern logging::logger sstable_cursor_log;

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

    // The most recent buffer read from the cursor, kept so that the common
    // rewind-and-reread pattern (a parser over-reads, consumes part of the
    // buffer, then rewinds the unconsumed tail with stop_consuming) is served
    // from memory instead of seeking the cursor backwards and re-reading - which
    // for a compressed cursor would re-decompress a whole chunk. `_last_buf`
    // holds the bytes at [_last_buf_pos, _last_buf_end_pos). Both bounds are real
    // file positions: _last_buf_pos is where the buffer starts and _last_buf_end_pos
    // is the position just past its last byte (the cursor position right after the
    // read that produced it). The buffer is served only when _pos lies in that
    // half-open range, which is tested with plain position comparisons (no chunk
    // walk). The byte offset of _pos into the buffer is then computed through the
    // cursor; because _pos is known to be within the buffer's chunk span, every
    // chunk the cursor must walk to find that offset was read by the same read and
    // is still in its metadata cache.
    tmp_buf _last_buf;
    sstable_datafile_position _last_buf_pos;
    sstable_datafile_position _last_buf_end_pos;

    // Default size of a forward read issued to the cursor. The cursor itself
    // caches and reads ahead, so this only bounds how much we ask for at once.
    static constexpr size_t read_size = 8 * 1024;

    // Advance _pos by `delta` bytes (which may be negative) through the cursor,
    // so position arithmetic stays in terms of sstable_datafile_position.
    void advance_pos(ssize_t delta) {
        _cursor.seek(_pos);
        _pos = _cursor.compute_relative_position(delta);
    }

    // If _pos falls inside the cached buffer, return up to `n` bytes from it
    // (sharing, not copying) without touching the cursor. Returns an empty
    // optional when the position is not covered and a real read is needed.
    std::optional<tmp_buf> read_from_last_buf(size_t n) {
        // Serve only when _pos lies in [_last_buf_pos, _last_buf_end_pos). The
        // bounds check uses position ordering alone, so it never walks chunks and
        // cannot reach an uncached one - unlike the byte-offset computation below,
        // which is only safe once _pos is known to be inside the buffer.
        if (_last_buf.empty() || _pos < _last_buf_pos || !(_pos < _last_buf_end_pos)) {
            return std::nullopt;
        }
        // The byte offset of _pos into the buffer; the cursor turns the two
        // positions into a distance without assuming they are logical offsets.
        int64_t off = _cursor.subtract_positions(_pos, _last_buf_pos);
        size_t len = std::min(n, _last_buf.size() - off);
        auto buf = _last_buf.share(off, len);
        advance_pos(len);
        return buf;
    }

    future<tmp_buf> read_at_pos(size_t n) {
        if (auto cached = read_from_last_buf(n)) {
            co_return std::move(*cached);
        }
        _cursor.seek(_pos);
        auto buf = co_await _cursor.read_forwards(n);
        _last_buf = buf.share();
        _last_buf_pos = _pos;
        advance_pos(buf.size());
        _last_buf_end_pos = _pos;
        co_return buf;
    }
public:
    cursor_input_stream_impl(sstable_datafile_cursor& cursor, sstable_datafile_position start) noexcept
        : _cursor(cursor), _pos(start) {
        sstable_cursor_log.trace("[cursor_stream@{}] construct: borrowed cursor start={}", fmt::ptr(this), start);
    }

    cursor_input_stream_impl(std::unique_ptr<sstable_datafile_cursor> cursor, sstable_datafile_position start) noexcept
        : _owned_cursor(std::move(cursor)), _cursor(*_owned_cursor), _pos(start) {
        SCYLLA_ASSERT(!start.holds_physical() || start.to_physical().offset_within_chunk >= 0);
        sstable_cursor_log.trace("[cursor_stream@{}] construct: owned cursor start={}", fmt::ptr(this), start);
    }

    future<tmp_buf> read_exactly(size_t n) noexcept override {
        sstable_cursor_log.trace("[cursor_stream@{}] read_exactly: enter pos={} n={}", fmt::ptr(this), _pos, n);
        // read_at_pos() may return a short buffer when it is served from the
        // cached buffer (which can end before n bytes). Loop until we have n
        // bytes or hit EOF, so the read_exactly() contract still holds.
        auto first = co_await read_at_pos(n);
        if (first.size() >= n || first.empty()) {
            sstable_cursor_log.trace("[cursor_stream@{}] read_exactly: exit pos={} size={}", fmt::ptr(this), _pos, first.size());
            co_return first;
        }
        tmp_buf out(n);
        size_t filled = 0;
        std::copy_n(first.get(), first.size(), out.get_write());
        filled += first.size();
        while (filled < n) {
            auto buf = co_await read_at_pos(n - filled);
            if (buf.empty()) {
                break; // EOF
            }
            std::copy_n(buf.get(), buf.size(), out.get_write() + filled);
            filled += buf.size();
        }
        out.trim(filled);
        sstable_cursor_log.trace("[cursor_stream@{}] read_exactly: exit pos={} size={}", fmt::ptr(this), _pos, filled);
        co_return out;
    }

    future<> consume(consumer_fn consumer) noexcept override {
        sstable_cursor_log.trace("[cursor_stream@{}] consume: enter pos={}", fmt::ptr(this), _pos);
        while (true) {
            auto buf = co_await read_at_pos(read_size);
            bool eof = buf.empty();
            auto result = co_await consumer(std::move(buf));
            bool stop = co_await seastar::visit(result.get(),
                [eof] (const continue_consuming&) {
                    // Whole buffer consumed; stop only at end of file.
                    return make_ready_future<bool>(eof);
                },
                [this] (stop_consuming<char>& stop) {
                    // The unconsumed tail must be produced again by the next
                    // read, so rewind our position over it.
                    advance_pos(-static_cast<ssize_t>(stop.get_buffer().size()));
                    return make_ready_future<bool>(true);
                },
                [this] (const skip_bytes& skip) {
                    return this->skip(skip.get_value()).then([] {
                        return false;
                    });
                });
            if (stop) {
                sstable_cursor_log.trace("[cursor_stream@{}] consume: exit pos={}", fmt::ptr(this), _pos);
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
        sstable_cursor_log.trace("[cursor_stream@{}] read: pos={}", fmt::ptr(this), _pos);
        return read_at_pos(read_size);
    }

    future<> close() noexcept override {
        sstable_cursor_log.trace("[cursor_stream@{}] close: pos={}", fmt::ptr(this), _pos);
        if (_owned_cursor) {
            return _owned_cursor->close();
        }
        // The cursor is owned elsewhere, not by this stream.
        return make_ready_future<>();
    }

    future<> skip(uint64_t n) noexcept override {
        sstable_cursor_log.trace("[cursor_stream@{}] skip: enter pos={} n={}", fmt::ptr(this), _pos, n);
        // Walk the cursor forward by n bytes. For a compressed physical cursor a
        // forward skip can cross chunks the cursor has not read yet, so this is
        // async (it reads each crossed chunk's length prefix) rather than the
        // synchronous compute_relative_position used by advance_pos.
        _pos = co_await _cursor.skip_forwards(_pos, n);
        sstable_cursor_log.trace("[cursor_stream@{}] skip: exit pos={}", fmt::ptr(this), _pos);
    }

    future<> skip_to(sstable_datafile_position target, sstable_datafile_position) noexcept override {
        sstable_cursor_log.trace("[cursor_stream@{}] skip_to: enter pos={} target={}", fmt::ptr(this), _pos, target);
        // This stream tracks its own position, so it can seek straight to the
        // target; the caller-supplied current position is not needed. We still
        // walk the cursor to the target (a zero-length forward skip) so that the
        // target chunk's metadata is loaded into the cursor's caches, which later
        // relative-position arithmetic around the new position depends on.
        _pos = co_await _cursor.skip_forwards(target, 0);
        sstable_cursor_log.trace("[cursor_stream@{}] skip_to: exit pos={}", fmt::ptr(this), _pos);
    }

    sstable_datafile_position compute_relative_position(sstable_datafile_position pos, ssize_t offset) override {
        auto prev = _cursor.compute_relative_position(0);
        _cursor.seek(pos);
        auto result = _cursor.compute_relative_position(offset);
        _cursor.seek(prev);
        sstable_cursor_log.trace("[cursor_stream@{}] compute_relative_position: pos={} offset={} result={}", fmt::ptr(this), pos, offset, result);
        return result;
    }

    int64_t subtract_positions(sstable_datafile_position b, sstable_datafile_position a) override {
        // The cursor owns the chunk metadata needed to turn two physical
        // positions into a byte distance, so let it do the arithmetic rather
        // than assuming positions are plain logical offsets.
        auto result = _cursor.subtract_positions(b, a);
        sstable_cursor_log.trace("[cursor_stream@{}] subtract_positions: b={} a={} result={}", fmt::ptr(this), b, a, result);
        return result;
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
        reader_permit permit, tracing::trace_state_ptr trace_state, std::optional<uint32_t> digest) {
    auto cursor = std::make_unique<sstable_datafile_cursor>(std::move(sst), std::move(permit), std::move(trace_state), digest);
    return sstable_datafile_input_stream(std::make_unique<cursor_input_stream_impl>(std::move(cursor), range.start));
}

} // namespace sstables
