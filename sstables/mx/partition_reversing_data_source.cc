/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <seastar/core/coroutine.hh>
#include <seastar/core/iostream.hh>
#include "partition_reversing_data_source.hh"
#include "reader_permit.hh"
#include "sstables/compressed_file_cursor.hh"
#include "sstables/consumer.hh"
#include "sstables/processing_result_generator.hh"
#include "sstables/sstable_datafile_position.hh"
#include "sstables/shared_sstable.hh"
#include "sstables/sstables.hh"
#include "sstables/types.hh"

namespace sstables {

extern logging::logger sstlog;

namespace mx {

// A `sstable_datafile_input_stream` that reads forwards from a shared
// `sstable_datafile_cursor`, starting at a given position.
//
// All IO in this file goes through a single cursor owned by the data source.
// The parsers (`continuous_data_consumer`s), however, want to own an input
// stream and drive it via the seastar consume protocol. This adapter bridges
// the two: it borrows the cursor (it does not own it) and reads forwards from
// it, starting at the stream's start position.
//
// The cursor is shared between several of these streams (and the backward row
// reads), so its internal position is not ours to rely on. Instead this stream
// tracks its own logical position `_pos` - the next byte it will return - and
// seeks the cursor to `_pos` before every read. This also makes repeated
// `consume()` calls on the same parser resume exactly where the previous one
// stopped, mirroring how a seastar input_stream retains its leftover buffer.
//
// Because the cursor is shared and outlives the stream, `close()` is a no-op
// and `detach()` is unsupported - the cursor is closed by the data source.
class cursor_input_stream_impl final : public sstable_datafile_input_stream::impl {
    using tmp_buf = sstable_datafile_input_stream::tmp_buf;
    using consumer_fn = sstable_datafile_input_stream::consumer_fn;

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
        // The cursor is owned by the data source, not by this stream.
        return make_ready_future<>();
    }

    future<> skip(uint64_t n) noexcept override {
        advance_pos(n);
        co_return;
    }

    sstable_datafile_position compute_relative_position(sstable_datafile_position base, int64_t delta) override {
        _cursor.seek(base);
        return _cursor.compute_relative_position(delta);
    }

    data_source detach() && override {
        on_internal_error(sstlog, "cursor_input_stream_impl does not support detach()");
    }
};

static sstable_datafile_input_stream make_cursor_input_stream(sstable_datafile_cursor& cursor, sstable_datafile_position start) {
    return sstable_datafile_input_stream(std::make_unique<cursor_input_stream_impl>(cursor, start));
}

// Parser for the partition header and the static row, if present.
//
// After consuming the input stream, allows reading the offset after the consumed
// segment using header_end_pos(). The offset is relative to the start of the
// stream (the partition start); the caller rebases it onto an absolute position.
// Parsing copied from the sstable reader, with verification removed.
//
class partition_header_context : public data_consumer::continuous_data_consumer<partition_header_context, sstables::sstable_datafile_input_stream> {
    uint64_t _header_end_pos;
    bool _finished = false;
    processing_result_generator _gen;
    temporary_buffer<char>* _processing_data;
public:
    bool non_consuming() const {
        return false;
    }
    void verify_end_state() const {
        if (!_finished) {
            throw std::runtime_error("partition_header_context - no more data but parsing is incomplete");
        }
    }
    uint64_t header_end_pos() {
        return _header_end_pos;
    }
    data_consumer::processing_result process_state(temporary_buffer<char>& data) {
        _processing_data = &data;
        auto ret = _gen.generate();
        if (ret == data_consumer::proceed::no) {
            _finished = true;
        }
        return ret;
    }
private:
    uint64_t current_position() {
        return position() - _processing_data->size();
    }
    processing_result_generator do_process_state() {
        // length of the partition key
        co_yield read_16(*_processing_data);
        co_yield skip(*_processing_data,
                // skip partition key
                uint32_t{_u16}
                // skip deletion_time::local_deletion_time
                + sizeof(uint32_t)
                // skip deletion_time::marked_for_delete_at
                + sizeof(uint64_t));

        // Peek the first row or tombstone. If it's a static row, determine where it ends,
        // i.e. where the sequence of clustering rows starts.

        co_yield read_8(*_processing_data);
        auto flags = unfiltered_flags_m(_u8);
        if (flags.is_end_of_partition() || flags.is_range_tombstone() || !flags.has_extended_flags()) {
            _header_end_pos = current_position() - 1;
            co_yield data_consumer::proceed::no;
        } else {
            co_yield read_8(*_processing_data);
            auto extended_flags = unfiltered_extended_flags_m(_u8);
            if (!extended_flags.is_static()) {
                _header_end_pos = current_position() - 2;
                co_yield data_consumer::proceed::no;
            }
        }

        // A static row is present.
        // There are no clustering blocks. Read the row body size:
        co_yield read_unsigned_vint(*_processing_data);
        // skip the row body
        _header_end_pos = current_position() + _u64;
        // _header_end_pos is where the clustering rows start
        co_yield data_consumer::proceed::no;
    }
public:

    // `maxlen` bounds the segment to parse. Positions reported by this context
    // (e.g. header_end_pos()) are relative to the start of `input`, so the
    // caller rebases them onto the absolute file position the stream starts at.
    partition_header_context(sstables::sstable_datafile_input_stream&& input, uint64_t maxlen, reader_permit permit)
                : continuous_data_consumer(std::move(permit), std::move(input), 0, maxlen)
                , _gen(do_process_state())
    {}
};

// Parser of rows/tombstones that skips their bodies.
//
// Reads rows in their file order, pausing consumption after each row.
// To read rows in reverse order, use the prev_len() value to find
// the start position of the previous row, and create a new context
// to read that row.
// After reading the end_of_partition flag, end_of_partition() returns
// true.
// After reading a tombstone, current_tombstone_reversing_info() returns
// information about the tombstone kind, as well as the offsets of its
// members, which is useful for reversing the tombstone.
//
// `row_body_skipping_context` does not handle the static row (if there is one in the partition),
// only `unfiltered`s (clustering rows and tombstones).
class row_body_skipping_context : public data_consumer::continuous_data_consumer<row_body_skipping_context, sstables::sstable_datafile_input_stream> {
    bool _end_of_partition = false;
    bool _finished = false;
    processing_result_generator _gen;
    temporary_buffer<char>* _processing_data;

public:
    struct tombstone_reversing_info {
        uint64_t kind_offset;
        bound_kind_m range_tombstone_kind;

        // Range tombstone markers in the sstable data file come in two kinds: bound markers and boundary markers.
        // Bound markers happen when a range tombstone opens or ends.
        // Boundary markers happen when one range tombstone ends but another opens at the same position.
        //
        // Bound markers have one `delta_deletion_time` structs (tombstone timestamp + local deletion time) at the end.
        // Boundary markers have two.
        //
        // `first_deletion_time_offset` gives the position of the first `delta_deletion_time` (which is present for both kinds),
        // after_first_deletion_time gives its end position (i.e. position of last byte plus one), which in case of boundary
        // markers is the start position of the second `delta_deletion_time` (in case of bound markers its the end of the whole marker).
        uint64_t first_deletion_time_offset;
        uint64_t after_first_deletion_time_offset;
    };
private:
    unfiltered_flags_m _flags{0};
    std::optional<tombstone_reversing_info> _current_tombstone_reversing_info;
    uint64_t _prev_unfiltered_size;

    // for calculating the clustering blocks
    std::ranges::subrange<std::vector<std::optional<uint32_t>>::const_iterator> _ck_column_value_fix_lengths;
    uint64_t _ck_blocks_header;
    uint32_t _ck_blocks_header_offset;
    bool _reading_range_tombstone_ck = false;
    bool _reading_row_ck = false; // includes single row tombstones
    uint16_t _ck_size;
    column_translation _column_translation;

    void setup_ck(const std::vector<std::optional<uint32_t>>& column_value_fix_lengths) {
        if (column_value_fix_lengths.empty()) {
            _ck_column_value_fix_lengths = std::ranges::subrange(column_value_fix_lengths);
        } else {
            _ck_column_value_fix_lengths = std::ranges::subrange(std::begin(column_value_fix_lengths),
                                                                 std::begin(column_value_fix_lengths) + _ck_size);
        }
        _ck_blocks_header_offset = 0u;
    }
    bool no_more_ck_blocks() const { return _ck_column_value_fix_lengths.empty(); }
    void move_to_next_ck_block() {
        _ck_column_value_fix_lengths.advance(1);
        ++_ck_blocks_header_offset;
        if (_ck_blocks_header_offset == 32u) {
            _ck_blocks_header_offset = 0u;
        }
    }
    std::optional<uint32_t> get_ck_block_value_length() const {
        return _ck_column_value_fix_lengths.front();
    }
    bool is_block_empty() const {
        return (_ck_blocks_header & (uint64_t(1) << (2 * _ck_blocks_header_offset))) != 0;
    }
    bool is_block_null() const {
        return (_ck_blocks_header & (uint64_t(1) << (2 * _ck_blocks_header_offset + 1))) != 0;
    }
    bool should_read_block_header() const {
        return _ck_blocks_header_offset == 0u;
    }

public:
    bool non_consuming() const {
        return false;
    }
    void verify_end_state() const {
        if (!_finished) {
            throw std::runtime_error("row_body_skipping_context - no more data but parsing is incomplete");
        }
    }
    bool end_of_partition() const {
        return _end_of_partition;
    }
    uint64_t prev_len() {
        return _prev_unfiltered_size;
    }
    std::optional<tombstone_reversing_info> current_tombstone_reversing_info() {
        // std::nullopt if the last consumed unfiltered was not a tombstone
        return _current_tombstone_reversing_info;
    }
    data_consumer::processing_result process_state(temporary_buffer<char>& data) {
        _processing_data = &data;
        auto ret = _gen.generate();
        return ret;
    }
private:
    uint64_t current_position() {
        return position() - _processing_data->size();
    }
    processing_result_generator do_process_state() {
        while (true) {
            _finished = false;
            co_yield read_8(*_processing_data);
            auto flags = unfiltered_flags_m(_u8);
            _current_tombstone_reversing_info.reset();
            if (flags.is_end_of_partition()) {
                _end_of_partition = true;
                _finished = true;
                co_yield data_consumer::proceed::no;
                break;
            } else if (flags.is_range_tombstone()) {
                _current_tombstone_reversing_info.emplace();
                _current_tombstone_reversing_info->kind_offset = current_position();
                co_yield read_8(*_processing_data);
                _current_tombstone_reversing_info->range_tombstone_kind = bound_kind_m(_u8);
                co_yield read_16(*_processing_data);
                _ck_size = _u16;
                if (_ck_size != 0) {
                    _reading_range_tombstone_ck = true;
                }
            } else {
                if (flags.has_extended_flags()) {
                    // we only read the flags to perform a sanity check
                    co_yield read_8(*_processing_data);
                    auto extended_flags = unfiltered_extended_flags_m(_u8);
                    if (extended_flags.is_static()) {
                        on_internal_error(sstlog, "partition_reversing_data_source: row_body_skipping_context constructed on a static row");
                    }
                }
                _ck_size = _column_translation.clustering_column_value_fix_legths().size();
                _reading_row_ck = true;
            }
            if (_reading_row_ck || _reading_range_tombstone_ck) {
                setup_ck(_column_translation.clustering_column_value_fix_legths());
                while (!no_more_ck_blocks()) {
                    if (should_read_block_header()) {
                        co_yield read_unsigned_vint(*_processing_data);
                        _ck_blocks_header = _u64;
                    }
                    if (is_block_null() || is_block_empty()) {
                        move_to_next_ck_block();
                        continue;
                    }
                    // possibly read the length of, and then skip the clustering cell
                    if (auto len = get_ck_block_value_length()) {
                        co_yield skip(*_processing_data, *len);
                    } else {
                        co_yield read_unsigned_vint(*_processing_data);
                        co_yield skip(*_processing_data, _u64);
                    }
                    move_to_next_ck_block();
                }
                _reading_row_ck = false;
                _reading_range_tombstone_ck = false;
            }
            co_yield read_unsigned_vint(*_processing_data);
            // marker_body_size or row_body_size
            uint64_t next_row_offset = current_position() + _u64;
            co_yield read_unsigned_vint(*_processing_data);
            _prev_unfiltered_size = _u64;
            if (_current_tombstone_reversing_info) {
                _current_tombstone_reversing_info->first_deletion_time_offset = current_position();
                // skip delta_marked_for_delete_at and delta_local_deletion_time
                co_yield read_unsigned_vint(*_processing_data);
                co_yield read_unsigned_vint(*_processing_data);
                _current_tombstone_reversing_info->after_first_deletion_time_offset = current_position();
            }
            _finished = true;
            // skip until the next row, allowing to read consecutive rows in disk order
            co_yield skip(*_processing_data, next_row_offset - current_position());
            co_yield data_consumer::proceed::no;
        }
    }
public:
    // `maxlen` bounds the segment to parse. Positions reported by this context
    // (position(), and the offsets in tombstone_reversing_info) are relative to
    // the start of `input`, i.e. to the row the stream starts at. The caller
    // rebases position() onto an absolute file position when it needs one, and
    // the tombstone offsets index directly into the row buffer.
    row_body_skipping_context(sstables::sstable_datafile_input_stream&& input, uint64_t maxlen, reader_permit permit, column_translation ct)
                : continuous_data_consumer(std::move(permit), std::move(input), 0, maxlen)
                , _gen(do_process_state())
                , _column_translation(std::move(ct))
    {}
};

// Precondition: `k` is not static_clustering or clustering
bound_kind_m reverse_tombstone_kind(bound_kind_m k) {
    switch (k) {
        case bound_kind_m::excl_end:
            return bound_kind_m::excl_start;
        case bound_kind_m::incl_start:
            return bound_kind_m::incl_end;
        case bound_kind_m::excl_end_incl_start:
            return bound_kind_m::incl_end_excl_start;
        case bound_kind_m::incl_end_excl_start:
            return bound_kind_m::excl_end_incl_start;
        case bound_kind_m::incl_end:
            return bound_kind_m::incl_start;
        case bound_kind_m::excl_start:
            return bound_kind_m::excl_end;
        default:
            on_internal_error(sstlog, format(
                "reverse_tombstone_kind: expected tombstone kind, got {}", k));
    }
}

// A 'row' consisting of a single byte, representing the end of partition in sstable data file.
static temporary_buffer<char> end_of_partition() {
    temporary_buffer<char> tmp(1);
    *tmp.get_write() = 1;
    return tmp;
}

// The intermediary data source that reads from an sstable, and produces
// data buffers, as if the sstable had all rows written in a reversed order.
//
// The intermediary always starts by reading the partition header and the
// static row using partition_header_context. The offset after the parsed
// segment is the new actual "partition end" in reversed order - when
// reached, an unfiltered with a single flag "partition_end" is produced.
//
// After reading the partition header, the data source advances to the end
// of the clustering range. Afterwards, we may encounter 2 situations:
// there is another unfiltered after the clustering range, or there is
// partition end. In the former case, we read the following unfiltered, and
// deduce the position of the first row of our actual range using
// row_body_skipping_context::prev_len(). If it's the latter, we find the
// last row by iterating over the entire last promoted index block.
//
// After finding the last row, we produce rows in reversed order one by one,
// parsing current row using row_body_skipping_context, and finding file
// offsets of the previous one using the start of the current row as the end,
// and the end decreased by row_body_skipping_context::prev_len() as the start
//
// We skip between clustering ranges using the index_reader's data range.
// When we detect that the range end has been decreased, we return to the same
// state as after reading the partition header, and continue as if the new
// range was the original.
//
// All IO into the data file goes through a single sstable_datafile_cursor.
// The parsers read forwards from it (through cursor_input_stream_impl), while
// the rows handed back to the sstable reader are read backwards from it. The
// vast majority of the data consumed by our parsers is later reused in the
// sstable reader; the cursor's own cache absorbs that reuse, so we don't read
// the same bytes from disk twice. When the index tells us a clustering range
// end has decreased, we drop the cursor's cache past the new end.
//
// Because the range tombstones are read in reversed order, we need to swap
// the start tombstones with the ends. We achieve that by finding the file
// offsets of the row tombstone member variables using row_body_skipping_context,
// and modifying them in the returned row buffer accordingly.
//
class partition_reversing_data_source_impl final : public data_source_impl {
    const schema& _schema;
    shared_sstable _sst;
    abstract_index_reader& _ir;
    reader_permit _permit;
    tracing::trace_state_ptr _trace_state;

    // The single cursor through which all IO into the sstable data file is done.
    // The parsers below read forwards from it (via cursor_input_stream_impl),
    // and the rows returned to the sstable reader are read backwards from it.
    sstable_datafile_cursor _cursor;

    std::optional<partition_header_context> _partition_header_context;
    std::optional<row_body_skipping_context> _row_skipping_context;
    // Absolute file position the current _row_skipping_context started at. The
    // context reports positions relative to its start, so we add this to turn
    // them back into absolute file positions.
    sstable_datafile_position _row_skipping_context_start;
    sstable_datafile_position _clustering_range_start;
    sstable_datafile_position _partition_start;
    sstable_datafile_position _partition_end;

    // _row_start denotes our current position in the input stream:
    // either _partition_end or the start of some row (_row_start never lands in the middle of a row).
    // We share this position with the user (they can only read it, not modify it)
    // so they can e.g. compare it with index positions.
    sstable_datafile_position _row_start;
    sstable_datafile_position _row_end;
    // Invariant: _row_start <= _row_end

    column_translation _cached_column_translation;

    enum class state {
        // Looking for the first row entry (last in original order) in the clustering range being read
        RANGE_END,

        // Returning a buffer containing a row entry
        ROWS,

        // Returning a partition end flag
        PARTITION_END,

        // Nothing more to return
        FINISHED
    } _state = state::RANGE_END;
private:
    // Reads [start, end) from the data file backwards through the cursor.
    // Used both for the partition header and for the rows we hand back to the
    // sstable reader. The returned buffer is freshly owned, so it can be mutated
    // in place (see modify_tombstone()).
    future<temporary_buffer<char>> data_read(sstable_datafile_position start, sstable_datafile_position end) {
        co_return co_await _cursor.read(start, end);
    }

    // Reverse the range tombstone bound/boundary stored in `row`, which holds
    // the bytes of the row spanning [_row_start, _row_end). `info`'s offsets are
    // relative to the row's start (the row_body_skipping_context that produced
    // them was started at _row_start), so they index into `row` directly.
    void modify_tombstone(temporary_buffer<char>& row, const row_body_skipping_context::tombstone_reversing_info& info) {
        char& out = row.get_write()[info.kind_offset];
        // reverse the kind of the range tombstone bound/boundary
        out = (char)reverse_tombstone_kind(info.range_tombstone_kind);
        if (is_boundary_between_adjacent_intervals(info.range_tombstone_kind)) {
            // if the tombstone is a boundary, we need to swap the order of end/start deletion times
            // Need to clone part of the buffer containing first_del_time because we overwrite it with second_del_time before using first_del_time
            auto first_del_time = row.share(info.first_deletion_time_offset, info.after_first_deletion_time_offset - info.first_deletion_time_offset).clone();
            // We also need to clone the part containing second_del_time as we may overwrite a prefix of that part while writing second_del_time
            // (if second_del_time is longer than first_del_time - it may be as we're dealing with varints here)
            auto second_del_time = row.share(info.after_first_deletion_time_offset, row.size() - info.after_first_deletion_time_offset).clone();
            std::copy(second_del_time.begin(), second_del_time.end(), row.get_write() + info.first_deletion_time_offset);
            std::copy(first_del_time.begin(), first_del_time.end(), row.get_write() + info.first_deletion_time_offset + second_del_time.size());
        }
    }

    // Given the start position of a row and the size of the row preceding it
    // (as reported by row_body_skipping_context::prev_len()), returns the start
    // position of that preceding row. Computed through the cursor so that the
    // position arithmetic stays in terms of sstable_datafile_position rather
    // than raw integers.
    sstable_datafile_position prev_row_start(sstable_datafile_position row_start, uint64_t prev_len) {
        _cursor.seek(row_start);
        return _cursor.compute_relative_position(-static_cast<ssize_t>(prev_len));
    }

    future<> emplace_row_skipping_context(sstable_datafile_position row_start, sstable_datafile_position row_end) {
        if (_row_skipping_context) {
            co_await _row_skipping_context->close();
        }
        _row_skipping_context_start = row_start;
        _row_skipping_context.emplace(make_cursor_input_stream(_cursor, row_start),
                -1,
                _permit, _cached_column_translation);
    }

    // The current row_skipping_context's position(), as an absolute file
    // position. The context counts from its own start (_row_skipping_context_start),
    // so we rebase through the cursor to keep the arithmetic typed.
    sstable_datafile_position row_skipping_position() {
        _cursor.seek(_row_skipping_context_start);
        return _cursor.compute_relative_position(_row_skipping_context->position());
    }
public:
    partition_reversing_data_source_impl(const schema& s,
            shared_sstable sst,
            abstract_index_reader& ir,
            uint64_t partition_start,
            size_t partition_len,
            reader_permit permit,
            tracing::trace_state_ptr trace_state)
        : _schema(s)
        , _sst(std::move(sst))
        , _ir(ir)
        , _permit(std::move(permit))
        , _trace_state(std::move(trace_state))
        , _cursor(_sst, _permit, _trace_state)
        , _partition_start(sstable_datafile_position::from_logical_fixme(partition_start))
        , _partition_end(sstable_datafile_position::from_logical_fixme(partition_start + partition_len))
        , _row_start(_partition_end)
        , _row_end(_partition_end)
        , _cached_column_translation(_sst->get_column_translation(_schema, _sst->get_serialization_header(), _sst->features()))
    { }

    virtual future<temporary_buffer<char>> get() override {
        if (!_partition_header_context) {
            _partition_header_context.emplace(make_cursor_input_stream(_cursor, _partition_start),
                    -1, _permit);
            co_await _partition_header_context->consume_input();
            // header_end_pos() is relative to the partition start; rebase it.
            _cursor.seek(_partition_start);
            _clustering_range_start = _cursor.compute_relative_position(_partition_header_context->header_end_pos());
            co_return co_await data_read(_partition_start, _clustering_range_start);
        }
        auto ir_end = _ir.sstable_datafile_positions().end;
        if (ir_end && *ir_end < _row_start) {
            // we can skip at least one row
            _row_start = *ir_end;
            // The cursor's cache for the rows past the new range end is no longer
            // needed; drop it so the cache doesn't grow without bound.
            _cursor.drop_caches_after(_row_start);
            _state = state::RANGE_END;
        }
        switch (_state) {
        case state::RANGE_END: {
            bool look_in_last_block = false;
            if (_row_start >= _row_end) {
                if (_row_start != _row_end) {
                    on_internal_error(sstlog, format(
                        "partition_reversing_data_source: invariant broken: _row_start({}) > _row_end({})",
                        _row_start, _row_end));
                }
                if (_row_start != _partition_end) {
                    on_internal_error(sstlog, format(
                        "partition_reversing_data_source: invariant broken: _row_start({}) == _row_end({}), but"
                        " != _partition_end({})", _row_start, _row_end, _partition_end));
                }
                look_in_last_block = true;
            } else {
                co_await emplace_row_skipping_context(_row_start, _row_end);
                co_await _row_skipping_context->consume_input();
                if (_row_skipping_context->end_of_partition()) {
                    look_in_last_block = true;
                } else {
                    _row_end = _row_start;
                    _row_start = prev_row_start(_row_start, _row_skipping_context->prev_len());
                }
            }
            if (look_in_last_block) {
                if (auto offset = co_await _ir.last_block_sstable_datafile_offset()) {
                    // there was a promoted index block in the partition, read from its beginning to find the last row
                    _row_start = _partition_start + offset.value();
                } else {
                    // no promoted index blocks in the partition, read from the beginning
                    _row_start = _clustering_range_start;
                }
                co_await emplace_row_skipping_context(_row_start, _partition_end);
                auto current_row_start = _row_skipping_context->position();
                auto last_row_start = current_row_start;
                co_await _row_skipping_context->consume_input();
                while (!_row_skipping_context->end_of_partition()) {
                    last_row_start = current_row_start;
                    current_row_start = _row_skipping_context->position();
                    co_await _row_skipping_context->consume_input();
                }
                _cursor.seek(_row_skipping_context_start);
                _row_end = _cursor.compute_relative_position(current_row_start);
                _row_start = _cursor.compute_relative_position(last_row_start);
                if (_row_start == _row_end) {
                    // empty partition
                    _state = state::FINISHED;
                    co_return end_of_partition();
                }
            }

            if (_row_start < _clustering_range_start) {
                // The first index block starts after the range being read,
                // i.e. the range being read is empty.
                if (prev_row_start(_clustering_range_start, _row_skipping_context->prev_len()) != _partition_start) {
                    on_internal_error(sstlog, format(
                        "partition_reversing_data_source: invariant broken: _row_start({}) < _clustering_range_start({})"
                        ", but _row_skipping_context->prev_len()({}) != _clustering_range_start - _partition_start({})",
                        _row_start, _clustering_range_start, _row_skipping_context->prev_len(), _partition_start));
                }
                _row_start = _clustering_range_start;
                _state = state::FINISHED;
                co_return end_of_partition();
            }

            _state = state::ROWS;
            [[fallthrough]];
        }
        case state::ROWS: {
            co_await emplace_row_skipping_context(_row_start, _row_end);
            co_await _row_skipping_context->consume_input();
            auto ret = co_await data_read(_row_start, _row_end);
            if (_row_skipping_context->current_tombstone_reversing_info()) {
                modify_tombstone(ret, *_row_skipping_context->current_tombstone_reversing_info());
            }
            _row_end = _row_start;
            _row_start = prev_row_start(_row_start, _row_skipping_context->prev_len());
            if (_row_end == _clustering_range_start) {
                _state = state::PARTITION_END;
            }
            co_return ret;
        }
        case state::PARTITION_END: {
            _state = state::FINISHED;
            co_return end_of_partition();
        }
        case state::FINISHED:
            co_return temporary_buffer<char>();
        }
    }

    virtual future<temporary_buffer<char>> skip(uint64_t n) override {
        // Skipping is implemented by checking the index.
        on_internal_error(sstlog, "partition_reversing_data_source does not support skipping");
    }

    // Must not be run concurrently with `get()`.
    virtual future<> close() noexcept override {
        auto close_partition_header_context = _partition_header_context ? _partition_header_context->close() : make_ready_future<>();
        auto close_row_skipping_context = _row_skipping_context ? _row_skipping_context->close() : make_ready_future();
        co_await when_all(std::move(close_partition_header_context), std::move(close_row_skipping_context));
    }

    // Points to the current position of the source over the sstable file, which
    // is either the end of partition or the beginning of some row.
    // Can only decrease.
    const sstable_datafile_position& current_position_in_sstable() const {
        return _row_start;
    }
};

partition_reversing_data_source make_partition_reversing_data_source(const schema& s, shared_sstable sst, abstract_index_reader& ir, uint64_t pos, size_t len,
                                                          reader_permit permit, tracing::trace_state_ptr trace_state) {
    auto source_impl = std::make_unique<partition_reversing_data_source_impl>(
            s, std::move(sst), ir, pos, len, std::move(permit), trace_state);
    auto& curr_pos = source_impl->current_position_in_sstable();
    return partition_reversing_data_source {
        .the_source = seastar::data_source{std::move(source_impl)},
        .current_position_in_sstable = curr_pos
    };
}

}

}
