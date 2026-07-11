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
#include "sstables/compress.hh"
#include "sstables/consumer.hh"
#include "sstables/processing_result_generator.hh"
#include "sstables/reversing_source_cursor.hh"
#include "sstables/sstable_position.hh"
#include "sstables/shared_sstable.hh"
#include "sstables/sstables.hh"
#include "sstables/types.hh"
#include "utils/to_string.hh"

namespace sstables {

extern logging::logger sstlog;
logging::logger sstable_cursor_log("sstable_cursor");

namespace mx {

// Parser for the partition header and the static row, if present.
//
// After consuming the input stream, allows reading the position after the consumed
// segment using header_end_pos(), as an absolute file position.
// Parsing copied from the sstable reader, with verification removed.
//
class partition_header_context : public data_consumer::continuous_data_consumer<partition_header_context> {
    sstable_position _header_end_pos;
    // Decompressed byte length of the header, i.e. the distance from the stream
    // start (the partition start) to _header_end_pos. Unlike offset(), which
    // reflects the stream position and may sit a couple of bytes past
    // _header_end_pos because the parser peeks ahead before backing off, this is
    // the exact number of bytes the backward reader must read back.
    uint64_t _header_size = 0;
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
    sstable_position header_end_pos() {
        return _header_end_pos;
    }
    uint64_t header_size() const {
        return _header_size;
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
    // Absolute file position of the current parse point (the first byte not yet
    // consumed from _processing_data), shifted by `delta` logical bytes.
    sstable_position current_position() {
        return compute_relative_position(-static_cast<int64_t>(_processing_data->size()));
    }
    // Record the end of the header as both an absolute position and a
    // decompressed byte length from the stream start. The byte length is the
    // logical offset of the parse point (offset() minus what is still buffered
    // in _processing_data), shifted by the same `delta`.
    void set_header_end() {
        _header_end_pos = current_position();
        _header_size = offset() - _processing_data->size();
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

        set_header_end();
        co_yield read_8(*_processing_data);
        auto flags = unfiltered_flags_m(_u8);
        if (flags.is_end_of_partition() || flags.is_range_tombstone() || !flags.has_extended_flags()) {
            co_yield data_consumer::proceed::no;
        } else {
            co_yield read_8(*_processing_data);
            auto extended_flags = unfiltered_extended_flags_m(_u8);
            if (!extended_flags.is_static()) {
                co_yield data_consumer::proceed::no;
            }
        }

        // A static row is present.
        // There are no clustering blocks. Read the row body size:
        co_yield read_unsigned_vint(*_processing_data);
        // skip the row body
        co_yield skip(*_processing_data, _u64);
        set_header_end();
        // _header_end_pos is where the clustering rows start
        co_yield data_consumer::proceed::no;
    }
public:

    // `start` is the absolute file position the stream begins at (the partition
    // start). Positions reported by this context (e.g. header_end_pos()) are
    // absolute file positions. The segment to parse is unbounded; parsing stops
    // when the header (and static row, if any) has been consumed.
    partition_header_context(std::unique_ptr<data_consumer::continuous_data_consumer_input_stream> input, sstable_position start, reader_permit permit)
                : continuous_data_consumer(std::move(permit), std::move(input), start, std::optional<sstable_position>())
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
class row_body_skipping_context : public data_consumer::continuous_data_consumer<row_body_skipping_context> {
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
        return offset() - _processing_data->size();
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
    // `start` is the absolute file position the stream begins at and `end` bounds
    // the segment to parse. position() reported by this context is an absolute file
    // position, while the offsets in tombstone_reversing_info are relative to the
    // start of `input`, i.e. to the row the stream starts at, so they index
    // directly into the row buffer.
    row_body_skipping_context(std::unique_ptr<data_consumer::continuous_data_consumer_input_stream> input, sstable_position start, sstable_position end, reader_permit permit, column_translation ct)
                : continuous_data_consumer(std::move(permit), std::move(input), start, std::optional<sstable_position>(end))
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

// A non-owning continuous_data_consumer_input_stream that forwards every
// operation to a borrowed one. The forward parsers (continuous_data_consumer)
// each want to own an input stream and close it when they are done, but the
// compressed reversing cursor drives a single shared compressed_file_cursor for
// both forward parsing and backward row reads. This adapter lets a parser drive
// that shared cursor without owning it: close() is a no-op (the cursor is closed
// by its owner) and every operation is delegated to the cursor.
class borrowed_cursor_stream final : public data_consumer::continuous_data_consumer_input_stream {
    data_consumer::continuous_data_consumer_input_stream& _cursor;
public:
    explicit borrowed_cursor_stream(data_consumer::continuous_data_consumer_input_stream& cursor) noexcept : _cursor(cursor) {}
    future<> skip_to(sstable_position target) override {
        return _cursor.skip_to(target);
    }
    future<> skip(uint64_t n) override {
        return _cursor.skip(n);
    }
    future<> close() override {
        // The cursor is owned by the reversing data source, not by this stream.
        return make_ready_future<>();
    }
    sstable_position compute_relative_position(int64_t offset) override {
        return _cursor.compute_relative_position(offset);
    }
    void init_stream_position(sstable_position start) override {
        _cursor.init_stream_position(start);
    }
    const reader_position_tracker& stream_position() const override {
        return _cursor.stream_position();
    }
    future<consumption_result<char>> consume_one(std::optional<sstable_position> end_position, consumer_one_fn consumer) override {
        return _cursor.consume_one(end_position, std::move(consumer));
    }
};

// The logical-position implementation of reversing_source_cursor, used for all
// sstables that are not the physical-position compressed format. It reads the
// data file at plain (uncompressed) byte offsets, exactly as the code did before
// the reversing cursor existed, and holds no long-lived stream state:
//
//   * make_forward_input_stream() hands each parser its own sst->data_stream over
//     the requested range; the parser owns and closes it.
//   * read_forwards() (the partition header) is a single sst->data_read.
//   * read_backwards() (the rows handed back in reverse order) goes through
//     sst->data_read, buffered in a single growing cache that mirrors the old
//     _cached_read: it holds the bytes ending at _back_cache_end, starts at 4KB
//     and doubles up to 128KB, and is trimmed as rows are handed off, so walking
//     back over consecutive rows is served from one read.
//   * prev_row_start() is pure arithmetic: logical positions are byte offsets, so
//     the preceding row starts prev_len bytes before the current one.
//
// All positions are logical byte offsets (sstable_position::*_logical).
class logical_reversing_cursor final : public reversing_source_cursor {
    shared_sstable _sst;
    reader_permit _permit;
    tracing::trace_state_ptr _trace_state;
    // Backward reads never go below this offset (the partition start).
    int64_t _lower_bound;

    // Single-buffer backward cache. Holds the bytes ending at _back_cache_end;
    // read_backwards() refills it (growing 4KB..128KB) when it does not already
    // cover the requested segment, and trims served bytes off its end so it stays
    // anchored at the top of the backward walk.
    temporary_buffer<char> _back_cache;
    int64_t _back_cache_end = 0;
    uint64_t _read_size = 4 * 1024;
    static constexpr uint64_t max_read_size = 128 * 1024;

public:
    logical_reversing_cursor(shared_sstable sst, reader_permit permit, tracing::trace_state_ptr trace_state,
            sstable_position partition_start)
        : _sst(std::move(sst))
        , _permit(std::move(permit))
        , _trace_state(std::move(trace_state))
        , _lower_bound(partition_start.to_logical())
    {}

    future<std::unique_ptr<data_consumer::continuous_data_consumer_input_stream>>
    make_forward_input_stream(sstable_position start, sstable_position end) override {
        co_return co_await _sst->data_stream(disk_read_range(start, end), _permit, _trace_state, {});
    }

    future<temporary_buffer<char>>
    read_forwards(sstable_position start, sstable_position, size_t size) override {
        co_return co_await _sst->data_read(start.to_logical(), size, _permit);
    }

    future<temporary_buffer<char>>
    read_backwards(sstable_position start, sstable_position end, size_t size) override {
        int64_t e = end.to_logical();
        // If the cache is not anchored at `end` (e.g. after a range shrink or a
        // non-contiguous jump), start a fresh one there.
        if (_back_cache_end != e) {
            _back_cache = {};
            _back_cache_end = e;
        }
        if (_back_cache.size() < size) {
            int64_t begin;
            if (_lower_bound + static_cast<int64_t>(_read_size) < e) {
                begin = std::min<int64_t>(e - static_cast<int64_t>(_read_size), start.to_logical());
            } else {
                begin = _lower_bound;
            }
            _back_cache = co_await _sst->data_read(begin, e - begin, _permit);
            _back_cache_end = e;
            _read_size = std::min<uint64_t>(max_read_size, _read_size * 2);
        }
        // Hand out the last `size` bytes and trim them off the cache so it stays
        // anchored at `start` for the next backward read. The returned view no
        // longer overlaps the cache, so the caller may mutate it.
        auto ret = _back_cache.share(_back_cache.size() - size, size);
        _back_cache.trim(_back_cache.size() - size);
        _back_cache_end = start.to_logical();
        co_return ret;
    }

    future<sstable_position> prev_row_start(sstable_position row_start, uint64_t prev_len) override {
        co_return sstable_position::from_logical(
                row_start.to_logical() - static_cast<int64_t>(prev_len));
    }

    void drop_read_ahead() override {
        // The cache re-anchors itself on the next read_backwards() (its `end` will
        // differ), but drop it now so it does not pin memory for the abandoned walk.
        _back_cache = {};
        _back_cache_end = 0;
    }

    future<> close() override {
        // Parsers own and close their own streams; data_read has no stream to close.
        return make_ready_future<>();
    }
};

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
// All IO into the data file goes through a single reversing_source_cursor,
// chosen at construction: a compressed_reversing_cursor for the physical-position
// compressed format (currently `mu`), or a logical_reversing_cursor otherwise.
// Each parser gets its own forward input stream from the cursor
// (make_forward_input_stream), the rows handed back to the sstable reader are
// read backwards from it (read_backwards), and the partition header -- handed
// back in file order -- is read forwards from it (read_forwards).
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

    // The single cursor through which all IO into the sstable data file is done:
    // it hands out the forward streams the parsers own, reads the partition header
    // forwards, and reads the rows handed back to the sstable reader backwards.
    std::unique_ptr<reversing_source_cursor> _cursor;

    std::optional<partition_header_context> _partition_header_context;
    std::optional<row_body_skipping_context> _row_skipping_context;
    sstable_position _clustering_range_start;
    sstable_position _partition_start;
    sstable_position _partition_end;

    // _row_start denotes our current position in the input stream:
    // either _partition_end or the start of some row (_row_start never lands in the middle of a row).
    // We share this position with the user (they can only read it, not modify it)
    // so they can e.g. compare it with index positions.
    sstable_position _row_start;
    sstable_position _row_end;
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

    future<> emplace_row_skipping_context(sstable_position row_start, sstable_position row_end) {
        if (_row_skipping_context) {
            co_await _row_skipping_context->close();
        }
        _row_skipping_context.emplace(
                co_await _cursor->make_forward_input_stream(row_start, row_end),
                row_start, row_end,
                _permit, _cached_column_translation);
    }

    // Builds the cursor backing all IO into the data file. For now this is always
    // a logical_reversing_cursor (data_stream/data_read); the physical-position
    // compressed cursor is added in a later commit.
    static std::unique_ptr<reversing_source_cursor> make_cursor(const shared_sstable& sst, reader_permit permit,
            tracing::trace_state_ptr trace_state, sstable_position partition_start) {
        return std::make_unique<logical_reversing_cursor>(sst, std::move(permit), std::move(trace_state),
                partition_start);
    }

public:
    partition_reversing_data_source_impl(const schema& s,
            shared_sstable sst,
            abstract_index_reader& ir,
            sstable_position partition_start,
            sstable_position partition_end,
            reader_permit permit,
            tracing::trace_state_ptr trace_state)
        : _schema(s)
        , _sst(std::move(sst))
        , _ir(ir)
        , _permit(std::move(permit))
        , _trace_state(std::move(trace_state))
        , _cursor(make_cursor(_sst, _permit, _trace_state, partition_start))
        , _partition_start(partition_start)
        , _partition_end(partition_end)
        , _row_start(_partition_end)
        , _row_end(_partition_end)
        , _cached_column_translation(_sst->get_column_translation(_schema, _sst->get_serialization_header(), _sst->features()))
    {
        sstable_cursor_log.trace("[reversing@{}] construct: partition_start={} partition_end={}", fmt::ptr(this), partition_start, partition_end);
    }

    virtual future<temporary_buffer<char>> get() override {
        sstable_cursor_log.trace("[reversing@{}] get: enter state={} row_start={} row_end={}", fmt::ptr(this), (int)_state, _row_start, _row_end);
        if (!_partition_header_context) {
            _partition_header_context.emplace(
                    co_await _cursor->make_forward_input_stream(_partition_start, _partition_end),
                    _partition_start, _permit);
            co_await _partition_header_context->consume_input();
            _clustering_range_start = _partition_header_context->header_end_pos();
            auto header_buf = co_await _cursor->read_forwards(_partition_start, _clustering_range_start, _partition_header_context->header_size());
            sstable_cursor_log.trace("[reversing@{}] get: exit (partition header) clustering_range_start={} size={}", fmt::ptr(this), _clustering_range_start, header_buf.size());
            co_return header_buf;
        }
        // The index reader still reports logical (uint64) data-file positions; wrap
        // them as logical sstable_positions here. (The typed index-reader interface
        // arrives in a later commit.)
        auto ir_end_raw = _ir.data_file_positions().end;
        std::optional<sstable_position> ir_end = ir_end_raw
                ? std::optional<sstable_position>(sstable_position::from_logical(*ir_end_raw))
                : std::nullopt;
        if (ir_end && *ir_end < _row_start) {
            // we can skip at least one row
            _row_start = *ir_end;
            // Any read-ahead the cursor kept belonged to the contiguous backward
            // walk we just abandoned; drop it so it can't be reused for the new range.
            _cursor->drop_read_ahead();
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
                    _row_start = co_await _cursor->prev_row_start(_row_start, _row_skipping_context->prev_len());
                }
            }
            if (look_in_last_block) {
                if (auto offset = co_await _ir.last_block_offset()) {
                    // there was a promoted index block in the partition, read from its beginning to find the last row.
                    // last_block_offset() still reports a logical (uint64) byte offset; wrap it.
                    _row_start = _partition_start + sstable_position_offset::from_logical(*offset);
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
                _row_end = current_row_start;
                _row_start = last_row_start;
                if (_row_start == _row_end) {
                    // empty partition
                    _state = state::FINISHED;
                    sstable_cursor_log.trace("[reversing@{}] get: exit (empty partition)", fmt::ptr(this));
                    co_return end_of_partition();
                }
            }

            if (_row_start < _clustering_range_start) {
                // The first index block starts after the range being read,
                // i.e. the range being read is empty.
                if (co_await _cursor->prev_row_start(_clustering_range_start, _row_skipping_context->prev_len()) != _partition_start) {
                    on_internal_error(sstlog, format(
                        "partition_reversing_data_source: invariant broken: _row_start({}) < _clustering_range_start({})"
                        ", but _row_skipping_context->prev_len()({}) != _clustering_range_start - _partition_start({})",
                        _row_start, _clustering_range_start, _row_skipping_context->prev_len(), _partition_start));
                }
                _row_start = _clustering_range_start;
                _state = state::FINISHED;
                sstable_cursor_log.trace("[reversing@{}] get: exit (empty range)", fmt::ptr(this));
                co_return end_of_partition();
            }

            _state = state::ROWS;
            [[fallthrough]];
        }
        case state::ROWS: {
            co_await emplace_row_skipping_context(_row_start, _row_end);
            co_await _row_skipping_context->consume_input();
            auto ret = co_await _cursor->read_backwards(_row_start, _row_end, _row_skipping_context->offset());
            if (_row_skipping_context->current_tombstone_reversing_info()) {
                modify_tombstone(ret, *_row_skipping_context->current_tombstone_reversing_info());
            }
            _row_end = _row_start;
            _row_start = co_await _cursor->prev_row_start(_row_start, _row_skipping_context->prev_len());
            if (_row_end == _clustering_range_start) {
                _state = state::PARTITION_END;
            }
            sstable_cursor_log.trace("[reversing@{}] get: exit (row) size={} row_start={} row_end={} state={}", fmt::ptr(this), ret.size(), _row_start, _row_end, (int)_state);
            co_return ret;
        }
        case state::PARTITION_END: {
            _state = state::FINISHED;
            sstable_cursor_log.trace("[reversing@{}] get: exit (partition end)", fmt::ptr(this));
            co_return end_of_partition();
        }
        case state::FINISHED:
            sstable_cursor_log.trace("[reversing@{}] get: exit (finished)", fmt::ptr(this));
            co_return temporary_buffer<char>();
        }
    }

    virtual future<temporary_buffer<char>> skip(uint64_t n) override {
        sstable_cursor_log.trace("[reversing@{}] skip: n={}", fmt::ptr(this), n);
        // Skipping is implemented by checking the index.
        on_internal_error(sstlog, "partition_reversing_data_source does not support skipping");
    }

    // Must not be run concurrently with `get()`.
    virtual future<> close() noexcept override {
        sstable_cursor_log.trace("[reversing@{}] close: row_start={}", fmt::ptr(this), _row_start);
        auto close_partition_header_context = _partition_header_context ? _partition_header_context->close() : make_ready_future<>();
        auto close_row_skipping_context = _row_skipping_context ? _row_skipping_context->close() : make_ready_future();
        auto close_cursor = _cursor ? _cursor->close() : make_ready_future<>();
        co_await when_all(std::move(close_partition_header_context), std::move(close_row_skipping_context), std::move(close_cursor));
    }

    // Points to the current position of the source over the sstable file, which
    // is either the end of partition or the beginning of some row.
    // Can only decrease.
    const sstable_position& current_position_in_sstable() const {
        return _row_start;
    }
};

partition_reversing_data_source make_partition_reversing_data_source(const schema& s, shared_sstable sst, abstract_index_reader& ir, sstable_position start, sstable_position end,
                                                          reader_permit permit, tracing::trace_state_ptr trace_state) {
    auto source_impl = std::make_unique<partition_reversing_data_source_impl>(
            s, std::move(sst), ir, start, end, std::move(permit), trace_state);
    auto& curr_pos = source_impl->current_position_in_sstable();
    return partition_reversing_data_source {
        .the_source = seastar::data_source{std::move(source_impl)},
        .current_position_in_sstable = curr_pos
    };
}

}

}
