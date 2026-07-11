/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <map>
#include <memory>
#include <optional>

#include <seastar/core/align.hh>
#include <seastar/core/byteorder.hh>
#include <seastar/core/deleter.hh>
#include <seastar/core/file.hh>
#include <seastar/core/format.hh>
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/temporary_buffer.hh>

#include "seastarx.hh"
#include "utils/assert.hh"
#include "reader_permit.hh"
#include "sstables/checksum_utils.hh"
#include "sstables/compress.hh"
#include "sstables/compressor.hh"
#include "sstables/consumer.hh"
#include "sstables/exceptions.hh"
#include "sstables/progress_monitor.hh"
#include "sstables/sstable_position.hh"

namespace sstables {

class file_cursor {
    seastar::file _file;
    uint64_t _position = 0;
    reader_permit _permit;
    uint64_t _lower_bound = 0;
    uint64_t _upper_bound;
    uint64_t _file_size;

    temporary_buffer<char> _current;
    uint64_t _current_end = 0;

    std::map<uint64_t, temporary_buffer<char>> _cache;
    bool _cache_enabled = false;

    constexpr static uint64_t initial_read_size = 4096;
    uint64_t _read_size = initial_read_size;

public:
    file_cursor(seastar::file f, uint64_t file_size, reader_permit permit)
        : _file(std::move(f))
        , _permit(std::move(permit))
        , _upper_bound(file_size)
        , _file_size(file_size)
    {}
    void set_bounds(uint64_t lower, uint64_t upper) {
        SCYLLA_ASSERT(lower <= upper);
        SCYLLA_ASSERT(upper <= _file_size);
        size_t align = _file.disk_read_dma_alignment();
        _lower_bound = align_down(lower, align);
        _upper_bound = std::min<uint64_t>(_file_size, align_up(upper, align));
        _position = std::clamp(_position, _lower_bound, _upper_bound);
    }
    void seek(uint64_t pos) {
        parse_assert(pos >= _lower_bound && pos <= _upper_bound);
        _position = std::clamp(pos, _lower_bound, _upper_bound);
        _read_size = initial_read_size;
    }
    void set_cache_enabled(bool enabled) {
        _cache_enabled = enabled;
    }
    static std::optional<temporary_buffer<char>> maybe_share_buffer_forwards(uint64_t position, uint64_t n, uint64_t buf_end, temporary_buffer<char>& buf) {
        uint64_t buf_start = buf_end - buf.size();
        if (buf_start <= position && position < buf_end) {
            n = std::min(buf_end - position, n);
            return buf.share(position - buf_start, n);
        }
        return std::nullopt;
    }
    static std::optional<temporary_buffer<char>> maybe_share_buffer_backwards(uint64_t position, uint64_t n, uint64_t buf_end, temporary_buffer<char>& buf) {
        uint64_t buf_start = buf_end - buf.size();
        if (buf_start < position && position <= buf_end) {
            n = std::min(position - buf_start, n);
            return buf.share(position - buf_start - n, n);
        }
        return std::nullopt;
    }
    future<> load_interval(decltype(_cache)::iterator hint, uint64_t begin, uint64_t end) {
        if (hint != _cache.end()) {
            end = std::min(end, hint->first - hint->second.size());
        }
        end = std::min(end, _upper_bound);
        if (hint != _cache.begin()) {
            begin = std::max(begin, std::prev(hint)->first);
        }
        begin = std::max(begin, _lower_bound);
        size_t align = _file.disk_read_dma_alignment();
        begin = align_down(begin, align);
        end = align_up(end, align);
        auto units = _permit.consume_memory(end - begin);
        auto buf = co_await _file.dma_read<char>(begin, end - begin);
        _current = temporary_buffer<char>(buf.get_write(), buf.size(), make_object_deleter(buf.release(), std::move(units)));
        _current_end = begin + _current.size();
        if (_current.size() && _cache_enabled) {
            _cache.emplace_hint(hint, std::make_pair(_current_end, _current.share()));
        }
    }
    future<temporary_buffer<char>> read_forwards_up_to(size_t n) {
        if (auto buf = maybe_share_buffer_forwards(_position, n, _current_end, _current)) {
            _position += buf->size();
            co_return std::move(*buf);
        }
        auto candidate = _cache.upper_bound(_position);
        if (candidate != _cache.end()) {
            if (auto buf = maybe_share_buffer_forwards(_position, n, candidate->first, candidate->second)) {
                _position += buf->size();
                co_return std::move(*buf);
            }
        }
        size_t begin = _position;
        size_t end = std::max(_position + n, _position + _read_size);
        co_await load_interval(candidate, begin, end);
        _read_size = std::min<uint64_t>(_read_size * 2, 128 * 1024);

        if (auto buf = maybe_share_buffer_forwards(_position, n, _current_end, _current)) {
            _position += buf->size();
            co_return std::move(*buf);
        }

        co_return temporary_buffer<char>();
    }
    future<temporary_buffer<char>> read_backwards_up_to(size_t n) {
        n = std::min(_position, n);
        if (auto buf = maybe_share_buffer_backwards(_position, n, _current_end, _current)) {
            _position -= buf->size();
            co_return std::move(*buf);
        }
        auto candidate = _cache.lower_bound(_position);
        if (candidate != _cache.end()) {
            if (auto buf = maybe_share_buffer_backwards(_position, n, candidate->first, candidate->second)) {
                _position -= buf->size();
                co_return std::move(*buf);
            }
        }
        size_t end = _position;
        size_t begin = std::min(_position - n, _position - std::clamp<uint64_t>(_read_size, 0, _position));
        co_await load_interval(candidate, begin, end);
        _read_size = std::min<uint64_t>(_read_size * 2, 128 * 1024);

        if (auto buf = maybe_share_buffer_backwards(_position, n, _current_end, _current)) {
            _position -= buf->size();
            co_return std::move(*buf);
        }

        co_return temporary_buffer<char>();
    }
    future<temporary_buffer<char>> read_forwards_exactly(size_t n) {
        auto units = _permit.consume_memory(n);
        auto out = temporary_buffer<char>(n);
        while (n) {
            auto frag = co_await read_forwards_up_to(n);
            if (frag.empty()) {
                break;
            }
            std::memcpy(out.get_write() + (out.size() - n), frag.get(), frag.size());
            n -= frag.size();
        }
        out = temporary_buffer<char>(out.get_write(), out.size(), make_object_deleter(out.release(), std::move(units)));
        out.trim(out.size() - n);
        co_return out;
    }
    future<temporary_buffer<char>> read_backwards_exactly(size_t n) {
        auto units = _permit.consume_memory(n);
        auto out = temporary_buffer<char>(n);
        while (n) {
            auto frag = co_await read_backwards_up_to(n);
            if (frag.empty()) {
                break;
            }
            std::memcpy(out.get_write() + (n - frag.size()), frag.get(), frag.size());
            n -= frag.size();
        }
        out = temporary_buffer<char>(out.get_write(), out.size(), make_object_deleter(out.release(), std::move(units)));
        out.trim_front(n);
        co_return out;
    }
    future<> close() {
        return _file.close();
    }
    uint64_t position() const {
        return _position;
    }
    uint64_t file_size() const {
        return _file_size;
    }
};

struct decompressed_chunk {
    uint64_t extent;
    temporary_buffer<char> decompressed;
};

// Reads a compressed sstable data file as a forward, decompressed byte stream,
// driven by physical positions (see sstable_position). It plugs into
// the parser framework as a continuous_data_consumer_input_stream, mirroring
// decompressing_input_stream, but backs its IO with a file_cursor -- which can
// also read backwards -- so the same object additionally serves the backward
// row walk used by the partition reversing data source (read_backwards_up_to).
//
// The stream position is tracked precisely as a physical position; there is no
// separate logical (uncompressed) offset. _current holds the single most
// recently decompressed chunk, which is always the chunk at
// _stream_position.position.chunk_position when engaged.
class compressed_file_cursor final : public data_consumer::continuous_data_consumer_input_stream {
    file_cursor _raw_cursor;
    reader_permit _permit;
    reader_position_tracker _stream_position{.position = sstable_position::from_physical(0, 0, 0)};
    std::optional<decompressed_chunk> _current;
    compressor& _compressor;
    uint64_t _uncompressed_chunk_length;
public:
    compressed_file_cursor(file_cursor raw_cursor, reader_permit permit, compressor& compressor, uint64_t uncompressed_chunk_length)
        : _raw_cursor(std::move(raw_cursor))
        , _permit(std::move(permit))
        , _compressor(compressor)
        , _uncompressed_chunk_length(uncompressed_chunk_length)
    {}
    void set_cache_enabled(bool enabled) {
        _raw_cursor.set_cache_enabled(enabled);
    }
    void set_bounds(sstable_position begin, sstable_position end) {
        uint64_t raw_begin = begin.as_physical().chunk_position;
        auto end_phys = end.as_physical();
        uint64_t raw_end;
        if (!end_phys.offset_within_chunk) {
            raw_end = end_phys.chunk_position;
        } else {
            // compressed_file_cursor::set_bounds is supposed to be called with bounds obtained from the index, which are supposed to have a chunk length hint.
            // If that's not the case, something's wrong.
            SCYLLA_ASSERT(end_phys.chunk_length_hint);
            raw_end = end_phys.chunk_position + end_phys.chunk_length_hint;
        }
        _raw_cursor.set_bounds(raw_begin, raw_end);
    }
    void seek(sstable_position pos) {
        SCYLLA_ASSERT(pos.is_physical());
        if (pos.as_physical().chunk_position != _stream_position.position.as_physical().chunk_position) {
            _current.reset();
        }
        _stream_position.position = pos;
    }
    static decompressed_chunk decompress(temporary_buffer<char> compressed, reader_permit& permit, compressor& decompressor, uint64_t uncompressed_chunk_length) {
        auto frame_len = chunk_length_field_size(uncompressed_chunk_length);
        if (compressed.size() < frame_len) {
            throw_malformed_sstable_exception(format("compressed chunk size {} smaller than minimal size {}", compressed.size(), frame_len));
        }

        // The header holds this chunk's compressed-data length; it is the
        // authoritative length, validated below by the checksum. read_chunk_length_field
        // already rejects an obviously-too-large value up front.
        const auto this_compressed_len = read_chunk_length_field(compressed.get(), uncompressed_chunk_length);
        const uint64_t total_expected_compressed_size = frame_len + this_compressed_len + 4 + frame_len;
        if (compressed.size() < total_expected_compressed_size) {
            throw_malformed_sstable_exception(format(
                    "Compressed chunk header declares size {}, but got buffer of size {}.",
                    total_expected_compressed_size, compressed.size()));
        }

        // The checksum covers the header and the compressed data, but not the
        // trailing footer.
        uint32_t actual_checksum = crc32_utils::checksum(compressed.get(), frame_len + this_compressed_len);
        uint32_t expected_checksum  = read_be<uint32_t>(compressed.get() + frame_len + this_compressed_len);
        if (expected_checksum != actual_checksum) {
            throw_malformed_sstable_exception(format(
                    "compressed chunk failed checksum, expected={}, actual={}",
                    expected_checksum, actual_checksum));
        }

        temporary_buffer<char> out(uncompressed_chunk_length);
        size_t n = decompressor.uncompress(compressed.get() + frame_len, this_compressed_len, out.get_write(), out.size());
        auto units = permit.consume_memory(uncompressed_chunk_length);
        out = temporary_buffer<char>(out.get_write(), out.size(), make_object_deleter(out.release(), std::move(units)));
        out.trim(n);
        return decompressed_chunk{
            .extent = frame_len + this_compressed_len + 4 + frame_len,
            .decompressed = std::move(out),
        };
    }
    future<> load_chunk_at_position(bool forward) {
        auto& pos = _stream_position.position.as_physical();
        auto chunk_length_hint = pos.chunk_length_hint;
        auto chunk_position = pos.chunk_position;
        temporary_buffer<char> raw;
        if (forward) {
            if (!chunk_length_hint) {
                // Read this chunk's header (at chunk_position) to learn its length,
                // then size the read of the whole chunk (header + data + checksum +
                // footer).
                _raw_cursor.seek(chunk_position);
                auto frame_len = chunk_length_field_size(_uncompressed_chunk_length);
                auto header_buf = co_await _raw_cursor.read_forwards_exactly(frame_len);
                auto this_len = read_chunk_length_field(header_buf.get(), _uncompressed_chunk_length);
                chunk_length_hint = 2 * frame_len + this_len + 4;
            }
            auto end = chunk_position + chunk_length_hint;
            _raw_cursor.seek(chunk_position);
            raw = co_await _raw_cursor.read_forwards_exactly(end - chunk_position);
        } else {
            if (!chunk_length_hint) {
                // Read this chunk's header (at chunk_position) to learn its length.
                auto frame_len = chunk_length_field_size(_uncompressed_chunk_length);
                _raw_cursor.seek(chunk_position + frame_len);
                auto header_buf = co_await _raw_cursor.read_backwards_exactly(frame_len);
                auto this_len = read_chunk_length_field(header_buf.get(), _uncompressed_chunk_length);
                chunk_length_hint = 2 * frame_len + this_len + 4;
            }
            // chunk_length_hint only bounds the on-disk chunk end from above and may
            // point past the end of the file when this chunk is the last content in
            // it; clamp to the file size so we never seek past EOF.
            auto end = std::min<uint64_t>(chunk_position + chunk_length_hint, _raw_cursor.file_size());
            _raw_cursor.seek(end);
            raw = co_await _raw_cursor.read_backwards_exactly(_raw_cursor.position() - chunk_position);
        }
        _current = decompress(std::move(raw), _permit, _compressor, _uncompressed_chunk_length);
        pos.chunk_length_hint = _current->extent;
    }
    future<temporary_buffer<char>> read_backwards_up_to(size_t n) {
        if (n == 0) {
            co_return temporary_buffer<char>();
        }
        auto& pos = _stream_position.position.as_physical();

        if (pos.offset_within_chunk == 0) {
            if (pos.chunk_position == 0) {
                co_return temporary_buffer<char>();
            }
            // We are at the start of this chunk and need to step back into the
            // previous one. The bytes immediately before this chunk are the
            // previous chunk's footer, holding its compressed-data length; read
            // just those to compute the previous chunk's extent and start, without
            // having to load and decompress this chunk first.
            auto frame_len = chunk_length_field_size(_uncompressed_chunk_length);
            _raw_cursor.seek(pos.chunk_position);
            auto footer_buf = co_await _raw_cursor.read_backwards_exactly(frame_len);
            auto prev_len = read_chunk_length_field(footer_buf.get(), _uncompressed_chunk_length);
            auto predecessor_extent = 2 * frame_len + prev_len + 4;
            if (predecessor_extent > static_cast<uint64_t>(pos.chunk_position)) {
                throw_malformed_sstable_exception(format(
                        "compressed chunk footer at file offset {} declares a previous chunk of size {} that starts before the file",
                        pos.chunk_position, predecessor_extent));
            }
            pos.chunk_position -= predecessor_extent;
            pos.chunk_length_hint = predecessor_extent;
            _current.reset();
        }
        if (!_current) {
            co_await load_chunk_at_position(false);
            if (pos.offset_within_chunk == 0) {
                pos.offset_within_chunk = _current->decompressed.size();
            }
        }
        n = std::min<uint64_t>(n, pos.offset_within_chunk);
        pos.offset_within_chunk -= n;
        co_return _current->decompressed.share(pos.offset_within_chunk, n);
    }
    future<temporary_buffer<char>> read_backwards_exactly(size_t n) {
        auto units = _permit.consume_memory(n);
        auto out = temporary_buffer<char>(n);
        while (n) {
            auto frag = co_await read_backwards_up_to(n);
            if (frag.empty()) {
                break;
            }
            std::memcpy(out.get_write() + (n - frag.size()), frag.get(), frag.size());
            n -= frag.size();
        }
        out = temporary_buffer<char>(out.get_write(), out.size(), make_object_deleter(out.release(), std::move(units)));
        out.trim_front(n);
        co_return out;
    }

    // --- continuous_data_consumer_input_stream interface ---

    void init_stream_position(sstable_position start) override {
        SCYLLA_ASSERT(start.is_physical());
        seek(start);
        _stream_position.offset = 0;
    }

    const reader_position_tracker& stream_position() const override {
        return _stream_position;
    }

    future<> skip_to(sstable_position target) override {
        // The cursor tracks its own position, so we seek straight to `target`.
        // seek() keeps the decompressed chunk if `target` lands in it and drops
        // it otherwise.
        seek(target);
        return make_ready_future<>();
    }

    future<> skip(uint64_t n) override {
        uint64_t remaining = n;
        while (remaining != 0) {
            co_await consume_one(std::nullopt, [&] (temporary_buffer<char> data) -> consumption_result<char> {
                if (data.empty()) {
                    throw_malformed_sstable_exception(format(
                            "compressed_file_cursor hit end-of-file while skipping {} bytes", remaining));
                }
                if (remaining >= data.size()) {
                    remaining -= data.size();
                    return continue_consuming{};
                }
                data.trim_front(remaining);
                remaining = 0;
                return stop_consuming<char>{std::move(data)};
            });
        }
    }

    future<consumption_result<char>> consume_one(std::optional<sstable_position> end_position, consumer_one_fn consumer) override {
        auto& pos = _stream_position.position.as_physical();
        if (static_cast<uint64_t>(pos.chunk_position) == _raw_cursor.file_size()) {
            co_return consumer(temporary_buffer<char>());
        }
        if (!_current) {
            co_await load_chunk_at_position(true);
        }

        const int64_t chunk_pos = pos.chunk_position;
        const size_t in_chunk = static_cast<size_t>(pos.offset_within_chunk);
        SCYLLA_ASSERT(in_chunk <= _current->decompressed.size());
        size_t size = _current->decompressed.size() - in_chunk;
        if (end_position) {
            // Clip the buffer so the consumer never sees past `end_position`.
            SCYLLA_ASSERT(end_position->is_physical());
            if (*end_position <= _stream_position.position) {
                size = 0;
            } else {
                const auto end_ph = end_position->as_physical();
                if (end_ph.chunk_position == chunk_pos) {
                    size = std::min<size_t>(size, end_ph.offset_within_chunk - in_chunk);
                }
            }
        }

        auto data = _current->decompressed.share(in_chunk, size);

        _stream_position.offset += size;
        pos.offset_within_chunk += size;
        bool whole_chunk_consumed = false;
        if (static_cast<uint64_t>(pos.offset_within_chunk) == _current->decompressed.size()) {
            pos.chunk_position = pos.chunk_position + _current->extent;
            pos.offset_within_chunk = 0;
            pos.chunk_length_hint = 0; // next chunk's extent is unknown until loaded
            whole_chunk_consumed = true;
        }

        auto result = consumer(std::move(data));
        if (auto* stop = std::get_if<stop_consuming<char>>(&result.get())) {
            const size_t remainder = stop->get_buffer().size();
            SCYLLA_ASSERT(remainder <= size);
            if (remainder != 0) {
                if (pos.offset_within_chunk == 0) {
                    pos.chunk_position = pos.chunk_position - _current->extent;
                    pos.offset_within_chunk = _current->decompressed.size();
                    pos.chunk_length_hint = _current->extent;
                }
                pos.offset_within_chunk -= remainder;
                _stream_position.offset -= remainder;
                whole_chunk_consumed = false;
            }
        }
        if (whole_chunk_consumed) {
            _current.reset();
        }
        co_return result;
    }

    sstable_position compute_relative_position(int64_t offset) override {
        if (offset == 0) {
            return _stream_position.position;
        }
        // Only ever called from within a consumer callback to locate a point
        // inside the buffer just handed out, so the target lies within _current
        // (the chunk the buffer came from) and the offset is non-positive.
        SCYLLA_ASSERT(offset <= 0);
        SCYLLA_ASSERT(_current.has_value());
        auto& pos = _stream_position.position.as_physical();
        if (pos.offset_within_chunk == 0) {
            // The position sits exactly on a chunk boundary (the buffer consumed
            // the previous chunk to its end); _current is still that previous
            // chunk, so step back into it.
            SCYLLA_ASSERT(static_cast<uint64_t>(-offset) <= _current->decompressed.size());
            const int64_t chunk_start = pos.chunk_position - _current->extent;
            return sstable_position::from_physical(
                    chunk_start, _current->extent,
                    _current->decompressed.size() + offset);
        }
        return sstable_position::from_physical(
                pos.chunk_position, _current->extent,
                pos.offset_within_chunk + offset);
    }

    future<> close() override {
        return _raw_cursor.close();
    }
};

} // namespace sstables
