/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "sstables/decompressing_input_stream.hh"

#include <algorithm>
#include <optional>

#include <seastar/core/align.hh>
#include <seastar/core/byteorder.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/iostream.hh>
#include <seastar/util/log.hh>
#include <seastar/util/noncopyable_function.hh>

#include "sstables/checksum_utils.hh"
#include "sstables/compress.hh"
#include "sstables/compressor.hh"
#include "sstables/exceptions.hh"
#include "sstables/sstables.hh"
#include "tracing/traced_file.hh"

template <>
struct fmt::formatter<std::optional<sstables::sstable_position>> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const std::optional<sstables::sstable_position>& p, fmt::format_context& ctx) const {
        if (p.has_value()) {
            return fmt::format_to(ctx.out(), "{}", "nullopt");
        }
        if (auto* l = std::get_if<sstables::sstable_position::logical>(&p.value()._value)) {
            return fmt::format_to(ctx.out(), "{}", l->value);
        }
        auto* ph = std::get_if<sstables::sstable_position::physical>(&p.value()._value);
        return fmt::format_to(ctx.out(), "{{chunk_position={}, chunk_length={}, offset_within_chunk={}}}",
            ph->chunk_position, ph->chunk_length_hint, ph->offset_within_chunk);
    }
};

namespace sstables {

logging::logger decompressing_input_stream_log("decompressing_input_stream");

namespace {

static constexpr uint64_t disk_block_size = 512;

struct chunk {
    uint64_t position;
    uint64_t extent;
    temporary_buffer<char> data;
};

class decompressing_input_stream final : public data_consumer::continuous_data_consumer_input_stream {
    decompressing_input_stream_source_opener _open_source;
    const compressor& _compressor;
    uint64_t _uncompressed_chunk_length;
    uint64_t _compressed_file_length;
    reader_position_tracker _stream_position;

    // Forward-only compressed byte stream. Buffering, block alignment and
    // read-ahead are handled by input_stream itself, so this class never
    // reimplements read_exactly()/skip(). _input_pos is the disk offset of the
    // next byte the stream would return.
    std::optional<input_stream<char>> _input;
    uint64_t _input_pos = 0;

    std::optional<chunk> _chunk;

    // Whole-file digest check. When _expected_digest is engaged (a digest check
    // was requested), each decompressed chunk folds its checksum into
    // _actual_digest, exactly as the old data-source impl did, so the running
    // digest reproduces the whole-file digest stored in Digest.crc. The digest is
    // only meaningful if every chunk is folded in exactly once, in order, so a
    // chunk is folded in only when its file position matches
    // _next_digest_chunk_position; that position advances past each folded chunk,
    // so a rewound re-read (whose position now lies before it) is naturally
    // ignored. _next_digest_chunk_position starts at 0 (the first chunk), and once
    // it reaches _compressed_file_length the whole file has been covered and the
    // digest is checked. This stream is only used for mc+ (crc32) sstables, so the
    // fold uses crc32 and also folds each chunk's trailing 4-byte checksum, like
    // the m-format data source.
    std::optional<uint32_t> _expected_digest;
    uint32_t _actual_digest = crc32_utils::init_checksum();
    uint64_t _next_digest_chunk_position = 0;

    future<> open_input() {
        _input = input_stream<char>(co_await _open_source());
    }

    future<temporary_buffer<char>> do_read_exactly(uint64_t n) {
        auto buf = co_await _input->read_exactly(n);
        if (buf.size() < n) {
            throw_malformed_sstable_exception(format("Unexpected eof at {}, wanted {}, got {}.",
                    _input_pos + buf.size(), n, buf.size()));
        }
        _input_pos += n;
        co_return buf;
    }

    future<> load_chunk() {
        if (!_input) {
            co_await open_input();
        }
        auto chunk_start = static_cast<uint64_t>(_stream_position.position.as_physical().chunk_position);
        auto offset_in_chunk = static_cast<uint64_t>(_stream_position.position.as_physical().offset_within_chunk);
        if (_input_pos != chunk_start) {
            SCYLLA_ASSERT(_input_pos < chunk_start);
            co_await _input->skip(chunk_start - _input_pos);
            _input_pos = chunk_start;
        }
        auto frame_len = chunk_length_field_size(_uncompressed_chunk_length);
        auto header = co_await do_read_exactly(frame_len);
        const auto compressed_len = read_chunk_length_field(header.get(), _uncompressed_chunk_length);
        // read_chunk_length_field only rejects lengths beyond compressed_chunk_length_limit,
        // so a corrupt header can still name a chunk that overruns the file. Reject
        // that here as a corrupt length rather than letting the body read run off
        // the end and surface as a generic end-of-file error: every valid chunk
        // ends at or before the compressed file length. The chunk's on-disk extent
        // is header + compressed data + checksum + footer.
        const auto chunk_extent = frame_len + compressed_len + 4 + frame_len;
        const auto chunk_end = chunk_start + chunk_extent;
        if (chunk_end > _compressed_file_length) {
            throw_malformed_sstable_exception(format(
                    "compressed chunk at file offset {} has a corrupt length header: chunk of size {} would end at {}, past the compressed file length {}",
                    chunk_start, chunk_extent, chunk_end, _compressed_file_length));
        }
        // Read the compressed data, its checksum and the trailing footer in one go.
        // The footer is not used when reading forward (it exists for backward
        // navigation) but we still consume it to advance to the next chunk.
        auto body = co_await do_read_exactly(compressed_len + 4 + frame_len);
        const uint32_t expected_checksum = read_be<uint32_t>(body.get() + compressed_len);
        uint32_t actual_checksum = crc32_utils::checksum(header.get(), header.size());
        actual_checksum = crc32_utils::checksum(actual_checksum, body.get(), compressed_len);
        if (expected_checksum != actual_checksum) {
            throw_malformed_sstable_exception(format(
                    "compressed chunk of size {} at file offset {} failed checksum, expected={}, actual={}",
                    chunk_extent, chunk_start, expected_checksum, actual_checksum));
        }
        temporary_buffer<char> out(_uncompressed_chunk_length);
        const size_t n = _compressor.uncompress(body.get(), compressed_len, out.get_write(), out.size());
        out.trim(n);
        // Fold this chunk into the whole-file digest, but only when it is the
        // chunk we still need (its file position matches). This skips chunks read
        // out of order or re-read after a rewind, so each chunk is folded once, in
        // order. Once the running position reaches end of file, the digest covers
        // the whole file and is verified.
        if (_expected_digest && chunk_start == _next_digest_chunk_position) {
            // The whole-file digest is a plain crc32 over every byte of the file,
            // so fold in the whole chunk in on-disk order: header, compressed data,
            // per-chunk checksum, footer.
            _actual_digest = crc32_utils::checksum(_actual_digest, header.get(), header.size());
            _actual_digest = crc32_utils::checksum(_actual_digest, body.get(), compressed_len);
            const uint32_t be_checksum = cpu_to_be(actual_checksum);
            _actual_digest = crc32_utils::checksum(_actual_digest,
                    reinterpret_cast<const char*>(&be_checksum), sizeof(be_checksum));
            _actual_digest = crc32_utils::checksum(_actual_digest,
                    body.get() + compressed_len + 4, frame_len);
            _next_digest_chunk_position = chunk_start + chunk_extent;
            if (_next_digest_chunk_position == _compressed_file_length) {
                if (_actual_digest != *_expected_digest) {
                    throw_malformed_sstable_exception(format(
                            "Digest mismatch: expected={}, actual={}", *_expected_digest, _actual_digest));
                }
                _expected_digest.reset();
            }
        }
        if (offset_in_chunk > out.size()) {
            throw_malformed_sstable_exception(format(
                    "In compressed chunk at position {}, ended up with in-chunk offset {} beyond chunk size {}",
                    chunk_start, offset_in_chunk, out.size()));
        }
        _chunk = chunk{chunk_start, chunk_extent, std::move(out)};
    }

    future<> ensure_chunk_if_not_eof() {
        if (_chunk) {
            return make_ready_future<>();
        }
        if (static_cast<uint64_t>(_stream_position.position.as_physical().chunk_position) == _compressed_file_length) {
            return make_ready_future<>();
        }
        return load_chunk();
    }

public:
    decompressing_input_stream(decompressing_input_stream_source_opener open_source, sstable_position start,
            const compressor& compressor, uint32_t uncompressed_chunk_length, uint64_t compressed_file_length,
            std::optional<uint32_t> digest)
        : _open_source(std::move(open_source))
        , _compressor(compressor)
        , _uncompressed_chunk_length(uncompressed_chunk_length)
        , _compressed_file_length(compressed_file_length)
        , _stream_position(start)
        , _input_pos(align_down<uint64_t>(start.as_physical().chunk_position, disk_block_size))
        , _expected_digest(digest)
    {
        SCYLLA_ASSERT(start.is_physical());
    }

    void init_stream_position(sstable_position pos) override {
        SCYLLA_ASSERT(_stream_position.position == pos);
    }

    const reader_position_tracker& stream_position() const override {
        return _stream_position;
    }

    future<> skip_to(sstable_position target) override {
        if (target < _stream_position.position) {
            throw_malformed_sstable_exception(format("decompressing_input_stream cannot seek backwards: current={}, target={}",
                    _stream_position.position, target));
        }
        auto ph = target.as_physical();
        SCYLLA_ASSERT(ph.offset_within_chunk >= 0);
        const bool target_in_cached_chunk = _chunk && _chunk->position == uint64_t(ph.chunk_position);
        if (target_in_cached_chunk) {
            _stream_position.position.as_physical().offset_within_chunk = ph.offset_within_chunk;
        } else {
            _stream_position.position = target;
            _chunk.reset();
        }
        return make_ready_future<>();
    }

    future<> skip(uint64_t n) override {
        uint64_t remaining = n;
        while (remaining != 0) {
            co_await ensure_chunk_if_not_eof();
            if (!_chunk) {
                throw_malformed_sstable_exception(format("decompressing_input_stream hit end-of-file while skipping {} bytes", remaining));
            }
            const int64_t chunk_size = _chunk->data.size();
            const uint64_t in_chunk = _stream_position.position.as_physical().offset_within_chunk;
            const uint64_t available = chunk_size - in_chunk;
            const uint64_t step = std::min(remaining, available);
            auto& physical = _stream_position.position.as_physical();
            physical.offset_within_chunk += step;
            _stream_position.offset += step;
            remaining -= step;
            if (physical.offset_within_chunk == chunk_size) {
                physical.offset_within_chunk = 0;
                physical.chunk_position = physical.chunk_position + _chunk->extent;
                _chunk.reset();
            }
        }
    }

    future<consumption_result<char>> consume_one(std::optional<sstable_position> end_position, consumer_one_fn consumer) override {
        decompressing_input_stream_log.trace("[decompressing_stream@{}] consume_one: enter pos={} end={}",
                fmt::ptr(this), _stream_position.position, end_position);
        auto& pos = _stream_position.position.as_physical();
        co_await ensure_chunk_if_not_eof();
        if (!_chunk) {
            temporary_buffer<char> empty;
            auto result = consumer(std::move(empty));
            co_return result;
        }

        const int64_t chunk_pos = static_cast<int64_t>(pos.chunk_position);
        const size_t in_chunk = static_cast<size_t>(pos.offset_within_chunk);
        size_t size = _chunk->data.size() - in_chunk;
        if (end_position) {
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

        auto data = _chunk->data.share(in_chunk, size);

        _stream_position.offset += size;
        pos.offset_within_chunk += size;
        bool whole_chunk_consumed = false;
        if (pos.offset_within_chunk == static_cast<int64_t>(_chunk->data.size())) {
            pos.chunk_position = pos.chunk_position + _chunk->extent;
            pos.offset_within_chunk = 0;
            whole_chunk_consumed = true;
        }

        auto result = consumer(std::move(data));
        if (auto* stop = std::get_if<stop_consuming<char>>(&result.get())) {
            const size_t remainder = stop->get_buffer().size();
            SCYLLA_ASSERT(remainder <= size);
            if (remainder != 0) {
                if (pos.offset_within_chunk == 0) {
                    pos.chunk_position -= _chunk->extent;
                    pos.offset_within_chunk = _chunk->data.size();
                }
                pos.offset_within_chunk -= remainder;
                _stream_position.offset -= remainder;
                whole_chunk_consumed = false;
            }
        }
        if (whole_chunk_consumed) {
            _chunk.reset();
        }
        co_return result;
    }

    sstable_position compute_relative_position(int64_t offset) override {
        if (offset == 0) {
            return _stream_position.position;
        }
        SCYLLA_ASSERT(offset <= 0);
        SCYLLA_ASSERT(_chunk.has_value());
        auto& pos = _stream_position.position.as_physical();
        if (pos.offset_within_chunk == 0) {
            SCYLLA_ASSERT(static_cast<uint64_t>(-offset) <= _chunk.value().data.size());
            const int64_t chunk_start = pos.chunk_position - _chunk->extent;
            return sstable_position::from_physical(
                    chunk_start, _chunk->extent, _chunk->data.size() + offset);
        }
        return sstable_position::from_physical(
                pos.chunk_position, _chunk->extent, pos.offset_within_chunk + offset);
    }

    future<> close() override {
        if (_input) {
            co_await _input->close();
            _input.reset();
        }
    }
};

} // anonymous namespace

std::unique_ptr<data_consumer::continuous_data_consumer_input_stream>
make_decompressing_input_stream(decompressing_input_stream_source_opener open_source,
        sstable_position start, const compressor& compressor,
        uint32_t uncompressed_chunk_length, uint64_t compressed_file_length,
        std::optional<uint32_t> digest) {
    decompressing_input_stream_log.trace("make_decompressing_input_stream(source_opener): enter start={} uncompressed_chunk_length={} compressed_file_length={}",
            start, uncompressed_chunk_length, compressed_file_length);
    auto result = std::make_unique<decompressing_input_stream>(
            std::move(open_source), start, compressor, uncompressed_chunk_length, compressed_file_length, digest);
    decompressing_input_stream_log.trace("make_decompressing_input_stream(source_opener): exit stream={}", fmt::ptr(result.get()));
    return result;
}

std::unique_ptr<data_consumer::continuous_data_consumer_input_stream>
make_decompressing_input_stream(shared_sstable sst, disk_read_range range,
        reader_permit permit, tracing::trace_state_ptr trace_state, std::optional<uint32_t> digest) {
    decompressing_input_stream_log.trace("make_decompressing_input_stream(sstable): enter range_start={} range_end={}",
            range.start, range.end);
    SCYLLA_ASSERT(range.start.is_physical());
    const auto start = range.start.as_physical();
    const uint64_t source_start = align_down<uint64_t>(start.chunk_position, disk_block_size);
    const uint64_t source_len = sst->ondisk_data_size() - source_start;
    auto opener = [sst, permit, trace_state, source_start, source_len] () -> future<data_source> {
        file_input_stream_options options;
        options.buffer_size = seastar::align_up<uint64_t>(4096, sst->get_data_file().disk_read_dma_alignment());
        file f = make_tracked_file(sst->get_data_file(), permit);
        if (trace_state) {
            f = tracing::make_traced_file(std::move(f), trace_state, seastar::format("{}:", sst->get_filename()));
        }
        return sst->get_storage().make_data_or_index_source(
                *sst, component_type::Data, std::move(f), source_start, source_len, std::move(options));
    };
    auto result = make_decompressing_input_stream(
            decompressing_input_stream_source_opener(std::move(opener)), range.start, sst->get_compression().get_compressor(),
            sst->get_compression().uncompressed_chunk_length(), sst->ondisk_data_size(), digest);
    decompressing_input_stream_log.trace("make_decompressing_input_stream(sstable): exit stream={} source_start={} source_len={}",
            fmt::ptr(result.get()), source_start, source_len);
    return result;
}

} // namespace sstables
