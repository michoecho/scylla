/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <stdexcept>
#include <cstdlib>

#include <seastar/core/align.hh>
#include <seastar/core/bitops.hh>
#include <seastar/core/byteorder.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/on_internal_error.hh>

#include "compress.hh"
#include "compression_info_cache.hh"
#include "compressor.hh"
#include "exceptions.hh"
#include "unimplemented.hh"
#include "segmented_compress_params.hh"
#include "utils/assert.hh"
#include "utils/class_registrator.hh"
#include "reader_permit.hh"
#include "data_source_types.hh"

namespace sstables {

extern logging::logger sstlog;

std::pair<bucket_info, segment_info> params_for_chunk_size(uint32_t chunk_size) {
    const uint8_t chunk_size_log2 = log2ceil(chunk_size);

    auto it = std::ranges::find_if(bucket_infos, [&] (const bucket_info& bi) {
        return bi.chunk_size_log2 == chunk_size_log2;
    });

    // This scenario should be so rare that we only fall back to a safe
    // set of parameters, not optimal ones.
    if (it == bucket_infos.end()) {
        const uint8_t data_size = bucket_infos.front().best_data_size_log2;
        return {{chunk_size_log2, data_size, (8 * bucket_size - 56) / data_size},
            {chunk_size_log2, data_size, uint8_t(1)}};
    }

    auto b = *it;
    auto s = *std::ranges::find_if(segment_infos, [&] (const segment_info& si) {
        return si.data_size_log2 == b.best_data_size_log2 && si.chunk_size_log2 == b.chunk_size_log2;
    });

    return {std::move(b), std::move(s)};
}

void compression::segmented_offsets::state::update_position_trackers(std::size_t index, uint16_t segment_size_bits,
        uint32_t segments_per_bucket, uint8_t grouped_offsets) {
    if (_current_index != index - 1) {
        _current_index = index;
        const uint64_t current_segment_index = _current_index / grouped_offsets;
        _current_bucket_segment_index = current_segment_index % segments_per_bucket;
        _current_segment_relative_index = _current_index % grouped_offsets;
        _current_bucket_index = current_segment_index / segments_per_bucket;
        _current_segment_offset_bits = (_current_bucket_segment_index % segments_per_bucket) * segment_size_bits;
    } else {
        ++_current_index;
        ++_current_segment_relative_index;

        // Crossed segment boundary.
        if (_current_segment_relative_index == grouped_offsets) {
            ++_current_bucket_segment_index;
            _current_segment_relative_index = 0;

            // Crossed bucket boundary.
            if (_current_bucket_segment_index == segments_per_bucket) {
                ++_current_bucket_index;
                _current_bucket_segment_index = 0;
                _current_segment_offset_bits = 0;
            } else {
                _current_segment_offset_bits += segment_size_bits;
            }
        }
    }
}

void compression::segmented_offsets::init(uint32_t chunk_size) {
    if (chunk_size == 0) {
        throw_malformed_sstable_exception("Segmented offsets chunk size is zero.");
    }

    _chunk_size = chunk_size;

    const auto params = params_for_chunk_size(chunk_size);

    sstlog.trace(
            "{} {}(): chunk size {} (log2)",
            fmt::ptr(this),
            __FUNCTION__,
            static_cast<int>(params.first.chunk_size_log2));

    const uint8_t grouped_offsets = params.second.grouped_offsets;
    const uint8_t segment_base_offset_size_bits = params.second.data_size_log2;
    const uint8_t segmented_offset_size_bits = static_cast<uint64_t>(log2ceil((_chunk_size + 64) * (grouped_offsets - 1)));
    _packing = offset_packing(segment_base_offset_size_bits, segmented_offset_size_bits, grouped_offsets);
    _segments_per_bucket = params.first.segments_per_bucket;
}

uint64_t compression::segmented_offsets::at(std::size_t i, compression::segmented_offsets::state& s) const {
    if (i >= _size) {
        throw std::out_of_range(format("{}: index {} is out of range", __FUNCTION__, i));
    }

    s.update_position_trackers(i, _packing.segment_bits(), _segments_per_bucket, _packing.grouped_offsets());
    const char* storage = _storage[s._current_bucket_index].storage.get();
    const uint64_t bucket_base_offset = _storage[s._current_bucket_index].base_offset;
    const uint64_t segment_base_offset = bucket_base_offset + _packing.read_base(storage, s._current_segment_offset_bits);

    if (s._current_segment_relative_index == 0) {
        return segment_base_offset;
    }

    return segment_base_offset
        + _packing.read_relative(storage, s._current_segment_offset_bits, s._current_segment_relative_index);
}

void compression::segmented_offsets::push_back(uint64_t offset, compression::segmented_offsets::state& s) {
    s.update_position_trackers(_size, _packing.segment_bits(), _segments_per_bucket, _packing.grouped_offsets());

    if (s._current_bucket_index == _storage.size()) {
        _storage.push_back(bucket{_last_written_offset, std::unique_ptr<char[]>(new char[bucket_size])});
    }

    char* storage = _storage[s._current_bucket_index].storage.get();
    const uint64_t bucket_base_offset = _storage[s._current_bucket_index].base_offset;

    if (s._current_segment_relative_index == 0) {
        _packing.write_base(storage, s._current_segment_offset_bits, offset - bucket_base_offset);
    } else {
        const uint64_t segment_base_offset = bucket_base_offset + _packing.read_base(storage, s._current_segment_offset_bits);
        _packing.write_relative(storage, s._current_segment_offset_bits, s._current_segment_relative_index,
                offset - segment_base_offset);
    }
    _last_written_offset = offset;
    ++_size;
}

void compression::set_compressor(compressor_ptr c) {
    options.elements.clear();
    if (c) {
        unqualified_name uqn(compression_parameters::name_prefix, c->name());
        const sstring& cn = uqn;
        name.value = bytes(cn.begin(), cn.end());
        for (auto& [k, v] : c->options()) {
            if (k != compression_parameters::SSTABLE_COMPRESSION) {
                options.elements.push_back({
                    {bytes(k.begin(), k.end())},
                    {bytes(v.begin(), v.end())}
                });
            }
        }
    }
    _compressor = std::move(c);
}

void compression::discard_hidden_options() {
    auto is_hidden_option = [] (const option& o) -> bool {
        auto k_str = std::string_view(reinterpret_cast<const char*>(o.key.value.data()), o.key.value.size());
        return compressor::is_hidden_option_name(k_str);
    };
    decltype(options) filtered_options;
    for (auto& e : options.elements) {
        if (!is_hidden_option(e)) {
            filtered_options.elements.emplace_back(std::move(e));
        }
    }
    options = std::move(filtered_options);
}

compressor& compression::get_compressor() const {
    SCYLLA_ASSERT(_compressor);
    return *_compressor.get();
}

void compression::update(uint64_t compressed_file_length) {
    _compressed_file_length = compressed_file_length;
}

std::map<sstring, sstring> options_from_compression(const compression& c) {
    std::map<sstring, sstring> result;
    result.emplace(compression_parameters::SSTABLE_COMPRESSION, sstring(c.name.value.begin(), c.name.value.end()));
    result.emplace(compression_parameters::CHUNK_LENGTH_KB, to_sstring(c.uncompressed_chunk_length() / 1024));
    for (const auto& [k, v] : c.options.elements) {
        auto k_str = sstring(k.value.begin(), k.value.end());
        auto v_str = sstring(v.value.begin(), v.value.end());
        if (compressor::is_hidden_option_name(k_str)) {
            continue;
        }
        result.emplace(std::move(k_str), std::move(v_str));
    }
    return result;
}

} // namespace sstables

// For SSTables 2.x (formats 'ka' and 'la'), the full checksum is a combination of checksums of compressed chunks.
// For SSTables 3.x (format 'mc'), however, it is supposed to contain the full checksum of the file written so
// the per-chunk checksums also count.
enum class compressed_checksum_mode {
    checksum_chunks_only,
    checksum_all,
};

template <ChecksumUtils ChecksumType, bool check_digest, compressed_checksum_mode mode>
class compressed_file_data_source_impl : public data_source_impl {
    sstables::stream_creator_fn _stream_creator;
    file_input_stream_options _options;
    std::optional<input_stream<char>> _input_stream;
    std::unique_ptr<sstables::compression_info_accessor> _compression_info;
    [[no_unique_address]] sstables::digest_members<check_digest> _digests;
    reader_permit _permit;
    uint64_t _underlying_pos;
    uint64_t _pos;
    uint64_t _beg_pos;
    // The position the stream was opened at. Unlike _beg_pos, it isn't moved by
    // skip(), because the extent of the underlying stream is determined once,
    // when it is created.
    uint64_t _initial_beg_pos;
    uint64_t _end_pos;
private:
    // Opens the underlying stream, if it isn't open yet.
    //
    // This is done lazily, and not in the constructor, because translating
    // [_beg_pos, _end_pos) to a range of compressed chunks requires looking up
    // the chunk offsets, which can block on reading CompressionInfo.db.
    future<> maybe_open_stream() {
        if (_input_stream) {
            co_return;
        }
        // _initial_beg_pos and _end_pos specify positions in the uncompressed
        // stream. We need to translate them into a range of compressed chunks,
        // and open a file_input_stream to read that range.
        auto start = co_await _compression_info->locate(_initial_beg_pos);
        auto end = co_await _compression_info->locate(_end_pos - 1);
        _underlying_pos = start.chunk_start;
        _input_stream = co_await _stream_creator(start.chunk_start, end.chunk_start + end.chunk_len - start.chunk_start, _options);
    }
public:
    compressed_file_data_source_impl(sstables::stream_creator_fn stream_creator,
                std::unique_ptr<sstables::compression_info_accessor> ca,
                uint64_t pos, size_t len, file_input_stream_options options,
                reader_permit permit, std::optional<uint32_t> digest)
            : _stream_creator(std::move(stream_creator))
            , _options(std::move(options))
            , _compression_info(std::move(ca))
            , _permit(std::move(permit))
    {
        _pos = _beg_pos = _initial_beg_pos = pos;
        if (pos > _compression_info->uncompressed_file_length()) {
            throw std::runtime_error("attempt to uncompress beyond end");
        }
        if (len == 0 || pos == _compression_info->uncompressed_file_length()) {
            // Nothing to read
            _end_pos = _pos;
            return;
        }
        if (len <= _compression_info->uncompressed_file_length() - pos) {
            _end_pos = pos + len;
        } else {
            _end_pos = _compression_info->uncompressed_file_length();
        }
        if constexpr (check_digest) {
            if (!digest) {
                on_internal_error(sstables::sstlog, "Requested digest check but no digest was provided.");
            }
            if (_end_pos - _pos < _compression_info->uncompressed_file_length()) {
                sstables::sstlog.debug("Compressed reader cannot calculate digest with partial read: current pos={}, end pos={}, uncompressed file len={}. Disabling digest check.",
                        _pos, _end_pos, _compression_info->uncompressed_file_length());
                _digests = {false};
            } else {
                _digests = {true, *digest, ChecksumType::init_checksum()};
            }
        }
    }
    virtual future<temporary_buffer<char>> get() override {
        if (_pos >= _end_pos) {
            co_return temporary_buffer<char>();
        }

        co_await maybe_open_stream();
        auto addr = co_await _compression_info->locate(_pos);
        // Uncompress the next chunk. We need to skip part of the first
        // chunk, but then continue to read from beginning of chunks.
        if (_pos != _beg_pos && addr.offset != 0) {
            throw std::runtime_error(format("compressed reader not aligned to chunk boundary: pos={} offset={}", _pos, addr.offset));
        }
        if (!addr.chunk_len) {
            sstables::throw_malformed_sstable_exception(format("compressed chunk_len must be greater than zero, chunk_start={}", addr.chunk_start));
        }
        auto buf = co_await _input_stream->read_exactly(addr.chunk_len);
        if (buf.size() != addr.chunk_len) {
            sstables::throw_malformed_sstable_exception(format("compressed reader hit premature end-of-file at file offset {}, expected chunk_len={}, actual={}", _underlying_pos, addr.chunk_len, buf.size()));
        }
        auto res_units = co_await _permit.request_memory(_compression_info->uncompressed_chunk_length());
        // The last 4 bytes of the chunk are the adler32/crc32 checksum
        // of the rest of the (compressed) chunk.
        auto compressed_len = addr.chunk_len - 4;
        // FIXME: Do not always calculate checksum - Cassandra has a
        // probability (defaulting to 1.0, but still...)
        auto expected_checksum = read_be<uint32_t>(buf.get() + compressed_len);
        auto actual_checksum = ChecksumType::checksum(buf.get(), compressed_len);
        if (expected_checksum != actual_checksum) {
            sstables::throw_malformed_sstable_exception(format("compressed chunk of size {} at file offset {} failed checksum, expected={}, actual={}", addr.chunk_len, _underlying_pos, expected_checksum, actual_checksum));
        }

        if constexpr (check_digest) {
            if (_digests.can_calculate_digest) {
                _digests.actual_digest = checksum_combine_or_feed<ChecksumType>(_digests.actual_digest, actual_checksum, buf.get(), compressed_len);
                if constexpr (mode == compressed_checksum_mode::checksum_all) {
                    uint32_t be_actual_checksum = cpu_to_be(actual_checksum);
                    _digests.actual_digest = ChecksumType::checksum(_digests.actual_digest,
                            reinterpret_cast<const char*>(&be_actual_checksum), sizeof(be_actual_checksum));
                }
            }
        }

        // We know that the uncompressed data will take exactly
        // chunk_length bytes (or less, if reading the last chunk).
        temporary_buffer<char> out(
                _compression_info->uncompressed_chunk_length());
        // The compressed data is the whole chunk, minus the last 4
        // bytes (which contain the checksum verified above).

        auto len = _compression_info->get_compressor().uncompress(buf.get(), compressed_len, out.get_write(), out.size());

        out.trim(len);
        out.trim_front(addr.offset);
        _pos += out.size();
        _underlying_pos += addr.chunk_len;

        if constexpr (check_digest) {
            if (_digests.can_calculate_digest
                    && _pos == _compression_info->uncompressed_file_length()
                    && _digests.expected_digest != _digests.actual_digest) {
                sstables::throw_malformed_sstable_exception(seastar::format("Digest mismatch: expected={}, actual={}", _digests.expected_digest, _digests.actual_digest));
            }
        }
        co_return make_tracked_temporary_buffer(std::move(out), std::move(res_units));
    }

    virtual future<> close() override {
        if (!_input_stream) {
            return make_ready_future<>();
        }
        return _input_stream->close();
    }

    virtual future<temporary_buffer<char>> skip(uint64_t n) override {
        if constexpr (check_digest) {
            if (_digests.can_calculate_digest) {
                sstables::sstlog.debug("Compressed reader cannot calculate digest with skipped data: current pos={}, end pos={}, skip len={}. Disabling digest check.", _pos, _end_pos, n);
                _digests.can_calculate_digest = false;
            }
        }
        if (_pos + n > _end_pos) {
            on_internal_error(sstables::sstlog, format("Skipping over the end position is disallowed: current pos={}, end pos={}, skip len={}", _pos, _end_pos, n));
        }
        _pos += n;
        if (_pos == _end_pos) {
            co_return temporary_buffer<char>();
        }
        co_await maybe_open_stream();
        auto addr = co_await _compression_info->locate(_pos);
        auto underlying_n = addr.chunk_start - _underlying_pos;
        _underlying_pos = addr.chunk_start;
        _beg_pos = _pos;
        co_await _input_stream->skip(underlying_n);
        co_return temporary_buffer<char>();
    }
};

template <bool check_digest>
class compressed_raw_file_data_source_impl : public data_source_impl {
    std::function<future<input_stream<char>>()> _stream_creator;
    std::optional<input_stream<char>> _input_stream;
    std::unique_ptr<sstables::compression_info_accessor> _compression_info;
    [[no_unique_address]] sstables::digest_members<check_digest> _digests;
    reader_permit _permit;
    uint64_t _pos{0};
    uint64_t _current_chunk_index{0};

public:
    compressed_raw_file_data_source_impl(sstables::stream_creator_fn stream_creator,
                std::unique_ptr<sstables::compression_info_accessor> ca,
                file_input_stream_options options,
                reader_permit permit, std::optional<uint32_t> digest)
            : _compression_info(std::move(ca))
            , _permit(std::move(permit))
    {
        if constexpr (check_digest) {
            if (!digest) {
                on_internal_error(sstables::sstlog, "Requested digest check but no digest was provided.");
            }
            _digests = {true, *digest, crc32_utils::init_checksum()};
        }

        _stream_creator = [stream_creator{std::move(stream_creator)}, start = _pos, length = _compression_info->compressed_file_length(), options] mutable {
            return stream_creator(start, length, std::move(options));
        };
    }

    virtual future<temporary_buffer<char>> get() override {
        if (_pos >= _compression_info->compressed_file_length()) {
            co_return temporary_buffer<char>();
        }

        if (!_input_stream) {
            _input_stream = co_await _stream_creator();
        }

        auto chunk_len = (co_await _compression_info->get_chunk_by_index(_current_chunk_index)).chunk_len;
        if (!chunk_len) {
            sstables::throw_malformed_sstable_exception(format("compressed raw reader chunk_len must be greater than zero, pos={}", _pos));
        }

        auto res_units = co_await _permit.request_memory(chunk_len);
        auto buf = co_await _input_stream->read_exactly(chunk_len);
        if (buf.size() != chunk_len) {
            sstables::throw_malformed_sstable_exception(format("compressed raw reader hit premature end-of-file at file offset {}, expected chunk_len={}, actual={}", _pos, chunk_len, buf.size()));
        }

        auto compressed_len = chunk_len - 4;
        auto expected_checksum = read_be<uint32_t>(buf.get() + compressed_len);
        auto actual_checksum = crc32_utils::checksum(buf.get(), compressed_len);
        if (expected_checksum != actual_checksum) {
            sstables::throw_malformed_sstable_exception(format("compressed chunk of size {} at file offset {} failed checksum, expected={}, actual={}", chunk_len, _pos, expected_checksum, actual_checksum));
        }

        if constexpr (check_digest) {
            if (_digests.can_calculate_digest) {
                _digests.actual_digest = checksum_combine_or_feed<crc32_utils>(_digests.actual_digest, actual_checksum, buf.get(), compressed_len);
                uint32_t be_actual_checksum = cpu_to_be(actual_checksum);
                _digests.actual_digest = crc32_utils::checksum(_digests.actual_digest,
                        reinterpret_cast<const char*>(&be_actual_checksum), sizeof(be_actual_checksum));
            }
        }

        _current_chunk_index++;
        _pos += buf.size();

        if constexpr (check_digest) {
            if (_digests.can_calculate_digest
                    && _current_chunk_index == _compression_info->chunk_count()
                    && _digests.expected_digest != _digests.actual_digest) {
                sstables::throw_malformed_sstable_exception(seastar::format("Digest mismatch: expected={}, actual={}", _digests.expected_digest, _digests.actual_digest));
            }
        }

        co_return make_tracked_temporary_buffer(std::move(buf), std::move(res_units));
    }

    virtual future<> close() override {
        if (!_input_stream) {
            return make_ready_future<>();
        }
        return _input_stream->close();
    }

    virtual future<temporary_buffer<char>> skip(uint64_t n) override {
        throw std::runtime_error("compressed raw file data source does not support skip()");
    }
};

template <bool check_digest>
class compressed_raw_file_data_source : public data_source {
public:
    compressed_raw_file_data_source(sstables::stream_creator_fn stream_creator,
            std::unique_ptr<sstables::compression_info_accessor> ca,
            file_input_stream_options options, reader_permit permit, std::optional<uint32_t> digest)
        : data_source(std::make_unique<compressed_raw_file_data_source_impl<check_digest>>(
                std::move(stream_creator), std::move(ca), std::move(options), std::move(permit), digest))
        {}
};

template <ChecksumUtils ChecksumType, bool check_digest, compressed_checksum_mode mode>
class compressed_file_data_source : public data_source {
public:
    compressed_file_data_source(sstables::stream_creator_fn stream_creator,
            std::unique_ptr<sstables::compression_info_accessor> ca,
            uint64_t offset, size_t len, file_input_stream_options options, reader_permit permit,
            std::optional<uint32_t> digest)
        : data_source(std::make_unique<compressed_file_data_source_impl<ChecksumType, check_digest, mode>>(
                std::move(stream_creator), std::move(ca), offset, len, std::move(options), std::move(permit), digest))
        {}
};

template <ChecksumUtils ChecksumType, compressed_checksum_mode mode>
inline input_stream<char> make_compressed_file_input_stream(sstables::stream_creator_fn stream_creator,
        std::unique_ptr<sstables::compression_info_accessor> ca, uint64_t offset, size_t len,
        file_input_stream_options options, reader_permit permit,
        std::optional<uint32_t> digest)
{
    if (digest) [[unlikely]] {
        return input_stream<char>(compressed_file_data_source<ChecksumType, true, mode>(
                std::move(stream_creator), std::move(ca), offset, len, std::move(options), std::move(permit), digest));
    }
    return input_stream<char>(compressed_file_data_source<ChecksumType, false, mode>(
            std::move(stream_creator), std::move(ca), offset, len, std::move(options), std::move(permit), digest));
}

// compressed_file_data_sink_impl works as a filter for a file output stream,
// where the buffer flushed will be compressed and its checksum computed, then
// the result passed to a regular output stream.
template <typename ChecksumType, compressed_checksum_mode mode>
requires ChecksumUtils<ChecksumType>
class compressed_file_data_sink_impl : public data_sink_impl {
    output_stream<char> _out;
    sstables::compression* _compression_metadata;
    sstables::compression::segmented_offsets::writer _offsets;
    size_t _pos = 0;
    uint32_t _full_checksum;
public:
    compressed_file_data_sink_impl(output_stream<char> out, sstables::compression* cm)
            : _out(std::move(out))
            , _compression_metadata(cm)
            , _offsets(_compression_metadata->offsets.get_writer())
            , _full_checksum(ChecksumType::init_checksum())
    {}

private:
    future<> do_put(temporary_buffer<char> buf) {
        auto output_len = _compression_metadata->get_compressor().compress_max_size(buf.size());

        // account space for checksum that goes after compressed data.
        temporary_buffer<char> compressed(output_len + 4);

        // compress flushed data.
        auto len = _compression_metadata->get_compressor().compress(buf.get(), buf.size(), compressed.get_write(), output_len);
        if (len > output_len) {
            return make_exception_future(std::runtime_error("possible overflow during compression"));
        }

        // total length of the uncompressed data.
        _compression_metadata->set_uncompressed_file_length(_compression_metadata->uncompressed_file_length() + buf.size());

        _offsets.push_back(_pos);
        // account compressed data + 32-bit checksum.
        _pos += len + 4;
        _compression_metadata->set_compressed_file_length(_pos);

        // compute 32-bit checksum for compressed data.
        uint32_t per_chunk_checksum = ChecksumType::checksum(compressed.get(), len);
        _full_checksum = checksum_combine_or_feed<ChecksumType>(_full_checksum, per_chunk_checksum, compressed.get(), len);

        // write checksum into buffer after compressed data.
        write_be<uint32_t>(compressed.get_write() + len, per_chunk_checksum);

        if constexpr (mode == compressed_checksum_mode::checksum_all) {
            uint32_t be_per_chunk_checksum = cpu_to_be(per_chunk_checksum);
            _full_checksum = ChecksumType::checksum(_full_checksum,
                reinterpret_cast<const char*>(&be_per_chunk_checksum), sizeof(be_per_chunk_checksum));
        }

        _compression_metadata->set_full_checksum(_full_checksum);

        compressed.trim(len + 4);

        auto f = _out.write(compressed.get(), compressed.size());
        return f.then([compressed = std::move(compressed)] {});
    }
public:
    virtual future<> put(std::span<temporary_buffer<char>> bufs) override {
        return data_sink_impl::fallback_put(bufs, [this] (temporary_buffer<char>&& buf) {
            return do_put(std::move(buf));
        });
    }

    virtual future<> close() override {
        return _out.close();
    }

    virtual size_t buffer_size() const noexcept override {
        return _compression_metadata->uncompressed_chunk_length();
    }
};

template <typename ChecksumType, compressed_checksum_mode mode>
requires ChecksumUtils<ChecksumType>
class compressed_file_data_sink : public data_sink {
public:
    compressed_file_data_sink(output_stream<char> out, sstables::compression* cm)
        : data_sink(std::make_unique<compressed_file_data_sink_impl<ChecksumType, mode>>(
                std::move(out), cm)) {}
};

template <typename ChecksumType, compressed_checksum_mode mode>
requires ChecksumUtils<ChecksumType>
inline output_stream<char> make_compressed_file_output_stream(output_stream<char> out,
         sstables::compression* cm,
         const compression_parameters& cp,
         compressor_ptr p) {
    cm->set_compressor(std::move(p));
    // buffer of output stream is set to chunk length, because flush must
    // happen every time a chunk was filled up.
    cm->set_uncompressed_chunk_length(cp.chunk_length());
    // FIXME: crc_check_chance can be configured by the user.
    // probability to verify the checksum of a compressed chunk we read.
    // defaults to 1.0.
    cm->options.elements.push_back({{"crc_check_chance"}, {"1.0"}});

    return output_stream<char>(compressed_file_data_sink<ChecksumType, mode>(std::move(out), cm));
}

input_stream<char> sstables::make_compressed_file_k_l_format_input_stream(stream_creator_fn stream_creator,
        std::unique_ptr<compression_info_accessor> ca, uint64_t offset, size_t len,
        class file_input_stream_options options, reader_permit permit,
        std::optional<uint32_t> digest)
{
    return make_compressed_file_input_stream<adler32_utils, compressed_checksum_mode::checksum_chunks_only>(
            std::move(stream_creator), std::move(ca), offset, len, std::move(options), std::move(permit), digest);
}

input_stream<char> sstables::make_compressed_file_m_format_input_stream(stream_creator_fn stream_creator,
        std::unique_ptr<compression_info_accessor> ca, uint64_t offset, size_t len,
        class file_input_stream_options options, reader_permit permit,
        std::optional<uint32_t> digest) {
    return make_compressed_file_input_stream<crc32_utils, compressed_checksum_mode::checksum_all>(
            std::move(stream_creator), std::move(ca), offset, len, std::move(options), std::move(permit), digest);
}

output_stream<char> sstables::make_compressed_file_m_format_output_stream(output_stream<char> out,
        sstables::compression* cm,
        const compression_parameters& cp,
        compressor_ptr p) {
    return make_compressed_file_output_stream<crc32_utils, compressed_checksum_mode::checksum_all>(
            std::move(out), cm, cp, std::move(p));
}

input_stream<char> sstables::make_compressed_raw_file_input_stream(sstables::stream_creator_fn stream_creator,
        std::unique_ptr<compression_info_accessor> ca,
        file_input_stream_options options, reader_permit permit, std::optional<uint32_t> digest)
{
    if (digest) [[unlikely]] {
        return input_stream<char>(compressed_raw_file_data_source<true>(
                std::move(stream_creator), std::move(ca), std::move(options), std::move(permit), digest));
    }
    return input_stream<char>(compressed_raw_file_data_source<false>(
            std::move(stream_creator), std::move(ca), std::move(options), std::move(permit), digest));
}
