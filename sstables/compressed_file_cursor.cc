/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <cstdlib>
#include <limits>
#include <map>
#include <optional>

#include <seastar/core/align.hh>
#include <seastar/core/file.hh>
#include <seastar/core/format.hh>
#include <seastar/core/byteorder.hh>
#include <seastar/core/on_internal_error.hh>

#include "sstables/compressed_file_cursor.hh"
#include "sstables/compress.hh"
#include "sstables/compressor.hh"
#include "sstables/checksum_utils.hh"
#include "sstables/exceptions.hh"
#include "sstables/sstables.hh"
#include "tracing/traced_file.hh"

namespace sstables {

class sstable_datafile_cursor::impl {
public:
    virtual ~impl() = default;
    virtual void seek(sstable_datafile_position pos) = 0;
    virtual future<temporary_buffer<char>> read_forwards(size_t n) = 0;
    virtual future<temporary_buffer<char>> read_backwards(size_t n) = 0;
    virtual sstable_datafile_position compute_relative_position(ssize_t offset) = 0;
    virtual void drop_caches_after(sstable_datafile_position pos) = 0;
    virtual void drop_caches_before(sstable_datafile_position pos) = 0;
    virtual future<> close() = 0;
};

namespace {

// Caches byte ranges of an immutable file, keyed by file offset.
//
// Each stored run [offset, offset + buf.size()) is contiguous, and runs never
// overlap. Because the backing file is immutable a cached byte never goes
// stale, so the cache only ever needs to add ranges and drop them on request.
class byte_range_cache {
    // Maps a file offset to a buffer holding the bytes at
    // [offset, offset + buf.size()). Entries never overlap.
    std::map<uint64_t, temporary_buffer<char>> _runs;

public:
    // Copy as many bytes as are contiguously cached starting at pos into dst
    // (up to len). Returns the number of bytes copied, which is 0 if pos itself
    // is not cached.
    size_t copy(uint64_t pos, char* dst, size_t len) const {
        size_t done = 0;
        while (done < len) {
            uint64_t want = pos + done;
            auto it = _runs.upper_bound(want);
            if (it == _runs.begin()) {
                break;
            }
            --it;
            uint64_t run_start = it->first;
            uint64_t run_end = run_start + it->second.size();
            if (want >= run_end) {
                break; // not cached
            }
            size_t avail = run_end - want;
            size_t n = std::min(avail, len - done);
            std::copy_n(it->second.get() + (want - run_start), n, dst + done);
            done += n;
        }
        return done;
    }

    // Copy the contiguously-cached suffix of [pos, pos+len) into the matching
    // tail of dst, working backwards from the end. Returns the number of bytes
    // copied, which is 0 if the last byte (pos+len-1) is not cached.
    size_t copy_tail(uint64_t pos, char* dst, size_t len) const {
        size_t done = 0;
        while (done < len) {
            uint64_t want = pos + len - done; // one past the next byte to fill
            auto it = _runs.upper_bound(want - 1);
            if (it == _runs.begin()) {
                break;
            }
            --it;
            uint64_t run_start = it->first;
            uint64_t run_end = run_start + it->second.size();
            if (want > run_end) {
                break; // gap just before want; suffix ends here
            }
            // Bytes available in this run ending at want, bounded by what's left.
            size_t avail = want - run_start;
            size_t n = std::min(avail, len - done);
            std::copy_n(it->second.get() + (want - run_start - n), n, dst + (len - done - n));
            done += n;
        }
        return done;
    }

    // Insert [offset, offset+buf.size()) into the cache. The range may overlap
    // runs that are already cached (e.g. from block-aligned reads), so only the
    // gaps not yet covered are stored; existing runs are left as-is.
    void insert(uint64_t offset, temporary_buffer<char> buf) {
        uint64_t pos = offset;
        uint64_t end = offset + buf.size();
        while (pos < end) {
            // Find the next cached run that starts at or after pos.
            auto next = _runs.lower_bound(pos);
            // If the preceding run covers pos, skip past it.
            if (next != _runs.begin()) {
                auto prev = std::prev(next);
                uint64_t prev_end = prev->first + prev->second.size();
                if (prev_end > pos) {
                    pos = prev_end;
                    continue;
                }
            }
            // Gap runs from pos up to the start of the next run (or end).
            uint64_t gap_end = next != _runs.end() ? std::min(end, next->first) : end;
            if (gap_end > pos) {
                _runs.emplace(pos, buf.share(pos - offset, gap_end - pos));
            }
            pos = gap_end;
        }
    }

    // Drop everything at or after p, trimming the run that straddles p.
    void drop_after(uint64_t p) {
        auto it = _runs.lower_bound(p);
        if (it != _runs.begin()) {
            auto prev = std::prev(it);
            if (prev->first + prev->second.size() > p) {
                prev->second.trim(p - prev->first);
            }
        }
        _runs.erase(it, _runs.end());
    }

    // Drop everything before p, trimming the run that straddles p.
    void drop_before(uint64_t p) {
        auto it = _runs.lower_bound(p);
        if (it != _runs.begin()) {
            auto prev = std::prev(it);
            uint64_t prev_end = prev->first + prev->second.size();
            if (prev_end > p) {
                auto tail = prev->second.share();
                tail.trim_front(p - prev->first);
                _runs.emplace(p, std::move(tail));
            }
        }
        _runs.erase(_runs.begin(), it);
    }
};

// Cursor implementation for uncompressed sstables.
//
// For uncompressed data the logical (uncompressed) position equals the
// physical file offset, so there is no chunk translation to do. All reads
// are cached by file offset (see byte_range_cache); since the data file is
// immutable, a cached byte never goes stale.
//
// read_backwards reads the requested range directly from the data file.
// read_forwards instead streams: it opens a forward data_source at the first
// uncached byte it needs and pulls buffers from it, which lets the storage
// layer read ahead. The stream and its position are kept between calls so a
// sequential scan reuses the same stream instead of reopening it.
class uncompressed_file_cursor_impl final : public sstable_datafile_cursor::impl {
    shared_sstable _sst;
    reader_permit _permit;
    tracing::trace_state_ptr _trace_state;
    std::optional<sstable_datafile_position> _position;

    // Cached file contents, keyed by file offset.
    byte_range_cache _cache;

    // Disk block size of the data file. Reads are aligned to this so that we
    // never read part of a block and throw the rest away; the whole block ends
    // up cached. Taken from the file rather than hardcoded because filesystems
    // and devices report different alignments.
    uint64_t _block_size;

    // Forward streaming state, reused across read_forwards() calls.
    std::optional<data_source> _forward_source;
    // File offset that _forward_source will return next. Everything the stream
    // has produced so far has been stored in _cache, so this is always on a
    // buffer boundary.
    uint64_t _forward_pos = 0;

    // Size of the next direct file read for a backward read. It doubles after
    // every read so that long chains of backward reads issue progressively
    // larger IO ops, up to a cap.
    uint64_t _file_read_size = min_file_read_size;
    static constexpr uint64_t min_file_read_size = 1024;
    static constexpr uint64_t max_file_read_size = 128 * 1024;

    // Length of the byte stream this cursor reads. For an uncompressed data
    // file this is the uncompressed data size; when the cursor is used to read
    // the raw compressed file (by the compressed cursor) it is the on-disk size.
    uint64_t _file_length;

public:
    explicit uncompressed_file_cursor_impl(shared_sstable sst, reader_permit permit, tracing::trace_state_ptr trace_state,
            std::optional<uint64_t> file_length = std::nullopt)
        : _sst(std::move(sst))
        , _permit(std::move(permit))
        , _trace_state(std::move(trace_state))
        , _block_size(_sst->get_data_file().disk_read_dma_alignment())
        , _file_length(file_length.value_or(_sst->data_size())) {
    }

    void seek(sstable_datafile_position pos) override {
        _position = pos;
    }

    future<temporary_buffer<char>> read_forwards(size_t n) override {
        SCYLLA_ASSERT(_position.has_value());
        uint64_t start = _position->to_logical_fixme();
        temporary_buffer<char> result(n);
        size_t filled = co_await fill_forwards(start, result.get_write(), n);
        result.trim(filled);
        _position = sstable_datafile_position::from_logical_fixme(start + filled);
        co_return result;
    }

    future<temporary_buffer<char>> read_backwards(size_t n) override {
        SCYLLA_ASSERT(_position.has_value());
        uint64_t end = _position->to_logical_fixme();
        uint64_t start = end >= n ? end - n : 0;
        size_t len = end - start;
        temporary_buffer<char> result(len);
        co_await fill_from_file(start, result.get_write(), len);
        _position = sstable_datafile_position::from_logical_fixme(start);
        co_return result;
    }

    sstable_datafile_position compute_relative_position(ssize_t offset) override {
        SCYLLA_ASSERT(_position.has_value());
        return sstable_datafile_position::from_logical_fixme(_position->to_logical_fixme() + offset);
    }

    void drop_caches_after(sstable_datafile_position pos) override {
        _cache.drop_after(pos.to_logical_fixme());
    }

    void drop_caches_before(sstable_datafile_position pos) override {
        _cache.drop_before(pos.to_logical_fixme());
    }

    future<> close() override {
        if (_forward_source) {
            co_await _forward_source->close();
        }
    }

private:
    // Copy len bytes starting at offset into dst, serving from cache where
    // possible and reading the rest forwards via the streaming source.
    // Returns the number of bytes actually produced (less than len at EOF).
    future<size_t> fill_forwards(uint64_t offset, char* dst, size_t len) {
        size_t done = 0;
        while (done < len) {
            uint64_t pos = offset + done;
            size_t from_cache = _cache.copy(pos, dst + done, len - done);
            if (from_cache) {
                done += from_cache;
                continue;
            }
            // First uncached byte is at pos; pull it (and following bytes)
            // from the forward stream, caching what we read.
            size_t produced = co_await pull_forwards(pos, dst + done, len - done);
            if (produced == 0) {
                break; // EOF
            }
            done += produced;
        }
        co_return done;
    }

    // Ensure pos is cached by reading forwards from the streaming source, then
    // copy out up to len contiguous cached bytes from pos. Returns bytes
    // produced (0 at EOF).
    future<size_t> pull_forwards(uint64_t pos, char* dst, size_t len) {
        // Reuse the stream only if it sits at or before pos (it can advance
        // forwards but not seek back); otherwise reopen it block-aligned.
        if (!_forward_source || _forward_pos > pos) {
            co_await open_forward_stream(pos);
        }
        // Skip whole blocks between the stream position and pos's block without
        // reading them: that data was not requested and may be far away.
        co_await skip_forward_to(seastar::align_down(pos, _block_size));
        // Pull (and cache) whole buffers until pos itself is covered. The bytes
        // before pos share a block with it, so they come from disk for free.
        while (_forward_pos <= pos) {
            auto buf = co_await _forward_source->get();
            if (buf.empty()) {
                co_return 0; // EOF before the requested byte
            }
            _cache.insert(_forward_pos, buf.share());
            _forward_pos += buf.size();
        }
        // pos is now cached; let the caller read it straight from the cache.
        co_return _cache.copy(pos, dst, len);
    }

    // Advance the stream to target (>= _forward_pos) without requesting the
    // bytes in between. skip() may over-read; cache whatever it hands back.
    future<> skip_forward_to(uint64_t target) {
        if (_forward_pos >= target) {
            co_return;
        }
        uint64_t to_skip = target - _forward_pos;
        auto over_read = co_await _forward_source->skip(to_skip);
        _cache.insert(target, std::move(over_read));
        _forward_pos = target;
    }

    // (Re)open the forward streaming source so that it covers pos. The stream
    // starts at the block boundary at or below pos so reads stay block-aligned;
    // callers advance it up to pos.
    future<> open_forward_stream(uint64_t pos) {
        if (_forward_source) {
            co_await _forward_source->close();
            _forward_source.reset();
        }
        uint64_t start = seastar::align_down(pos, _block_size);
        uint64_t file_len = _file_length;
        uint64_t len = start <= file_len ? file_len - start : 0;
        file_input_stream_options options;
        options.buffer_size = seastar::align_up<uint64_t>(4096, _block_size);
        file f = make_tracked_file(_sst->get_data_file(), _permit);
        if (_trace_state) {
            f = tracing::make_traced_file(std::move(f), _trace_state, seastar::format("{}:", _sst->get_filename()));
        }
        _forward_source = co_await _sst->get_storage().make_data_or_index_source(
                *_sst, component_type::Data, std::move(f), start, len, std::move(options));
        _forward_pos = start;
    }

    // Fill dst with [offset, offset+len) for a backward read, reading uncached
    // bytes directly from the data file. Works from the tail: each iteration
    // serves the cached suffix, then reads one growing chunk ending just past
    // the next gap, so long backward-read chains issue progressively larger IOs.
    future<> fill_from_file(uint64_t offset, char* dst, size_t len) {
        size_t filled = 0; // bytes filled at the tail of [offset, offset+len)
        while (filled < len) {
            // Serve whatever contiguous suffix is already cached.
            filled += _cache.copy_tail(offset, dst, len - filled);
            if (filled >= len) {
                break;
            }
            // The byte just before the cached tail is uncached; that is the end
            // of the range we read. Align it up to a whole block, then extend
            // the start back by the current read size (also block-aligned).
            uint64_t gap_end = offset + len - filled;
            uint64_t read_end = seastar::align_up(gap_end, _block_size);
            uint64_t read_start = read_end > _file_read_size ? seastar::align_down(read_end - _file_read_size, _block_size) : 0;

            file f = make_tracked_file(_sst->get_data_file(), _permit);
            auto buf = co_await f.dma_read<char>(read_start, read_end - read_start);
            // Grow the read size for the next disk read in the chain.
            _file_read_size = std::min(_file_read_size * 2, max_file_read_size);
            if (read_start + buf.size() < gap_end) {
                break; // EOF before the byte we need
            }
            _cache.insert(read_start, std::move(buf));
        }
    }
};

// Cursor implementation for compressed sstables.
//
// The cursor works in logical (uncompressed) positions. Compressed data is
// stored as a sequence of independently-compressed chunks of a fixed
// uncompressed length (the last chunk may be shorter). The compression
// metadata maps a logical position to the chunk that contains it and to the
// chunk's byte range in the data file.
//
// All physical IO is delegated to an inner uncompressed_file_cursor_impl that
// reads the raw compressed data file: we seek it to a chunk's compressed start
// and read the chunk's compressed bytes. The inner cursor's own cache absorbs
// the block-aligned over-reads, so reading the chunks of a range in order
// reuses a single forward stream. We then verify the chunk's trailing checksum
// and decompress it.
//
// Two things are cached:
//  - Chunk metadata (logical/physical position and size) keyed by chunk index,
//    so repeated lookups don't re-walk the segmented offsets.
//  - The single most recently decompressed chunk. We deliberately keep only
//    one (and not a generic map) so that small sequential reads within a chunk
//    cost a plain bounds check rather than a map lookup.
class compressed_file_cursor_impl final : public sstable_datafile_cursor::impl {
    shared_sstable _sst;
    reader_permit _permit;
    tracing::trace_state_ptr _trace_state;

    const compression& _compression;
    compression::segmented_offsets::accessor _offsets;
    // Reads the raw compressed data file; all physical IO goes through here.
    uncompressed_file_cursor_impl _file;

    std::optional<sstable_datafile_position> _position;

    // Metadata of a single compressed chunk.
    struct chunk_meta {
        uint64_t logical_pos;   // uncompressed offset of the chunk's first byte
        uint64_t logical_len;   // uncompressed length of the chunk
        uint64_t physical_pos;  // compressed offset in the data file
        uint64_t physical_len;  // compressed length, including the 4-byte checksum
    };

    // Cache of chunk metadata, keyed by chunk index.
    std::map<uint64_t, chunk_meta> _chunk_meta_cache;

    // The single cached decompressed chunk.
    // FIXME: this is uncool. Use std::optional instead of an in-band value. 
    uint64_t _cached_chunk_index = std::numeric_limits<uint64_t>::max();
    temporary_buffer<char> _cached_chunk; // uncompressed bytes of _cached_chunk_index

    uint64_t _uncompressed_chunk_length;
    uint64_t _uncompressed_file_length;

public:
    explicit compressed_file_cursor_impl(shared_sstable sst, reader_permit permit, tracing::trace_state_ptr trace_state)
        : _sst(std::move(sst))
        , _permit(std::move(permit))
        , _trace_state(std::move(trace_state))
        , _compression(_sst->get_compression())
        , _offsets(_compression.offsets.get_accessor())
        , _file(_sst, _permit, _trace_state, _sst->ondisk_data_size())
        , _uncompressed_chunk_length(_compression.uncompressed_chunk_length())
        , _uncompressed_file_length(_compression.uncompressed_file_length()) {
    }

    void seek(sstable_datafile_position pos) override {
        _position = pos;
    }

    future<temporary_buffer<char>> read_forwards(size_t n) override {
        SCYLLA_ASSERT(_position.has_value());
        uint64_t start = _position->to_logical_fixme();
        uint64_t end = std::min<uint64_t>(start + n, _uncompressed_file_length);
        size_t len = end > start ? end - start : 0;
        temporary_buffer<char> result(len);
        size_t filled = co_await fill(start, result.get_write(), len);
        result.trim(filled);
        _position = sstable_datafile_position::from_logical_fixme(start + filled);
        co_return result;
    }

    future<temporary_buffer<char>> read_backwards(size_t n) override {
        SCYLLA_ASSERT(_position.has_value());
        uint64_t end = _position->to_logical_fixme();
        uint64_t start = end >= n ? end - n : 0;
        size_t len = end - start;
        temporary_buffer<char> result(len);
        co_await fill(start, result.get_write(), len);
        _position = sstable_datafile_position::from_logical_fixme(start);
        co_return result;
    }

    sstable_datafile_position compute_relative_position(ssize_t offset) override {
        SCYLLA_ASSERT(_position.has_value());
        return sstable_datafile_position::from_logical_fixme(_position->to_logical_fixme() + offset);
    }

    void drop_caches_after(sstable_datafile_position pos) override {
        uint64_t p = pos.to_logical_fixme();
        // FIXME: doesn't this erase the current chunk even if pos is in the middle?
        // We only want to drop things that are fully in the [pos; +inf) range.
        uint64_t chunk_index = p / _uncompressed_chunk_length;
        _chunk_meta_cache.erase(_chunk_meta_cache.lower_bound(chunk_index), _chunk_meta_cache.end());
        if (_cached_chunk_index >= chunk_index) {
            drop_cached_chunk();
        }
        // Translate to the physical position and drop the underlying file cache.
        _file.drop_caches_after(sstable_datafile_position::from_logical_fixme(physical_pos_of(p)));
    }

    void drop_caches_before(sstable_datafile_position pos) override {
        uint64_t p = pos.to_logical_fixme();
        uint64_t chunk_index = p / _uncompressed_chunk_length;
        _chunk_meta_cache.erase(_chunk_meta_cache.begin(), _chunk_meta_cache.lower_bound(chunk_index));
        if (_cached_chunk_index < chunk_index) {
            drop_cached_chunk();
        }
        _file.drop_caches_before(sstable_datafile_position::from_logical_fixme(physical_pos_of(p)));
    }

    future<> close() override {
        return _file.close();
    }

private:
    void drop_cached_chunk() {
        _cached_chunk_index = std::numeric_limits<uint64_t>::max();
        _cached_chunk = {};
    }

    // Physical (compressed) file offset of the chunk that contains logical
    // position p, clamped so EOF maps to the end of the compressed file.
    uint64_t physical_pos_of(uint64_t p) {
        if (p >= _uncompressed_file_length) {
            return _sst->ondisk_data_size();
        }
        return get_chunk_meta(p / _uncompressed_chunk_length).physical_pos;
    }

    // Look up (and cache) the metadata of a chunk by its index.
    const chunk_meta& get_chunk_meta(uint64_t chunk_index) {
        auto it = _chunk_meta_cache.find(chunk_index);
        if (it != _chunk_meta_cache.end()) {
            return it->second;
        }
        uint64_t logical_pos = chunk_index * _uncompressed_chunk_length;
        auto addr = _compression.locate(logical_pos, _offsets);
        uint64_t logical_len = std::min<uint64_t>(_uncompressed_chunk_length, _uncompressed_file_length - logical_pos);
        chunk_meta m{logical_pos, logical_len, addr.chunk_start, addr.chunk_len};
        return _chunk_meta_cache.emplace(chunk_index, m).first->second;
    }

    // Copy len uncompressed bytes starting at logical position offset into dst,
    // decompressing whatever chunks are needed. Returns the number of bytes
    // produced (less than len only at EOF).
    future<size_t> fill(uint64_t offset, char* dst, size_t len) {
        size_t done = 0;
        while (done < len) {
            uint64_t pos = offset + done;
            if (pos >= _uncompressed_file_length) {
                break; // EOF
            }
            uint64_t chunk_index = pos / _uncompressed_chunk_length;
            co_await ensure_chunk_cached(chunk_index);
            const auto& m = get_chunk_meta(chunk_index);
            uint64_t in_chunk = pos - m.logical_pos;
            size_t avail = _cached_chunk.size() - in_chunk;
            size_t n = std::min(avail, len - done);
            std::copy_n(_cached_chunk.get() + in_chunk, n, dst + done);
            done += n;
        }
        co_return done;
    }

    // Ensure the decompressed contents of chunk_index are in _cached_chunk.
    future<> ensure_chunk_cached(uint64_t chunk_index) {
        if (_cached_chunk_index == chunk_index) {
            co_return;
        }
        const auto& m = get_chunk_meta(chunk_index);
        auto compressed = co_await read_compressed_chunk(m);
        _cached_chunk = decompress_chunk(m, std::move(compressed));
        _cached_chunk_index = chunk_index;
    }

    // Read a chunk's compressed bytes from the data file via the inner cursor.
    future<temporary_buffer<char>> read_compressed_chunk(const chunk_meta& m) {
        _file.seek(sstable_datafile_position::from_logical_fixme(m.physical_pos));
        temporary_buffer<char> buf(m.physical_len);
        size_t filled = 0;
        while (filled < m.physical_len) {
            auto part = co_await _file.read_forwards(m.physical_len - filled);
            if (part.empty()) {
                break;
            }
            std::copy_n(part.get(), part.size(), buf.get_write() + filled);
            filled += part.size();
        }
        if (filled != m.physical_len) {
            throw_malformed_sstable_exception(format(
                    "compressed cursor hit premature end-of-file at file offset {}, expected chunk_len={}, actual={}",
                    m.physical_pos, m.physical_len, filled));
        }
        co_return buf;
    }

    // Verify the chunk's trailing checksum and decompress it.
    temporary_buffer<char> decompress_chunk(const chunk_meta& m, temporary_buffer<char> compressed) {
        if (m.physical_len < 4) {
            throw_malformed_sstable_exception(format(
                    "compressed chunk_len must be greater than 4, chunk_start={}", m.physical_pos));
        }
        // The last 4 bytes of the chunk are the checksum of the rest.
        size_t compressed_len = m.physical_len - 4;
        uint32_t expected = read_be<uint32_t>(compressed.get() + compressed_len);
        uint32_t actual = checksum(compressed.get(), compressed_len);
        if (expected != actual) {
            throw_malformed_sstable_exception(format(
                    "compressed chunk of size {} at file offset {} failed checksum, expected={}, actual={}",
                    m.physical_len, m.physical_pos, expected, actual));
        }
        temporary_buffer<char> out(m.logical_len);
        size_t n = _compression.get_compressor().uncompress(compressed.get(), compressed_len, out.get_write(), out.size());
        out.trim(n);
        return out;
    }

    uint32_t checksum(const char* input, size_t len) const {
        if (_sst->get_version() >= sstable_version_types::mc) {
            return crc32_utils::checksum(input, len);
        }
        return adler32_utils::checksum(input, len);
    }
};

std::unique_ptr<sstable_datafile_cursor::impl> make_impl(shared_sstable sst, reader_permit permit, tracing::trace_state_ptr trace_state) {
    if (sst->get_compression()) {
        return std::make_unique<compressed_file_cursor_impl>(std::move(sst), std::move(permit), std::move(trace_state));
    }
    return std::make_unique<uncompressed_file_cursor_impl>(std::move(sst), std::move(permit), std::move(trace_state));
}

} // anonymous namespace

sstable_datafile_cursor::sstable_datafile_cursor(shared_sstable sst, reader_permit permit, tracing::trace_state_ptr trace_state)
    : _impl(make_impl(std::move(sst), std::move(permit), std::move(trace_state))) {
}

sstable_datafile_cursor::~sstable_datafile_cursor() = default;

void sstable_datafile_cursor::seek(sstable_datafile_position pos) {
    _impl->seek(pos);
}
future<temporary_buffer<char>> sstable_datafile_cursor::read_forwards(size_t n) {
    return _impl->read_forwards(n);
}
future<temporary_buffer<char>> sstable_datafile_cursor::read_backwards(size_t n) {
    return _impl->read_backwards(n);
}
sstable_datafile_position sstable_datafile_cursor::compute_relative_position(ssize_t offset) {
    return _impl->compute_relative_position(offset);
}
void sstable_datafile_cursor::drop_caches_after(sstable_datafile_position pos) {
    _impl->drop_caches_after(pos);
}
void sstable_datafile_cursor::drop_caches_before(sstable_datafile_position pos) {
    _impl->drop_caches_before(pos);
}
future<> sstable_datafile_cursor::close() {
    return _impl->close();
}

} // namespace sstables
