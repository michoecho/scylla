/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <cstdlib>
#include <map>
#include <optional>
#include <vector>

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
#include "utils/div_ceil.hh"

namespace sstables {

class sstable_datafile_cursor::impl {
public:
    virtual ~impl() = default;
    virtual void seek(sstable_datafile_position pos) = 0;
    virtual future<temporary_buffer<char>> read_forwards(size_t n) = 0;
    virtual future<temporary_buffer<char>> read(sstable_datafile_position start, sstable_datafile_position end) = 0;
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
            // The run that could cover pos is the last one starting at or before
            // pos, i.e. the one just before the first run starting after pos.
            auto next = _runs.upper_bound(pos);
            if (next != _runs.begin()) {
                auto covering = std::prev(next);
                uint64_t covering_end = covering->first + covering->second.size();
                if (covering_end > pos) {
                    pos = covering_end; // already cached; skip past it
                    continue;
                }
            }
            // pos is uncached. The gap runs up to the start of the next run (or end).
            uint64_t gap_end = next != _runs.end() ? std::min(end, next->first) : end;
            _runs.emplace(pos, buf.share(pos - offset, gap_end - pos));
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
        // Salvage the [p, prev_end) tail of a run straddling p before we erase.
        std::optional<temporary_buffer<char>> tail;
        if (it != _runs.begin()) {
            auto prev = std::prev(it);
            uint64_t prev_end = prev->first + prev->second.size();
            if (prev_end > p) {
                tail = prev->second.share();
                tail->trim_front(p - prev->first);
            }
        }
        // Erase first; the salvaged tail is reinserted afterwards so it can't be
        // caught by the erase (which runs up to `it`, i.e. past key p). `it`
        // survives the erase (it's outside the erased range) and points just
        // past where key p belongs, so it is the right hint for the reinsert.
        _runs.erase(_runs.begin(), it);
        if (tail) {
            _runs.emplace_hint(it, p, std::move(*tail));
        }
    }
};

// Cursor implementation for uncompressed sstables.
//
// For uncompressed data the logical (uncompressed) position equals the
// physical file offset, so there is no chunk translation to do. All reads
// are cached by file offset (see byte_range_cache); since the data file is
// immutable, a cached byte never goes stale.
//
// read reads the requested range directly from the data file (backwards).
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
        n = std::min(n, _file_length - start);
        temporary_buffer<char> result(n);
        size_t filled = co_await fill_forwards(start, result.get_write(), n);
        result.trim(filled);
        _position = sstable_datafile_position::from_logical_fixme(start + filled);
        co_return result;
    }

    future<temporary_buffer<char>> read(sstable_datafile_position start_pos, sstable_datafile_position end_pos) override {
        uint64_t start = start_pos.to_logical_fixme();
        uint64_t end = end_pos.to_logical_fixme();
        SCYLLA_ASSERT(start <= end);
        size_t len = end - start;
        temporary_buffer<char> result(len);
        co_await fill_from_file(start, result.get_write(), len);
        _position = start_pos;
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

    // The single cached decompressed chunk, if any.
    struct cached_chunk {
        uint64_t index;
        temporary_buffer<char> data; // uncompressed bytes of the chunk
    };
    std::optional<cached_chunk> _cached_chunk;

    uint64_t _uncompressed_chunk_length;
    uint64_t _uncompressed_file_length;

    // Whole-file digest check. When _expected_digest is engaged (a digest check
    // was requested), every decompressed chunk folds its checksum into
    // _actual_digest, exactly as the old data-source impl did: the per-chunk
    // checksum is combined in (and, for m-format, the chunk's trailing 4-byte
    // checksum too), so the running digest reproduces the whole-file digest
    // stored in Digest.crc. The digest is only meaningful if every chunk is
    // covered in order, so _calculating_digest is dropped as soon as a chunk is
    // included that is not the immediate successor of the last included one
    // (i.e. on any non-sequential read). _next_digest_chunk_index is the chunk
    // index that must come next to keep the run going; it starts at 0 so the
    // run can only begin at the first chunk.
    std::optional<uint32_t> _expected_digest;
    uint32_t _actual_digest;
    bool _calculating_digest;
    uint64_t _next_digest_chunk_index = 0;

public:
    explicit compressed_file_cursor_impl(shared_sstable sst, reader_permit permit, tracing::trace_state_ptr trace_state,
            std::optional<uint32_t> digest = std::nullopt)
        : _sst(std::move(sst))
        , _permit(std::move(permit))
        , _trace_state(std::move(trace_state))
        , _compression(_sst->get_compression())
        , _offsets(_compression.offsets.get_accessor())
        , _file(_sst, _permit, _trace_state, _sst->ondisk_data_size())
        , _uncompressed_chunk_length(_compression.uncompressed_chunk_length())
        , _uncompressed_file_length(_compression.uncompressed_file_length())
        , _expected_digest(digest)
        , _actual_digest(_sst->get_version() >= sstable_version_types::mc
                ? crc32_utils::init_checksum() : adler32_utils::init_checksum())
        , _calculating_digest(digest.has_value()) {
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

    future<temporary_buffer<char>> read(sstable_datafile_position start_pos, sstable_datafile_position end_pos) override {
        uint64_t start = start_pos.to_logical_fixme();
        uint64_t end = end_pos.to_logical_fixme();
        SCYLLA_ASSERT(start <= end);
        size_t len = end - start;
        temporary_buffer<char> result(len);
        co_await fill(start, result.get_write(), len);
        _position = start_pos;
        co_return result;
    }

    sstable_datafile_position compute_relative_position(ssize_t offset) override {
        SCYLLA_ASSERT(_position.has_value());
        return sstable_datafile_position::from_logical_fixme(_position->to_logical_fixme() + offset);
    }

    void drop_caches_after(sstable_datafile_position pos) override {
        uint64_t p = pos.to_logical_fixme();
        // Drop only chunks that lie entirely in [p; +inf). The chunk that
        // contains p straddles the boundary (unless p is exactly on a chunk
        // boundary), so the first chunk we may drop is the one starting at or
        // after p, i.e. ceil(p / chunk_length).
        uint64_t first_dropped_chunk = div_ceil(p, _uncompressed_chunk_length);
        _chunk_meta_cache.erase(_chunk_meta_cache.lower_bound(first_dropped_chunk), _chunk_meta_cache.end());
        if (_cached_chunk && _cached_chunk->index >= first_dropped_chunk) {
            drop_cached_chunk();
        }
        // Translate to the physical position and drop the underlying file cache.
        // physical_pos_of maps a logical position to its chunk's compressed start,
        // so passing the first dropped chunk's start keeps the straddling chunk's
        // compressed bytes intact.
        uint64_t first_dropped_logical = first_dropped_chunk * _uncompressed_chunk_length;
        _file.drop_caches_after(sstable_datafile_position::from_logical_fixme(physical_pos_of(first_dropped_logical)));
    }

    void drop_caches_before(sstable_datafile_position pos) override {
        uint64_t p = pos.to_logical_fixme();
        // The chunk containing p straddles the boundary and must be kept, so the
        // last chunk to drop is the one before it; keep chunk_index onward.
        uint64_t chunk_index = p / _uncompressed_chunk_length;
        _chunk_meta_cache.erase(_chunk_meta_cache.begin(), _chunk_meta_cache.lower_bound(chunk_index));
        if (_cached_chunk && _cached_chunk->index < chunk_index) {
            drop_cached_chunk();
        }
        _file.drop_caches_before(sstable_datafile_position::from_logical_fixme(physical_pos_of(p)));
    }

    future<> close() override {
        return _file.close();
    }

private:
    void drop_cached_chunk() {
        _cached_chunk.reset();
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
            size_t avail = _cached_chunk->data.size() - in_chunk;
            size_t n = std::min(avail, len - done);
            std::copy_n(_cached_chunk->data.get() + in_chunk, n, dst + done);
            done += n;
        }
        co_return done;
    }

    // Ensure the decompressed contents of chunk_index are in _cached_chunk.
    future<> ensure_chunk_cached(uint64_t chunk_index) {
        if (_cached_chunk && _cached_chunk->index == chunk_index) {
            co_return;
        }
        const auto& m = get_chunk_meta(chunk_index);
        auto compressed = co_await read_compressed_chunk(m);
        _cached_chunk = cached_chunk{chunk_index, decompress_chunk(m, std::move(compressed))};
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
        update_digest(m, compressed.get(), compressed_len, actual);
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

    // Fold a just-verified chunk into the running whole-file digest, and, once
    // the last chunk has been folded in, check the digest against the expected
    // one. compressed_data/compressed_len are the chunk's compressed bytes
    // (without the trailing 4-byte checksum); chunk_checksum is that checksum.
    void update_digest(const chunk_meta& m, const char* compressed_data, size_t compressed_len, uint32_t chunk_checksum) {
        if (!_calculating_digest) {
            return;
        }
        uint64_t chunk_index = m.logical_pos / _uncompressed_chunk_length;
        // The digest only reproduces the whole-file checksum if every chunk is
        // folded in exactly once, in order, starting at chunk 0.
        // _next_digest_chunk_index is the next chunk we still need.
        //  - chunk_index <  next: this chunk was already folded in; the read
        //    rewound and re-decompressed it (the parser over-reads then backs
        //    up). Ignore it - re-folding would corrupt the digest.
        //  - chunk_index >  next: a chunk was skipped, so the run has a gap and
        //    can never cover the whole file. Abandon the digest check.
        //  - chunk_index == next: the chunk we were waiting for; fold it in.
        if (chunk_index < _next_digest_chunk_index) {
            return;
        }
        if (chunk_index > _next_digest_chunk_index) {
            sstlog.debug("Compressed cursor cannot calculate digest: chunk {} read with a gap (expected {}). Disabling digest check.",
                    chunk_index, _next_digest_chunk_index);
            _calculating_digest = false;
            return;
        }
        if (_sst->get_version() >= sstable_version_types::mc) {
            fold_chunk_into_digest<crc32_utils, /*checksum_all=*/true>(compressed_data, compressed_len, chunk_checksum);
        } else {
            fold_chunk_into_digest<adler32_utils, /*checksum_all=*/false>(compressed_data, compressed_len, chunk_checksum);
        }
        _next_digest_chunk_index = chunk_index + 1;
        // _offsets.size() is the number of chunks; the last chunk's index is one
        // less. Once it has been folded in, the digest covers the whole file.
        if (chunk_index + 1 == _compression.offsets.size()) {
            if (_actual_digest != *_expected_digest) {
                throw_malformed_sstable_exception(seastar::format(
                        "Digest mismatch: expected={}, actual={}", *_expected_digest, _actual_digest));
            }
            _calculating_digest = false;
        }
    }

    template <ChecksumUtils ChecksumType, bool checksum_all>
    void fold_chunk_into_digest(const char* compressed_data, size_t compressed_len, uint32_t chunk_checksum) {
        _actual_digest = checksum_combine_or_feed<ChecksumType>(_actual_digest, chunk_checksum, compressed_data, compressed_len);
        if constexpr (checksum_all) {
            uint32_t be_chunk_checksum = cpu_to_be(chunk_checksum);
            _actual_digest = ChecksumType::checksum(_actual_digest,
                    reinterpret_cast<const char*>(&be_chunk_checksum), sizeof(be_chunk_checksum));
        }
    }
};

// Cursor implementation for compressed sstables, driven by physical positions.
//
// This is the sister of compressed_file_cursor_impl: it does exactly the same
// chunk reading, checksum verification, decompression and digest folding, but
// the positions it consumes and produces are `physical` rather than `logical`
// (see sstable_datafile_position).
//
// Crucially, this cursor is *not* aware of uncompressed (logical) positions.
// They are not a well-defined thing here: the cursor never computes "the byte
// offset into the decompressed stream" of anything. A physical position locates
// a point purely in compressed-file terms, as the triple
//   (chunk_position, chunk_length, offset_within_chunk)
// where chunk_position/chunk_length are the chunk's compressed byte range in the
// data file and offset_within_chunk is the byte offset of the point within that
// chunk *after* decompression. (The position type also carries a fourth field,
// uncompressed_position, but it exists only for debugging and is about to be
// removed; this cursor must neither read nor produce a meaningful value for it.)
//
// Everything the cursor needs follows from the triple:
//  - To materialize a point, decompress the chunk at [chunk_position,
//    chunk_position + chunk_length) and index into it at offset_within_chunk.
//  - To move forward past the end of a chunk, the next chunk begins on disk
//    right after this one (its chunk_position is chunk_position + chunk_length),
//    and its compressed length comes from the compression offsets. We follow
//    those offsets by chunk *index* (a plain array index, not a byte position),
//    starting from the index we recover for the seeked chunk and incrementing as
//    we cross chunk boundaries.
//
// The caching, IO delegation and digest logic mirror the logical cursor; only
// the position bookkeeping differs.
class compressed_physical_file_cursor_impl final : public sstable_datafile_cursor::impl {
    shared_sstable _sst;
    reader_permit _permit;
    tracing::trace_state_ptr _trace_state;

    const compression& _compression;
    compression::segmented_offsets::accessor _offsets;
    // Reads the raw compressed data file; all physical IO goes through here.
    uncompressed_file_cursor_impl _file;

    // The compressed byte range of a single chunk in the data file.
    struct chunk_coords {
        uint64_t chunk_position; // compressed offset of the chunk's first byte
        uint64_t chunk_length;   // compressed length, including the 4-byte checksum
    };

    // The current point: which chunk it falls in and how far into the chunk's
    // decompressed bytes. _chunk_index is that chunk's index in the compression
    // offsets; we keep it so we can fetch the next chunk's coordinates when a
    // forward read crosses a chunk boundary.
    struct cursor_pos {
        uint64_t chunk_index;
        chunk_coords chunk;
        uint64_t offset_within_chunk;
    };
    std::optional<cursor_pos> _position;

    // The single cached decompressed chunk, if any, keyed by its chunk_position.
    struct cached_chunk {
        uint64_t chunk_position;
        temporary_buffer<char> data; // uncompressed bytes of the chunk
    };
    std::optional<cached_chunk> _cached_chunk;

    // The end-of-data sentinel: chunk_position == chunk_length-end of the
    // compressed file, no bytes within it. Forward reads stop here.
    uint64_t _compressed_file_length;

    // Whole-file digest check. See the identical machinery in
    // compressed_file_cursor_impl for the full explanation.
    std::optional<uint32_t> _expected_digest;
    uint32_t _actual_digest;
    bool _calculating_digest;
    uint64_t _next_digest_chunk_index = 0;

public:
    explicit compressed_physical_file_cursor_impl(shared_sstable sst, reader_permit permit, tracing::trace_state_ptr trace_state,
            std::optional<uint32_t> digest = std::nullopt)
        : _sst(std::move(sst))
        , _permit(std::move(permit))
        , _trace_state(std::move(trace_state))
        , _compression(_sst->get_compression())
        , _offsets(_compression.offsets.get_accessor())
        , _file(_sst, _permit, _trace_state, _sst->ondisk_data_size())
        , _compressed_file_length(_sst->ondisk_data_size())
        , _expected_digest(digest)
        , _actual_digest(_sst->get_version() >= sstable_version_types::mc
                ? crc32_utils::init_checksum() : adler32_utils::init_checksum())
        , _calculating_digest(digest.has_value()) {
    }

    void seek(sstable_datafile_position pos) override {
        _position = decode_position(pos);
    }

    future<temporary_buffer<char>> read_forwards(size_t n) override {
        SCYLLA_ASSERT(_position.has_value());
        temporary_buffer<char> result(n);
        size_t filled = co_await fill_forwards(result.get_write(), n);
        result.trim(filled);
        co_return result;
    }

    future<temporary_buffer<char>> read(sstable_datafile_position start_pos, sstable_datafile_position end_pos) override {
        SCYLLA_ASSERT(start_pos <= end_pos);
        // Read from start to end by replaying the forward path from start. Unlike
        // a logical position, a physical position does not expose a byte distance,
        // so we walk chunk by chunk until we reach end rather than subtracting.
        _position = decode_position(start_pos);
        auto end = decode_position(end_pos);
        auto result = co_await collect_forwards(end);
        // Mirror the logical cursor: a range read leaves the cursor at its start.
        _position = decode_position(start_pos);
        co_return result;
    }

    sstable_datafile_position compute_relative_position(ssize_t offset) override {
        SCYLLA_ASSERT(_position.has_value());
        return relative_position(*_position, offset);
    }

    void drop_caches_after(sstable_datafile_position pos) override {
        auto p = decode_position(pos);
        // Keep the chunk that p falls into (it straddles the boundary unless p is
        // exactly on a chunk boundary); the first chunk we may drop starts after
        // it on disk. When offset_within_chunk is 0, p is on the boundary and the
        // chunk itself can go; otherwise the boundary is the next chunk's start.
        uint64_t boundary = p.offset_within_chunk == 0
                ? p.chunk.chunk_position
                : p.chunk.chunk_position + p.chunk.chunk_length;
        if (_cached_chunk && _cached_chunk->chunk_position >= boundary) {
            drop_cached_chunk();
        }
        _file.drop_caches_after(sstable_datafile_position::from_logical_fixme(boundary));
    }

    void drop_caches_before(sstable_datafile_position pos) override {
        auto p = decode_position(pos);
        // The chunk containing p straddles the boundary and must be kept, so drop
        // only chunks that end at or before its compressed start.
        uint64_t boundary = p.chunk.chunk_position;
        if (_cached_chunk && _cached_chunk->chunk_position + cached_chunk_compressed_length() <= boundary) {
            drop_cached_chunk();
        }
        _file.drop_caches_before(sstable_datafile_position::from_logical_fixme(boundary));
    }

    future<> close() override {
        return _file.close();
    }

private:
    void drop_cached_chunk() {
        _cached_chunk.reset();
    }

    // The compressed length of the currently cached chunk. Used only to decide
    // whether the cache lies entirely before a drop boundary; the cached chunk's
    // own coordinates are not stored, so we recover the length from the offsets.
    uint64_t cached_chunk_compressed_length() {
        SCYLLA_ASSERT(_cached_chunk.has_value());
        return chunk_length_at(chunk_index_at(_cached_chunk->chunk_position), _cached_chunk->chunk_position);
    }

    // Recover the chunk index of the chunk whose compressed range starts at
    // chunk_position, by binary-searching the (ascending) compression offsets.
    // This is the only place we map a compressed position back to an index; once
    // we have it, forward steps just increment it.
    uint64_t chunk_index_at(uint64_t chunk_position) {
        uint64_t lo = 0;
        uint64_t hi = _compression.offsets.size(); // number of chunks
        while (lo < hi) {
            uint64_t mid = lo + (hi - lo) / 2;
            if (_offsets.at(mid) < chunk_position) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        SCYLLA_ASSERT(lo < _compression.offsets.size() && _offsets.at(lo) == chunk_position);
        return lo;
    }

    // Compressed length of chunk `index`, whose compressed range starts at
    // chunk_position. The last chunk runs to the end of the compressed file;
    // every other chunk runs to where the next one begins.
    uint64_t chunk_length_at(uint64_t index, uint64_t chunk_position) {
        uint64_t next_start = index + 1 == _compression.offsets.size()
                ? _compressed_file_length
                : _offsets.at(index + 1);
        return next_start - chunk_position;
    }

    // Decode an incoming physical position into the cursor's working form. The
    // end-of-data sentinel (the chunk coordinates pointing at the end of the
    // compressed file) has no chunk index; we mark it with chunk_index ==
    // offsets.size() and an empty chunk so forward reads see immediate EOF.
    cursor_pos decode_position(sstable_datafile_position pos) {
        auto ph = pos.to_physical();
        chunk_coords chunk{uint64_t(ph.chunk_position), uint64_t(ph.chunk_length)};
        if (chunk.chunk_position >= _compressed_file_length) {
            return cursor_pos{_compression.offsets.size(), chunk, uint64_t(ph.offset_within_chunk)};
        }
        return cursor_pos{chunk_index_at(chunk.chunk_position), chunk, uint64_t(ph.offset_within_chunk)};
    }

    // Encode the cursor's working position back into a physical position. The
    // uncompressed_position field is debug-only and being removed, so we emit 0
    // for it rather than computing a logical offset the cursor must not know.
    static sstable_datafile_position encode_position(const cursor_pos& p) {
        return sstable_datafile_position::from_physical(
                p.chunk.chunk_position, p.chunk.chunk_length, p.offset_within_chunk, /*uncompressed_position=*/0);
    }

    bool at_eof(const cursor_pos& p) const {
        return p.chunk.chunk_position >= _compressed_file_length;
    }

    // Advance p by `offset` decompressed bytes, walking across chunk boundaries
    // as needed. `offset` may be negative. We never count in a global logical
    // position: each step moves relative to the chunk we are in, and chunk
    // boundaries are crossed using the fixed uncompressed chunk length (a
    // structural constant of the compression format, not a per-point logical
    // offset) together with the on-disk chunk offsets.
    sstable_datafile_position relative_position(cursor_pos p, ssize_t offset) {
        if (at_eof(p)) {
            // Past the last chunk there is nothing to step relative to; the only
            // sensible relative position from EOF is EOF itself.
            SCYLLA_ASSERT(offset == 0);
            return encode_position(p);
        }
        uint64_t chunk_len = _compression.uncompressed_chunk_length();
        int64_t in_chunk = int64_t(p.offset_within_chunk) + offset;
        // Step forward whole chunks while the running offset overflows the chunk.
        while (in_chunk >= int64_t(chunk_len)) {
            step_to_next_chunk_pos(p);
            in_chunk -= chunk_len;
            if (at_eof(p)) {
                // We have walked to the end of data; remaining offset must be 0,
                // otherwise the caller asked for a point past end of file.
                SCYLLA_ASSERT(in_chunk == 0);
                return encode_position(p);
            }
        }
        // Step backward whole chunks while the running offset underflows.
        while (in_chunk < 0) {
            step_to_prev_chunk_pos(p);
            in_chunk += chunk_len;
        }
        p.offset_within_chunk = in_chunk;
        return encode_position(p);
    }

    // Move p to the start of the next chunk, updating its coordinates from the
    // offsets. Past the last chunk this yields the end-of-data sentinel.
    void step_to_next_chunk_pos(cursor_pos& p) {
        uint64_t next_index = p.chunk_index + 1;
        uint64_t next_start = p.chunk.chunk_position + p.chunk.chunk_length;
        uint64_t next_len = next_index < _compression.offsets.size()
                ? chunk_length_at(next_index, next_start)
                : 0;
        p = cursor_pos{next_index, chunk_coords{next_start, next_len}, 0};
    }

    // Move p to the start of the preceding chunk, reading its coordinates from
    // the offsets.
    void step_to_prev_chunk_pos(cursor_pos& p) {
        SCYLLA_ASSERT(p.chunk_index > 0);
        uint64_t prev_index = p.chunk_index - 1;
        uint64_t prev_start = _offsets.at(prev_index);
        uint64_t prev_len = chunk_length_at(prev_index, prev_start);
        p = cursor_pos{prev_index, chunk_coords{prev_start, prev_len}, 0};
    }

    // Read forward from the current position into dst (up to len bytes),
    // decompressing chunks and stepping across boundaries. Advances _position.
    // Returns the number of bytes produced (less than len only at EOF).
    future<size_t> fill_forwards(char* dst, size_t len) {
        size_t done = 0;
        while (done < len && !at_eof(*_position)) {
            co_await ensure_chunk_cached(_position->chunk);
            const auto& data = _cached_chunk->data;
            uint64_t in_chunk = _position->offset_within_chunk;
            if (in_chunk >= data.size()) {
                // Exhausted this chunk; step to the next one.
                step_to_next_chunk();
                continue;
            }
            size_t avail = data.size() - in_chunk;
            size_t n = std::min(avail, len - done);
            std::copy_n(data.get() + in_chunk, n, dst + done);
            done += n;
            _position->offset_within_chunk += n;
        }
        co_return done;
    }

    // Read forward from the current position up to `end`, returning the bytes.
    // Used by read(start, end): we do not know the byte distance from a physical
    // position, so we accumulate chunk by chunk until the position reaches end.
    future<temporary_buffer<char>> collect_forwards(const cursor_pos& end) {
        std::vector<temporary_buffer<char>> parts;
        size_t total = 0;
        while (encode_position(*_position) < encode_position(end) && !at_eof(*_position)) {
            co_await ensure_chunk_cached(_position->chunk);
            const auto& data = _cached_chunk->data;
            uint64_t in_chunk = _position->offset_within_chunk;
            if (in_chunk >= data.size()) {
                step_to_next_chunk();
                continue;
            }
            // Bytes to take from this chunk: up to its end, but not past `end` if
            // `end` falls inside this same chunk.
            size_t avail = data.size() - in_chunk;
            if (_position->chunk.chunk_position == end.chunk.chunk_position) {
                avail = std::min<size_t>(avail, end.offset_within_chunk - in_chunk);
            }
            parts.emplace_back(data.get() + in_chunk, avail);
            total += avail;
            _position->offset_within_chunk += avail;
        }
        temporary_buffer<char> result(total);
        size_t off = 0;
        for (auto& part : parts) {
            std::copy_n(part.get(), part.size(), result.get_write() + off);
            off += part.size();
        }
        co_return result;
    }

    // Move _position to the start of the next chunk on disk.
    void step_to_next_chunk() {
        step_to_next_chunk_pos(*_position);
    }

    // Ensure the decompressed contents of the chunk at `chunk` are in _cached_chunk.
    future<> ensure_chunk_cached(const chunk_coords& chunk) {
        if (_cached_chunk && _cached_chunk->chunk_position == chunk.chunk_position) {
            co_return;
        }
        auto compressed = co_await read_compressed_chunk(chunk);
        _cached_chunk = cached_chunk{chunk.chunk_position, decompress_chunk(chunk, std::move(compressed))};
    }

    // Read a chunk's compressed bytes from the data file via the inner cursor.
    future<temporary_buffer<char>> read_compressed_chunk(const chunk_coords& chunk) {
        _file.seek(sstable_datafile_position::from_logical_fixme(chunk.chunk_position));
        temporary_buffer<char> buf(chunk.chunk_length);
        size_t filled = 0;
        while (filled < chunk.chunk_length) {
            auto part = co_await _file.read_forwards(chunk.chunk_length - filled);
            if (part.empty()) {
                break;
            }
            std::copy_n(part.get(), part.size(), buf.get_write() + filled);
            filled += part.size();
        }
        if (filled != chunk.chunk_length) {
            throw_malformed_sstable_exception(format(
                    "compressed cursor hit premature end-of-file at file offset {}, expected chunk_len={}, actual={}",
                    chunk.chunk_position, chunk.chunk_length, filled));
        }
        co_return buf;
    }

    // Verify the chunk's trailing checksum and decompress it.
    temporary_buffer<char> decompress_chunk(const chunk_coords& chunk, temporary_buffer<char> compressed) {
        if (chunk.chunk_length < 4) {
            throw_malformed_sstable_exception(format(
                    "compressed chunk_len must be greater than 4, chunk_start={}", chunk.chunk_position));
        }
        // The last 4 bytes of the chunk are the checksum of the rest.
        size_t compressed_len = chunk.chunk_length - 4;
        uint32_t expected = read_be<uint32_t>(compressed.get() + compressed_len);
        uint32_t actual = checksum(compressed.get(), compressed_len);
        if (expected != actual) {
            throw_malformed_sstable_exception(format(
                    "compressed chunk of size {} at file offset {} failed checksum, expected={}, actual={}",
                    chunk.chunk_length, chunk.chunk_position, expected, actual));
        }
        update_digest(chunk, compressed.get(), compressed_len, actual);
        // We do not know the chunk's decompressed length a priori (that would be
        // a logical quantity); decompress into a full-chunk-sized buffer and trim
        // to whatever uncompress() actually produced.
        temporary_buffer<char> out(_compression.uncompressed_chunk_length());
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

    // Fold a just-verified chunk into the running whole-file digest. See the
    // identical method in compressed_file_cursor_impl for details.
    void update_digest(const chunk_coords& chunk, const char* compressed_data, size_t compressed_len, uint32_t chunk_checksum) {
        if (!_calculating_digest) {
            return;
        }
        uint64_t chunk_index = chunk_index_at(chunk.chunk_position);
        if (chunk_index < _next_digest_chunk_index) {
            return;
        }
        if (chunk_index > _next_digest_chunk_index) {
            sstlog.debug("Compressed cursor cannot calculate digest: chunk {} read with a gap (expected {}). Disabling digest check.",
                    chunk_index, _next_digest_chunk_index);
            _calculating_digest = false;
            return;
        }
        if (_sst->get_version() >= sstable_version_types::mc) {
            fold_chunk_into_digest<crc32_utils, /*checksum_all=*/true>(compressed_data, compressed_len, chunk_checksum);
        } else {
            fold_chunk_into_digest<adler32_utils, /*checksum_all=*/false>(compressed_data, compressed_len, chunk_checksum);
        }
        _next_digest_chunk_index = chunk_index + 1;
        if (chunk_index + 1 == _compression.offsets.size()) {
            if (_actual_digest != *_expected_digest) {
                throw_malformed_sstable_exception(seastar::format(
                        "Digest mismatch: expected={}, actual={}", *_expected_digest, _actual_digest));
            }
            _calculating_digest = false;
        }
    }

    template <ChecksumUtils ChecksumType, bool checksum_all>
    void fold_chunk_into_digest(const char* compressed_data, size_t compressed_len, uint32_t chunk_checksum) {
        _actual_digest = checksum_combine_or_feed<ChecksumType>(_actual_digest, chunk_checksum, compressed_data, compressed_len);
        if constexpr (checksum_all) {
            uint32_t be_chunk_checksum = cpu_to_be(chunk_checksum);
            _actual_digest = ChecksumType::checksum(_actual_digest,
                    reinterpret_cast<const char*>(&be_chunk_checksum), sizeof(be_chunk_checksum));
        }
    }
};

std::unique_ptr<sstable_datafile_cursor::impl> make_impl(shared_sstable sst, reader_permit permit, tracing::trace_state_ptr trace_state,
        std::optional<uint32_t> digest) {
    if (sst->get_compression()) {
        return std::make_unique<compressed_file_cursor_impl>(std::move(sst), std::move(permit), std::move(trace_state), digest);
    }
    // The uncompressed cursor reads an unchecksummed data file; the whole-file
    // digest is verified by the data_source layer, not the cursor, so the
    // digest is unused here.
    return std::make_unique<uncompressed_file_cursor_impl>(std::move(sst), std::move(permit), std::move(trace_state));
}

} // anonymous namespace

sstable_datafile_cursor::sstable_datafile_cursor(shared_sstable sst, reader_permit permit, tracing::trace_state_ptr trace_state,
        std::optional<uint32_t> digest)
    : _impl(make_impl(std::move(sst), std::move(permit), std::move(trace_state), digest)) {
}

sstable_datafile_cursor::~sstable_datafile_cursor() = default;

void sstable_datafile_cursor::seek(sstable_datafile_position pos) {
    _impl->seek(pos);
}
future<temporary_buffer<char>> sstable_datafile_cursor::read_forwards(size_t n) {
    return _impl->read_forwards(n);
}
future<temporary_buffer<char>> sstable_datafile_cursor::read(sstable_datafile_position start, sstable_datafile_position end) {
    return _impl->read(start, end);
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
