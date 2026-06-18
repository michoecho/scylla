/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <cstdlib>
#include <functional>
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

logging::logger sstable_cursor_log("sstable_cursor");

namespace {

// Opens a forward-streaming data_source over the byte range [start, start+len)
// of the underlying file. The cursor uses this to read ahead sequentially; the
// closure captures whatever is needed to reach the bytes (permit, tracing,
// storage layer), so the cursor itself never refers to an sstable.
using forward_source_factory = std::function<future<data_source>(uint64_t start, uint64_t len)>;

// Everything a cursor needs to do physical IO against a file, with no reference
// to an sstable. This is the decoupled form of what an sstable provides: the
// raw data file (for its block size and for direct backward reads), a permit
// for IO accounting, a factory for forward streaming sources, and the length of
// the byte stream being read.
struct datafile_io {
    file f;
    reader_permit permit;
    forward_source_factory make_forward_source;
    uint64_t file_length;
};

// Build the IO pieces for reading an sstable's raw data file. `file_length` is
// the length of the byte stream to read (the on-disk data size when reading the
// compressed bytes, or the uncompressed data size otherwise).
datafile_io make_sstable_datafile_io(shared_sstable sst, reader_permit permit, tracing::trace_state_ptr trace_state,
        uint64_t file_length) {
    file data_file = sst->get_data_file();
    auto make_forward_source = [sst, permit, trace_state] (uint64_t start, uint64_t len) -> future<data_source> {
        file_input_stream_options options;
        options.buffer_size = seastar::align_up<uint64_t>(4096, sst->get_data_file().disk_read_dma_alignment());
        file f = make_tracked_file(sst->get_data_file(), permit);
        if (trace_state) {
            f = tracing::make_traced_file(std::move(f), trace_state, seastar::format("{}:", sst->get_filename()));
        }
        return sst->get_storage().make_data_or_index_source(
                *sst, component_type::Data, std::move(f), start, len, std::move(options));
    };
    return datafile_io{std::move(data_file), std::move(permit), std::move(make_forward_source), file_length};
}

// Everything the compressed cursors need to know about how the data is
// compressed, with no reference to an sstable. The compression metadata
// (chunk length, file length, compressor, chunk locations) plus the two
// format details that the cursors otherwise derive from the sstable version:
// whether each chunk is prefixed with its length, and whether chunk checksums
// use crc32 (true) or adler32 (false).
struct compression_format {
    const compression& comp;
    uint64_t compressed_file_length;
    size_t chunk_prefix;  // 0 if chunks carry no length prefix
    bool use_crc32;       // crc32 (mc+) vs adler32 chunk checksums

    uint32_t init_digest() const {
        return use_crc32 ? crc32_utils::init_checksum() : adler32_utils::init_checksum();
    }
};

// Build the compression format pieces from an sstable.
compression_format make_sstable_compression_format(const sstable& sst) {
    return compression_format{
        sst.get_compression(),
        sst.ondisk_data_size(),
        chunk_has_length_prefix(sst.get_version()) ? chunk_length_prefix_size : 0,
        sst.get_version() >= sstable_version_types::mc,
    };
}

} // anonymous namespace

class sstable_datafile_cursor::impl {
public:
    virtual ~impl() = default;
    virtual void seek(sstable_datafile_position pos) = 0;
    virtual future<temporary_buffer<char>> read_forwards(size_t n) = 0;
    virtual future<temporary_buffer<char>> read(sstable_datafile_position start, sstable_datafile_position end) = 0;
    virtual sstable_datafile_position compute_relative_position(ssize_t offset) = 0;
    // The number of decompressed bytes between `a` and `b` (b - a). Synchronous;
    // every chunk between the two positions must already be in the cursor's
    // metadata cache, exactly as for compute_relative_position.
    virtual int64_t subtract_positions(sstable_datafile_position b, sstable_datafile_position a) = 0;
    // Advance `from` forward by `n` decompressed bytes, returning the resulting
    // position. Unlike compute_relative_position (which is synchronous and only
    // consults already-cached chunk metadata), this may read from the file to
    // discover the chunks it walks across, so it can move past chunks the cursor
    // has not seen yet. It also primes the cursor's metadata cache for the chunks
    // it touches, so a later synchronous compute_relative_position around the
    // returned position succeeds.
    virtual future<sstable_datafile_position> skip_forwards(sstable_datafile_position from, size_t n) = 0;
    // Move `from` backward by `n` decompressed bytes, returning the resulting
    // position. The backward counterpart of skip_forwards: unlike
    // compute_relative_position (which is synchronous and only consults
    // already-cached chunk metadata), this may read from the file to discover the
    // chunks it walks across, so it can move past chunks the cursor has not seen
    // yet. It also primes the cursor's metadata cache for the chunks it touches,
    // so a later synchronous compute_relative_position around the returned
    // position succeeds.
    virtual future<sstable_datafile_position> read_backwards(sstable_datafile_position from, size_t n) = 0;
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
    // Raw data file, used for its block size and for direct backward reads.
    file _data_file;
    reader_permit _permit;
    // Opens a forward streaming source over a byte range of the file.
    forward_source_factory _make_forward_source;
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
    // Construct from the decoupled IO pieces; no sstable needed.
    explicit uncompressed_file_cursor_impl(datafile_io io)
        : _data_file(std::move(io.f))
        , _permit(std::move(io.permit))
        , _make_forward_source(std::move(io.make_forward_source))
        , _block_size(_data_file.disk_read_dma_alignment())
        , _file_length(io.file_length) {
    }

    // Convenience constructor that extracts the IO pieces from an sstable.
    explicit uncompressed_file_cursor_impl(shared_sstable sst, reader_permit permit, tracing::trace_state_ptr trace_state,
            std::optional<uint64_t> file_length = std::nullopt)
        : uncompressed_file_cursor_impl(make_sstable_datafile_io(sst, std::move(permit), std::move(trace_state),
                file_length.value_or(sst->data_size()))) {
    }

    void seek(sstable_datafile_position pos) override {
        sstable_cursor_log.trace("[uncompressed@{}] seek: pos={}", fmt::ptr(this), pos);
        _position = pos;
    }

    future<temporary_buffer<char>> read_forwards(size_t n) override {
        SCYLLA_ASSERT(_position.has_value());
        sstable_cursor_log.trace("[uncompressed@{}] read_forwards: enter pos={} n={}", fmt::ptr(this), *_position, n);
        uint64_t start = _position->to_logical_fixme();
        n = std::min(n, _file_length - start);
        temporary_buffer<char> result(n);
        size_t filled = co_await fill_forwards(start, result.get_write(), n);
        result.trim(filled);
        _position = sstable_datafile_position::from_logical_fixme(start + filled);
        sstable_cursor_log.trace("[uncompressed@{}] read_forwards: exit pos={} filled={}", fmt::ptr(this), *_position, filled);
        co_return result;
    }

    future<temporary_buffer<char>> read(sstable_datafile_position start_pos, sstable_datafile_position end_pos) override {
        sstable_cursor_log.trace("[uncompressed@{}] read: enter start={} end={}", fmt::ptr(this), start_pos, end_pos);
        uint64_t start = start_pos.to_logical_fixme();
        uint64_t end = end_pos.to_logical_fixme();
        SCYLLA_ASSERT(start <= end);
        size_t len = end - start;
        temporary_buffer<char> result(len);
        co_await fill_from_file(start, result.get_write(), len);
        _position = start_pos;
        sstable_cursor_log.trace("[uncompressed@{}] read: exit len={}", fmt::ptr(this), len);
        co_return result;
    }

    sstable_datafile_position compute_relative_position(ssize_t offset) override {
        SCYLLA_ASSERT(_position.has_value());
        auto result = sstable_datafile_position::from_logical_fixme(_position->to_logical_fixme() + offset);
        sstable_cursor_log.trace("[uncompressed@{}] compute_relative_position: pos={} offset={} result={}", fmt::ptr(this), *_position, offset, result);
        return result;
    }

    int64_t subtract_positions(sstable_datafile_position b, sstable_datafile_position a) override {
        auto result = b.to_logical_fixme() - a.to_logical_fixme();
        sstable_cursor_log.trace("[uncompressed@{}] subtract_positions: b={} a={} result={}", fmt::ptr(this), b, a, result);
        return result;
    }

    future<sstable_datafile_position> skip_forwards(sstable_datafile_position from, size_t n) override {
        auto result = sstable_datafile_position::from_logical_fixme(from.to_logical_fixme() + n);
        sstable_cursor_log.trace("[uncompressed@{}] skip_forwards: from={} n={} result={}", fmt::ptr(this), from, n, result);
        // Logical positions are plain byte offsets, so a forward skip is just
        // addition; nothing needs to be read to know the result.
        return make_ready_future<sstable_datafile_position>(result);
    }

    future<sstable_datafile_position> read_backwards(sstable_datafile_position from, size_t n) override {
        auto result = sstable_datafile_position::from_logical_fixme(from.to_logical_fixme() - n);
        sstable_cursor_log.trace("[uncompressed@{}] read_backwards: from={} n={} result={}", fmt::ptr(this), from, n, result);
        // Logical positions are plain byte offsets, so a backward step is just
        // subtraction; nothing needs to be read to know the result.
        return make_ready_future<sstable_datafile_position>(result);
    }

    void drop_caches_after(sstable_datafile_position pos) override {
        sstable_cursor_log.trace("[uncompressed@{}] drop_caches_after: pos={}", fmt::ptr(this), pos);
        _cache.drop_after(pos.to_logical_fixme());
    }

    void drop_caches_before(sstable_datafile_position pos) override {
        sstable_cursor_log.trace("[uncompressed@{}] drop_caches_before: pos={}", fmt::ptr(this), pos);
        _cache.drop_before(pos.to_logical_fixme());
    }

    future<> close() override {
        sstable_cursor_log.trace("[uncompressed@{}] close", fmt::ptr(this));
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
        // (skip_forward_to may already have over-read past pos into the cache, in
        // which case _forward_pos is already past pos and this loop is skipped.)
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
    // bytes in between. skip() may over-read; cache whatever it hands back and
    // advance _forward_pos past it. The over-read can reach end of stream (it
    // returns the tail of the last buffer it touched), so _forward_pos must
    // reflect where the stream actually stopped - target plus the over-read -
    // rather than just target. Otherwise the next get() would be issued against
    // an already-exhausted stream and wrongly report EOF, even though the
    // over-read bytes (which may cover the position we were after) are cached.
    future<> skip_forward_to(uint64_t target) {
        if (_forward_pos >= target) {
            co_return;
        }
        uint64_t to_skip = target - _forward_pos;
        auto over_read = co_await _forward_source->skip(to_skip);
        _forward_pos = target + over_read.size();
        _cache.insert(target, std::move(over_read));
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
        _forward_source = co_await _make_forward_source(start, len);
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

            file f = make_tracked_file(_data_file, _permit);
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
    const compression& _compression;
    compression::segmented_offsets::accessor _offsets;
    // Compressed (on-disk) length of the whole data file.
    uint64_t _compressed_file_length;
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

    // Per-chunk length prefix size for this version (0 if the version stores no
    // prefix). The prefix sits before the compressed data; the trailing
    // checksum still covers only the compressed data.
    size_t _chunk_prefix;

    // Whether chunk checksums use crc32 (true) or adler32 (false).
    bool _use_crc32;

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
    // Construct from the decoupled pieces; no sstable needed. `io` reads the raw
    // compressed data file, `fmt` describes how it is compressed.
    explicit compressed_file_cursor_impl(datafile_io io, compression_format fmt,
            std::optional<uint32_t> digest = std::nullopt)
        : _compression(fmt.comp)
        , _offsets(_compression.offsets.get_accessor())
        , _compressed_file_length(fmt.compressed_file_length)
        , _file(std::move(io))
        , _uncompressed_chunk_length(_compression.uncompressed_chunk_length())
        , _uncompressed_file_length(_compression.uncompressed_file_length())
        , _chunk_prefix(fmt.chunk_prefix)
        , _use_crc32(fmt.use_crc32)
        , _expected_digest(digest)
        , _actual_digest(fmt.init_digest())
        , _calculating_digest(digest.has_value()) {
    }

    // Convenience constructor that extracts the pieces from an sstable.
    explicit compressed_file_cursor_impl(shared_sstable sst, reader_permit permit, tracing::trace_state_ptr trace_state,
            std::optional<uint32_t> digest = std::nullopt)
        : compressed_file_cursor_impl(
                make_sstable_datafile_io(sst, std::move(permit), std::move(trace_state), sst->ondisk_data_size()),
                make_sstable_compression_format(*sst), digest) {
    }

    void seek(sstable_datafile_position pos) override {
        sstable_cursor_log.trace("[compressed@{}] seek: pos={}", fmt::ptr(this), pos);
        _position = pos;
    }

    future<temporary_buffer<char>> read_forwards(size_t n) override {
        SCYLLA_ASSERT(_position.has_value());
        sstable_cursor_log.trace("[compressed@{}] read_forwards: enter pos={} n={}", fmt::ptr(this), *_position, n);
        uint64_t start = _position->to_logical_fixme();
        uint64_t end = std::min<uint64_t>(start + n, _uncompressed_file_length);
        size_t len = end > start ? end - start : 0;
        temporary_buffer<char> result(len);
        size_t filled = co_await fill(start, result.get_write(), len);
        result.trim(filled);
        _position = sstable_datafile_position::from_logical_fixme(start + filled);
        sstable_cursor_log.trace("[compressed@{}] read_forwards: exit pos={} filled={}", fmt::ptr(this), *_position, filled);
        co_return result;
    }

    future<temporary_buffer<char>> read(sstable_datafile_position start_pos, sstable_datafile_position end_pos) override {
        sstable_cursor_log.trace("[compressed@{}] read: enter start={} end={}", fmt::ptr(this), start_pos, end_pos);
        uint64_t start = start_pos.to_logical_fixme();
        uint64_t end = end_pos.to_logical_fixme();
        SCYLLA_ASSERT(start <= end);
        size_t len = end - start;
        temporary_buffer<char> result(len);
        co_await fill(start, result.get_write(), len);
        _position = start_pos;
        sstable_cursor_log.trace("[compressed@{}] read: exit len={}", fmt::ptr(this), len);
        co_return result;
    }

    sstable_datafile_position compute_relative_position(ssize_t offset) override {
        SCYLLA_ASSERT(_position.has_value());
        auto result = sstable_datafile_position::from_logical_fixme(_position->to_logical_fixme() + offset);
        sstable_cursor_log.trace("[compressed@{}] compute_relative_position: pos={} offset={} result={}", fmt::ptr(this), *_position, offset, result);
        return result;
    }

    int64_t subtract_positions(sstable_datafile_position b, sstable_datafile_position a) override {
        auto result = b.to_logical_fixme() - a.to_logical_fixme();
        sstable_cursor_log.trace("[compressed@{}] subtract_positions: b={} a={} result={}", fmt::ptr(this), b, a, result);
        return result;
    }

    future<sstable_datafile_position> skip_forwards(sstable_datafile_position from, size_t n) override {
        auto result = sstable_datafile_position::from_logical_fixme(from.to_logical_fixme() + n);
        sstable_cursor_log.trace("[compressed@{}] skip_forwards: from={} n={} result={}", fmt::ptr(this), from, n, result);
        // Logical positions are plain uncompressed byte offsets, so a forward
        // skip is just addition; the chunk lookup happens lazily on the next read.
        return make_ready_future<sstable_datafile_position>(result);
    }

    future<sstable_datafile_position> read_backwards(sstable_datafile_position from, size_t n) override {
        auto result = sstable_datafile_position::from_logical_fixme(from.to_logical_fixme() - n);
        sstable_cursor_log.trace("[compressed@{}] read_backwards: from={} n={} result={}", fmt::ptr(this), from, n, result);
        // Logical positions are plain uncompressed byte offsets, so a backward
        // step is just subtraction; the chunk lookup happens lazily on the next read.
        return make_ready_future<sstable_datafile_position>(result);
    }

    void drop_caches_after(sstable_datafile_position pos) override {
        sstable_cursor_log.trace("[compressed@{}] drop_caches_after: pos={}", fmt::ptr(this), pos);
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
        sstable_cursor_log.trace("[compressed@{}] drop_caches_before: pos={}", fmt::ptr(this), pos);
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
        sstable_cursor_log.trace("[compressed@{}] close", fmt::ptr(this));
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
            return _compressed_file_length;
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
        if (m.physical_len < _chunk_prefix + 4) {
            throw_malformed_sstable_exception(format(
                    "compressed chunk_len must be greater than {}, chunk_start={}", _chunk_prefix + 4, m.physical_pos));
        }
        // The chunk is an optional length prefix, the compressed data, and a
        // trailing 4-byte checksum of the compressed data. The compressed data
        // starts after the prefix and the checksum covers only it.
        const char* compressed_data = compressed.get() + _chunk_prefix;
        size_t compressed_len = m.physical_len - _chunk_prefix - 4;
        uint32_t expected = read_be<uint32_t>(compressed_data + compressed_len);
        uint32_t actual = checksum(compressed_data, compressed_len);
        if (expected != actual) {
            throw_malformed_sstable_exception(format(
                    "compressed chunk of size {} at file offset {} failed checksum, expected={}, actual={}",
                    m.physical_len, m.physical_pos, expected, actual));
        }
        update_digest(m, compressed_data, compressed_len, actual);
        temporary_buffer<char> out(m.logical_len);
        size_t n = _compression.get_compressor().uncompress(compressed_data, compressed_len, out.get_write(), out.size());
        out.trim(n);
        return out;
    }

    uint32_t checksum(const char* input, size_t len) const {
        if (_use_crc32) {
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
        if (_use_crc32) {
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
//    right after this one (its chunk_position is chunk_position + chunk_length);
//    its length is read from the on-disk per-chunk length prefix (the first u32
//    of every chunk is this chunk's compressed-data length; see
//    chunk_length_prefix_size). To move backward, the second u32 of the prefix
//    is the previous chunk's compressed-data length, from which the previous
//    chunk's start and full extent follow.
//
// This cursor never consults the external compression offsets; all chunk
// navigation is relative, using the length prefixes alone. The prefixes of
// chunks we have read are cached in _chunk_length_cache (keyed by chunk start),
// so synchronous relative navigation needs no IO. Because the prefix of a chunk
// carries both this and the previous chunk's length, reading one chunk caches
// the coordinates of both it and its predecessor.
//
// The caching, IO delegation and digest logic mirror the logical cursor; only
// the position bookkeeping differs.
class compressed_physical_file_cursor_impl final : public sstable_datafile_cursor::impl {
    const compression& _compression;
    uint64_t _uncompressed_chunk_length; 
    // Reads the raw compressed data file; all physical IO goes through here.
    uncompressed_file_cursor_impl _file;

    // The compressed byte range of a single chunk in the data file.
    struct chunk_coords {
        uint64_t chunk_position; // compressed offset of the chunk's first byte
        uint64_t chunk_length;   // full on-disk extent: prefix + data + checksum
    };

    // The current point: which chunk it falls in and how far into the chunk's
    // decompressed bytes.
    struct cursor_pos {
        chunk_coords chunk;
        uint64_t offset_within_chunk;
    };
    std::optional<cursor_pos> _position;

    // Maps a chunk's compressed start position to its full on-disk extent
    // (prefix + compressed data + checksum), learned from the per-chunk length
    // prefixes as chunks are read. Entries are contiguous and non-overlapping
    // (chunk N ends exactly where chunk N+1 begins). Relative navigation
    // consults this cache instead of the external compression offsets.
    std::map<uint64_t, uint64_t> _chunk_length_cache;

    // The single cached decompressed chunk, if any, keyed by its chunk_position.
    struct cached_chunk {
        uint64_t chunk_position;
        temporary_buffer<char> data; // uncompressed bytes of the chunk
    };
    std::optional<cached_chunk> _cached_chunk;

    // The end-of-data sentinel: chunk_position == chunk_length-end of the
    // compressed file, no bytes within it. Forward reads stop here.
    uint64_t _compressed_file_length;

    // Per-chunk length prefix size for this version (0 if none). See the
    // identical member in compressed_file_cursor_impl.
    size_t _chunk_prefix;

    // Whether chunk checksums use crc32 (true) or adler32 (false).
    bool _use_crc32;

    // Whole-file digest check. See the identical machinery in
    // compressed_file_cursor_impl for the full explanation. We track the next
    // chunk by its compressed start position (chunks must be folded in order,
    // starting at position 0) rather than by index, since this cursor has no
    // chunk indices.
    std::optional<uint32_t> _expected_digest;
    uint32_t _actual_digest;
    bool _calculating_digest;
    uint64_t _next_digest_chunk_position = 0;

public:
    // Construct from the decoupled pieces; no sstable needed. `io` reads the raw
    // compressed data file, `fmt` describes how it is compressed.
    explicit compressed_physical_file_cursor_impl(datafile_io io, compression_format fmt,
            std::optional<uint32_t> digest = std::nullopt)
        : _compression(fmt.comp)
        , _uncompressed_chunk_length(_compression.uncompressed_chunk_length())
        , _file(std::move(io))
        , _compressed_file_length(fmt.compressed_file_length)
        , _chunk_prefix(fmt.chunk_prefix)
        , _use_crc32(fmt.use_crc32)
        , _expected_digest(digest)
        , _actual_digest(fmt.init_digest())
        , _calculating_digest(digest.has_value()) {
    }

    // Convenience constructor that extracts the pieces from an sstable.
    explicit compressed_physical_file_cursor_impl(shared_sstable sst, reader_permit permit, tracing::trace_state_ptr trace_state,
            std::optional<uint32_t> digest = std::nullopt)
        : compressed_physical_file_cursor_impl(
                make_sstable_datafile_io(sst, std::move(permit), std::move(trace_state), sst->ondisk_data_size()),
                make_sstable_compression_format(*sst), digest) {
    }

    void seek(sstable_datafile_position pos) override {
        sstable_cursor_log.trace("[compressed_physical@{}] seek: pos={}", fmt::ptr(this), pos);
        _position = decode_position(pos);
    }

    future<temporary_buffer<char>> read_forwards(size_t n) override {
        SCYLLA_ASSERT(_position.has_value());
        sstable_cursor_log.trace("[compressed_physical@{}] read_forwards: enter n={} chunk_pos={} offset_in_chunk={}", fmt::ptr(this), n, _position->chunk.chunk_position, _position->offset_within_chunk);
        temporary_buffer<char> result(n);
        size_t filled = co_await fill_forwards(result.get_write(), n);
        result.trim(filled);
        sstable_cursor_log.trace("[compressed_physical@{}] read_forwards: exit filled={} chunk_pos={} offset_in_chunk={}", fmt::ptr(this), filled, _position->chunk.chunk_position, _position->offset_within_chunk);
        co_return result;
    }

    future<temporary_buffer<char>> read(sstable_datafile_position start_pos, sstable_datafile_position end_pos) override {
        sstable_cursor_log.trace("[compressed_physical@{}] read: enter start={} end={}", fmt::ptr(this), start_pos, end_pos);
        SCYLLA_ASSERT(start_pos <= end_pos);
        // Read from start to end by replaying the forward path from start. Unlike
        // a logical position, a physical position does not expose a byte distance,
        // so we walk chunk by chunk until we reach end rather than subtracting.
        _position = decode_position(start_pos);
        auto end = decode_position(end_pos);
        auto result = co_await collect_forwards(end);
        // Mirror the logical cursor: a range read leaves the cursor at its start.
        _position = decode_position(start_pos);
        sstable_cursor_log.trace("[compressed_physical@{}] read: exit result_size={}", fmt::ptr(this), result.size());
        co_return result;
    }

    sstable_datafile_position compute_relative_position(ssize_t offset) override {
        SCYLLA_ASSERT(_position.has_value());
        auto result = relative_position(*_position, offset);
        sstable_cursor_log.trace("[compressed_physical@{}] compute_relative_position: chunk_pos={} offset_in_chunk={} delta={} result={}", fmt::ptr(this), _position->chunk.chunk_position, _position->offset_within_chunk, offset, result);
        return result;
    }

    int64_t subtract_positions(sstable_datafile_position b, sstable_datafile_position a) override {
        auto result = positions_distance(decode_position(a), decode_position(b));
        sstable_cursor_log.trace("[compressed_physical@{}] subtract_positions: b={} a={} result={}", fmt::ptr(this), b, a, result);
        return result;
    }

    future<sstable_datafile_position> skip_forwards(sstable_datafile_position from, size_t n) override {
        sstable_cursor_log.trace("[compressed_physical@{}] skip_forwards: enter from={} n={}", fmt::ptr(this), from, n);
        // Walk forward from `from` by n decompressed bytes, crossing whole chunks
        // using the fixed uncompressed chunk length and reading each chunk's
        // on-disk length prefix to learn where the next one begins. This both
        // computes the target position and primes the metadata cache for every
        // chunk crossed, so a later synchronous compute_relative_position around
        // the result has the chunk extents it needs.
        cursor_pos p = decode_position(from);
        uint64_t chunk_len = _uncompressed_chunk_length;
        uint64_t remaining = n;
        uint64_t in_chunk = p.offset_within_chunk;
        while (!at_eof(p) && in_chunk + remaining >= chunk_len) {
            // Learn p's chunk extent so we know where the next chunk begins.
            p.chunk.chunk_length = co_await chunk_length_of(p.chunk.chunk_position);
            remaining -= (chunk_len - in_chunk);
            in_chunk = 0;
            uint64_t next_start = p.chunk.chunk_position + p.chunk.chunk_length;
            p = cursor_pos{chunk_coords{next_start, 0}, 0};
        }
        if (at_eof(p)) {
            // We walked to end of data; the skip must land exactly at EOF.
            SCYLLA_ASSERT(remaining == 0);
            auto result = encode_position(p);
            sstable_cursor_log.trace("[compressed_physical@{}] skip_forwards: exit (eof) result={}", fmt::ptr(this), result);
            co_return result;
        }
        // The target lies inside p's chunk; record its extent too so the position
        // we hand back carries a valid chunk_length.
        p.chunk.chunk_length = co_await chunk_length_of(p.chunk.chunk_position);
        p.offset_within_chunk = in_chunk + remaining;
        auto result = encode_position(p);
        sstable_cursor_log.trace("[compressed_physical@{}] skip_forwards: exit result={}", fmt::ptr(this), result);
        co_return result;
    }

    future<sstable_datafile_position> read_backwards(sstable_datafile_position from, size_t n) override {
        sstable_cursor_log.trace("[compressed_physical@{}] read_backwards: enter from={} n={}", fmt::ptr(this), from, n);
        // Walk backward from `from` by n decompressed bytes, crossing whole chunks
        // using the fixed uncompressed chunk length and reading each chunk's
        // on-disk length prefix to learn where the previous one begins (the prefix
        // carries the previous chunk's compressed length). This both computes the
        // target position and primes the metadata cache for every chunk crossed,
        // so a later synchronous compute_relative_position around the result has
        // the chunk extents it needs.
        cursor_pos p = decode_position(from);
        // Stepping back from EOF would first have to cross the last chunk, which
        // may be shorter than _uncompressed_chunk_length; we would need to
        // decompress it to know by how much. No caller reads backward from EOF
        // (the reversing source always starts from a real row), so reject it
        // rather than carry a possibly-wrong byte count for an unused path.
        SCYLLA_ASSERT(!at_eof(p));
        uint64_t chunk_len = _uncompressed_chunk_length;
        uint64_t remaining = n;
        uint64_t in_chunk = p.offset_within_chunk;
        while (remaining > in_chunk) {
            remaining -= in_chunk;
            // Step to the previous chunk; learn its extent from `p`'s on-disk
            // length prefix so we know where it begins and how long it is.
            co_await step_to_prev_chunk_with_io(p);
            in_chunk = chunk_len;
        }
        p.offset_within_chunk = in_chunk - remaining;
        // p's chunk extent is already known: either it was carried by `from` (no
        // step taken) or step_to_prev_chunk_with_io recorded it when we stepped
        // into this chunk.
        auto result = encode_position(p);
        sstable_cursor_log.trace("[compressed_physical@{}] read_backwards: exit result={}", fmt::ptr(this), result);
        co_return result;
    }

    void drop_caches_after(sstable_datafile_position pos) override {
        sstable_cursor_log.trace("[compressed_physical@{}] drop_caches_after: pos={}", fmt::ptr(this), pos);
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
        sstable_cursor_log.trace("[compressed_physical@{}] drop_caches_before: pos={}", fmt::ptr(this), pos);
        auto p = decode_position(pos);
        // The chunk containing p straddles the boundary and must be kept, so drop
        // only chunks that end at or before its compressed start.
        uint64_t boundary = p.chunk.chunk_position;
        if (_cached_chunk && _cached_chunk->chunk_position + cached_chunk_length() <= boundary) {
            drop_cached_chunk();
        }
        _file.drop_caches_before(sstable_datafile_position::from_logical_fixme(boundary));
    }

    future<> close() override {
        sstable_cursor_log.trace("[compressed_physical@{}] close", fmt::ptr(this));
        return _file.close();
    }

private:
    void drop_cached_chunk() {
        _cached_chunk.reset();
    }

    // The full on-disk extent of the currently cached chunk. Used only to decide
    // whether the cache lies entirely before a drop boundary. The cache always
    // holds the cached chunk's length (it was learned when the chunk was read).
    uint64_t cached_chunk_length() {
        SCYLLA_ASSERT(_cached_chunk.has_value());
        return chunk_length_at(_cached_chunk->chunk_position);
    }

    // Record a chunk's full on-disk extent (prefix + data + checksum) in the
    // navigation cache. Learned from the per-chunk length prefixes; see
    // remember_chunk_layout.
    void remember_chunk_length(uint64_t chunk_position, uint64_t chunk_length) {
        _chunk_length_cache.insert_or_assign(chunk_position, chunk_length);
    }

    // The full on-disk extent of the chunk starting at chunk_position. Relative
    // navigation only reaches a chunk whose prefix has already been read, so the
    // length is expected to be cached; a miss is a programming error (a caller
    // computed a relative position whose metadata it had not first read).
    uint64_t chunk_length_at(uint64_t chunk_position) {
        auto it = _chunk_length_cache.find(chunk_position);
        SCYLLA_ASSERT(it != _chunk_length_cache.end());
        return it->second;
    }

    // Parse the per-chunk length prefix at the front of `chunk`'s on-disk bytes
    // and remember the resulting coordinates of both this chunk and its
    // predecessor. The prefix is two little-endian u32s: this chunk's
    // compressed-data length and the previous chunk's (0 for the first chunk).
    // The full extent of a chunk is prefix + compressed data + 4-byte checksum.
    void remember_chunk_layout(const chunk_coords& chunk, const temporary_buffer<char>& bytes) {
        SCYLLA_ASSERT(_chunk_prefix == chunk_length_prefix_size);
        SCYLLA_ASSERT(bytes.size() >= chunk_length_prefix_size);
        uint32_t this_compressed_len = read_le<uint32_t>(bytes.get());
        uint32_t prev_compressed_len = read_le<uint32_t>(bytes.get() + sizeof(uint32_t));
        SCYLLA_ASSERT(chunk.chunk_length == _chunk_prefix + this_compressed_len + 4);
        remember_chunk_length(chunk.chunk_position, chunk.chunk_length);
        if (chunk.chunk_position > 0) {
            uint64_t prev_length = _chunk_prefix + prev_compressed_len + 4;
            SCYLLA_ASSERT(prev_length <= chunk.chunk_position);
            remember_chunk_length(chunk.chunk_position - prev_length, prev_length);
        }
    }

    // Decode an incoming physical position into the cursor's working form. The
    // chunk's compressed byte range is carried directly by the physical position
    // (chunk_position, chunk_length), so no external lookup is needed. A position
    // at or past the end of the compressed file is the end-of-data sentinel; a
    // forward read from it sees immediate EOF.
    cursor_pos decode_position(sstable_datafile_position pos) {
        auto ph = pos.to_physical();
        chunk_coords chunk{uint64_t(ph.chunk_position), uint64_t(ph.chunk_length)};
        return cursor_pos{chunk, uint64_t(ph.offset_within_chunk)};
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
    // offset) together with the cached chunk extents learned from the on-disk
    // length prefixes. This is synchronous, so every chunk it steps onto must
    // already be in the navigation cache; reaching an uncached chunk asserts.
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

    // The number of decompressed bytes between `a` and `b` (b - a), with a <= b.
    // We never count in a global logical position: starting at `a`, we add the
    // bytes left in its chunk, step whole chunks across the cached extents until
    // we reach b's chunk, then add b's offset within it. The fixed uncompressed
    // chunk length (a structural constant of the format) gives each whole chunk's
    // byte count; the cached extents (learned from the on-disk length prefixes)
    // let us walk chunk_position forward. Synchronous: every chunk between a and b
    // must already be in the navigation cache.
    int64_t positions_distance(cursor_pos a, const cursor_pos& b) {
        uint64_t chunk_len = _compression.uncompressed_chunk_length();
        int64_t distance = 0;
        while (a.chunk.chunk_position != b.chunk.chunk_position) {
            // a is before b, so a is not at EOF and has a full chunk's worth of
            // bytes remaining from its current offset.
            SCYLLA_ASSERT(!at_eof(a));
            distance += chunk_len - a.offset_within_chunk;
            step_to_next_chunk_pos(a);
            a.offset_within_chunk = 0;
        }
        distance += int64_t(b.offset_within_chunk) - int64_t(a.offset_within_chunk);
        return distance;
    }

    // Move p to the start of the next chunk on disk. The next chunk begins right
    // after this one (chunk_position + chunk_length); its extent comes from the
    // navigation cache. Past the last chunk this yields the end-of-data sentinel
    // (start at the end of the compressed file, empty chunk).
    void step_to_next_chunk_pos(cursor_pos& p) {
        uint64_t next_start = p.chunk.chunk_position + p.chunk.chunk_length;
        if (next_start >= _compressed_file_length) {
            p = cursor_pos{chunk_coords{next_start, 0}, 0};
            return;
        }
        uint64_t next_len = chunk_length_at(next_start);
        p = cursor_pos{chunk_coords{next_start, next_len}, 0};
    }

    // Move p to the start of the preceding chunk. The cache holds contiguous,
    // non-overlapping extents, so the chunk just before p is the cache entry
    // immediately below p's start; its extent must end exactly at p's start.
    void step_to_prev_chunk_pos(cursor_pos& p) {
        SCYLLA_ASSERT(p.chunk.chunk_position > 0);
        auto it = _chunk_length_cache.lower_bound(p.chunk.chunk_position);
        SCYLLA_ASSERT(it != _chunk_length_cache.begin());
        --it;
        uint64_t prev_start = it->first;
        uint64_t prev_len = it->second;
        SCYLLA_ASSERT(prev_start + prev_len == p.chunk.chunk_position);
        p = cursor_pos{chunk_coords{prev_start, prev_len}, 0};
    }

    // Move p to the start of the preceding chunk, reading from the file as
    // needed. The backward counterpart of step_to_next_chunk: unlike
    // step_to_prev_chunk_pos (synchronous, requires the previous chunk's extent
    // already cached), this learns the previous chunk's extent from p's own
    // on-disk length prefix (whose second u32 is the previous chunk's compressed
    // length) and primes the navigation cache for it. p must not be at the very
    // first chunk (there is no predecessor).
    future<> step_to_prev_chunk_with_io(cursor_pos& p) {
        // Only ever called while walking backward from a real (non-EOF) chunk;
        // the end-of-data sentinel has no length prefix to read.
        SCYLLA_ASSERT(!at_eof(p));
        SCYLLA_ASSERT(p.chunk.chunk_position > 0);
        // Read p's length prefix; its second u32 is the previous chunk's
        // compressed-data length, from which the previous chunk's full extent and
        // start follow.
        auto prefix = co_await read_exactly_from_file(p.chunk.chunk_position, _chunk_prefix);
        uint32_t this_compressed_len = read_le<uint32_t>(prefix.get());
        uint32_t prev_compressed_len = read_le<uint32_t>(prefix.get() + sizeof(uint32_t));
        // Remember p's own extent while we have it.
        remember_chunk_length(p.chunk.chunk_position, _chunk_prefix + this_compressed_len + 4);
        uint64_t prev_len = _chunk_prefix + prev_compressed_len + 4;
        SCYLLA_ASSERT(prev_len <= p.chunk.chunk_position);
        uint64_t prev_start = p.chunk.chunk_position - prev_len;
        remember_chunk_length(prev_start, prev_len);
        p = cursor_pos{chunk_coords{prev_start, prev_len}, 0};
    }

    // Read forward from the current position into dst (up to len bytes),
    // decompressing chunks and stepping across boundaries. Advances _position.
    // Returns the number of bytes produced (less than len only at EOF).
    future<size_t> fill_forwards(char* dst, size_t len) {
        size_t done = 0;
        while (done < len && !at_eof(*_position)) {
            co_await ensure_position_chunk_length();
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
        if (_position->offset_within_chunk >= _uncompressed_chunk_length) {
            step_to_next_chunk();
        }
        if (!at_eof(*_position)) {
            co_await ensure_position_chunk_length();
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
            co_await ensure_position_chunk_length();
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
        if (_position->offset_within_chunk >= _uncompressed_chunk_length) {
            step_to_next_chunk();
        }
        if (!at_eof(*_position)) {
            co_await ensure_position_chunk_length();
        }
        co_return result;
    }

    // Move _position to the start of the next chunk on disk during a forward
    // read. Unlike step_to_next_chunk_pos (which serves synchronous relative
    // navigation and requires the next chunk to be cached), this leaves the next
    // chunk's length unknown (0); the forward read resolves it with IO via
    // ensure_position_chunk_length before touching the chunk.
    void step_to_next_chunk() {
        uint64_t next_start = _position->chunk.chunk_position + _position->chunk.chunk_length;
        _position = cursor_pos{chunk_coords{next_start, 0}, 0};
    }

    // Ensure _position->chunk.chunk_length is known, reading the on-disk length
    // prefix of the chunk if necessary. step_to_next_chunk leaves the length
    // unknown (0) because it cannot do IO; a forward read fills it in here before
    // reading the chunk. Not called at EOF (the sentinel has no chunk).
    future<> ensure_position_chunk_length() {
        if (_position->chunk.chunk_length != 0) {
            co_return;
        }
        _position->chunk.chunk_length = co_await chunk_length_of(_position->chunk.chunk_position);
    }

    // The full on-disk extent of the chunk starting at chunk_position, reading
    // (and caching) its length prefix from disk if it is not already cached.
    future<uint64_t> chunk_length_of(uint64_t chunk_position) {
        if (auto it = _chunk_length_cache.find(chunk_position); it != _chunk_length_cache.end()) {
            co_return it->second;
        }
        // Read just the length prefix; its first u32 is this chunk's
        // compressed-data length, from which the full extent follows.
        _file.seek(sstable_datafile_position::from_logical_fixme(chunk_position));
        auto prefix = co_await read_exactly_from_file(chunk_position, _chunk_prefix);
        uint32_t this_compressed_len = read_le<uint32_t>(prefix.get());
        uint64_t chunk_length = _chunk_prefix + this_compressed_len + 4;
        remember_chunk_length(chunk_position, chunk_length);
        co_return chunk_length;
    }

    // Ensure the decompressed contents of the chunk at `chunk` are in _cached_chunk.
    future<> ensure_chunk_cached(const chunk_coords& chunk) {
        if (_cached_chunk && _cached_chunk->chunk_position == chunk.chunk_position) {
            co_return;
        }
        auto compressed = co_await read_compressed_chunk(chunk);
        // Learn this chunk's (and its predecessor's) extent from the on-disk
        // length prefix so that later relative navigation can step across these
        // boundaries without IO.
        remember_chunk_layout(chunk, compressed);
        _cached_chunk = cached_chunk{chunk.chunk_position, decompress_chunk(chunk, std::move(compressed))};
    }

    // Read a chunk's compressed bytes (the whole on-disk extent, including its
    // length prefix) from the data file via the inner cursor.
    future<temporary_buffer<char>> read_compressed_chunk(const chunk_coords& chunk) {
        return read_exactly_from_file(chunk.chunk_position, chunk.chunk_length);
    }

    // Read exactly `len` bytes from the raw compressed file at `file_pos` via the
    // inner cursor, throwing if the file ends first.
    future<temporary_buffer<char>> read_exactly_from_file(uint64_t file_pos, size_t len) {
        _file.seek(sstable_datafile_position::from_logical_fixme(file_pos));
        temporary_buffer<char> buf(len);
        size_t filled = 0;
        while (filled < len) {
            auto part = co_await _file.read_forwards(len - filled);
            if (part.empty()) {
                break;
            }
            std::copy_n(part.get(), part.size(), buf.get_write() + filled);
            filled += part.size();
        }
        if (filled != len) {
            throw_malformed_sstable_exception(format(
                    "compressed cursor hit premature end-of-file at file offset {}, expected {} bytes, actual={}",
                    file_pos, len, filled));
        }
        co_return buf;
    }

    // Verify the chunk's trailing checksum and decompress it.
    temporary_buffer<char> decompress_chunk(const chunk_coords& chunk, temporary_buffer<char> compressed) {
        if (chunk.chunk_length < _chunk_prefix + 4) {
            throw_malformed_sstable_exception(format(
                    "compressed chunk_len must be greater than {}, chunk_start={}", _chunk_prefix + 4, chunk.chunk_position));
        }
        // The chunk is an optional length prefix, the compressed data, and a
        // trailing 4-byte checksum of the compressed data. The compressed data
        // starts after the prefix and the checksum covers only it.
        const char* compressed_data = compressed.get() + _chunk_prefix;
        size_t compressed_len = chunk.chunk_length - _chunk_prefix - 4;
        uint32_t expected = read_be<uint32_t>(compressed_data + compressed_len);
        uint32_t actual = checksum(compressed_data, compressed_len);
        if (expected != actual) {
            throw_malformed_sstable_exception(format(
                    "compressed chunk of size {} at file offset {} failed checksum, expected={}, actual={}",
                    chunk.chunk_length, chunk.chunk_position, expected, actual));
        }
        update_digest(chunk, compressed_data, compressed_len, actual);
        // We do not know the chunk's decompressed length a priori (that would be
        // a logical quantity); decompress into a full-chunk-sized buffer and trim
        // to whatever uncompress() actually produced.
        temporary_buffer<char> out(_compression.uncompressed_chunk_length());
        size_t n = _compression.get_compressor().uncompress(compressed_data, compressed_len, out.get_write(), out.size());
        out.trim(n);
        return out;
    }

    uint32_t checksum(const char* input, size_t len) const {
        if (_use_crc32) {
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
        // Chunks must be folded in exactly once, in order, starting at the first
        // chunk (compressed position 0). _next_digest_chunk_position is the start
        // of the chunk we still need. We identify chunks by their compressed start
        // position rather than by index, since this cursor has no chunk indices.
        //  - chunk before next: already folded in (the read rewound and
        //    re-decompressed it). Ignore it - re-folding would corrupt the digest.
        //  - chunk after next: a chunk was skipped, so the run has a gap and can
        //    never cover the whole file. Abandon the digest check.
        //  - chunk == next: the chunk we were waiting for; fold it in.
        if (chunk.chunk_position < _next_digest_chunk_position) {
            return;
        }
        if (chunk.chunk_position > _next_digest_chunk_position) {
            sstlog.debug("Compressed cursor cannot calculate digest: chunk at {} read with a gap (expected {}). Disabling digest check.",
                    chunk.chunk_position, _next_digest_chunk_position);
            _calculating_digest = false;
            return;
        }
        if (_use_crc32) {
            fold_chunk_into_digest<crc32_utils, /*checksum_all=*/true>(compressed_data, compressed_len, chunk_checksum);
        } else {
            fold_chunk_into_digest<adler32_utils, /*checksum_all=*/false>(compressed_data, compressed_len, chunk_checksum);
        }
        // The next chunk we need begins right after this one on disk.
        _next_digest_chunk_position = chunk.chunk_position + chunk.chunk_length;
        // Once a chunk reaching the end of the compressed file has been folded in,
        // the digest covers the whole file.
        if (chunk.chunk_position + chunk.chunk_length == _compressed_file_length) {
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
        // Versions whose index stores a full physical position (i.e. those for
        // which holds_compressed_position() is false, currently `mu`) prefix each
        // chunk with its length and are read by the physical cursor, which
        // navigates by those prefixes. Older versions are read by the logical
        // cursor, which navigates by uncompressed position via the offsets array.
        if (!holds_logical_position(sst->get_version())) {
            return std::make_unique<compressed_physical_file_cursor_impl>(std::move(sst), std::move(permit), std::move(trace_state), digest);
        }
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
int64_t sstable_datafile_cursor::subtract_positions(sstable_datafile_position b, sstable_datafile_position a) {
    return _impl->subtract_positions(b, a);
}
future<sstable_datafile_position> sstable_datafile_cursor::skip_forwards(sstable_datafile_position from, size_t n) {
    return _impl->skip_forwards(from, n);
}
future<sstable_datafile_position> sstable_datafile_cursor::read_backwards(sstable_datafile_position from, size_t n) {
    return _impl->read_backwards(from, n);
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
