/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <deque>
#include <functional>
#include <optional>

#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/temporary_buffer.hh>

#include "sstables/compress.hh"
#include "sstables/compression_info_cache_stats.hh"
#include "sstables/offset_packing.hh"
#include "sstables/types.hh"
#include "tracing/trace_state.hh"
#include "utils/assert.hh"
#include "utils/logalloc.hh"
#include "utils/lru.hh"

namespace sstables {

// An upper bound on the post-compression size of a compressed Data.db chunk.
//
// It is used to pick the bitwidth for the bitpacked offsets,
// so it really has to be an upper bound, otherwise we could construct a series
// of chunks which would overflow the bitpacked field and fail the write.
//
// It has to cover both the compressed data and any metadata we append.
// (Like the checksum).
//
// The compressors' own bounds are, for an input of n bytes, are:
//   snappy:  n + n/6 + 32   (see snappy::MaxCompressedLength())
//   lz4:     n + n/255 + 16 (see LZ4_compressBound())
//   zstd:    n + n/256 + 64 (see ZSTD_COMPRESSBOUND())
//   deflate: n + n/4096 + n/16384 + n/1048576 + 13 (zlib's compressBound())
// Snappy's bound dominates all the others, and the extra 64 bytes are reserved for
// our own metadata (the 4-byte checksum which follows the chunk, and the headers
// some of the compressors prepend to it).
//
// Note that this is different from the bound assumed by segmented_offsets (chunk_size + 64),
// That one is wrong. (But with the parameters used in practice, it still manages
// to yield sufficiently big bitfields, barely).
constexpr uint64_t max_compressed_chunk_length(uint32_t chunk_size) noexcept {
    return uint64_t(chunk_size) + chunk_size / 6 + 32 + 64;
}

// Describes the bit-packed layout of the chunk offsets inside a "bucket" (i.e. a packed page of offsets).
//
// The encoding is the same as used by compression::segmented_offsets (see offset_packing),
// but the parameters are different, because the constraint on the size of a bucket
// is different. In segmented_offsets, a bucket has a fixed post-encoding size limit
// of 4 kiB and tries to fit as many offsets as possible in it;
// here, a bucket holds a fixed number of offsets - one page worth
// of CompressionInfo.db - and tries to take as little memory as it can.
struct compression_info_bucket_layout {
    // The number of chunk offsets held by one bucket.
    //
    // This is chosen so that each bucket spans a 4 kiB range of CompressionInfo.db.
    // This keeps the disk reads reasonably small.
    // (2 filesystem pages, probably, because that 4 kiB range is unaligned).
    static constexpr uint32_t offsets_per_bucket = 4096 / sizeof(uint64_t);

    // The parameters of the bitpacking layout used by this bucket.
    offset_packing packing;
    uint32_t segments_per_bucket;
    // The size of the packed storage of a bucket, in bytes. Contains slack for the
    // unaligned 64-bit words the packed offsets are read and written through.
    uint32_t storage_size;

    static compression_info_bucket_layout for_chunk_size(uint32_t chunk_size);
};

// Packs offsets, given in their on-disk (64-bit big endian) form,
// into `storage`, which must be at least `layout.storage_size` bytes long.
// The whole of `storage` is overwritten: the bits which don't belong to any offset are zeroed.
//
// The offsets are stored relative to the first one, which is returned, and which the
// unpacker needs to be given back.
//
// Throws malformed_sstable_exception if the offsets don't fit the packing, which
// happens if they aren't monotonic, or if the chunk lengths they describe exceed
// max_compressed_chunk_length().
// (Those conditions are supposed to never happen in real usage).
//
// `bucket_idx` is only used to identify the bucket in potential error messages.
uint64_t pack_bucket_offsets(const compression_info_bucket_layout& layout, char* storage,
        const char* raw_offsets, uint32_t count, uint64_t bucket_idx);

// The offset with index `i` within a bucket packed by pack_bucket_offsets(), where
// `base` is the value that call returned. `i` must be below the `count` it was given.
uint64_t unpack_bucket_offset(const compression_info_bucket_layout& layout, const char* storage,
        uint64_t base, uint32_t i) noexcept;

// Reads the `len` bytes at position `pos` of CompressionInfo.db, which hold the raw
// chunk offsets of one bucket.
//
// In practice, this is a layer of indirection over a file.
// It is added so that tests don't have to deal with the whole file interface. 
using bucket_reader_fn = std::function<future<temporary_buffer<char>>(uint64_t pos, size_t len)>;

// A bucket reader serving the offsets from an open CompressionInfo.db. The file is
// held alive by the returned function, so it stays open as long as the reader lives.
// Returns an empty function if the file isn't open.
bucket_reader_fn make_file_bucket_reader(file f);

// Serves the contents of CompressionInfo.db, to the readers of the Data.db.
// The fetched parts of CompressionInfo.db are cached and evicted on demand.
//
// The offsets are divided into buckets, each containing a fixed number of offsets.
// A bucket is read from CompressionInfo.db on demand and kept in a bit-packed form in LSA.
// Buckets which aren't actively held by some reader are linked in an LRU and can be evicted.
//
// Must not be destroyed until the handles it returned are gone.
class compression_info_cache {
public:
    class handle;

private:
    // One bucket of chunk offsets.
    //
    // Unlike the entries of partition_index_cache, the entry object itself lives in
    // the standard allocator, and only its packed storage lives in LSA. Thus it is
    // never moved by LSA compaction and can be pointed to directly.
    // (This is similar to cached_file::cached_page).
    class entry final : public evictable {
        friend class compression_info_cache;
        friend class handle;

        compression_info_cache& _parent;
        // The index of this bucket. It holds the offsets of the chunks
        // [_idx * offsets_per_bucket, _idx * offsets_per_bucket + _count).
        uint64_t _idx;
        // The number of live handles to this entry. While it is non-zero, the entry
        // isn't linked in the LRU, and thus can't be evicted.
        size_t _use_count = 0;
        // Set while the bucket is being read from the file. Resolved (or failed)
        // when the read completes.
        lw_shared_ptr<shared_promise<>> _loading;
        // The packed offsets. Engaged only after a successful load.
        logalloc::lsa_buffer _storage;
        // The value of the first offset of the bucket. The packed offsets are
        // relative to it.
        uint64_t _base = 0;
    public:
        entry(compression_info_cache& parent, uint64_t idx) noexcept
            : _parent(parent)
            , _idx(idx)
        { }

        entry(entry&&) = delete;
        entry(const entry&) = delete;

        ~entry() {
            SCYLLA_ASSERT(!_use_count);
        }

        void on_evicted() noexcept override;

        bool ready() const noexcept { return !_loading; }

        // True if the entry is reachable through compression_info_cache::_buckets.
        // It isn't if it was created for a BYPASS CACHE read, or if it was detached
        // after a failed load. Such an entry is destroyed when its last handle dies,
        // rather than being linked in the LRU.
        bool attached() const noexcept;

        // Fills the bucket with `count` offsets, read from CompressionInfo.db in
        // their on-disk (64-bit big endian) form. See pack_bucket_offsets(), which
        // does the packing and states what it throws.
        void populate(const char* raw_offsets, uint32_t count);

        // The offset with the given index within this bucket, i.e. the start of the
        // corresponding chunk.
        uint64_t at(uint32_t i) const noexcept;
    };

public:
    // A pin on a bucket. As long as it is alive, the bucket isn't evicted, and the
    // offsets it holds can be read without deferring.
    //
    // Held by compression_info_accessor between the calls it makes to the cache, so
    // that consecutive lookups falling into the same bucket don't have to go to the
    // cache at all.
    class handle {
        friend class compression_info_cache;

        entry* _entry = nullptr;
        // The lookup state used when the offsets are served from the in-memory copy
        // in sstables::compression instead of from the file. See _paged.
        std::optional<compression::segmented_offsets::accessor> _accessor;

    public:
        handle() = default;

        handle(handle&& o) noexcept
            : _entry(std::exchange(o._entry, nullptr))
            , _accessor(std::move(o._accessor))
        { }

        handle& operator=(handle&& o) noexcept {
            if (this != &o) {
                release();
                _entry = std::exchange(o._entry, nullptr);
                // segmented_offsets::accessor holds a reference, so it can be
                // copied and moved, but not assigned.
                _accessor.reset();
                if (o._accessor) {
                    _accessor.emplace(*o._accessor);
                    o._accessor.reset();
                }
            }
            return *this;
        }

        handle(const handle&) = delete;

        ~handle() {
            release();
        }

        explicit operator bool() const noexcept {
            return _entry;
        }

        void release() noexcept;
    };

private:
    // Reads the raw offsets of a bucket on demand. Empty if there is nothing to read
    // them from, in which case the offsets are served from `_compression` instead.
    bucket_reader_fn _read_bucket;
    // The name of CompressionInfo.db, for tracing. Empty if unknown.
    sstring _file_name;
    // Used as a delegate if evictability of CompressionInfo is disabled.
    const compression& _compression;
    // The LRU the buckets are linked in when unused. Separate from the LRU of the
    // row cache, so that compression info can be given an eviction policy of its own.
    lru& _lru;
    // The region the buckets are allocated in.
    logalloc::region& _region;
    logalloc::allocating_section _as;
    compression_info_cache_stats& _stats;
    // True if the offsets are read from CompressionInfo.db on demand. False if there
    // is nothing to read them from, in which case they are served from the in-memory
    // copy kept in `_compression`.
    bool _paged;
    compression_info_bucket_layout _layout;
    // The size a bucket is accounted for in _stats. The same for every bucket,
    // including the last one, which can hold fewer offsets -- the number is only
    // used for accounting, so the small inaccuracy doesn't matter.
    size_t _bucket_size_in_allocator;
    // The buckets, by index. A null slot means that the bucket isn't cached.
    // This is the only part of the cache whose size is proportional to the size of
    // the data, and it's smaller than the offsets themselves by a factor of
    // ~offsets_per_bucket.
    std::deque<entry*> _buckets;

private:
    handle share(entry&) noexcept;
    void detach(entry&) noexcept;
    void on_evicted(entry&) noexcept;
    future<handle> get_bucket(uint64_t bucket_idx, use_caching caching, const tracing::trace_state_ptr& trace_state);
    future<> load(entry&);
    future<compression::chunk_and_offset> get_chunk(uint64_t chunk_index, unsigned offset_in_chunk,
            handle& h, use_caching caching, const tracing::trace_state_ptr& trace_state);

public:
    // Serves the offsets of the given CompressionInfo.db, reading them from `f`.
    compression_info_cache(file f, const compression& c, lru& lru_, logalloc::region& region,
            compression_info_cache_stats& stats, sstring file_name = {});
    // Serves the offsets through the given reader instead of through a file. For
    // tests; see bucket_reader_fn.
    compression_info_cache(bucket_reader_fn read_bucket, const compression& c, lru& lru_, logalloc::region& region,
            compression_info_cache_stats& stats, sstring file_name = {});
    ~compression_info_cache();

    compression_info_cache(compression_info_cache&&) = delete;
    compression_info_cache(const compression_info_cache&) = delete;

    // Locates, in the compressed file, the given byte position of the uncompressed
    // data: the byte range containing the appropriate compressed chunk, and the
    // offset into the chunk after decompression.
    //
    // May only be used for positions of actual bytes; in particular the
    // end-of-file position (one past the last byte) MUST not be used.
    // Fails with std::out_of_range if the position is beyond the last chunk.
    future<compression::chunk_and_offset> locate(uint64_t position, handle& h, use_caching caching,
            const tracing::trace_state_ptr& trace_state) {
        auto ucl = _compression.uncompressed_chunk_length();
        return get_chunk(position / ucl, position % ucl, h, caching, trace_state);
    }

    // Returns the extent of the compressed chunk with the given index.
    // The returned offset into the uncompressed chunk is always 0.
    future<compression::chunk_and_offset> get_chunk_by_index(uint64_t chunk_index, handle& h, use_caching caching,
            const tracing::trace_state_ptr& trace_state) {
        return get_chunk(chunk_index, 0, h, caching, trace_state);
    }

    uint64_t uncompressed_chunk_length() const noexcept {
        return _compression.uncompressed_chunk_length();
    }

    // The number of chunks the Data.db is split into, i.e. the number of
    // offsets in CompressionInfo.db.
    uint64_t chunk_count() const noexcept {
        return _compression.chunk_count();
    }

    uint64_t uncompressed_file_length() const noexcept {
        return _compression.uncompressed_file_length();
    }

    uint64_t compressed_file_length() const noexcept {
        return _compression.compressed_file_length();
    }

    compressor& get_compressor() const {
        return _compression.get_compressor();
    }

    // True if the offsets are read from CompressionInfo.db on demand, rather than
    // served from the in-memory copy in sstables::compression. For tests.
    bool paged() const noexcept {
        return _paged;
    }

    const compression_info_bucket_layout& layout() const noexcept {
        return _layout;
    }

    // Evicts all buckets which aren't held by a handle, yielding as needed.
    //
    // Used to release the memory of a cache which is about to become useless (e.g.
    // because its sstable is being closed) without doing it all in one preemption
    // period, and without waiting for the LRU to get around to it.
    future<> evict_gently();
};

// The view of compression info used by readers of the compressed file.
//
// Exposes only what they need, so that the caching and packing of compression
// info is hidden as an implementation detail.
// Holds per-reader state, so each reader needs its own accessor.
class compression_info_accessor {
    compression_info_cache& _cache;
    compression_info_cache::handle _handle;
    // Whether the buckets this reader touches should be cached. A BYPASS CACHE read
    // doesn't insert into (and doesn't hit) the shared cache. Instead, the handle
    // holds alive a private "cache entry", which dies with the handle.
    // This way we avoid polluting the cache while still avoid repeated disk loads
    // within the same CompressionInfo.db page.
    use_caching _caching;
    // The trace state of the read this accessor serves, if it is traced.
    tracing::trace_state_ptr _trace_state;
public:
    explicit compression_info_accessor(compression_info_cache& cache, use_caching caching = use_caching::yes,
            tracing::trace_state_ptr trace_state = {}) noexcept
        : _cache(cache)
        , _caching(caching)
        , _trace_state(std::move(trace_state))
    { }

    compression_info_accessor(compression_info_accessor&&) = delete;
    compression_info_accessor(const compression_info_accessor&) = delete;

    future<compression::chunk_and_offset> locate(uint64_t position) {
        return _cache.locate(position, _handle, _caching, _trace_state);
    }

    future<compression::chunk_and_offset> get_chunk_by_index(uint64_t chunk_index) {
        return _cache.get_chunk_by_index(chunk_index, _handle, _caching, _trace_state);
    }

    uint64_t uncompressed_chunk_length() const noexcept {
        return _cache.uncompressed_chunk_length();
    }

    uint64_t chunk_count() const noexcept {
        return _cache.chunk_count();
    }

    uint64_t uncompressed_file_length() const noexcept {
        return _cache.uncompressed_file_length();
    }

    uint64_t compressed_file_length() const noexcept {
        return _cache.compressed_file_length();
    }

    compressor& get_compressor() const {
        return _cache.get_compressor();
    }
};

}
