/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <bit>

#include <seastar/core/byteorder.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/format.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include "sstables/compression_info_cache.hh"
#include "sstables/exceptions.hh"
#include "utils/bit_cast.hh"
#include "utils/div_ceil.hh"

namespace sstables {

static uint64_t read_raw_offset(const char* raw_offsets, uint32_t i) noexcept {
    return net::ntoh(read_unaligned<uint64_t>(raw_offsets + i * sizeof(uint64_t)));
}

compression_info_bucket_layout compression_info_bucket_layout::for_chunk_size(uint32_t chunk_size) {
    const uint64_t max_chunk_length = max_compressed_chunk_length(chunk_size);

    // Every offset in a bucket is stored relative to the base offset of the bucket.
    // The last offset of the bucket lies at most that many chunks away from it.
    //
    // Since chunk_size is a uint32_t, max_chunk_length fits in 33 bits, so this is
    // at most 42 bits wide, comfortably within max_field_bits.
    const uint8_t base_bits = std::bit_width(uint64_t(offsets_per_bucket - 1) * max_chunk_length);

    // Pick the grouping which makes a bucket take the least memory. A larger group
    // means fewer segment base offsets to store, but wider relative offsets within the segment.
    //
    // Note that the parameters precomputed for segmented_offsets don't apply here:
    // they were chosen to fit as many offsets as possible into a bucket of a fixed
    // *packed* size, while here it's the *unpacked* size of a bucket which is fixed
    // (at one page of CompressionInfo.db) and the packed size which is minimized.
    //
    // Groupings above 255 aren't considered, both because offset_packing can't support that,
    // and because the optimum is always a small number:
    // doubling the group size only saves a bit of the base offset per offset, while
    // it costs a bit on each of the relative offsets.
    constexpr uint32_t max_grouped_offsets = std::min<uint32_t>(offsets_per_bucket, 255);
    uint8_t grouped_offsets = 1;
    uint64_t best_bits = std::numeric_limits<uint64_t>::max();
    for (uint32_t g = 1; g <= max_grouped_offsets; ++g) {
        const uint64_t relative_bits = g > 1 ? std::bit_width((g - 1) * max_chunk_length) : 0;
        const uint64_t segments = (offsets_per_bucket + g - 1) / g;
        const uint64_t bits = segments * (base_bits + (g - 1) * relative_bits);
        if (bits < best_bits) {
            best_bits = bits;
            grouped_offsets = g;
        }
    }

    compression_info_bucket_layout l;
    const uint8_t relative_bits = grouped_offsets > 1
            ? std::bit_width((grouped_offsets - 1) * max_chunk_length)
            : 0;
    l.packing = offset_packing(base_bits, relative_bits, grouped_offsets);
    // The last segment of a bucket can be partially filled, if the grouping doesn't
    // divide the number of offsets in a bucket.
    l.segments_per_bucket = (offsets_per_bucket + grouped_offsets - 1) / grouped_offsets;
    l.storage_size = l.packing.storage_size(l.segments_per_bucket);
    return l;
}

uint64_t pack_bucket_offsets(const compression_info_bucket_layout& layout, char* storage,
        const char* raw_offsets, uint32_t count, uint64_t bucket_idx) {
    const auto& p = layout.packing;
    std::fill(storage, storage + layout.storage_size, 0);

    // The offsets come straight from the file, so they can be anything. A delta
    // which doesn't fit into the bits the layout gives it means either that the
    // offsets aren't monotonic (in which case the subtraction below underflows into
    // a huge value), or that a chunk is longer than any compressor could have made
    // it.
    auto checked = [bucket_idx] (uint64_t delta, uint32_t size_bits) {
        if (delta >> size_bits) {
            throw_malformed_sstable_exception(format(
                    "CompressionInfo.db is malformed: the chunk offsets of bucket {} are not monotonically"
                    " growing, or the chunks they describe are too long: a delta of {} doesn't fit in {} bits",
                    bucket_idx, delta, size_bits));
        }
        return delta;
    };

    const uint64_t base = count ? read_raw_offset(raw_offsets, 0) : 0;
    uint64_t segment_base = base;
    for (uint32_t i = 0; i < count; ++i) {
        const uint64_t segment_bit = p.segment_bit_offset(i);
        const uint32_t relative_index = i % p.grouped_offsets();
        const uint64_t offset = read_raw_offset(raw_offsets, i);
        if (relative_index == 0) {
            segment_base = offset;
            p.write_base(storage, segment_bit, checked(offset - base, p.base_bits()));
        } else {
            p.write_relative(storage, segment_bit, relative_index,
                    checked(offset - segment_base, p.relative_bits()));
        }
    }

    return base;
}

uint64_t unpack_bucket_offset(const compression_info_bucket_layout& layout, const char* storage,
        uint64_t base, uint32_t i) noexcept {
    const auto& p = layout.packing;
    const uint64_t segment_bit = p.segment_bit_offset(i);
    const uint32_t relative_index = i % p.grouped_offsets();

    const uint64_t segment_base = base + p.read_base(storage, segment_bit);
    if (relative_index == 0) {
        return segment_base;
    }
    return segment_base + p.read_relative(storage, segment_bit, relative_index);
}

void compression_info_cache::entry::populate(const char* raw_offsets, uint32_t count) {
    const auto& layout = _parent._layout;
    _storage = _parent._region.alloc_buf(layout.storage_size);
    _base = pack_bucket_offsets(layout, _storage.get(), raw_offsets, count, _idx);
}

uint64_t compression_info_cache::entry::at(uint32_t i) const noexcept {
    return unpack_bucket_offset(_parent._layout, _storage.get(), _base, i);
}

void compression_info_cache::entry::on_evicted() noexcept {
    _parent.on_evicted(*this);
}

bool compression_info_cache::entry::attached() const noexcept {
    return _parent._buckets[_idx] == this;
}

void compression_info_cache::handle::release() noexcept {
    if (!_entry) {
        return;
    }
    entry* e = std::exchange(_entry, nullptr);
    if (--e->_use_count == 0) {
        if (e->attached()) {
            e->_parent._lru.add(*e);
        } else {
            delete e;
        }
    }
}

bucket_reader_fn make_file_bucket_reader(file f) {
    if (!f) {
        return {};
    }
    return [f = std::move(f)] (uint64_t pos, size_t len) mutable {
        return f.dma_read_exactly<char>(pos, len);
    };
}

compression_info_cache::compression_info_cache(file f, const compression& c, lru& lru_,
        logalloc::region& region, compression_info_cache_stats& stats, sstring file_name)
    : compression_info_cache(make_file_bucket_reader(std::move(f)), c, lru_, region, stats, std::move(file_name))
{ }

compression_info_cache::compression_info_cache(bucket_reader_fn read_bucket, const compression& c, lru& lru_,
        logalloc::region& region, compression_info_cache_stats& stats, sstring file_name)
    : _read_bucket(std::move(read_bucket))
    , _file_name(std::move(file_name))
    , _compression(c)
    , _lru(lru_)
    , _region(region)
    , _stats(stats)
    // Without a reader there is nothing to read the buckets from, so we have to fall
    // back to the copy of the offsets kept in memory by sstables::compression. This
    // happens for sstables which were never opened for reading, e.g. in some of the
    // tools and tests.
    // A CompressionInfo.db which was never parsed or written through this process
    // (which happens in some tests) has no recorded position of its offsets, so it
    // can't be read from either.
    , _paged(bool(_read_bucket) && c.chunk_count() > 0 && c.offsets_start_pos() > 0)
    , _layout(compression_info_bucket_layout::for_chunk_size(c.uncompressed_chunk_length()))
    , _bucket_size_in_allocator(sizeof(entry) + _layout.storage_size)
{
    if (_paged) {
        _buckets.resize(div_ceil(c.chunk_count(), _layout.offsets_per_bucket), nullptr);
    }
}

compression_info_cache::~compression_info_cache() {
    for (entry* e : _buckets) {
        if (!e) {
            continue;
        }
        // All the handles must be gone by now, so every live bucket is in the LRU.
        SCYLLA_ASSERT(!e->_use_count);
        _lru.remove(*e);
        _stats.used_bytes -= _bucket_size_in_allocator;
        delete e;
    }
}

compression_info_cache::handle compression_info_cache::share(entry& e) noexcept {
    if (e._use_count++ == 0 && e.is_linked()) {
        _lru.remove(e);
    }
    handle h;
    h._entry = &e;
    return h;
}

void compression_info_cache::detach(entry& e) noexcept {
    if (e.attached()) {
        _buckets[e._idx] = nullptr;
    }
}

void compression_info_cache::on_evicted(entry& e) noexcept {
    _stats.used_bytes -= _bucket_size_in_allocator;
    ++_stats.evictions;
    _buckets[e._idx] = nullptr;
    delete &e;
}

future<> compression_info_cache::load(entry& e) {
    const uint64_t chunk_count = _compression.chunk_count();
    const uint64_t first = e._idx * _layout.offsets_per_bucket;
    const uint32_t count = std::min<uint64_t>(_layout.offsets_per_bucket, chunk_count - first);
    const size_t size = count * sizeof(uint64_t);

    const uint64_t pos = _compression.offsets_start_pos() + first * sizeof(uint64_t);
    temporary_buffer<char> buf;
    try {
        buf = co_await _read_bucket(pos, size);
    } catch (const file::eof_error&) {
        // The offsets this bucket describes aren't all there, which means that
        // CompressionInfo.db was truncated (or that it never was as long as its
        // header says). Let's give some context to the bare IO error. 
        throw_malformed_sstable_exception(format(
                "CompressionInfo.db is truncated: the {} bytes of the chunk offsets of bucket {}"
                " at position {} could not be read",
                size, e._idx, pos));
    }
    // A reader is required to fail rather than return a short buffer, and the one
    // over a file does (dma_read_exactly() throws at EOF). Checked anyway, because
    // populate() reads exactly `count` offsets out of the buffer, and a short one
    // would make it overread.
    if (buf.size() < size) {
        throw_malformed_sstable_exception(format(
                "CompressionInfo.db is truncated: only {} of the {} bytes of the chunk offsets of bucket {} could be read",
                buf.size(), size, e._idx));
    }

    _as(_region, [&] {
        e.populate(buf.get(), count);
    });
}

future<compression_info_cache::handle> compression_info_cache::get_bucket(uint64_t bucket_idx, use_caching caching,
        const tracing::trace_state_ptr& trace_state) {
    if (caching == use_caching::yes) {
        if (entry* e = _buckets[bucket_idx]) {
            auto h = share(*e);
            if (e->ready()) {
                ++_stats.hits;
                tracing::trace(trace_state, "compression info cache hit: file={}, bucket={}", _file_name, bucket_idx);
                co_return std::move(h);
            }
            // Somebody else is already reading this bucket; wait for them.
            ++_stats.blocks;
            tracing::trace(trace_state, "compression info cache blocked: file={}, bucket={}", _file_name, bucket_idx);
            co_await e->_loading->get_shared_future();
            co_return std::move(h);
        }
    }

    ++_stats.misses;
    ++_stats.blocks;
    tracing::trace(trace_state, "compression info cache miss: file={}, bucket={}, caching={}",
            _file_name, bucket_idx, caching == use_caching::yes);

    // A BYPASS CACHE read gets a bucket of its own, which isn't inserted into
    // _buckets and dies together with the last handle to it.
    const bool attach = caching == use_caching::yes;
    auto e = std::make_unique<entry>(*this, bucket_idx);
    auto h = share(*e);
    if (attach) {
        _buckets[bucket_idx] = e.get();
    }
    entry& entry_ref = *e.release();
    entry_ref._loading = make_lw_shared<shared_promise<>>();
    auto pr = entry_ref._loading;

    std::exception_ptr ex;
    try {
        co_await load(entry_ref);
    } catch (...) {
        ex = std::current_exception();
    }
    entry_ref._loading = nullptr;
    if (ex) {
        // Make the entry unreachable, so that the next lookup retries the read. It
        // is destroyed when the last handle to it (including the handles of the
        // waiters resolved below) dies.
        detach(entry_ref);
        pr->set_exception(ex);
        std::rethrow_exception(std::move(ex));
    }
    pr->set_value();
    if (attach) {
        _stats.used_bytes += _bucket_size_in_allocator;
        ++_stats.populations;
    }
    co_return std::move(h);
}

future<> compression_info_cache::evict_gently() {
    // The size of _buckets is fixed at construction, so the indices stay valid
    // across the yields below, even though the slots can change.
    for (uint64_t idx = 0; idx < _buckets.size(); ++idx) {
        entry* e = _buckets[idx];
        // A bucket which is in use (which includes one which is still loading) can't
        // be evicted.
        if (e && e->_use_count == 0) {
            // An attached and unused bucket is always linked in the LRU.
            _lru.remove(*e);
            on_evicted(*e);
        }
        co_await coroutine::maybe_yield();
    }
}

future<compression::chunk_and_offset> compression_info_cache::get_chunk(uint64_t chunk_index,
        unsigned offset_in_chunk, handle& h, use_caching caching, const tracing::trace_state_ptr& trace_state) {
    if (!_paged) {
        if (!h._accessor) {
            h._accessor.emplace(_compression.offsets.get_accessor());
        }
        const uint64_t start = h._accessor->at(chunk_index);
        const uint64_t end = (chunk_index + 1 == _compression.offsets.size())
                ? _compression.compressed_file_length()
                : h._accessor->at(chunk_index + 1);
        co_return compression::chunk_and_offset{start, end - start, offset_in_chunk};
    }

    if (chunk_index >= _compression.chunk_count()) {
        throw std::out_of_range(format("compression_info_cache: chunk index {} is out of range", chunk_index));
    }

    const uint64_t bucket_idx = chunk_index / _layout.offsets_per_bucket;
    const uint32_t i = chunk_index - bucket_idx * _layout.offsets_per_bucket;
    // The chunk ends where the next one starts, and the last chunk of the file ends
    // with the file.
    const bool ends_in_next_bucket = i + 1 == _layout.offsets_per_bucket
            && chunk_index + 1 < _compression.chunk_count();

    if (!ends_in_next_bucket) {
        // If h._entry->_idx == bucket_idx, we can reuse the entry in the handle
        // without looking up a new bucket.
        // This matters for BYPASS CACHE reads, where an unconditional lookup
        // would refetch the same bucket from disk.
        if (!h._entry || h._entry->_idx != bucket_idx) {
            h = co_await get_bucket(bucket_idx, caching, trace_state);
        }
        const uint64_t start = h._entry->at(i);
        const uint64_t end = chunk_index + 1 == _compression.chunk_count()
                ? _compression.compressed_file_length()
                : h._entry->at(i + 1);
        co_return compression::chunk_and_offset{start, end - start, offset_in_chunk};
    }

    // The last chunk of a bucket ends at the first offset of the next bucket, so
    // both buckets are needed. The returned handle holds the *later* bucket, because
    // reads usually go forward, and the next lookup of a forward read falls into it.
    handle prev;
    if (h._entry && h._entry->_idx == bucket_idx) {
        prev = std::move(h);
    } else {
        prev = co_await get_bucket(bucket_idx, caching, trace_state);
    }
    if (!h._entry || h._entry->_idx != bucket_idx + 1) {
        h = co_await get_bucket(bucket_idx + 1, caching, trace_state);
    }
    const uint64_t start = prev._entry->at(i);
    const uint64_t end = h._entry->at(0);
    co_return compression::chunk_and_offset{start, end - start, offset_in_chunk};
}

}
