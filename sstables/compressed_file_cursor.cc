/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <cstdlib>

#include <seastar/core/align.hh>
#include <seastar/core/file.hh>
#include <seastar/core/format.hh>

#include "sstables/compressed_file_cursor.hh"
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

class compressed_file_cursor_impl final : public sstable_datafile_cursor::impl {
    shared_sstable _sst;
    reader_permit _permit;
    tracing::trace_state_ptr _trace_state;
    //FIXME
};

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

public:
    explicit uncompressed_file_cursor_impl(shared_sstable sst, reader_permit permit, tracing::trace_state_ptr trace_state)
        : _sst(std::move(sst))
        , _permit(std::move(permit))
        , _trace_state(std::move(trace_state))
        , _block_size(_sst->get_data_file().disk_read_dma_alignment()) {
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
        uint64_t file_len = _sst->data_size();
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
