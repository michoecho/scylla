/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "sstables/key.hh"
#include "mutation/tombstone.hh"
#include "utils/cached_file.hh"
#include "trie_serializer.hh"

namespace trie {

struct page_ptr : cached_file::ptr_type {
    using parent = cached_file::ptr_type;
    page_ptr() noexcept = default;
    page_ptr(parent&& x) noexcept : parent(std::move(x)) {}
    page_ptr(const page_ptr& other) noexcept : parent(other ? other->share() : nullptr) {}
    page_ptr(page_ptr&&) noexcept = default;
    page_ptr& operator=(page_ptr&&) noexcept = default;
    page_ptr& operator=(const page_ptr& other) noexcept {
        parent::operator=(other ? other->share() : nullptr);
        return *this;
    }
};

// Each row index trie in Rows.db is followed by a header containing some partition metadata.
// The partition's entry in Partitions.db points into that header.
struct row_index_header {
    // The partiition key, in BIG serialization format (i.e. the same as in Data.db or Index.db).
    sstables::key partition_key = bytes();
    // The global position of the root node of this partition's row index within Rows.db.
    uint64_t trie_root;
    // The global position of the partition inside Data.db.
    uint64_t data_file_offset;
    // The partition tombstone of this partition.
    tombstone partition_tombstone;
};

struct bti_node_reader {
    page_ptr _cached_page;
    std::reference_wrapper<cached_file> _file;

    bti_node_reader(cached_file& f) : _file(f) {}

    bool cached(int64_t pos) const {
        return _cached_page && _cached_page->pos() / cached_file::page_size == pos / cached_file::page_size;
    }

    future<> load(int64_t pos) {
        return _file.get().get_page_view(pos, nullptr).then(
            [this] (auto page) {
                _cached_page = std::move(page.first);
        });
    }

    trie::load_final_node_result read_final_node(int64_t pos) {
        SCYLLA_ASSERT(cached(pos));
        auto sp = _cached_page->get_view().subspan(pos % cached_file::page_size);
        auto type = uint8_t(sp[0]) >> 4;
        trie::load_final_node_result result;
        auto single = [&] (uint8_t payload_bits) {
            result.n_children = 1;
            result.payload_bits = payload_bits;
            return result;
        };
        auto sparse = [&] (int type) {
            int n_children = int(sp[1]);
            result.n_children = n_children;
            result.payload_bits = uint8_t(sp[0]) & 0xf;
            return result;
        };
        auto dense = [&] (int type) {
            auto dense_span = uint64_t(sp[2]) + 1;
            result.n_children = dense_span;
            result.payload_bits = uint8_t(sp[0]) & 0xf;
            return result;
        };
        switch (type) {
        case PAYLOAD_ONLY:
            result.payload_bits = uint8_t(sp[0]) & 0xf;
            result.n_children = 0;
            return result;
        case SINGLE_NOPAYLOAD_4:
            return single(0);
        case SINGLE_NOPAYLOAD_12:
            return single(0);
        case SINGLE_8:
            return single(uint8_t(sp[0]) & 0xf);
        case SINGLE_16:
            return single(uint8_t(sp[0]) & 0xf);
        case SPARSE_8:
            return sparse(type);
        case SPARSE_12:
            return sparse(type);
        case SPARSE_16:
            return sparse(type);
        case SPARSE_24:
            return sparse(type);
        case SPARSE_40:
            return sparse(type);
        case DENSE_12:
            return dense(type);
        case DENSE_16:
            return dense(type);
        case DENSE_24:
            return dense(type);
        case DENSE_32:
            return dense(type);
        case DENSE_40:
            return dense(type);
        case LONG_DENSE:
            return dense(type);
        }
        abort();
    }
    trie::node_traverse_result traverse_node_by_key(int64_t pos, const_bytes key) {
        SCYLLA_ASSERT(cached(pos));
        auto sp = _cached_page->get_view().subspan(pos % cached_file::page_size);
        auto type = uint8_t(sp[0]) >> 4;
        trie::node_traverse_result result;
        result.body_pos = pos;
        result.traversed_key_bytes = 0;
        auto single = [&] (std::byte edge, uint64_t offset, uint8_t payload_bits) {
            result.n_children = 1;
            result.payload_bits = payload_bits;
            result.child_offset = offset;
            if (key[0] <= edge) {
                result.found_idx = 0;
                result.found_byte = int(edge);
            } else {
                result.found_idx = 1;
                result.found_byte = -1;
            }
            return result;
        };
        auto sparse = [&] (int type) {
            int n_children = int(sp[1]);
            auto idx = std::lower_bound(&sp[2], &sp[2 + n_children], key[0]) - &sp[2];
            result.n_children = n_children;
            result.payload_bits = uint8_t(sp[0]) & 0xf;
            result.found_idx = idx;
            if (idx < n_children) {
                auto bpp = bits_per_pointer_arr[type];
                result.child_offset = read_offset(sp.subspan(2+n_children), idx, bpp);
                result.found_byte = int(sp[2 + idx]);
            } else {
                result.child_offset = -1;
                result.found_byte = -1;
            }
            return result;
        };
        auto dense = [&] (int type) {
            auto start = int(sp[1]);
            auto idx = std::max<int>(0, int(key[0]) - start);
            auto dense_span = uint64_t(sp[2]) + 1;
            auto bpp = bits_per_pointer_arr[type];
            result.n_children = dense_span;
            result.payload_bits = uint8_t(sp[0]) & 0xf;
            while (idx < int(dense_span)) {
                if (auto off = read_offset(sp.subspan(3), idx, bpp)) {
                    result.child_offset = off;
                    result.found_idx = idx;
                    result.found_byte = start + idx;
                    return result;
                } else {
                    ++idx;
                }
            }
            result.found_idx = dense_span;
            result.child_offset = -1;
            result.found_byte = -1;
            return result;
        };
        switch (type) {
        case PAYLOAD_ONLY:
            result.payload_bits = uint8_t(sp[0]) & 0xf;
            result.n_children = 0;
            result.found_idx = 0;
            result.found_byte = -1;
            return result;
        case SINGLE_NOPAYLOAD_4:
            return single(sp[1], uint64_t(sp[0]) & 0xf, 0);
        case SINGLE_NOPAYLOAD_12:
            return single(sp[2], (uint64_t(sp[0]) & 0xf) << 8 | uint64_t(sp[1]), 0);
        case SINGLE_8:
            return single(sp[1], uint64_t(sp[2]), uint8_t(sp[0]) & 0xf);
        case SINGLE_16:
            return single(sp[1], uint64_t(sp[2]) << 8 | uint64_t(sp[3]), uint8_t(sp[0]) & 0xf);
        case SPARSE_8:
            return sparse(type);
        case SPARSE_12:
            return sparse(type);
        case SPARSE_16:
            return sparse(type);
        case SPARSE_24:
            return sparse(type);
        case SPARSE_40:
            return sparse(type);
        case DENSE_12:
            return dense(type);
        case DENSE_16:
            return dense(type);
        case DENSE_24:
            return dense(type);
        case DENSE_32:
            return dense(type);
        case DENSE_40:
            return dense(type);
        case LONG_DENSE:
            return dense(type);
        }
        abort();
    }
    trie::node_traverse_sidemost_result traverse_node_leftmost(int64_t pos) {
        SCYLLA_ASSERT(cached(pos));
        auto sp = _cached_page->get_view().subspan(pos % cached_file::page_size);
        auto type = uint8_t(sp[0]) >> 4;
        trie::node_traverse_sidemost_result result;
        result.body_pos = pos;
        auto single = [&] (uint64_t offset, uint8_t payload_bits) {
            result.n_children = 1;
            result.payload_bits = payload_bits;
            result.child_offset = offset;
            return result;
        };
        auto sparse = [&] (int type) {
            int n_children = int(sp[1]);
            auto bpp = bits_per_pointer_arr[type];
            result.n_children = n_children;
            result.payload_bits = uint8_t(sp[0]) & 0xf;
            result.child_offset = read_offset(sp.subspan(2+n_children), 0, bpp);
            return result;
        };
        auto dense = [&] (int type) {
            auto dense_span = uint64_t(sp[2]) + 1;
            auto bpp = bits_per_pointer_arr[type];
            result.n_children = dense_span;
            result.payload_bits = uint8_t(sp[0]) & 0xf;
            result.child_offset = read_offset(sp.subspan(3), 0, bpp);
            return result;
        };
        switch (type) {
        case PAYLOAD_ONLY:
            result.payload_bits = uint8_t(sp[0]) & 0xf;
            result.n_children = 0;
            result.child_offset = -1;
            return result;
        case SINGLE_NOPAYLOAD_4:
            return single(uint64_t(sp[0]) & 0xf, 0);
        case SINGLE_NOPAYLOAD_12:
            return single((uint64_t(sp[0]) & 0xf) << 8 | uint64_t(sp[1]), 0);
        case SINGLE_8:
            return single(uint64_t(sp[2]), uint8_t(sp[0]) & 0xf);
        case SINGLE_16:
            return single(uint64_t(sp[2]) << 8 | uint64_t(sp[3]), uint8_t(sp[0]) & 0xf);
        case SPARSE_8:
            return sparse(type);
        case SPARSE_12:
            return sparse(type);
        case SPARSE_16:
            return sparse(type);
        case SPARSE_24:
            return sparse(type);
        case SPARSE_40:
            return sparse(type);
        case DENSE_12:
            return dense(type);
        case DENSE_16:
            return dense(type);
        case DENSE_24:
            return dense(type);
        case DENSE_32:
            return dense(type);
        case DENSE_40:
            return dense(type);
        case LONG_DENSE:
            return dense(type);
        }
        abort();
    }
    trie::node_traverse_sidemost_result traverse_node_rightmost(int64_t pos) {
        SCYLLA_ASSERT(cached(pos));
        auto sp = _cached_page->get_view().subspan(pos % cached_file::page_size);
        auto type = uint8_t(sp[0]) >> 4;
        trie::node_traverse_sidemost_result result;
        result.body_pos = pos;
        auto single = [&] (uint64_t offset, uint8_t payload_bits) {
            result.n_children = 1;
            result.payload_bits = payload_bits;
            result.child_offset = offset;
            return result;
        };
        auto sparse = [&] (int type) {
            int n_children = int(sp[1]);
            auto bpp = bits_per_pointer_arr[type];
            result.n_children = n_children;
            result.payload_bits = uint8_t(sp[0]) & 0xf;
            result.child_offset = read_offset(sp.subspan(2+n_children), n_children - 1, bpp);
            return result;
        };
        auto dense = [&] (int type) {
            auto dense_span = uint64_t(sp[2]) + 1;
            auto bpp = bits_per_pointer_arr[type];
            result.n_children = dense_span;
            result.payload_bits = uint8_t(sp[0]) & 0xf;
            result.child_offset = read_offset(sp.subspan(3), dense_span - 1, bpp);
            return result;
        };
        switch (type) {
        case PAYLOAD_ONLY:
            result.payload_bits = uint8_t(sp[0]) & 0xf;
            result.n_children = 0;
            result.child_offset = -1;
            return result;
        case SINGLE_NOPAYLOAD_4:
            return single(uint64_t(sp[0]) & 0xf, 0);
        case SINGLE_NOPAYLOAD_12:
            return single((uint64_t(sp[0]) & 0xf) << 8 | uint64_t(sp[1]), 0);
        case SINGLE_8:
            return single(uint64_t(sp[2]), uint8_t(sp[0]) & 0xf);
        case SINGLE_16:
            return single(uint64_t(sp[2]) << 8 | uint64_t(sp[3]), uint8_t(sp[0]) & 0xf);
        case SPARSE_8:
            return sparse(type);
        case SPARSE_12:
            return sparse(type);
        case SPARSE_16:
            return sparse(type);
        case SPARSE_24:
            return sparse(type);
        case SPARSE_40:
            return sparse(type);
        case DENSE_12:
            return dense(type);
        case DENSE_16:
            return dense(type);
        case DENSE_24:
            return dense(type);
        case DENSE_32:
            return dense(type);
        case DENSE_40:
            return dense(type);
        case LONG_DENSE:
            return dense(type);
        }
        abort();
    }
    trie::get_child_result get_child(int64_t pos, int child_idx, bool forward) {
        SCYLLA_ASSERT(cached(pos));
        auto sp = _cached_page->get_view().subspan(pos % cached_file::page_size);
        auto type = uint8_t(sp[0]) >> 4;
        trie::get_child_result result;
        auto single = [&] (uint64_t offset) {
            result.offset = offset;
            result.idx = 0;
            return result;
        };
        auto sparse = [&] (int type) {
            auto bpp = bits_per_pointer_arr[type];
            result.idx = child_idx;
            result.offset = read_offset(sp.subspan(2+int(sp[1])), child_idx, bpp);
            return result;
        };
        auto dense = [&] (int type) {
            auto bpp = bits_per_pointer_arr[type];
            auto dense_span = uint64_t(sp[2]) + 1;
            int idx = child_idx;
            int increment = forward ? 1 : -1;
            while (idx < int(dense_span) && idx >= 0) {
                if (auto off = read_offset(sp.subspan(3), idx, bpp)) {
                    result.idx = idx;
                    result.offset = off;
                    return result;
                } else {
                    idx += increment;
                }
            }
            abort();
        };
        switch (type) {
        case PAYLOAD_ONLY:
            abort();
        case SINGLE_NOPAYLOAD_4:
            return single(uint64_t(sp[0]) & 0xf);
        case SINGLE_NOPAYLOAD_12:
            return single((uint64_t(sp[0]) & 0xf) << 8 | uint64_t(sp[1]));
        case SINGLE_8:
            return single(uint64_t(sp[2]));
        case SINGLE_16:
            return single(uint64_t(sp[2]) << 8 | uint64_t(sp[3]));
        case SPARSE_8:
            return sparse(type);
        case SPARSE_12:
            return sparse(type);
        case SPARSE_16:
            return sparse(type);
        case SPARSE_24:
            return sparse(type);
        case SPARSE_40:
            return sparse(type);
        case DENSE_12:
            return dense(type);
        case DENSE_16:
            return dense(type);
        case DENSE_24:
            return dense(type);
        case DENSE_32:
            return dense(type);
        case DENSE_40:
            return dense(type);
        case LONG_DENSE:
            return dense(type);
        }
        abort();
    }
    const_bytes get_payload(int64_t pos) const {
        SCYLLA_ASSERT(cached(pos));
        auto sp = _cached_page->get_view().subspan(pos % cached_file::page_size);
        auto type = uint8_t(sp[0]) >> 4;
        switch (type) {
        case PAYLOAD_ONLY:
            return sp.subspan(1);
        case SINGLE_NOPAYLOAD_4:
        case SINGLE_NOPAYLOAD_12:
            return sp.subspan(1 + div_ceil(bits_per_pointer_arr[type], 8));
        case SINGLE_8:
        case SINGLE_16:
            return sp.subspan(2 + div_ceil(bits_per_pointer_arr[type], 8));
        case SPARSE_8:
        case SPARSE_12:
        case SPARSE_16:
        case SPARSE_24:
        case SPARSE_40: {
            auto n_children = uint8_t(sp[1]);
            return sp.subspan(2 + div_ceil(n_children * (8 + bits_per_pointer_arr[type]), 8));
        }
        case DENSE_12:
        case DENSE_16:
        case DENSE_24:
        case DENSE_32:
        case DENSE_40:
        case LONG_DENSE: {
            auto dense_span = uint8_t(sp[2]) + 1;
            return sp.subspan(3 + div_ceil(dense_span * bits_per_pointer_arr[type], 8));
        }
        }
        abort();
    }

    future<row_index_header> read_row_index_header(uint64_t pos, reader_permit rp);
};
    
} // namespace trie
