/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "file_writer.hh"
#include "trie_writer.hh"
#include "utils/div_ceil.hh"

namespace trie {

// The order of this enum is meaningful.
// Each node type has a 4-bit identifier which is a part of the on-disk format.
enum node_type {
    PAYLOAD_ONLY,
    SINGLE_NOPAYLOAD_4,
    SINGLE_8,
    SINGLE_NOPAYLOAD_12,
    SINGLE_16,
    SPARSE_8,
    SPARSE_12,
    SPARSE_16,
    SPARSE_24,
    SPARSE_40,
    DENSE_12,
    DENSE_16,
    DENSE_24,
    DENSE_32,
    DENSE_40,
    LONG_DENSE,
    NODE_TYPE_COUNT, // Not a valid value.
};

// For each node type, contains the number of bits used for each child offset.
constexpr static const uint8_t bits_per_pointer_arr[] = {
    0,
    4,
    8,
    12,
    16,
    8,
    12,
    16,
    24,
    40,
    12,
    16,
    24,
    32,
    40,
    64,
};
static_assert(std::size(bits_per_pointer_arr) == NODE_TYPE_COUNT);

// Assuming the values of _node_size, _branch_size and _id in children are accurate,
// computes the maximum integer that we will have to write in the node's list of child offsets.
inline sink_offset max_offset_from_child(const writer_node& x, sink_pos pos) {
    expensive_log("max_offset_from_child: x={} pos={}", fmt::ptr(&x), pos.value);
    // Max offset noticed so far.
    auto result = sink_offset{0};
    // Offset to the next yet-unwritten child.
    // We iterate over children in reverse order and, for the ones which aren't written yet,
    // we compute their expected output position based on the accumulated values of _node_size and _branch_size.
    auto offset = sink_offset{0};
    for (auto it = x.get_children().rbegin(); it != x.get_children().rend(); ++it) {
        if ((*it)->_pos.valid()) {
            expensive_assert((*it)->_pos < pos);
            result = std::max<sink_offset>(result, pos - (*it)->_pos);
        } else {
            auto delta = ((*it)->_transition_meta > 1) ? (offset + sink_offset(2)) : (offset + (*it)->_node_size);
            result = std::max<sink_offset>(result, delta);
        }
        offset = offset + (*it)->_node_size + (*it)->_branch_size;
    }
    expensive_log("max_offset_from_child: x={} pos={} result={}", fmt::ptr(&x), pos.value, result.value);
    return result;
}

// Finds the node type which will yield the smallest valid on-disk representation of the given writer_node,
// assuming that the values of _node_size, _branch_size and _output_pos in children are accurate,
// and that the given node would be written at output position `pos`.
inline node_type choose_node_type_impl(const writer_node& x, sink_pos id) {
    const auto n_children = x.get_children().size();
    if (n_children == 0) {
        // If there is no children, the answer is obvious.
        return PAYLOAD_ONLY;
    }
    auto max_offset = max_offset_from_child(x, id);
    // For a given offset bitwidth, contains the index of the leanest node typ
    // in singles[], sparses[] and denses[] which can be used to represent a node with such offsets. 
    constexpr uint8_t widths_lookup[] = {
        0,
        0, 0, 0, 0,
        1, 1, 1, 1,
        2, 2, 2, 2,
        3, 3, 3, 3,
        4, 4, 4, 4,
        4, 4, 4, 4,
        5, 5, 5, 5,
        5, 5, 5, 5,
        6, 6, 6, 6,
        6, 6, 6, 6,
        7, 7, 7, 7,
        7, 7, 7, 7,
        7, 7, 7, 7,
        7, 7, 7, 7,
        7, 7, 7, 7,
        7, 7, 7, 7,
    };
    auto width_idx = widths_lookup[std::bit_width<uint64_t>(max_offset.value)];
    // Nodes with one, close child are very common, so they have dedicated node types.
    // For offset widths which don't fit into the dedicated types,
    // singles[] returns the smallest valid non-dedicated type.
    constexpr node_type singles[] = {SINGLE_NOPAYLOAD_4, SINGLE_8, SINGLE_NOPAYLOAD_12, SINGLE_16, DENSE_24, DENSE_32, DENSE_40, LONG_DENSE};
    if (n_children == 1) {
        const auto has_payload = x._payload._payload_bits;
        if (has_payload && (width_idx == 0 || width_idx == 2)) {
            // SINGLE_NOPAYLOAD_4 and SINGLE_NOPAYLOAD_12 can't hold nodes with a payload.
            // Use the next smallest node type which can.
            return singles[width_idx + 1];
        }
        return singles[width_idx];
    }
    // For nodes with 2 or more children, we calculate the sizes that would result from choosing
    // either the leanest dense node or the leanest sparse node, and we pick the one which
    // results in smaller size.
    constexpr node_type sparses[] = {SPARSE_8, SPARSE_8, SPARSE_12, SPARSE_16, SPARSE_24, SPARSE_40, SPARSE_40, LONG_DENSE};
    constexpr node_type denses[] = {DENSE_12, DENSE_12, DENSE_12, DENSE_16, DENSE_24, DENSE_32, DENSE_40, LONG_DENSE};
    const size_t dense_span = 1 + size_t(x.get_children().back()->transition()) - size_t(x.get_children().front()->transition());
    auto sparse_size = 16 + div_ceil((bits_per_pointer_arr[sparses[width_idx]] + 8) * n_children, 8);
    auto dense_size = 24 + div_ceil(bits_per_pointer_arr[denses[width_idx]] * dense_span, 8);
    if (sparse_size < dense_size) {
        return sparses[width_idx];
    } else {
        return denses[width_idx];
    }
}

static node_type choose_node_type(const writer_node& x, sink_pos id) {
    auto res = choose_node_type_impl(x, id);
    expensive_log("choose_node_type: x={} res={}", fmt::ptr(&x), int(res));
    return res;
}

// Turns a stream of writer_node nodes into a stream of bytes fed to a file_writer.
// Doesn't have any state of its own.
class bti_trie_sink_impl {
    sstables::file_writer& _w;
    size_t _page_size;
    constexpr static size_t max_page_size = 64 * 1024;
public:
    bti_trie_sink_impl(sstables::file_writer& w, size_t page_size) : _w(w), _page_size(page_size) {
        expensive_assert(_page_size <= max_page_size);
    }
private:
    void write_int(uint64_t x, size_t bytes) {
        uint64_t be = cpu_to_be(x);
        expensive_log("write_int: {}", fmt_hex({reinterpret_cast<const signed char*>(&be) + sizeof(be) - bytes, bytes}));
        _w.write(reinterpret_cast<const char*>(&be) + sizeof(be) - bytes, bytes);
    }
    void write_bytes(const_bytes x) {
        expensive_log("write_bytes: {}", fmt_hex({reinterpret_cast<const signed char*>(x.data()), x.size()}));
        _w.write(reinterpret_cast<const char*>(x.data()), x.size());
    }
    size_t write_sparse(const writer_node& x, node_type type, int bytes_per_pointer, sink_pos pos) {
        write_int((type << 4) | x._payload._payload_bits, 1);
        write_int(x.get_children().size(), 1);
        for (const auto& c : x.get_children()) {
            write_int(uint8_t(c->transition()), 1);
        }
        for (const auto& c : x.get_children()) {
            uint64_t offset = (pos - c->_pos).value;
            write_int(offset, bytes_per_pointer);
        }
        write_bytes(x._payload.blob());
        return 2 + x.get_children().size() * (1+bytes_per_pointer) + x._payload.blob().size();
    }
    node_size size_sparse(const writer_node& x, int bits_per_pointer) const {
        return node_size(2 + div_ceil(x.get_children().size() * (8+bits_per_pointer), 8) + x._payload.blob().size());
    }
    size_t write_dense(const writer_node& x, node_type type, int bytes_per_pointer, sink_pos pos) {
        int start = int(x.get_children().front()->transition());
        auto dense_span = 1 + int(x.get_children().back()->transition()) - int(x.get_children().front()->transition()); 
        write_int((type << 4) | x._payload._payload_bits, 1);
        write_int(start, 1);
        write_int(dense_span - 1, 1);
        auto it = x.get_children().begin();
        auto end_it = x.get_children().end();
        for (int next = start; next < start + dense_span; ++next) {
            uint64_t offset = 0;
            if (it != end_it && int((*it)->transition()) == next) {
                offset = (pos - (*it)->_pos).value;
                ++it;
            }
            write_int(offset, bytes_per_pointer);
        }
        write_bytes(x._payload.blob());
        return 3 + dense_span * (bytes_per_pointer) + x._payload.blob().size();
    }
    node_size size_dense(const writer_node& x, int bits_per_pointer) const  {
        int first = int(x.get_children().front()->transition());
        int last = int(x.get_children().back()->transition());
        return node_size(3 + div_ceil(bits_per_pointer * (1 + last - first), 8) + x._payload.blob().size());
    }
public:
    // Writes the final BTI node of the chain represented by this writer_node.
    void write_body(const writer_node& x, sink_pos id, node_type type) {
        switch (type) {
        case PAYLOAD_ONLY: {
            write_int(type << 4 | x._payload._payload_bits, 1);
            write_bytes(x._payload.blob());
            return;
        }
        case SINGLE_NOPAYLOAD_4: {
            uint64_t offset = (id - x.get_children().front()->_pos).value;
            uint8_t transition = uint8_t(x.get_children().front()->transition());
            uint8_t arr[2];
            arr[0] = (type << 4) | offset;
            arr[1] = transition;
            write_bytes({reinterpret_cast<const std::byte*>(arr), 2});
            return;
        }
        case SINGLE_8: {
            uint64_t offset = (id - x.get_children().front()->_pos).value;
            uint8_t transition = uint8_t(x.get_children().front()->transition());
            uint8_t arr[64];
            arr[0] = (type << 4) | x._payload._payload_bits;
            arr[1] = transition;
            arr[2] = offset;
            auto sz = x._payload.blob().size();
            memcpy(&arr[3], x._payload.blob().data(), sz);
            write_bytes({reinterpret_cast<const std::byte*>(arr), 3 + sz});
            return;
        }
        case SINGLE_NOPAYLOAD_12: {
            uint64_t offset = (id - x.get_children().front()->_pos).value;
            uint8_t transition = uint8_t(x.get_children().front()->transition());
            write_int((type << 4) | (offset >> 8), 1);
            write_int(offset & 0xff, 1);
            write_int(transition, 1);
            return;
        }
        case SINGLE_16: {
            uint64_t offset = (id - x.get_children().front()->_pos).value;
            uint8_t transition = uint8_t(x.get_children().front()->transition());
            write_int((type << 4) | x._payload._payload_bits, 1);
            write_int(transition, 1);
            write_int(offset, 2);
            write_bytes(x._payload.blob());
            return;
        }
        case SPARSE_8: {
            write_sparse(x, type, 1, id);
            return;
        }
        case SPARSE_12: {
            write_int((type << 4) | x._payload._payload_bits, 1);
            write_int(x.get_children().size(), 1);
            for (const auto& c : x.get_children()) {
                write_int(uint8_t(c->transition()), 1);
            }
            size_t i;
            for (i = 0; i + 1 < x.get_children().size(); i += 2) {
                uint64_t offset1 = (id - x.get_children()[i]->_pos).value;
                uint64_t offset2 = (id - x.get_children()[i+1]->_pos).value;
                write_int(offset1 << 12 | offset2, 3);
            }
            if (i < x.get_children().size()) {
                uint64_t offset = (id - x.get_children()[i]->_pos).value;
                write_int(offset << 4, 2);
            }
            write_bytes(x._payload.blob());
            return;
        }
        case SPARSE_16: {
            write_sparse(x, type, 2, id);
            return;
        }
        case SPARSE_24: {
            write_sparse(x, type, 3, id);
            return;
        }
        case SPARSE_40: {
            write_sparse(x, type, 5, id);
            return;
        }
        case DENSE_12: {
            int start = int(x.get_children().front()->transition());
            auto dense_span = 1 + int(x.get_children().back()->transition()) - int(x.get_children().front()->transition());
            write_int((type << 4) | x._payload._payload_bits, 1);
            write_int(start, 1);
            write_int(dense_span - 1, 1);
            auto it = x.get_children().begin();
            auto end_it = x.get_children().end();
            int next = start;
            for (; next + 1 < start + dense_span; next += 2) {
                uint64_t offset_1 = 0;
                uint64_t offset_2 = 0;
                if (it != end_it && int((*it)->transition()) == next) {
                    offset_1 = (id - (*it)->_pos).value;
                    ++it;
                }
                if (it != end_it && int((*it)->transition()) == next + 1) {
                    offset_2 = (id - (*it)->_pos).value;
                    ++it;
                }
                write_int(offset_1 << 12 | offset_2, 3);
            }
            if (next < start + dense_span) {
                uint64_t offset = 0;
                if (it != end_it && int((*it)->transition()) == next) {
                    offset = (id - (*it)->_pos).value;
                    ++it;
                }
                write_int(offset << 4, 2);
            }
            write_bytes(x._payload.blob());
            return;
        }
        case DENSE_16: {
            write_dense(x, type, 2, id);
            return;
        }
        case DENSE_24: {
            write_dense(x, type, 3, id);
            return;
        }
        case DENSE_32: {
            write_dense(x, type, 4, id);
            return;
        }
        case DENSE_40: {
            write_dense(x, type, 5, id);
            return;
        }
        case LONG_DENSE: {
            write_dense(x, type, 8, id);
            return;
        }
        default: abort();
        }
    }
    // Writes the BTI nodes representing the transition chain of this node,
    // starting from index 1. (Transition byte 0 is in the parent).  
    sink_pos write_chain(const writer_node& x, node_size body_offset) {
        int i = x._transition_meta;
        expensive_assert(i >= 2);

        const std::byte* __restrict__ transition = x._transition.get();
        sink_pos c1_pos = pos();
    
        // Second-to-last node in the chain can have size 2 or 3 bytes, depending on how big the last node is.
        uint64_t offset = body_offset.value;
        if (offset >= 16) {
            expensive_assert(offset < 4096);
            write_int(uint64_t(transition[i - 1]) | offset << 8 | uint64_t(SINGLE_NOPAYLOAD_12 << 20), 3);
        } else {
            write_int(uint64_t(transition[i - 1]) | offset << 8 | uint64_t(SINGLE_NOPAYLOAD_4 << 12), 2);
        }

        i -= 1;
        if (i == 1) {
            return c1_pos;
        }

        // Third-to-last node in the chain has always size 2, but the offset can be equal to 3 or 2, depending
        // on how big the second-to-last node was.
        offset = (pos() - c1_pos).value; 
        write_int(uint64_t(transition[i - 1]) | offset << 8 | uint64_t(SINGLE_NOPAYLOAD_4 << 12), 2);

        i -= 1;

        // Fourth-to-last and earlier nodes in the chain always have the form 0x12??, where ?? is the transition byte.
        // This is SIMDable -- we can load a vector of transition bytes from memory,
        // reverse the order (earlier bytes in the transition chain become later nodes in the file)
        // and add a 0x12 byte before every transition byte.
        constexpr int s = 16; // Vector size.
        typedef unsigned char vector2x __attribute__((__vector_size__(s*2))); // Output vector.
        typedef unsigned char vector1x __attribute__((__vector_size__(s))); // Input vector.
        auto z = uint8_t(SINGLE_NOPAYLOAD_4 << 4 | 2); // 0x12
        vector1x zv = {z};

        // An extra buffer above the file_writer. It allows us to do constexpr-sized
        // memcpy, but incurs an extra copy overall. Is it worth it?
        std::array<std::byte, 1024> outbuf;
        static_assert(std::size(outbuf) % s == 0);
        size_t outbuf_pos = 0;

        // Serialize the chain in SIMD blocks of `s` transition bytes.
        for (; i - s > 0; i -= s) {
            vector1x v;
            memcpy(&v, &transition[i - s], s);
            vector2x d = __builtin_shufflevector(v, zv,
                // 31, s, 30, s, 29, s, 28, s, 27, s, 26, s, 25, s, 24, s,
                // 23, s, 22, s, 21, s, 20, s, 19, s, 18, s, 17, s, 16, s,
                s, 15, s, 14, s, 13, s, 12, s, 11, s, 10, s, 9, s, 8,
                s, 7, s, 6, s, 5, s, 4, s, 3, s, 2, s, 1, s, 0
            );
            memcpy(&outbuf[outbuf_pos], &d, sizeof(d));
            outbuf_pos += sizeof(d);
            if (outbuf_pos == 1024) [[unlikely]] {
                write_bytes(outbuf);
                outbuf_pos = 0;
            }
        }

        // Write the remaining `i - 1` first bytes (excluding idx 0) of the transition chain.
        // This is separated from the previous loop so that the previous loop enjoys the benefit
        // of constexpr-sized memory ops. 
        {
            vector1x v;
            // Load into the `i - i` last bytes of `v`.
            memcpy(reinterpret_cast<char*>(&v) + sizeof(v) - (i - 1), &transition[1], (i - 1));
            vector2x d = __builtin_shufflevector(v, zv,
                // 31, s, 30, s, 29, s, 28, s, 27, s, 26, s, 25, s, 24, s,
                // 23, s, 22, s, 21, s, 20, s, 19, s, 18, s, 17, s, 16, s,
                s, 15, s, 14, s, 13, s, 12, s, 11, s, 10, s, 9, s, 8,
                s, 7, s, 6, s, 5, s, 4, s, 3, s, 2, s, 1, s, 0
            );
            // Write out the first `2*i` first bytes of `v`.
            memcpy(&outbuf[outbuf_pos], &d, 2 * (i - 1));
            outbuf_pos += 2 * (i - 1);
            write_bytes(std::span(outbuf).subspan(0, outbuf_pos));
        }

        return pos() - sink_offset(2);
    }
    sink_pos write(const writer_node& x, sink_pos id) {
        expensive_assert(x._transition_meta >= 1);

        // Write last node in the chain.
        sink_pos start_pos = pos();
        auto type = choose_node_type(x, id);
        write_body(x, id, type);

        if (x._transition_meta == 1) {
            return start_pos;
        }
    
        return write_chain(x, node_size((pos() - start_pos).value));
    }
    node_size serialized_size(const writer_node& x, sink_pos id) const {
        expensive_assert(x._transition_meta >= 1);
        auto inner = serialized_size_body(x, id);
        return node_size((sink_offset(inner) + serialized_size_chain(x, inner)).value);
    }
    node_size serialized_size_chain(const writer_node& x, node_size body_offset) const {
        return node_size(x._transition_meta == 1 ? 0 : (body_offset.value >= 16 ? 1 : 0) + (x._transition_meta - 1) * 2);
    }
    node_size serialized_size_body_type(const writer_node& x, node_type type) const {
        switch (type) {
        case PAYLOAD_ONLY: {
            return node_size(1 + x._payload.blob().size());
        }
        case SINGLE_NOPAYLOAD_4: {
            return node_size(2);
        }
        case SINGLE_8: {
            return node_size(3 + x._payload.blob().size());
        }
        case SINGLE_NOPAYLOAD_12: {
            return node_size(3);
        }
        case SINGLE_16: {
            return node_size(4 + x._payload.blob().size());
        }
        case SPARSE_8: {
            return size_sparse(x, 8);
        }
        case SPARSE_12: {
            return size_sparse(x, 12);
        }
        case SPARSE_16: {
            return size_sparse(x, 16);
        }
        case SPARSE_24: {
            return size_sparse(x, 24);
        }
        case SPARSE_40: {
            return size_sparse(x, 40);
        }
        case DENSE_12: {
            return size_dense(x, 12);
        }
        case DENSE_16: {
            return size_dense(x, 16);
        }
        case DENSE_24: {
            return size_dense(x, 24);
        }
        case DENSE_32: {
            return size_dense(x, 32);
        }
        case DENSE_40: {
            return size_dense(x, 40);
        }
        case LONG_DENSE: {
            return size_dense(x, 64);
        }
        default: abort();
        }
    }
    node_size serialized_size_body(const writer_node& x, sink_pos id) const {
        return serialized_size_body_type(x, choose_node_type(x, id));
    }
    uint64_t page_size() const {
        return _page_size;
    }
    uint64_t bytes_left_in_page() const {
        return round_up(pos().value + 1, page_size()) - pos().value;
    };
    void pad_to_page_boundary() {
        const static std::array<std::byte, max_page_size> zero_page = {};
        _w.write(reinterpret_cast<const char*>(zero_page.data()), bytes_left_in_page());
    }
    sink_pos pos() const {
        return sink_pos(_w.offset());
    }
};

// Given an array of offsets, each of size bits_per_pointer, read the one with index `idx`.
// 
// Assumes that bits_per_pointer is divisible by 8 or equal to 12.
//
// Ordering note: an array of 12-bit offsets [0x123, 0x456, 0x789, 0xabc] is
// represented as the byte array 123456789abc.
//
// We want this to be always inlined so that bits_per_pointer is substituted with a constant,
// and this compiles to a simple load, not to a full-fledged memcpy.
[[gnu::always_inline]]
inline uint64_t read_offset(const_bytes sp, int idx, int bits_per_pointer) {
    if (bits_per_pointer % 8 == 0) {
        auto n = bits_per_pointer / 8;
        uint64_t be = 0;
        memcpy((char*)&be + 8 - n, (const char*)sp.data() + n * idx, n);
        return seastar::be_to_cpu(be);
    } else {
        if (idx % 2 == 0) {
            return seastar::read_be<uint16_t>((const char*)sp.data() + 3 * (idx / 2)) >> 4;
        } else {
            return seastar::read_be<uint16_t>((const char*)sp.data() + 3 * (idx / 2) + 1) & 0xfff;     
        }
    }
}

struct trail_entry {
    uint64_t pos;
    uint16_t n_children;
    int16_t child_idx;
    uint8_t payload_bits;
    // For tests
    std::strong_ordering operator<=>(const trail_entry&) const = default;
};

struct traversal_state {
    int64_t next_pos = -1;
    int edges_traversed = 0;
    utils::small_vector<trail_entry, 8> trail;
};

struct node_traverse_result {
    uint8_t payload_bits;
    int n_children;
    int found_idx;
    int found_byte;
    int traversed_key_bytes;
    int64_t body_pos;
    int64_t child_offset;
};

struct node_traverse_sidemost_result {
    uint8_t payload_bits;
    int n_children;
    int64_t body_pos;
    int64_t child_offset;
};

struct get_child_result {
    int idx;
    uint64_t offset;    
};

struct load_final_node_result {
    uint16_t n_children;
    uint8_t payload_bits;
};

template <typename T>
concept node_reader = requires(T& o, int64_t pos, const_bytes key, int child_idx) {
    { o.cached(pos) } -> std::same_as<bool>;
    { o.load(pos) } -> std::same_as<future<>>;
    { o.read_final_node(pos) } -> std::same_as<load_final_node_result>;
    { o.traverse_node_by_key(pos, key) } -> std::same_as<node_traverse_result>;
    { o.traverse_node_leftmost(pos) } -> std::same_as<node_traverse_sidemost_result>;
    { o.traverse_node_rightmost(pos) } -> std::same_as<node_traverse_sidemost_result>;
    { o.get_child(pos, child_idx, bool()) } -> std::same_as<get_child_result>;
    { o.get_payload(pos) } -> std::same_as<const_bytes>;
};

inline void traverse_single_page(
    node_reader auto& page,
    const_bytes key,
    traversal_state& state
) {
    while (page.cached(state.next_pos) && state.edges_traversed < int(key.size())) {
        node_traverse_result traverse_one = page.traverse_node_by_key(state.next_pos, key.subspan(state.edges_traversed));
        state.edges_traversed += traverse_one.traversed_key_bytes;
        bool can_continue = state.edges_traversed < int(key.size()) && traverse_one.found_byte == int(key[state.edges_traversed]);
        if (can_continue) {
            state.next_pos = traverse_one.body_pos - traverse_one.child_offset;
            state.edges_traversed += 1;
        } else {
            state.next_pos = -1;
        }
        bool add_to_trail = traverse_one.payload_bits || traverse_one.n_children > 1 || !can_continue;
        expensive_log("traversing pos={} add_to_trail={} found_byte={} can_continue={} body_pos={} child_offset={}",
                state.next_pos, add_to_trail, traverse_one.found_byte, can_continue, traverse_one.body_pos, traverse_one.child_offset);
        if (add_to_trail) {
            state.trail.push_back(trail_entry{
                .pos = traverse_one.body_pos,
                .n_children = traverse_one.n_children,
                .child_idx = traverse_one.found_idx,
                .payload_bits = traverse_one.payload_bits});
        }
    }
}

inline future<> traverse(
    node_reader auto& input,
    const_bytes key,
    traversal_state& state
) {
    while (state.next_pos >= 0 && state.edges_traversed < int(key.size())) {
        co_await input.load(state.next_pos);
        traverse_single_page(input, key, state);
    }
    if (state.next_pos >= 0) {
        co_await input.load(state.next_pos);
        load_final_node_result final_node = input.read_final_node(state.next_pos);
        state.trail.push_back(trail_entry{
                .pos = state.next_pos,
                .n_children = final_node.n_children,
                .child_idx = -1,
                .payload_bits = final_node.payload_bits});
    }
}

inline void descend_leftmost_single_page(
    node_reader auto& page,
    traversal_state& state
) {
    while (page.cached(state.next_pos)) {
        node_traverse_sidemost_result traverse_one = page.traverse_node_leftmost(state.next_pos);
        if (traverse_one.payload_bits || traverse_one.n_children > 1) {
            if (!(state.trail.back().payload_bits || state.trail.back().n_children > 1)) {
                state.trail.pop_back();
            }
            state.trail.push_back(trail_entry{
                .pos = traverse_one.body_pos,
                .n_children = traverse_one.n_children,
                .child_idx = 0,
                .payload_bits = traverse_one.payload_bits});
        }

        if (traverse_one.payload_bits) {
            state.next_pos = -1;
            state.trail.back().child_idx = -1;
        } else {
            SCYLLA_ASSERT(traverse_one.n_children >= 1);
            state.next_pos = traverse_one.body_pos - traverse_one.child_offset;
        }
    }
}

inline future<> descend_leftmost(
    node_reader auto& input,
    traversal_state& state
) {
    while (state.next_pos >= 0) {
        co_await input.load(state.next_pos);
        descend_leftmost_single_page(input, state);
    }
}

inline bool ascend_to_right_edge(traversal_state& state) {
    if (state.trail.back().child_idx != -1) {
        state.trail.back().child_idx -= 1;
    }
    for (int i = state.trail.size() - 1; i >= 0; --i) {
        if (state.trail[i].child_idx + 1 < state.trail[i].n_children) {
            state.trail[i].child_idx += 1;
            state.trail.resize(i + 1);
            return true;
        }
    }
    state.trail[0].child_idx = state.trail[0].n_children;
    state.trail.resize(1);
    return false;
}

inline future<> step(
    node_reader auto& input,
    traversal_state& state
) {
    if (ascend_to_right_edge(state)) {
        co_await input.load(state.trail.back().pos);
        get_child_result child = input.get_child(state.trail.back().pos, state.trail.back().child_idx, true);

        state.trail.back().child_idx = child.idx;
        state.next_pos = state.trail.back().pos - child.offset;
        co_await descend_leftmost(input, state);
    }
    state.edges_traversed = -1;
}

inline void descend_rightmost_single_page(
    node_reader auto& page,
    traversal_state& state
) {
    while (page.cached(state.next_pos)) {
        node_traverse_sidemost_result traverse_one = page.traverse_node_rightmost(state.next_pos);
        if (traverse_one.payload_bits || traverse_one.n_children > 1) {
            if (!(state.trail.back().payload_bits || state.trail.back().n_children > 1)) {
                state.trail.pop_back();
            }
            state.trail.push_back(trail_entry{
                .pos = traverse_one.body_pos,
                .n_children = traverse_one.n_children,
                .child_idx = traverse_one.n_children - 1,
                .payload_bits = traverse_one.payload_bits});
        }

        if (traverse_one.n_children >= 1) {
            state.next_pos = traverse_one.body_pos - traverse_one.child_offset;
        } else {
            state.next_pos = -1;
            state.trail.back().child_idx = -1;
        }
    }
}

inline future<> descend_rightmost(
    node_reader auto& input,
    traversal_state& state
) {
    while (state.next_pos >= 0) {
        co_await input.load(state.next_pos);
        descend_rightmost_single_page(input, state);
    }
}

inline bool ascend_to_left_edge(traversal_state& state) {
    for (int i = state.trail.size() - 1; i >= 0; --i) {
        if (state.trail[i].child_idx > 0 || (state.trail[i].child_idx == 0 && state.trail[i].payload_bits)) {
            state.trail[i].child_idx -= 1;
            state.trail.resize(i + 1);
            return true;
        }
    }
    state.trail[0].child_idx = state.trail[0].payload_bits ? -1 : 0;
    state.trail.resize(1);
    return false;
}

inline future<> step_back(
    node_reader auto& input,
    traversal_state& state
) {
    bool didnt_go_past_start = ascend_to_left_edge(state);
    if (state.trail.back().child_idx >= 0 && state.trail.back().child_idx < state.trail.back().n_children) {
        co_await input.load(state.trail.back().pos);
        get_child_result child = input.get_child(state.trail.back().pos, state.trail.back().child_idx, false);

        state.trail.back().child_idx = child.idx;
        state.next_pos = state.trail.back().pos - child.offset;
        if (didnt_go_past_start) {
            co_await descend_rightmost(input, state);
        } else {
            co_await descend_leftmost(input, state);
        }
    }
    state.edges_traversed = -1;
}

} // namespace trie