#pragma clang optimize off

/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

//
// This implementation of a trie-based index follows Cassandra's BTI implementation. 
//
// For an overview of the format, see:
// https://github.com/apache/cassandra/blob/9dfcfaee6585a3443282f56d54e90446dc4ff012/src/java/org/apache/cassandra/io/sstable/format/bti/BtiFormat.md
//
// For the writer logic, and it's arbitrary heuristics for splitting the trie into pages, see:
// https://github.com/apache/cassandra/blob/9dfcfaee6585a3443282f56d54e90446dc4ff012/src/java/org/apache/cassandra/io/tries/IncrementalTrieWriterPageAware.java#L32
//
// (The reader logic doesn't have much in the way of design -- the design of readers must follow the format).

#include "trie.hh"
#include <algorithm>
#include <cassert>
#include <immintrin.h>
#include <set>
#include "sstables/index_reader.hh"
#include "utils/small_vector.hh"

static seastar::logger trie_logger("trie");

// Trace logs are mainly useful for debugging during development,
// where the cost of logging doesn't matter at all.
// And during development it's useful to have A LOT of them, for example one for each function call.
//
// But in production, if the log is potentially frequent enough, we might care even about the small
// cost of dynamically checking if the log is enabled, which discourages adding more trace logs.
// This conflicts with their purpose.
//
// I think the right thing is to do is to have a special most verbose log level disabled at compile time by default,
// and only enabled by developers who are actively working on the relevant feature.
// This way the developer is free to add as many logs as he pleases, without worrying at all about the performance cost.
constexpr bool developer_build = true;

#define LOG_TRACE(...) do if constexpr (developer_build) trie_logger.trace(__VA_ARGS__); while (0)

// FIXME: potential strict aliasing violation.
// 
// AFAIK this isn't a problem in practice, because both major compilers
// conservatively allow `signed char` (== `bytes::value_type`) to alias anything.
// (Unlike the standard, which only treats `char`, `unsigned char` and `std::byte` like that). 
// 
// But still.
[[maybe_unused]]
static fmt_hex format_byte_span(const_bytes cb) {
    return {{reinterpret_cast<const bytes::value_type*>(cb.data()), cb.size()}};
}

struct writer_node;

// A trie writer turns a stream of keys into a stream of writer_node objects.
// This is a concept for a type which can accept such a stream.
template <typename T>
concept trie_writer_sink = requires(T& o, const T& co, const writer_node& x, size_t pos) {
    { o.serialized_size(x, pos) } -> std::same_as<size_t>;
    { o.write(x) };
    { o.pad_to_page_boundary() } -> std::same_as<size_t>;
    { co.pos() } -> std::same_as<size_t>;
    { co.page_size() } -> std::same_as<size_t>;
    { co.bytes_left_in_page() } -> std::same_as<size_t>;
};

struct payload {
    // What the reader will see is a 4-bit "payload bits" header, and a pointer to the payload bytes.
    // The header is used to determine both the length and the meaning
    // of the "bytes".
    //
    // The meaning of "bits" and "bytes" is generally opaque to the trie-writing layer,
    // but the value `bits == 0` is used to represent "no payload", so payloads with
    // `bits == 0` mustn't be used.
    uint8_t _payload_bits = {};
    // We store the payload in a std::array + size pair,
    // since we don't want any allocation indirection due to performance reasons.
    // 
    // 20 bytes is the biggest maximum possible payload in the BTI index format. 
    std::array<std::byte, 20> _payload_buf = {};
    // For the writer, the meaning of the "bits" is opaque,
    // we just store an explicit size of the "bytes".
    uint8_t _payload_size = {};

    payload() noexcept;
    payload(uint8_t bits, const_bytes blob) noexcept;
    const_bytes blob() const noexcept;
};

// The trie writer holds a trie of these nodes while it's building the index pages.
// Whenever a new key is fed to the writer:
// 1. The "latest node" pointer climbs up the rightmost path to the point of mismatch between
// the previous key and the new key, and "completes" nodes along the way.
// (A node is "complete" when we climb above it, which means that it won't receive any new children).   
// 2. If a newly-completed subtree can't fit into the current page of the file writer, 
// branches of the subtree are flushed to disk, and are discarded from RAM,
// and only the subtree root remains in RAM, with metadata updated accordingly.
// 3. The "latest node" pointer climbs down the new key from the mismatch point,
// adding a node for each character along the way.
struct writer_node {
    // The payload of this node. It might be absent -- this is represented by _payload_bits == 0. 
    //
    // A node has a payload iff it corresponds to some key.
    // Invariant: all leaf nodes (with the potential exception of the root,
    // if it's the only node) contain a payload.
    payload _payload;
    // This is a byte-based trie. A path from the root to a node corresponds
    // to some key prefix, with each successive node corresponding to a successive byte.
    // Each node stores the last character of its corresponding prefix.
    //
    // In the root node, this is meaningless.
    std::byte _transition = {};
    // True iff any of the direct children lies on a different page (i.e. has already been written out).
    // It means that the size of this node might change from the initial estimate at completion
    // (because the offset to children might have grow, and the offset integers need more bits to fit),
    // so it has to be recalculated before the actual write.
    //
    // After the node itself is written out, this is reset to `false`.
    bool _has_out_of_page_children = false;
    // True iff some yet-unwritten descendant has out-of-page children.
    // This means that the sizes of our children (and thus also our size) might change from the initial
    // estimate at completion time (because some offsets in their subtree might have grown, and the offset
    // integers now require more bits), so they have to be recalculated before the actual write.
    //
    // After the node's children are written out, this is always `false`.
    bool _has_out_of_page_descendants = false;
    // A list of children, ordered by their `_transition`.
    // small_vector because most nodes are expected to be leaves,
    // or to only have 1-2 children.
    utils::small_vector<std::unique_ptr<writer_node>, 2> _children;
    // The expected serialized size of the node.
    // Depending on the stage of the node's life, and on the value
    // of _has_out_of_page_children,
    // this might be an approximation, or it might have to be exact.
    //
    // A negative value means "node not completed yet".
    ssize_t _node_size = -1;
    // The expected total serialized size of the nodes descendants.
    // (If we wrote the node's entire subtree to a file, without any padding,
    // we would expect the file to grow by _branch_size + _node_size).
    //
    // Depending on the stage of the node's life, and on the values
    // of _has_out_of_page_children and _has_out_of_page_descendants,
    // this might be an approximation, or it might have to be exact.
    //
    // A negative value means "node not completed yet".
    ssize_t _branch_size = -1;
    // The position of the node in the output file/stream.
    // Set after the node is flushed to disk.
    // Implies that all descendants of the nodes were also flushed (before it).
    //
    // When _output_pos is set, _children is cleared because it's not longer needed.
    // The node object is only kept in memory to remember its _output_pos and _transition,
    // because we need those to write the parent node. 
    ssize_t _output_pos = -1;

    // Only constructor. For the root node, the passed value doesn't matter.
    writer_node(std::byte b) noexcept;
    // Emplaces a new child node into `_children`,
    // and returns a pointer to it.
    // Children have to be added in order of growing `_transition`.
    writer_node* add_child(std::byte b);
    // Must only be used on nodes immediately after they are created,
    // i.e. before any other nodes are added.
    //
    // It's separate from `add_child` because the root might need a payload too,
    // but it isn't created by `add_child`. 
    void set_payload(const payload&) noexcept;
    // Re-calculates _node_size and/or _branch_size of this node and/or its descendants
    // (as needed by _has_out_of_page_children or _has_out_of_page_children), under
    // the assumption that we will write out the entire subtree without any padding
    // starting from starting_pos. If this assumption is true, then the values of _node_size
    // and _branch_size are exact after this call.
    size_t recalc_sizes(const trie_writer_sink auto&, size_t starting_pos);
    // Writes out the entire subtree.
    //
    // Must only be called after it's known that the entire write will fit within a single page.
    // If recalc_sizes() was called with the target output position before the `write`,
    // `write` will grow the file by exactly `_branch_size + _node_size`.
    //  
    // After this call, it's true that:
    // _children.empty() && _output_pos >= 0 && !_has_out_of_page_children && !_has_out_of_page_descendants
    void write(trie_writer_sink auto&);
};

inline constexpr size_t round_down(size_t a, size_t factor) {
    return a - a % factor;
}

inline constexpr size_t round_up(size_t a, size_t factor) {
    return round_down(a + factor - 1, factor);
}

// Short-circuiting logical implication operator
#define IMPLIES(p, q) ((!p) || (q))

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

// Assuming the values of _node_size, _branch_size and _output_pos in children are accurate,
// computes the maximum integer that we will have to write in the node's list of child offsets.
static size_t max_offset_from_child(const writer_node& x, size_t pos) {
    // Max offset noticed so far.
    size_t result = 0;
    // Offset to the next yet-unwritten child.
    // We iterate over children in reverse order and, for the ones which aren't written yet,
    // we compute their expected output position based on the accumulated values of _node_size and _branch_size.
    size_t offset = 0;
    for (auto it = x._children.rbegin(); it != x._children.rend(); ++it) {
        offset += it->get()->_node_size;
        if (it->get()->_output_pos >= 0) {
            SCYLLA_ASSERT(size_t(it->get()->_output_pos) < pos);
            result = std::max<size_t>(result, pos - it->get()->_output_pos);
        } else {
            result = std::max<size_t>(result, offset);
        }
        offset += it->get()->_branch_size;
    }
    return result;
}

// Finds the node type which will yield the smallest valid on-disk representation of the given writer_node,
// assuming that the values of _node_size, _branch_size and _output_pos in children are accurate,
// and that the given node would be written at output position `pos`.
static node_type choose_node_type(const writer_node& x, size_t pos) {
    const auto n_children = x._children.size();
    if (n_children == 0) {
        // If there is no children, the answer is obvious.
        return PAYLOAD_ONLY;
    }
    auto max_offset = max_offset_from_child(x, pos);
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
    auto width_idx = widths_lookup[std::bit_width(max_offset)];
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
    const size_t dense_span = 1 + size_t(x._children.back()->_transition) - size_t(x._children.front()->_transition);
    auto sparse_size = 16 + div_ceil((bits_per_pointer_arr[sparses[width_idx]] + 8) * n_children, 8);
    auto dense_size = 24 + div_ceil(bits_per_pointer_arr[denses[width_idx]] * dense_span, 8);
    if (sparse_size < dense_size) {
        return sparses[width_idx];
    } else {
        return denses[width_idx];
    }
}

// Turns a stream of writer_node nodes into a stream of bytes fed to a file_writer.
// Doesn't have any state of its own.
class bti_trie_sink::impl {
    sstables::file_writer& _w;
    size_t _page_size;
    constexpr static size_t max_page_size = 64 * 1024;
public:
    impl(sstables::file_writer& w, size_t page_size) : _w(w), _page_size(page_size) {
        SCYLLA_ASSERT(_page_size <= max_page_size);
    }
private:
    void write_int(uint64_t x, size_t bytes) {
        uint64_t be = cpu_to_be(x);
        _w.write(reinterpret_cast<const char*>(&be) + sizeof(be) - bytes, bytes);
    }
    void write_bytes(const_bytes x) {
        _w.write(reinterpret_cast<const char*>(x.data()), x.size());
    }
    size_t write_sparse(const writer_node& x, node_type type, int bytes_per_pointer, size_t pos) {
        write_int((type << 4) | x._payload._payload_bits, 1);
        write_int(x._children.size(), 1);
        for (const auto& c : x._children) {
            write_int(uint8_t(c->_transition), 1);
        }
        for (const auto& c : x._children) {
            size_t offset = pos - c->_output_pos;
            write_int(offset, bytes_per_pointer);
        }
        write_bytes(x._payload.blob());
        return 2 + x._children.size() * (1+bytes_per_pointer) + x._payload.blob().size();
    }
    size_t size_sparse(const writer_node& x, int bits_per_pointer) const {
        return 2 + div_ceil(x._children.size() * (8+bits_per_pointer), 8) + x._payload.blob().size();
    }
    size_t write_dense(const writer_node& x, node_type type, int bytes_per_pointer, size_t pos) {
        int start = int(x._children.front()->_transition);
        auto dense_span = 1 + int(x._children.back()->_transition) - int(x._children.front()->_transition); 
        write_int((type << 4) | x._payload._payload_bits, 1);
        write_int(start, 1);
        write_int(dense_span - 1, 1);
        auto it = x._children.begin();
        auto end_it = x._children.end();
        for (int next = start; next < start + dense_span; ++next) {
            size_t offset = 0;
            if (it != end_it && int(it->get()->_transition) == next) {
                offset = pos - it->get()->_output_pos;
                ++it;
            }
            write_int(offset, bytes_per_pointer);
        }
        write_bytes(x._payload.blob());
        return 3 + dense_span * (bytes_per_pointer) + x._payload.blob().size();
    }
    size_t size_dense(const writer_node& x, int bits_per_pointer) const  {
        return 3 + div_ceil(bits_per_pointer * (1 + int(x._children.back()->_transition) - int(x._children.front()->_transition)), 8) + x._payload.blob().size();
    }
public:
    void write(const writer_node& x) {
        const auto my_pos = x._output_pos;
        auto type = choose_node_type(x, my_pos);
        switch (type) {
        case PAYLOAD_ONLY: {
            write_int(type << 4 | x._payload._payload_bits, 1);
            write_bytes(x._payload.blob());
            return;
        }
        case SINGLE_NOPAYLOAD_4: {
            size_t offset = my_pos - x._children.front()->_output_pos;
            uint8_t transition = uint8_t(x._children.front()->_transition);
            uint8_t arr[2];
            arr[0] = (type << 4) | offset;
            arr[1] = transition;
            _w.write(reinterpret_cast<const char*>(arr), 2);
            return;
        }
        case SINGLE_8: {
            size_t offset = my_pos - x._children.front()->_output_pos;
            uint8_t transition = uint8_t(x._children.front()->_transition);
            uint8_t arr[64];
            arr[0] = (type << 4) | x._payload._payload_bits;
            arr[1] = transition;
            arr[2] = offset;
            auto sz = x._payload.blob().size();
            memcpy(&arr[3], x._payload.blob().data(), sz);
            _w.write(reinterpret_cast<const char*>(arr), 3 + sz);
            return;
        }
        case SINGLE_NOPAYLOAD_12: {
            size_t offset = my_pos - x._children.front()->_output_pos;
            uint8_t transition = uint8_t(x._children.front()->_transition);
            write_int((type << 4) | (offset >> 8), 1);
            write_int(offset & 0xff, 1);
            write_int(transition, 1);
            return;
        }
        case SINGLE_16: {
            size_t offset = my_pos - x._children.front()->_output_pos;
            uint8_t transition = uint8_t(x._children.front()->_transition);
            write_int((type << 4) | x._payload._payload_bits, 1);
            write_int(transition, 1);
            write_int(offset, 2);
            write_bytes(x._payload.blob());
            return;
        }
        case SPARSE_8: {
            write_sparse(x, type, 1, my_pos);
            return;
        }
        case SPARSE_12: {
            write_int((type << 4) | x._payload._payload_bits, 1);
            write_int(x._children.size(), 1);
            for (const auto& c : x._children) {
                write_int(uint8_t(c->_transition), 1);
            }
            size_t i;
            for (i = 0; i + 1 < x._children.size(); i += 2) {
                size_t offset1 = my_pos - x._children[i]->_output_pos;
                size_t offset2 = my_pos - x._children[i+1]->_output_pos;
                write_int(offset1 << 12 | offset2, 3);
            }
            if (i < x._children.size()) {
                size_t offset = my_pos - x._children[i]->_output_pos;
                write_int(offset << 4, 2);
            }
            write_bytes(x._payload.blob());
            return;
        }
        case SPARSE_16: {
            write_sparse(x, type, 2, my_pos);
            return;
        }
        case SPARSE_24: {
            write_sparse(x, type, 3, my_pos);
            return;
        }
        case SPARSE_40: {
            write_sparse(x, type, 5, my_pos);
            return;
        }
        case DENSE_12: {
            int start = int(x._children.front()->_transition);
            auto dense_span = 1 + int(x._children.back()->_transition) - int(x._children.front()->_transition);
            write_int((type << 4) | x._payload._payload_bits, 1);
            write_int(start, 1);
            write_int(dense_span - 1, 1);
            auto it = x._children.begin();
            auto end_it = x._children.end();
            int next = start;
            for (; next + 1 < start + dense_span; next += 2) {
                size_t offset_1 = 0;
                size_t offset_2 = 0;
                if (it != end_it && int(it->get()->_transition) == next) {
                    offset_1 = my_pos - it->get()->_output_pos;
                    ++it;
                }
                if (it != end_it && int(it->get()->_transition) == next + 1) {
                    offset_2 = my_pos - it->get()->_output_pos;
                    ++it;
                }
                write_int(offset_1 << 12 | offset_2, 3);
            }
            if (next < start + dense_span) {
                size_t offset = 0;
                if (it != end_it && int(it->get()->_transition) == next) {
                    offset = my_pos - it->get()->_output_pos;
                    ++it;
                }
                write_int(offset << 4, 2);
            }
            write_bytes(x._payload.blob());
            return;
        }
        case DENSE_16: {
            write_dense(x, type, 2, my_pos);
            return;
        }
        case DENSE_24: {
            write_dense(x, type, 3, my_pos);
            return;
        }
        case DENSE_32: {
            write_dense(x, type, 4, my_pos);
            return;
        }
        case DENSE_40: {
            write_dense(x, type, 5, my_pos);
            return;
        }
        case LONG_DENSE: {
            write_dense(x, type, 8, my_pos);
            return;
        }
        default: abort();
        }
    }
    size_t serialized_size(const writer_node& x, size_t start_pos) const {
        const auto my_pos = start_pos;
        auto type = choose_node_type(x, my_pos);
        switch (type) {
        case PAYLOAD_ONLY: {
            return 1 + x._payload.blob().size();
        }
        case SINGLE_NOPAYLOAD_4: {
            return 2;
        }
        case SINGLE_8: {
            return 3 + x._payload.blob().size();
        }
        case SINGLE_NOPAYLOAD_12: {
            return 3;
        }
        case SINGLE_16: {
            return 4 + x._payload.blob().size();
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
    size_t page_size() const {
        return _page_size;
    }
    size_t bytes_left_in_page() const {
        return round_up(pos() + 1, page_size()) - pos();
    };
    size_t pad_to_page_boundary() {
        const static std::array<std::byte, max_page_size> zero_page = {};
        size_t pad = bytes_left_in_page();
        _w.write(reinterpret_cast<const char*>(zero_page.data()), pad);
        return pad;
    }
    size_t pos() const {
        return _w.offset();
    }
};

// [[gnu::always_inline]]
// uint64_t read_offset(const_bytes sp, int idx, int bpp) {
//     if (bpp % 8 == 0) {
//         auto b = bpp / 8;
//         uint64_t a = seastar::read_be<uint64_t>((const char*)sp.data() + b * idx);
//         return a >> 8*(8 - b);
//     } else {
//         // uint64_t off = 0;
//         // for (int i = 0; i < div_ceil(bpp, 8); ++i) {
//         //     off = off << 8 | uint64_t(sp[(idx * bpp) / 8 + i]);
//         // }
//         // off >>= (8 - (bpp * (idx + 1) % 8)) % 8;
//         // off &= uint64_t(-1) >> (64 - bpp);
//         auto z = (seastar::read_be<uint32_t>((const char*)sp.data() + 3 * (idx / 2)) >> (20 - 12 * (idx % 2))) & 0xfff;
//         // assert(z == off);
//         return z;
//     }
// }

// Pimpl boilerplate
bti_trie_sink::bti_trie_sink() = default;
bti_trie_sink::~bti_trie_sink() = default;
bti_trie_sink& bti_trie_sink::operator=(bti_trie_sink&&) = default;
bti_trie_sink::bti_trie_sink(std::unique_ptr<impl> x) : _impl(std::move(x)) {}

static_assert(trie_writer_sink<bti_trie_sink::impl>);

payload::payload() noexcept {
}

payload::payload(uint8_t payload_bits, const_bytes payload) noexcept {
    SCYLLA_ASSERT(payload.size() <= _payload_buf.size());
    SCYLLA_ASSERT(payload_bits < 16);
    _payload_bits = payload_bits;
    _payload_size = payload.size();
    SCYLLA_ASSERT(bool(_payload_size) == bool(_payload_bits));
    std::ranges::copy(payload, _payload_buf.data());
}

const_bytes payload::blob() const noexcept {
    return {_payload_buf.data(), _payload_size};
}

writer_node::writer_node(std::byte b) noexcept
    : _transition(b) { }

writer_node* writer_node::add_child(std::byte b) {
    SCYLLA_ASSERT(_children.empty() || b > _children.back()->_transition);
    // FIXME: the writer is doing lots of small allocations. This has a considerable cost.
    // But those allocations have a nested lifetime. Perhaps a bump allocator (stack-like allocator)
    // would be a better fit for this task than the global allocator.
    _children.push_back(std::make_unique<writer_node>(b));
    return _children.back().get();
}

void writer_node::set_payload(const payload& p) noexcept {
    SCYLLA_ASSERT(_output_pos < 0);
    _payload = p;
}

void writer_node::write(trie_writer_sink auto& out) {
    size_t starting_pos = out.pos();
    SCYLLA_ASSERT(_node_size > 0);
    SCYLLA_ASSERT(round_down(out.pos(), out.page_size()) == round_down(out.pos() + _branch_size + _node_size - 1, out.page_size()));
    SCYLLA_ASSERT(_output_pos < 0);
    // A trie can be arbitrarily deep, so we can't afford to use recursion.
    // We have to maintain a walk stack manually.
    struct local_state {
        writer_node* _node;
        // The index of the next child to be visited.
        // When equal to the number of children, it's time to walk out of the node.
        int _stage;
        // The start position of the node's entire subtree.
        int _startpos;
    };
    // Note: partition index should almost always have depth smaller than 6. 
    // Row index can have depth of several thousand even in a reasonable workload. 
    utils::small_vector<local_state, 8> stack;
    // Depth-first walk.
    // We write out the subtree in postorder.
    stack.push_back({this, 0, out.pos()});
    while (!stack.empty()) {
        // Caution: note that `node`, `pos`, `stage` are references to `stack`.
        // Any pushing or popping will invalidate them, so be sure not to use them afterwards!
        auto& [node, stage, startpos] = stack.back();
        if (stage < static_cast<int>(node->_children.size())) {
            stage += 1;
            if (node->_children[stage - 1]->_output_pos < 0) {
                stack.push_back({node->_children[stage - 1].get(), 0, out.pos()});
            }
            continue;
        }
        SCYLLA_ASSERT(static_cast<ssize_t>(out.pos() - startpos) == node->_branch_size);
        SCYLLA_ASSERT(node->_payload._payload_bits || node->_children.size());
        node->_output_pos = out.pos();
        LOG_TRACE("writer_node::write: pos={} n_children={} size={}", out.pos(), node->_children.size(), node->_node_size);
        out.write(*node);
        SCYLLA_ASSERT(static_cast<ssize_t>(out.pos() - startpos) == node->_branch_size + node->_node_size);
        stack.pop_back();
        // It's important to call this clear() manually here.
        // If we just let it occur automatically when the `_children.clear()` is called after the loop,
        // the resulting destructor chain could overflow the small seastar::thread stack.
        // (It happened to me in early development, resulting in hard-to-understand memory corruption).
        node->_children.clear();
    }
    SCYLLA_ASSERT(static_cast<ssize_t>(out.pos() - starting_pos) == _branch_size + _node_size);
    _children.clear();
    _branch_size = 0;
    _node_size = 0;
    _has_out_of_page_children = false;
    _has_out_of_page_descendants = false;
}

// This routine is very similar to `write` in its structure.
size_t writer_node::recalc_sizes(const trie_writer_sink auto& out, size_t global_pos) {
    // A trie can be arbitrarily deep, so we can't afford to use recursion.
    // We have to maintain a walk stack manually.
    struct local_state {
        writer_node* _node;
        // The index of the next child to be visited.
        // When equal to the number of children, it's time to walk out of the node.
        int _stage;
        // The start position of the node's entire subtree.
        size_t _startpos;
    };
    utils::small_vector<local_state, 8> stack;
    // Depth-first walk.
    // To calculate the sizes, we essentially simulate a write of the tree.
    stack.push_back({this, 0, global_pos});
    while (!stack.empty()) {
        // Caution: note that `node`, `pos`, `stage` are references to `stack`.
        // Any pushing or popping will invalidate them, so be sure not to use them afterwards!
        auto& [node, stage, startpos] = stack.back();
        // If the node has out of page grandchildren, the sizes of children might have changed from the estimate,
        // so we have to recurse into the children and update them.
        if (stage < static_cast<int>(node->_children.size()) && node->_has_out_of_page_descendants) {
            stage += 1;
            stack.push_back({node->_children[stage - 1].get(), 0, global_pos});
            continue;
        }
        // If we got here, then either we have recursed into the children
        // (and then global_pos was updated accordingly),
        // or we skipped that because there was no need. In the latter case,
        // we have to update global_pos manually here.
        if (!node->_has_out_of_page_descendants) {
            global_pos += node->_branch_size;
        }
        node->_branch_size = global_pos - startpos;
        if (node->_has_out_of_page_children || node->_has_out_of_page_descendants) {
            // The size of children might have changed, which might have in turn changed
            // our offsets to children, which might have changed our size.
            // We have to recalculate.
            node->_node_size = out.serialized_size(*node, global_pos);
        }
        global_pos += node->_node_size;
        stack.pop_back();
    }
    return _branch_size + _node_size;
}

template <trie_writer_sink Output>
class trie_writer {
    std::unique_ptr<writer_node> _root;
    // Holds pointers to all nodes in the rightmost path in the tree.
    // Never empty. _stack[0] points to *_root.
    utils::small_vector<writer_node*, 8> _stack;
    Output& _out;
private:
    void complete(writer_node* x);
    void write(writer_node* x);
    void lay_out_children(writer_node* x);
    size_t recalc_total_size(writer_node* x, size_t start_pos) const noexcept;
public:
    trie_writer(Output&);
    ~trie_writer() = default;
    void add(size_t depth, const_bytes key_tail, const payload&);
    ssize_t finish();
};

template <trie_writer_sink Output>
trie_writer<Output>::trie_writer(Output& out)
    : _out(out) {
    _root = std::make_unique<writer_node>(std::byte(0));
    _stack.push_back(_root.get());
}

// Called when the writer walks out of the node, because it's done receiving children.
// This will initialize the members involved in size calculations.
// If the size of the subtree grows big enough, the node's children will be written
// out to disk. After this call, the total expected (on-disk) size of the still unwritten nodes
// in the subtree should be smaller than the space remaining in the current page of the writer.  
template <trie_writer_sink Output>
void trie_writer<Output>::complete(writer_node* x) {
    SCYLLA_ASSERT(x->_branch_size < 0);
    SCYLLA_ASSERT(x->_node_size < 0);
    SCYLLA_ASSERT(x->_has_out_of_page_children == false);
    SCYLLA_ASSERT(x->_has_out_of_page_descendants == false);
    SCYLLA_ASSERT(x->_output_pos < 0);
    bool has_out_of_page_children = false;
    bool has_out_of_page_descendants = false;
    size_t branch_size = 0;
    for (const auto& c : x->_children) {
        branch_size += c->_branch_size + c->_node_size;
        has_out_of_page_children |= c->_output_pos >= 0;
        has_out_of_page_descendants |= c->_has_out_of_page_descendants || c->_has_out_of_page_children;
    }
    size_t node_size = _out.serialized_size(*x, _out.pos() + branch_size);
    // We try to keep parents in the same page as their children as much as possible.
    //
    // If the completed subtree fits into the remainder of the current page, good -- we don't have to split anything across
    // pages yet. We don't have to do anything for now.
    //
    // If it doesn't fit, we have to do something: split it across pages, pad the stream to the next page boundary and hope it fits
    // into the full page without splitting, or maybe do nothing for now and try to cram a later sibling into the current page first.
    // 
    // We choose to split it here. We write out our children. Some of them children will go into the current page,
    // some will go into the next ones. We (the root of the subtree) will be written out later by our ancestor.
    // 
    // The choice of split points is fairly arbitrary. But we aren't trying to be optimal.
    // We assume that our greedy strategy should be good enough.
    //
    // See https://github.com/apache/cassandra/blob/9dfcfaee6585a3443282f56d54e90446dc4ff012/src/java/org/apache/cassandra/io/tries/IncrementalTrieWriterPageAware.java#L32
    // for an alternative description of the process.
    if (branch_size + node_size <= _out.page_size()) {
        x->_branch_size = branch_size;
        x->_node_size = node_size;
        x->_has_out_of_page_children = has_out_of_page_children;
        x->_has_out_of_page_descendants = has_out_of_page_descendants;
    } else {
        lay_out_children(x);
    }
}

template <trie_writer_sink Output>
void trie_writer<Output>::lay_out_children(writer_node* x) {
    SCYLLA_ASSERT(x->_output_pos < 0);
    // FIXME: is std::set the right choice here?
    auto cmp = [](writer_node* a, writer_node* b) { return std::make_pair(a->_branch_size + a->_node_size, a->_transition) < std::make_pair(b->_branch_size + b->_node_size, b->_transition); };
    auto unwritten_children = std::set<writer_node*, decltype(cmp)>(cmp);
    for (const auto& c : x->_children) {
        if (c->_output_pos < 0) {
            unwritten_children.insert(c.get());
        }
    }
    // In this routine, we write out all child branches.
    // We are free to do it in an arbitrary order.
    //
    // In an attempt to minimize padding, we always first try to write out
    // the largest child which still fits into the current page.
    // If none of them fit, we pad to the next page boundary and try again.
    while (unwritten_children.size()) {
        writer_node* candidate;

        writer_node selection_key(std::byte(255));
        selection_key._branch_size = _out.bytes_left_in_page();
        selection_key._node_size = 0;
        // Find the smallest child which doesn't fit. (All might fit, then we will get the past-the-end iterator).
        // Its predecessor will be the biggest child which does fit.
        auto choice_it = unwritten_children.upper_bound(&selection_key);
        if (choice_it == unwritten_children.begin()) {
            // None of the still-unwritten children fits into the current page,
            // so we pad to page boundary and "try again".
            //
            // We don't have to call upper_bound again, though.
            // All children should fit into a full page,
            // so we can just the biggest child.
            _out.pad_to_page_boundary();
            SCYLLA_ASSERT(_out.bytes_left_in_page() == _out.page_size());
            // Pick the biggest child branch.
            choice_it = std::end(unwritten_children);
        }
        // The predecessor of upper_bound is the biggest child which still fits.
        choice_it = std::prev(choice_it);
        candidate = *choice_it;
        unwritten_children.erase(choice_it);

        // We picked the candidate based on size estimates which might not be exact.
        // In reality, the candidate might not fit into the page.
        // If the estimates aren't known to be exact, we have to update the estimates and check for fit again.
        if (candidate->_has_out_of_page_children || candidate->_has_out_of_page_descendants) {
            size_t true_size = recalc_total_size(candidate, _out.pos());
            if (true_size > _out.bytes_left_in_page()) {
                if (true_size > _out.page_size()) {
                    // After updating the estimates, we see that the candidate branch actually
                    // won't fit even in a full page.
                    // We have no choice but to split it now.
                    // That's unfortunate, because it means it will be separated both from its parent
                    // and from its children. But we assume that this kind of situation is rare
                    // and/or not worth preventing.
                    lay_out_children(candidate);
                }
                // After updating the estimates, we see that the node wasn't the right candidate.
                // So we don't write it -- we return it to the pool of candidates and choose a candidate that fits.
                unwritten_children.insert(candidate);
                continue;
            }
        }
        // Write the candidate branch, in postorder, to the file.
        write(candidate);
    }
    x->_branch_size = 0;
    x->_has_out_of_page_children = true;
    x->_has_out_of_page_descendants = false;
    x->_node_size = _out.serialized_size(*x, _out.pos());
    SCYLLA_ASSERT(x->_node_size <= static_cast<ssize_t>(_out.page_size()));
}

template <trie_writer_sink Output>
size_t trie_writer<Output>::recalc_total_size(writer_node* branch, size_t global_pos) const noexcept {
    return branch->recalc_sizes(_out, global_pos);
}

template <trie_writer_sink Output>
void trie_writer<Output>::write(writer_node* branch) {
    branch->write(_out);
}

// `depth` describes the level of the trie where we have to fork off a new branch
// (a chain with the content `key_tail` and payload `p` in the new leaf at the end).
//
// The fact that our caller calculates the `depth`, not us, is somewhat awkward,
// but since the callers already look for the mismatch point for other reasons,
// I figured this API spares us a second search.
template <trie_writer_sink Output>
void trie_writer<Output>::add(size_t depth, const_bytes key_tail, const payload& p) {
    SCYLLA_ASSERT(_stack.size() >= 1);
    SCYLLA_ASSERT(_stack.size() - 1 >= depth);
    // As an exception, we allow one case where adding a payload to an already-existing node is legal:
    // the root, when inserting an empty key.
    //
    // The BTI index never inserts an empty key though. Perhaps the logic would be slightly simpler
    // if we just disallowed the empty key?
    SCYLLA_ASSERT(IMPLIES(key_tail.empty(), depth == 0));

    while (_stack.size() - 1 > depth) {
        complete(_stack.back());
        _stack.pop_back();
    }
    for (size_t i = 0; i < key_tail.size(); ++i) {
        _stack.push_back(_stack.back()->add_child(key_tail[i]));
    }
    _stack.back()->set_payload(p);

    SCYLLA_ASSERT(_stack.size() == 1 + depth + key_tail.size());
}

template <trie_writer_sink Output>
ssize_t trie_writer<Output>::finish() {
    while (_stack.size()) {
        complete(_stack.back());
        _stack.pop_back();
    }
    ssize_t root_pos = -1;
    if (_root->_children.size() || _root->_payload._payload_bits) {
        writer_node superroot(std::byte(0));
        superroot._children.push_back(std::move(_root));
        lay_out_children(&superroot);
        root_pos = superroot._children[0]->_output_pos;
    }
    // FIXME: don't recreate, just reset.
    _root = std::make_unique<writer_node>(std::byte(0));
    _stack.push_back(_root.get());
    return root_pos;
}

// The job of this class is to take a stream of partition keys and their index payloads
// (Data.db or Rows.db position, and hash bits for filtering false positives out)
// and translate it to a minimal trie which still allows for efficient queries.
//
// For that, each key in the trie is trimmed to the shortest length which still results
// in a unique leaf.
// For example, assume that keys ABCXYZ, ABDXYZ, ACXYZ are added to the trie.
// A "naive" trie would look like this:
//     A
//     B---C
//     C-D X
//     X X Y
//     Y Y Z
//     Z Z
//
// But this writer trims that to:
//     A
//     B---C
//     C-D
// 
// Note: the trie-serialization format of partition keys is prefix-free,
// which guarantees that only leaf nodes carry a payload. 
// 
// In practice, Output == trie_serializer.
// But for testability reasons, we don't hardcode the output type.
// (So that tests can test just the logic of this writer, without
// roping in file I/O and the serialization format)
// For performance reasons we don't want to make the output virtual,
// so we instead make it a template parameter.
template <trie_writer_sink Output>
class partition_index_trie_writer_impl {
public:
    partition_index_trie_writer_impl(Output&);
    void add(const_bytes key, int64_t data_file_pos, uint8_t hash_bits);
    ssize_t finish();
private:
    // The lower trie-writing layer, oblivious to the semantics of the partition index.
    trie_writer<Output> _wr;
    // Counter of added keys.
    size_t _added_keys = 0;

    // We "buffer" one key in the writer. To write an entry to the trie_writer,
    // we want to trim the key to the shortest prefix which is still enough
    // to differentiate the key from its neighbours. For that, we need to see
    // the *next* entry first.
    //
    // The first position in _last_key where it differs from its predecessor.
    // After we see the next entry, we will also know the first position where it differs
    // from the successor, and we will be able to determine the shortest unique prefix. 
    size_t _last_key_mismatch = 0;
    // The key added most recently.
    std::vector<std::byte> _last_key;
    // The payload of _last_key: the Data.db position and the hash bits. 
    int64_t _last_data_file_pos;
    uint8_t _last_hash_bits;
};

template <trie_writer_sink Output>
partition_index_trie_writer_impl<Output>::partition_index_trie_writer_impl(Output& out)
    : _wr(out) {
}

template <trie_writer_sink Output>
void partition_index_trie_writer_impl<Output>::add(const_bytes key, int64_t data_file_pos, uint8_t hash_bits) {
    LOG_TRACE("partition_index_trie_writer_impl::add: this={} key={}, data_file_pos={}", fmt::ptr(this), format_byte_span(key), data_file_pos); 
    if (_added_keys > 0) {
        // First position where the new key differs from the last key.
        size_t mismatch = std::ranges::mismatch(key, _last_key).in2 - _last_key.begin();
        // From `_last_key_mismatch` (mismatch position between `_last_key` and its predecessor)
        // and `mismatch` (mismatch position between `_last_key` and its successor),
        // compute the minimal needed prefix of `_last_key`.
        // FIXME: the std::min(..., _last_key.size()) isn't really needed, since a key can't be a prefix of another key.
        size_t needed_prefix = std::min(std::max(_last_key_mismatch, mismatch) + 1, _last_key.size());
        // The new nodes/characters we have to add to the trie.
        auto tail = std::span(_last_key).subspan(_last_key_mismatch, needed_prefix - _last_key_mismatch);
        // Serialize the payload.
        std::array<std::byte, 9> payload_bytes;
        uint64_t pos_be = seastar::cpu_to_be(_last_data_file_pos);
        payload_bytes[0] = std::byte(_last_hash_bits);
        auto payload_bits = div_ceil(std::bit_width<uint64_t>(std::abs(_last_data_file_pos)) + 1, 8);
        std::memcpy(&payload_bytes[1], (const char*)(&pos_be) + 8 - payload_bits, payload_bits);
        // Pass the new node chain and its payload to the lower layer.
        // Note: we pass (payload_bits | 0x8) because the additional 0x8 bit indicates that hash bits are present.
        // (Even though currently they are always present, and the reader assumes so).
        _wr.add(_last_key_mismatch, tail, payload(payload_bits | 0x8, std::span(payload_bytes).subspan(0, payload_bits + 1)));
        // Update _last_* variables with the new key.
        _last_key_mismatch = mismatch;
    }
    _added_keys += 1;
    // Update _last_* variables with the new key.
    _last_key.assign(key.begin(), key.end());
    _last_data_file_pos = data_file_pos;
    _last_hash_bits = hash_bits;
}
template <trie_writer_sink Output>
ssize_t partition_index_trie_writer_impl<Output>::finish() {
    if (_added_keys > 0) {
        // Mostly duplicates the code from add(), except there is only one mismatch position to care about,
        // not two which we have to take a max() of.
        //
        // FIXME: the std::min(..., _last_key.size()) isn't really needed, since a key can't be a prefix of another key.
        //
        // FIXME: maybe somehow get rid of the code duplication between here and add().
        size_t needed_prefix = std::min(_last_key_mismatch + 1, _last_key.size());
        // The new nodes/characters we have to add to the trie.
        auto tail = std::span(_last_key).subspan(_last_key_mismatch, needed_prefix - _last_key_mismatch);
        // Serialize the payload.
        std::array<std::byte, 9> payload_bytes;
        uint64_t pos_be = seastar::cpu_to_be(_last_data_file_pos);
        payload_bytes[0] = std::byte(_last_hash_bits);
        auto payload_bits = div_ceil(std::bit_width<uint64_t>(std::abs(_last_data_file_pos)) + 1, 8);
        std::memcpy(&payload_bytes[1], (const char*)(&pos_be) + 8 - payload_bits, payload_bits);
        // Pass the new node chain and its payload to the lower layer.
        // Note: we pass (payload_bits | 0x8) because the additional 0x8 bit indicates that hash bits are present.
        // (Even though currently they are always present, and the reader assumes so).
        _wr.add(_last_key_mismatch, tail, payload(payload_bits | 0x8, std::span(payload_bytes).subspan(0, payload_bits + 1)));
    }
    return _wr.finish();
}

// Instantiation of partition_index_trie_writer_impl with `Output` == `bti_trie_sink::impl`.
// This is the partition trie writer which is actually used in practice.
// Other instantiations of partition_index_trie_writer_impl are only for testing.
class partition_trie_writer::impl : public partition_index_trie_writer_impl<bti_trie_sink::impl> {
    using partition_index_trie_writer_impl::partition_index_trie_writer_impl;
};

// Pimpl boilerplate
partition_trie_writer::partition_trie_writer() = default;
partition_trie_writer::~partition_trie_writer() = default;
partition_trie_writer& partition_trie_writer::operator=(partition_trie_writer&&) = default;
partition_trie_writer::partition_trie_writer(bti_trie_sink& out)
    : _impl(std::make_unique<impl>(*out._impl)){
}
void partition_trie_writer::add(const_bytes key, int64_t data_file_pos, uint8_t hash_bits) {
    _impl->add(key, data_file_pos, hash_bits);
}
ssize_t partition_trie_writer::finish() {
    return _impl->finish();
}

// Returned by methods of reader_node which return metadata about one of its children.
struct lookup_result {
    // Index of the queried child.
    // An integer in range [0, n_children].
    // (As is customary: when its equal to n_children, it means that there is no child fitting the query).
    int idx;
    // The transition byte of the selected child. (Meaningless when idx == n_children).
    std::byte byte;
    // The offset of child with respect to its parent.
    // (The position of the child is `parent's position` - `offset`.
    // Children always positions lower than their parent).
    uint64_t offset;
};

struct payload_result {
    uint8_t bits;
    const_bytes bytes;
};

// A pointer (i.e. file position) to a node on disk, with some initial metadata parsed. 
struct reader_node {
    struct page_ptr : cached_file::ptr_type {
        using parent = cached_file::ptr_type;
        page_ptr() noexcept = default;
        page_ptr(parent&& x) noexcept : parent(std::move(x)) {}
        page_ptr(const page_ptr& other) noexcept : parent(other->share()) {}
        page_ptr(page_ptr&&) noexcept = default;
        page_ptr& operator=(page_ptr&&) noexcept = default;
        page_ptr& operator=(const page_ptr& other) noexcept {
            parent::operator=(other->share());
            return *this;
        }
    };
    // Position of this node in the input file.
    size_t pos;
    // Number of children preemptively extracted from the representation.
    uint16_t n_children;
    // Payload bits preemptivelyextracted from the representation.
    uint8_t payload_bits;

    // Looks up the first child of this node greater with transition greater or equal to the given one.
    // If such child doesn't exist, the `idx` of the result will be negative.
    lookup_result lookup(std::byte transition, const page_ptr& ptr);
    // Looks up the child with the given index.
    // If there is no child with such an index (can happen in DENSE nodes, which have empty slots),
    // picks the closest child with idx greater (if `forward == true`) or smaller (if `forward == false`)
    // than the given. If there is no such child, the `idx` of the result will be negative.
    lookup_result get_child(int idx, bool forward, const page_ptr& ptr);
    // Returns a view of the payload of this node.
    // The `bytes` view can extend beyond 
    payload_result payload(const page_ptr& ptr) const;
    // Returns a view of the raw on-disk representation of the node.
    const_bytes raw(const page_ptr& ptr) const;
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

// For iterating over the trie, we need to keep a stack of those,
// forming a path from the root to the current node.
//
// child_idx is the index of the child currently visited by the containing trie_cursor.
// child_idx might be equal to -1, this means that the node itself, not any of its children,
// is currently visited. 
struct node_cursor {
    reader_node node;
    int child_idx;
};

template <typename T>
concept trie_reader_source = requires(T& o, uint64_t pos, reader_permit rp) {
    { o.read(pos) } -> std::same_as<future<reader_node>>;
    { o.read_row_index_header(pos, rp) } -> std::same_as<future<row_index_header>>;
};

// Parses some basic type-oblivious metadata.
static reader_node pv_to_reader_node(size_t pos, const cached_file::ptr_type& pv) {
    auto sp = pv->get_view().subspan(pos % cached_file::page_size);
    LOG_TRACE("my_trie_reader_input::read: pos={} {}", pos, format_byte_span(sp.subspan(0, 32)));

    auto type = uint8_t(sp[0]) >> 4;
    switch (type) {
    case PAYLOAD_ONLY:
        return reader_node{pos, 0, uint8_t(sp[0]) & 0xf};
    case SINGLE_NOPAYLOAD_4:
    case SINGLE_NOPAYLOAD_12:
        return reader_node{pos, 1, 0};
    case SINGLE_8:
    case SINGLE_16:
        return reader_node{pos, 1, uint8_t(sp[0]) & 0xf};
    case SPARSE_8:
    case SPARSE_12:
    case SPARSE_16:
    case SPARSE_24:
    case SPARSE_40: {
        auto n_children = uint8_t(sp[1]);
        return reader_node{pos, n_children, uint8_t(sp[0]) & 0xf};
    }
    case DENSE_12:
    case DENSE_16:
    case DENSE_24:
    case DENSE_32:
    case DENSE_40:
    case LONG_DENSE: {
        auto dense_span = uint8_t(sp[2]) + 1;
        return reader_node{pos, dense_span, uint8_t(sp[0]) & 0xf};
    }
    default: abort();
    }
}

enum class row_index_header_parser_state {
    START,
    KEY_SIZE,
    KEY_BYTES,
    DATA_FILE_POSITION,
    OFFSET_FROM_TRIE_ROOT,
    LOCAL_DELETION_TIME,
    MARKED_FOR_DELETE_AT,
    END,
};

inline std::string_view format_as(row_index_header_parser_state s) {
    using enum row_index_header_parser_state;
    switch (s) {
    case START: return "START";
    case KEY_SIZE: return "KEY_SIZE";
    case KEY_BYTES: return "KEY_BYTES";
    case DATA_FILE_POSITION: return "DATA_FILE_POSITION";
    case OFFSET_FROM_TRIE_ROOT: return "OFFSET_TO_TRIE_ROOT";
    case LOCAL_DELETION_TIME: return "LOCAL_DELETION_TIME";
    case MARKED_FOR_DELETE_AT: return "MARKED_FOR_DELETE_AT";
    case END: return "END";
    default: abort();
    }
}

// We use this to parse the row_index_header.
// FIXME: I didn't look at the performance of this at all yet. Maybe it's very inefficient.
struct row_index_header_parser : public data_consumer::continuous_data_consumer<row_index_header_parser> {
    using processing_result = data_consumer::processing_result;
    using proceed = data_consumer::proceed;
    using state = row_index_header_parser_state;
    state _state = state::START;
    row_index_header _result;
    uint64_t _position_offset;
    temporary_buffer<char> _key;
    void verify_end_state() {
        if (_state != state::END) {
            throw sstables::malformed_sstable_exception(fmt::format("row_index_header_parser: verify_end_state: expected END, got {}", _state));
        }
    }
    bool non_consuming() const {
        return ((_state == state::END) || (_state == state::START));
    }
    processing_result process_state(temporary_buffer<char>& data) {
        auto current_pos = [&] { return this->position() - data.size(); };
        switch (_state) {
        // START comes first, to make the handling of the 0-quantity case simpler
        case state::START:
            LOG_TRACE("{}: pos {} state {} - data.size()={}", fmt::ptr(this), current_pos(), state::START, data.size());
            _state = state::KEY_SIZE;
            break;
        case state::KEY_SIZE:
            LOG_TRACE("{}: pos {} state {}", fmt::ptr(this), current_pos(), state::KEY_SIZE);
            if (this->read_16(data) != continuous_data_consumer::read_status::ready) {
                _state = state::KEY_BYTES;
                break;
            }
            [[fallthrough]];
        case state::KEY_BYTES:
            LOG_TRACE("{}: pos {} state {} - size={}", fmt::ptr(this), current_pos(), state::KEY_BYTES, this->_u16);
            if (this->read_bytes_contiguous(data, this->_u16, _key) != continuous_data_consumer::read_status::ready) {
                _state = state::DATA_FILE_POSITION;
                break;
            }
            [[fallthrough]];
        case state::DATA_FILE_POSITION:
            _result.partition_key = sstables::key(to_bytes(to_bytes_view(_key)));
            _position_offset = current_pos();
            LOG_TRACE("{}: pos {} state {}", fmt::ptr(this), current_pos(), state::DATA_FILE_POSITION);
            if (read_unsigned_vint(data) != continuous_data_consumer::read_status::ready) {
                _state = state::OFFSET_FROM_TRIE_ROOT;
                break;
            }
            [[fallthrough]];
        case state::OFFSET_FROM_TRIE_ROOT:
            LOG_TRACE("{}: pos {} state {}", fmt::ptr(this), current_pos(), state::OFFSET_FROM_TRIE_ROOT);
            _result.data_file_offset = this->_u64;
            if (read_unsigned_vint(data) != continuous_data_consumer::read_status::ready) {
                _state = state::LOCAL_DELETION_TIME;
                break;
            }
            [[fallthrough]];
        case state::LOCAL_DELETION_TIME: {
            _result.trie_root = _position_offset - this->_u64;
            LOG_TRACE("{}: pos {} state {}", fmt::ptr(this), current_pos(), state::LOCAL_DELETION_TIME);
            if (this->read_32(data) != continuous_data_consumer::read_status::ready) {
                _state = state::MARKED_FOR_DELETE_AT;
                break;
            }
        }
            [[fallthrough]];
        case state::MARKED_FOR_DELETE_AT:
            LOG_TRACE("{}: pos {} state {}", fmt::ptr(this), current_pos(), state::MARKED_FOR_DELETE_AT);
            _result.partition_tombstone.deletion_time = gc_clock::time_point(gc_clock::duration(this->_u32));
            if (this->read_64(data) != continuous_data_consumer::read_status::ready) {
                _state = state::END;
                break;
            }
            [[fallthrough]];
        case state::END: {
            _state = row_index_header_parser_state::END;
            _result.partition_tombstone.timestamp = this->_u64;
        }
        }
        LOG_TRACE("{}: exit pos {} state {}", fmt::ptr(this), current_pos(), _state);
        return _state == state::END ? proceed::no : proceed::yes;
    }
public:
    row_index_header_parser(reader_permit rp, input_stream<char>&& input, uint64_t start, uint64_t maxlen)
        : continuous_data_consumer(std::move(rp), std::move(input), start, maxlen)
    {}
};

class bti_trie_source::impl {
    cached_file& _f;
    reader_permit _permit;
public:
    impl(cached_file& f, reader_permit p) : _f(f), _permit(p) {}
    [[gnu::always_inline]]
    future<reader_node> read(uint64_t pos) {
        auto x = _f.try_get_page_ptr(pos / cached_file::page_size, nullptr);
        if (x) {
            return make_ready_future<reader_node>(pv_to_reader_node(pos, std::move(x)));
        }
        return _f.get_page_view(pos, nullptr).then([pos] (auto&& pv) { return pv_to_reader_node(pos, std::move(pv)); });

    }
    future<cached_file::ptr_type> read_page(uint64_t pos) {
        return _f.get_page_view(pos, nullptr);

    }
    // FIXME: I didn't look at the performance of this at all yet. Maybe it's very inefficient.
    future<row_index_header> read_row_index_header(uint64_t pos, reader_permit rp) {
        auto ctx = row_index_header_parser(
            std::move(rp),
            make_file_input_stream(file(make_shared<cached_file_impl>(_f)), pos, _f.size() - pos, {.buffer_size = 4096, .read_ahead = 0}),
            pos,
            _f.size() - pos);
        std::exception_ptr ex;
        try {
            co_await ctx.consume_input();
            co_await ctx.close();
            LOG_TRACE("read_row_index_header this={} result={}", fmt::ptr(this), ctx._result.data_file_offset);
            co_return std::move(ctx._result);
        } catch (...) {
            ex = std::current_exception();
        }
        co_await ctx.close();
        std::rethrow_exception(ex);
    }
};

// Pimpl boilerplate
bti_trie_source::bti_trie_source() = default;
bti_trie_source::bti_trie_source(bti_trie_source&&) = default;
bti_trie_source::~bti_trie_source() = default;
bti_trie_source& bti_trie_source::operator=(bti_trie_source&&) = default;
bti_trie_source::bti_trie_source(std::unique_ptr<impl> x) : _impl(std::move(x)) {}

bti_trie_source make_bti_trie_source(cached_file& f, reader_permit p) {
    return std::make_unique<bti_trie_source::impl>(f, p);
}

// set_before*() methods of the cursors can return one of the below:
//
// eof: the searched key is greater than all key in the trie, and the cursor was set to an "eof" position
// definitely_not_a_match: the cursor was set to some key which is sure not to be an exact match for the searched key.
// possible_match: the cursor was set to some key which might possibly be an exact match for the searched key.
//
// Some other methods also return this type, but with different semantics: their return value only
// communicates "eof" or "not eof". The "match"/"no_match" distinction is meaningless for them.
// That's not very clean, but we can't easily return a different type from them, since e.g. set_before()
// tail-calls them, and converting the result type would require adding an additional continuation to the future<>,
// and that isn't worth it.
enum class set_result {
    eof,
    definitely_not_a_match,
    possible_match,
};

// An iterator over the (payloaded) keys in the trie,
// which can be set before and after a given search prefix,
// and stepped in each direction.
//
// Invariants upheld before calling every public method:
//
// 1. The cursor can be in one of the following mutually-exclusive states:
//   - Uninitialized: _path is empty.
//   - Pointing at EOF: _path contains exactly one entry (root), which has child_idx == n_children. 
//   - Pointing at a node:
//     _path is non-empty,
//     all non-last entries in _path have child_idx ∈ [0, n_children),
//     last entry has child_idx == -1.
//     The pointee can be payloaded (corresponding to some inserted key) or not.
//   - Pointing at a transition:
//     _path is non-empty,
//     all entries in _path have child_idx ∈ [0, n_children).
//     Semantically, this state means that the cursor is pointing at a fake "position" just before
//     the child node with index (child_idx + 1) of the last entry in _path.
//   Each method specifices the states it can be called with, and states guaranteed after they return.
// 2. The sequence of pages in _pages matches _path.
//   Informally: [p.pos for p in _pages] == unique([round_down(p.pos, page_size) for p in _paths]),
//   where unique("AACCCCBBB") means "ACB".
// 3. The sequence of positions in _pages and the sequence of positions in _paths are strictly declining. 
//
// Assumptions:
// 1. All leaves in the trie have a payload.
// 2. Weak exception guarantee. The only legal thing to do with a cursor after an exception is to destroy it.
template <trie_reader_source Input>
class trie_cursor {
    // Reference wrapper to allow copying the cursor.
    std::reference_wrapper<Input> _in;
    // A stack holding the path from the root to the currently visited node.
    // When initialized, _path[0] is root.
    //
    // FIXME: To support stepping, the iterator has to keep
    // a stack of visited nodes (the path from root to the current node).
    // But single-partition reads don't need to support stepping, and
    // it this case maintaining the stack only adds overhead.
    // Consider adding a separate cursor type for single-partition reads,
    // or a special single-partititon variant of `set_before()` which won't bother maintaining the stack.
    utils::small_vector<node_cursor, 8> _path;
    // A stack holding the pages corresponding to `_path`, in order.
    // Each entry in `_pages` can cover many neighbouring entries in `_path`.
    // 
    // Note: a node always has a greater position than its children.
    // Therefore, `_pages` always contains pages in order of decreasing position.
    //
    // FIXME: on second thought, maitaining _pages is an unnecessary complication.
    // It guarantees that we never have to re-read a page when we step up to the parent,
    // but that's not particularly useful. In most cases, it should be enough to only
    // hold the topmost page, instead of the entire stack.
    //
    // FIXME: account memory in a reader_permit.
    utils::small_vector<reader_node::page_ptr, 8> _pages;
private:
    // If the page at the top of the stack covers the node at position `pos`,
    // pushes that node to _path and returns true.
    // Otherwise returns false.
    bool try_push(uint64_t pos);
    // Pushes the page covering `pos` to `_pages`,
    // and pushes the node at `pos` to `_path`.
    //
    // Before the call, the topmost entry in `_pages` mustn't be already covering `pos`. 
    future<> push_page_and_node(uint64_t pos);
    // If the topmost entry in `_pages` covers `pos`, pushes `pos` to `_path`.
    // Otherwise loads the relevant page and pushes it to `_pages`, and pushes
    // `pos` to `_path`.
    future<> push_maybe_page_and_node(uint64_t pos);
    // Pops the top of `_path`.
    // If it was the only node covered by the top of `_pages`, pops the top of `_pages`.
    void pop();
    // Checks various invariants which every public method must uphold.
    // (See the comment before the class name).
    // For the purposes of debugging during development.
    void check_invariants() const;
    bool is_initialized() const;
    bool points_at_eof() const;
    bool points_at_node() const;
    bool points_at_payloaded_node() const;
public:
    trie_cursor(Input&);
    ~trie_cursor();
    trie_cursor& operator=(const trie_cursor&) = default;
    // Preconditions: none.
    // Postconditions: points at the root node.
    future<void> init(uint64_t root_pos);
    // Checks whether the cursor is initialized.
    // Preconditions: none.
    bool initialized() const;
    // Let S be the set of keys present in the trie.
    //
    // set_before(K) sets the cursor to some key P: S ∪ {+∞},
    // such that S ∩ [K, +∞) ⊆ S ∩ [P, +∞)
    //
    // In other words, it sets the cursor to *some* position smaller-or-equal to any key from S ∩ [K, +∞).
    // However, in general it is *NOT* guaranteed to be the greatest such position.
    // In other words, if we call `set_before(K)` and read the Data file forward starting
    // from the cursor's position, the first several keys might be *smaller* than K!
    // 
    // But if K is present (inserted) in the trie, then the cursor is guaranteed to be set exactly to K.
    //
    // set_after(K) sets the cursor to some position P: S ∪ {+∞},
    // such that S ∩ (-∞, K] ⊆ S ∩ (-∞, P).
    //
    // Similarly as with set_before(), the Data file bound obtained in this way doesn't have to be tight:
    // they might exist several entries smaller than the cursor, but greater than K. 
    // But if K is present in the trie, the cursor is guaranteed to be set to the position
    // immediately following K, and the bound is tight.
    //
    // Performance-critical.
    //
    // Preconditions: points at eof, node, or transition.
    // Postconditions: points at eof or a payloaded node.
    future<set_result> set_before(const_bytes key);
    future<set_result> set_after(const_bytes key);
    // Moves the cursor to the next key (or EOF).
    //
    // step() returns a set_result, but it can only return `eof` (when it steps beyond all keys),
    // or `definitely_not_a_match` otherwise.
    // Returning a "not EOF" result as "definitely_not_a_match" isn't clean,
    // but we can't easily return a more meaningful enum because that would
    // require adding futures to the continuation chains.
    //
    // Preconditions: points at a node or transition.
    // Postconditions: points at eof or a payloaded node.
    future<set_result> step();
    // Moves the cursor to the previous key.
    // If there is no previous key, doesn't do anything. 
    //
    // Preconditions: points at eof or a payloaded (sic!) node.
    // Postconditions: points at eof or a payloaded node.
    future<> step_back();
    // Preconditions: points at a payloaded node.
    payload_result payload() const;
    // Checks whether the cursor in the EOF position.
    // 
    // Preconditions: points at eof or a node.
    bool eof() const;
    // Preconditions: none.
    // Postconditions: uninitialized.
    void reset();
};

// An index cursor which can be used to obtain the Data.db bounds
// (and tombstones, where appropriate) for the given partition and row ranges.
template <trie_reader_source Input>
class index_cursor {
    // A cursor into Partitions.db.
    trie_cursor<Input> _partition_cursor;
    // A cursor into Rows.db. Only initialized when the pointed-to partition has an entry in Rows.db.
    trie_cursor<Input> _row_cursor;
    // Holds the row index header for the current partition, iff the current partition has a row index.  
    std::optional<row_index_header> _partition_metadata;
    // _row_cursor reads the tries written in Rows.db.
    // Rerefence wrapper to allow for copying the cursor.
    std::reference_wrapper<Input> _in_row;
    // Accounts the memory consumed by the cursor.
    // FIXME: it actually doesn't do that, yet.
    reader_permit _permit;
private:
    // If the current partition has a row index, reads its header.
    future<> maybe_read_metadata();
    // The colder part of set_after_row, just to hint at inlining the hotter part.
    future<set_result> set_after_row_cold(const_bytes);
public:
    index_cursor(Input& par, Input& row, reader_permit);
    index_cursor& operator=(const index_cursor&) = default;
    future<> init(uint64_t root_pos);
    // Returns the data file position of the cursor. Can only be called on a set cursor.
    //
    // If the row cursor is set, returns the position of the pointed-to clustering key block.
    // Otherwise, if the partition cursor is set to a partition, returns the position of the partition.
    // Otherwise, if the partition cursor is set to EOF, returns `file_size`. (This part of the API is somewhat awkward)
    uint64_t data_file_pos(uint64_t file_size) const;
    // If the row index is set to a clustering key block, returns the range tombstone active at the start of the block.
    // Otherwise returns an empty tombstone.
    tombstone open_tombstone() const;
    // Returns the current partition's row index header, if it has one.
    const std::optional<row_index_header>& partition_metadata() const;
    // Sets the partition cursor to *some* position smaller-or-equal to all entries greater-or-equal to K.
    // See the comments at trie_cursor::set_before for more elaboration.
    //
    // Resets the row cursor.
    future<set_result> set_before_partition(const_bytes K);
    // Sets the partition cursor to *some* position strictly greater than all entries smaller-or-equal to K. 
    // See the comments at trie_cursor::set_after for details.
    //
    // Resets the row cursor.
    future<set_result> set_after_partition(const_bytes K);
    // Moves the partition cursor to the next position (next partition or eof).
    // Resets the row cursor.
    future<set_result> next_partition();
    // Sets the row cursor to *some* position (within the current partition)
    // smaller-or-equal to all entries greater-or-equal to K.
    // See the comments at trie_cursor::set_before for more elaboration.
    future<set_result> set_before_row(const_bytes);
    // Sets the row cursor to *some* position (within the current partition)
    // smaller-or-equal to all entries greater-or-equal to K.
    // See the comments at trie_cursor::set_before for more elaboration.
    //
    // If the row cursor would go beyond the end of the partition,
    // it instead resets moves the partition cursor to the next partition
    // and resets the row cursor.
    future<set_result> set_after_row(const_bytes);
    // Checks if the row cursor is set.
    bool row_cursor_set() const;
};

class trie_index_reader : public sstables::index_reader {
    // Trie sources for Partitions.db and Rows.db
    //
    // FIXME: should these be owned by the sstable object instead?
    bti_trie_source _in_par;
    bti_trie_source _in_row;
    // We need the schema solely to parse the partition keys serialized in row index headers.
    schema_ptr _s;
    // Supposed to account the memory usage of the index reader.
    // FIXME: it doesn't actually do that yet.
    // FIXME: it should probably also mark the permit as blocked on disk?
    reader_permit _permit;
    // The index is, in essence, a pair of cursors.
    index_cursor<bti_trie_source::impl> _lower;
    index_cursor<bti_trie_source::impl> _upper;
    // We don't need this for anything, only the cursors do.
    // But they take it via the asynchronous init() function, not via constructor,
    // so we need to temporarily hold on to it between the constructor and the init().
    uint64_t _root;
    // Partitions.db doesn't store the size of Data.db, so the cursor doesn't know by itself
    // what Data.db position to return when its position is EOF. We need to pass the file size
    // from the above.
    uint64_t _total_file_size;
    // Before any operation, we have to initialize the two cursors.
    // It can't be done in constructor, since their init() is async.
    // Our choice is to have an explicit init(), or a lazy init().
    // We do the latter, by calling `maybe_init()` on every operation.
    bool _initialized = false;

    // Helper which reads a row index header from the given position in Rows.db.
    future<row_index_header> read_row_index_header(uint64_t);
    // Helper which translates a ring_position_view to a BTI byte-comparable form.
    std::vector<std::byte> translate_key(dht::ring_position_view key);
    // Initializes the reader, if it isn't initialized yet.
    // We must call it before doing anything interesting with the cursors.
    // 
    // FIXME: our semantics are different from the semantics of the old (BIG) index reader.
    //
    // This new index reader is *unset* after creation, and initially doesn't point at anything.
    // The old index reader points at the start of the sstable immediately after creation.
    //
    // So in our case, some operations (like advance_to_next_partition()) can only be legal
    // after the cursor is explicitly set to something, while in the old reader's case
    // they were always legal.
    //
    // In particular, we arbitrarily decide that the only legal operations for the new index reader
    // immediately after creation are advance_to(partition_range) and advance_lower_and_check_if_present().
    // That's why we only call maybe_init() from those functions -- other calls are assumed to be used
    // only after the index reader was initialized via one of those 2 chosen functions.
    //
    // In practice, the MX reader always starts by setting the index reader with advance_to(partition_range)
    // (for multi-partition reads), or with advance_lower_and_check_if_present().
    // But it's not documented that it has to stay this way, and it's dangerous to rely on that.
    // We should harden the API, via better docs and/or asserts and/or changing the semantics of this index
    // reader to match the old index reader.
    future<> maybe_init();
public:
    trie_index_reader(bti_trie_source in, bti_trie_source in_row, uint64_t root_pos, uint64_t total_file_size, schema_ptr s, reader_permit);
    // No-op.
    virtual future<> close() noexcept override;
    // Returns the Data.db positions of lower and upper cursor.
    virtual sstables::data_file_positions_range data_file_positions() const override;
    // FIXME: implement.
    virtual future<std::optional<uint64_t>> last_block_offset() override;
    // Sets the lower bound before <key>, and sets the upper bound after <key, pos>.
    virtual future<bool> advance_lower_and_check_if_present(dht::ring_position_view key, std::optional<position_in_partition_view> pos = {}) override;
    virtual future<> advance_to_next_partition() override;
    // Checks whether the index is pointing to a partition or to a clustering key block within a partition. 
    virtual sstables::indexable_element element_kind() const override;
    virtual future<> advance_to(dht::ring_position_view pos) override;
    virtual future<> advance_to(position_in_partition_view pos) override;
    virtual std::optional<sstables::deletion_time> partition_tombstone() override;
    virtual std::optional<partition_key> get_partition_key() override;
    virtual partition_key get_partition_key_prefix() override;
    virtual bool partition_data_ready() const override;
    virtual future<> read_partition_data() override;
    virtual future<> advance_reverse(position_in_partition_view pos) override;
    virtual future<> advance_to(const dht::partition_range& range) override;
    virtual future<> advance_reverse_to_next_partition() override;
    virtual std::optional<sstables::open_rt_marker> end_open_marker() const override;
    virtual std::optional<sstables::open_rt_marker> reverse_end_open_marker() const override;
    virtual sstables::clustered_index_cursor* current_clustered_cursor() override;
    virtual uint64_t get_data_file_position() override;
    virtual uint64_t get_promoted_index_size() override;
    virtual bool eof() const override;
};

// We want to be always inlined so that bits_per_pointer is substituted with a constant,
// and this compiles to a simple load, not to a full-fledged memcpy.
[[gnu::always_inline]]
static uint64_t read_offset(const_bytes sp, int idx, int bits_per_pointer) {
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
        // uint64_t off = 0;
        // for (int i = 0; i < div_ceil(bpp, 8); ++i) {
        //     off = off << 8 | uint64_t(sp[(idx * bpp) / 8 + i]);
        // }
        // off >>= (8 - (bpp * (idx + 1) % 8)) % 8;
        // off &= uint64_t(-1) >> (64 - bpp);
        // auto z = (seastar::read_be<uint32_t>((const char*)sp.data() + 3 * (idx / 2)) >> (20 - 12 * (idx % 2))) & 0xfff;
        // SCYLLA_ASSERT(z == off);
        // return z;
    }
}

// We want to be always inlined so that bits_per_pointer is substituted with a constant,
// and the read_offset can be simplified.
[[gnu::always_inline]]
static lookup_result lookup_child_sparse(int type, const_bytes raw, std::byte transition) {
    auto bpp = bits_per_pointer_arr[type];
    auto n_children = uint8_t(raw[1]);
    auto idx = std::lower_bound(&raw[2], &raw[2 + n_children], transition) - &raw[2];
    if (idx < n_children) {
        return {idx, raw[2 + idx], read_offset(raw.subspan(2+n_children), idx, bpp)};
    } else {
        return {idx, std::byte(0), 0};
    }
}

// We want to be always inlined so that bits_per_pointer is substituted with a constant,
// and the read_offset can be simplified.
[[gnu::always_inline]]
static lookup_result lookup_child_dense(int type, const_bytes raw, std::byte transition) {
    auto start = int(raw[1]);
    auto idx = std::max<int>(0, int(transition) - start);
    auto dense_span = uint64_t(raw[2]) + 1;
    auto bpp = bits_per_pointer_arr[type];
    while (idx < int(dense_span)) {
        if (auto off = read_offset(raw.subspan(3), idx, bpp)) {
            return {idx, std::byte(start + idx), off};
        } else {
            ++idx;
        }
    }
    return {dense_span, std::byte(0), 0};
}

// Looks up the first child of this node greater with transition greater or equal to the given one.
// If such child doesn't exist, the `idx` of the result will be negative.
static lookup_result lookup_child(const_bytes raw, std::byte transition) {
    auto type = uint8_t(raw[0]) >> 4;
    switch (type) {
    case PAYLOAD_ONLY:
        abort();
    case SINGLE_NOPAYLOAD_4:
        if (transition <= raw[1]) {
            return {0, raw[1], uint64_t(raw[0]) & 0xf};
        }
        return {1, std::byte(0), 0};
    case SINGLE_8:
        if (transition <= raw[1]) {
            return {0, raw[1], uint64_t(raw[2])};
        }
        return {1, std::byte(0), 0};
    case SINGLE_NOPAYLOAD_12:
        if (transition <= raw[2]) {
            return {0, raw[2], (uint64_t(raw[0]) & 0xf) << 8 | uint64_t(raw[1])};
        }
        return {1, std::byte(0), 0};
    case SINGLE_16:
        if (transition <= raw[1]) {
            return {0, raw[1], uint64_t(raw[1]) << 8 | uint64_t(raw[2])};
        }
        return {1, std::byte(0), 0};
    case SPARSE_8:
        return lookup_child_sparse(type, raw, transition);
    case SPARSE_12:
        return lookup_child_sparse(type, raw, transition);
    case SPARSE_16:
        return lookup_child_sparse(type, raw, transition);
    case SPARSE_24:
        return lookup_child_sparse(type, raw, transition);
    case SPARSE_40:
        return lookup_child_sparse(type, raw, transition);
    case DENSE_12:
        return lookup_child_dense(type, raw, transition);
    case DENSE_16:
        return lookup_child_dense(type, raw, transition);
    case DENSE_24:
        return lookup_child_dense(type, raw, transition);
    case DENSE_32:
        return lookup_child_dense(type, raw, transition);
    case DENSE_40:
        return lookup_child_dense(type, raw, transition);
    case LONG_DENSE:
        return lookup_child_dense(type, raw, transition);
    default: abort();
    }
}

// We want to be always inlined so that bits_per_pointer is substituted with a constant,
// and the read_offset can be simplified.
[[gnu::always_inline]]
static lookup_result get_child_sparse(int type, const_bytes raw, int idx) {
    auto bpp = bits_per_pointer_arr[type];
    auto n_children = uint8_t(raw[1]);
    SCYLLA_ASSERT(idx < n_children);
    return {idx, raw[2 + idx], read_offset(raw.subspan(2+n_children), idx, bpp)};
}

// We want to be always inlined so that bits_per_pointer is substituted with a constant,
// and the read_offset can be simplified.
[[gnu::always_inline]]
static lookup_result get_child_dense(int type, const_bytes raw, int idx, bool forward) {
        auto dense_span = uint64_t(raw[2]) + 1;
        auto bpp = bits_per_pointer_arr[type];
        SCYLLA_ASSERT(idx < int(dense_span));
        while (idx < int(dense_span) && idx >= 0) {
            if (auto off = read_offset(raw.subspan(3), idx, bpp)) {
                auto transition = std::byte(uint8_t(raw[1]) + idx);
                return {idx, transition, off};
            } else {
                idx += forward ? 1 : -1;
            }
        }
        return {dense_span, std::byte(0), 0};
}

// Looks up the child with the given index.
// If there is no child with such an index (can happen in DENSE nodes, which have empty slots),
// picks the closest child with idx greater (if `forward == true`) or smaller (if `forward == false`)
// than the given. If there is no such child, the `idx` of the result will be negative.
static lookup_result get_child(const_bytes raw, int idx, bool forward) {
    auto type = uint8_t(raw[0]) >> 4;
    switch (type) {
    case PAYLOAD_ONLY:
        abort();
    case SINGLE_NOPAYLOAD_4:
        SCYLLA_ASSERT(idx == 0);
        return {idx, raw[1], uint64_t(raw[0]) & 0xf};
    case SINGLE_8:
        SCYLLA_ASSERT(idx == 0);
        return {idx, raw[1], uint64_t(raw[2])};
    case SINGLE_NOPAYLOAD_12:
        SCYLLA_ASSERT(idx == 0);
        return {idx, raw[2], (uint64_t(raw[0]) & 0xf) << 8 | uint64_t(raw[1])};
    case SINGLE_16:
        SCYLLA_ASSERT(idx == 0);
        return {idx, raw[1], uint64_t(raw[2]) << 8 | uint64_t(raw[3])};
    // We copy-paste the code so that each case is separately inlined and simplified.
    // TODO: verify that the compiler does what we expect.
    case SPARSE_8:
        return get_child_sparse(type, raw, idx);
    case SPARSE_12:
        return get_child_sparse(type, raw, idx);
    case SPARSE_16:
        return get_child_sparse(type, raw, idx);
    case SPARSE_24:
        return get_child_sparse(type, raw, idx);
    case SPARSE_40:
        return get_child_sparse(type, raw, idx);
    case DENSE_12:
        return get_child_dense(type, raw, idx, forward);
    case DENSE_16:
        return get_child_dense(type, raw, idx, forward);
    case DENSE_24:
        return get_child_dense(type, raw, idx, forward);
    case DENSE_32:
        return get_child_dense(type, raw, idx, forward);
    case DENSE_40:
        return get_child_dense(type, raw, idx, forward);
    case LONG_DENSE:
        return get_child_dense(type, raw, idx, forward);
    default: abort();
    }
}

const_bytes reader_node::raw(const page_ptr& ptr) const {
    return ptr->get_view().subspan(pos % cached_file::page_size);
}

payload_result reader_node::payload(const page_ptr& ptr) const {
    auto sp = raw(ptr);
    auto type = uint8_t(sp[0]) >> 4;
    uint64_t payload_offset;
    switch (type) {
    case PAYLOAD_ONLY:
        payload_offset = 1;
        break;
    case SINGLE_NOPAYLOAD_4:
    case SINGLE_NOPAYLOAD_12:
        payload_offset = 1 + div_ceil(bits_per_pointer_arr[type], 8);
        break;
    case SINGLE_8:
    case SINGLE_16:
        payload_offset = 2 + div_ceil(bits_per_pointer_arr[type], 8);
        break;
    case SPARSE_8:
    case SPARSE_12:
    case SPARSE_16:
    case SPARSE_24:
    case SPARSE_40: {
        payload_offset = 2 + div_ceil(n_children * (8 + bits_per_pointer_arr[type]), 8);
        break;
    }
    case DENSE_12:
    case DENSE_16:
    case DENSE_24:
    case DENSE_32:
    case DENSE_40:
    case LONG_DENSE: {
        auto dense_span = uint8_t(sp[2]) + 1;
        payload_offset = 3 + div_ceil(dense_span * bits_per_pointer_arr[type], 8);
        break;
    }
    default: abort();
    }
    auto tail = sp.subspan(payload_offset);
    return {payload_bits, tail};
}

lookup_result reader_node::lookup(std::byte transition, const page_ptr& ptr) {
    return ::lookup_child(raw(ptr), transition);
}

lookup_result reader_node::get_child(int idx, bool forward, const page_ptr& ptr) {
    return ::get_child(raw(ptr), idx, forward);
}

template <trie_reader_source Input>
trie_cursor<Input>::trie_cursor(Input& in)
    : _in(in)
{
    check_invariants();
}

template <trie_reader_source Input>
trie_cursor<Input>::~trie_cursor()
{
    check_invariants();
}

template <trie_reader_source Input>
bool trie_cursor<Input>::points_at_eof() const {
    return is_initialized() && size_t(_path.begin()->child_idx) == _path.begin()->node.n_children;
}

template <trie_reader_source Input>
bool trie_cursor<Input>::points_at_node() const {
    return is_initialized() && !points_at_eof() && _path.back().child_idx == -1;
}

template <trie_reader_source Input>
bool trie_cursor<Input>::points_at_payloaded_node() const {
    return points_at_node() && _path.back().node.payload_bits;
}

template <trie_reader_source Input>
bool trie_cursor<Input>::is_initialized() const {
    return !_path.empty();
}

// Documented near the declaration.
template <trie_reader_source Input>
void trie_cursor<Input>::check_invariants() const {
    if constexpr (!developer_build) {
        return;
    }

    {
        std::vector<uint64_t> pages_idxs;
        for (const auto& p : _pages) {
            pages_idxs.push_back(p->pos());
        }
        // Pages should be in descending order, since children always lie in the file before their parents.
        SCYLLA_ASSERT(std::is_sorted(pages_idxs.rbegin(), pages_idxs.rend()));
        std::vector<uint64_t> path_idxs;
        for (const auto& p : _path) {
            path_idxs.push_back(p.node.pos / cached_file::page_size);
        }
        auto [a, b] = std::ranges::unique(path_idxs);
        path_idxs.erase(a, b);
        SCYLLA_ASSERT(std::ranges::equal(pages_idxs, path_idxs));
    }

    for (size_t i = 0; i + 1 < _path.size(); ++i) {
        SCYLLA_ASSERT(_path[i].child_idx >= 0 && _path[i].child_idx < _path[i].node.n_children);
    }

    bool is_initialized = !_path.empty();
    if (is_initialized) {
        bool is_eof = _path.front().child_idx == _path.front().node.n_children;
        if (is_eof) {
            SCYLLA_ASSERT(_path.size() == 1);
        } else {
            SCYLLA_ASSERT(_path.back().child_idx >= -1 && _path.back().child_idx < _path.back().node.n_children);
        }
    }
}

// Documented near the declaration. 
template <trie_reader_source Input>
[[gnu::noinline]]
future<> trie_cursor<Input>::push_page_and_node(uint64_t root_pos) {
    return _in.get().read_page(root_pos).then([this, root_pos] (auto v) {
        _pages.emplace_back(std::move(v));
        _path.push_back({pv_to_reader_node(root_pos, _pages.back()), -1});
        assert(_path.back().node.pos / cached_file::page_size == _pages.back()->pos() / cached_file::page_size);
    });
}

// Documented near the declaration.
template <trie_reader_source Input>
[[gnu::always_inline]]
future<> trie_cursor<Input>::push_maybe_page_and_node(uint64_t pos) {
    assert(_path.back().node.pos / cached_file::page_size == _pages.back()->pos() / cached_file::page_size);
    if (try_push(pos)) {
        assert(_path.back().node.pos / cached_file::page_size == _pages.back()->pos() / cached_file::page_size);
        return make_ready_future<>();
    } else {
        return push_page_and_node(pos);
    }
}

// Documented near the declaration.
template <trie_reader_source Input>
future<void> trie_cursor<Input>::init(uint64_t root_pos) {
    check_invariants();
    reset();
    return push_page_and_node(root_pos);
}

// Documented near the declaration.
template <trie_reader_source Input>
bool trie_cursor<Input>::try_push(uint64_t pos) {
    assert(_path.back().node.pos / cached_file::page_size == _pages.back()->pos() / cached_file::page_size);
    if (_path.back().node.pos / cached_file::page_size == pos / cached_file::page_size) {
        _path.push_back({pv_to_reader_node(pos, _pages.back()), -1});
        return true;
    }
    return false;
}

// Documented near the declaration.
template <trie_reader_source Input>
void trie_cursor<Input>::pop() {
    if (_path.size() > 1) {
        if (_path[_path.size() - 1].node.pos / cached_file::page_size != _path[_path.size() - 2].node.pos / cached_file::page_size) {
            _pages.pop_back();
        }
    }
    _path.pop_back();
    assert(_path.back().node.pos / cached_file::page_size == _pages.back()->pos() / cached_file::page_size);
}

// Documented near the declaration.
template <trie_reader_source Input>
future<set_result> trie_cursor<Input>::set_before(const_bytes key) {
    check_invariants();
    SCYLLA_ASSERT(initialized());
    auto post_check = defer([this] {
        check_invariants();
        SCYLLA_ASSERT(points_at_eof() || points_at_payloaded_node());
    });
    LOG_TRACE("set_before, root_pos={}, key={}", _path[0].node.pos, format_byte_span(key));
    SCYLLA_ASSERT(_path.back().child_idx == -1 || eof());
    // Reset the cursor back to a freshly-initialized, unset state, with only the root in _path.
    // 
    // FIXME: as an optmization, maybe it makes sense to clear
    // the tail of _path only up to the mismatch point of `key` and `_path`?
    // But it doesn't seem worthwile, since AFAIK forwarding to something
    // different than the next partition isn't a hot path in practice.
    while (_path.size() > 1) {
        pop();
    }
    _path.back().child_idx = -1;

    size_t i = 0;
    while (i < key.size()) {
        if (!_path.back().node.n_children) {
            break;
        }
        lookup_result it;
        // FIXME: consider a special case optimization for long chains of SINGLE_NOPAYLOAD_4.
        // These arise in practice when there are groups of keys with a long common prefix.
        // 
        // For such workloads, traversing the trie node-by-node can be extremely expensive.
        // As an example special optimization, the code below can traverse 16 of these at once.
        //
        // const uint8_t* __restrict__ p = reinterpret_cast<const uint8_t* __restrict__>(_pages.back()->get_view().data() + _path.back().node.pos % cached_file::page_size);
        // if (*p == (SINGLE_NOPAYLOAD_4 << 4 | 2) & *(p + 1) == uint8_t(key[i])) {
        //     const uint8_t* start = p; 
        //     const uint8_t* beg = p - _path.back().node.pos % cached_file::page_size;
        //     const size_t keysize = key.size();
        //     while (p - 32 >= beg && i+16 <= keysize - 1) {
        //         typedef unsigned char  vector32b  __attribute__((__vector_size__(32)));
        //         typedef unsigned char  vector16b  __attribute__((__vector_size__(16)));
        //         vector32b a = {};
        //         memcpy(&a, p - 32, 32);
        //         auto z = uint8_t(SINGLE_NOPAYLOAD_4 << 4 | 2);
        //         vector16b b = {};
        //         memcpy(&b, &key[i], 16);
        //         vector16b c = {z, z, z, z, z, z, z, z, z, z, z, z, z, z, z, z};
        //         vector32b d = __builtin_shufflevector(c, b, 0, 16, 1, 17, 2, 18, 3, 19, 4, 20, 5, 21, 6, 22, 7, 23, 8, 24, 9, 25, 10, 26, 11, 27, 12, 28, 13, 29, 14, 30, 15, 31);
        //         if (!__builtin_reduce_and(a == d)) {
        //             break;
        //         }
        //         p -= 32;
        //         i += 16;
        //     }
        //     _path.back() = node_cursor{reader_node{_path.back().node.pos - (start - p), 1, (*p)&0xf, std::byte(*(p+1))}, -1};
        // }
        //
        it = _path.back().node.lookup(key[i], _pages.back());
        assert(it.idx <= _path.back().node.n_children);
        _path.back().child_idx = it.idx;
        LOG_TRACE("set_before, lookup query: (pos={} key={:x} n_children={}), lookup result: (offset={}, transition={:x} idx={})",
            _path.back().node.pos, uint8_t(key[i]),_path.back().node.n_children, it.offset, it.byte, it.idx);
        if (size_t(_path.back().child_idx) == _path.back().node.n_children
            || it.byte != key[i]) {
            _path.back().child_idx -= 1;
            co_return co_await step();
        }
        co_await push_maybe_page_and_node(_path.back().node.pos - it.offset);
        i += 1;
    }
    if (_path.back().node.payload_bits) {
        // If we got here while descending,
        // then either we ran out of key bytes while on a payloaded inner node,
        // (so the current node is surely an exact match for the key)
        // or we arrived at a leaf before running out of key bytes
        // (in which case it's a possible much, but we don't know for sure, because
        // we don't can't confirm that the rest of the key matches).
        //
        // In either case the current node is a possible match, and this is the only possible
        // path where it's a match. 
        co_return set_result::possible_match;
    } else {
        // If we ended up on some unpayloaded node, we step forward to the closest payloaded node (or EOF).
        co_return co_await step();
    }
}

// Documented near the declaration.
template <trie_reader_source Input>
future<set_result> trie_cursor<Input>::set_after(const_bytes key) {
    check_invariants();
    SCYLLA_ASSERT(initialized());
    auto post_check = defer([this] {
        check_invariants();
        SCYLLA_ASSERT(points_at_eof() || points_at_payloaded_node());
    });
    auto res = co_await set_before(key);
    if (res == set_result::possible_match) {
        co_return co_await step();
    }
    co_return res;
}

// Documented near the declaration.
template <trie_reader_source Input>
future<set_result> trie_cursor<Input>::step() {
    check_invariants();
    SCYLLA_ASSERT(initialized());
    auto post_check = defer([this] {
        check_invariants();
        SCYLLA_ASSERT(points_at_eof() || points_at_payloaded_node());
    });
    SCYLLA_ASSERT(initialized() && !eof());

    // Ascend to the leafmost ancestor which isn't childless and isn't followed in _path by its rightmost child,
    // (or, if there is no such ancestor, ascend to the root)
    // and increment its child_idx by 1.
    _path.back().child_idx += 1;
    while (size_t(_path.back().child_idx) == _path.back().node.n_children) {
        if (_path.size() == 1) {
            // If we ascended to the root, we stop and return EOF, even though `child_idx == n_children`.
            // The root is the only node for which `child_idx == n_children` is a legal postcondition.
            // That's is how EOF is represented.
            co_return set_result::eof;
        }
        pop();
        _path.back().child_idx += 1;
        assert(_path.back().child_idx <= _path.back().node.n_children);
    }

    // Descend (starting at the child with index child_idx) along the leftmost path to the first payloaded node.
    // It is assumed that every leaf node is payloaded, so we are guaranteed to succeed.
    auto getc = _path.back().node.get_child(_path.back().child_idx, true, _pages.back());
    if (!try_push(_path.back().node.pos - getc.offset)) {
        co_await push_maybe_page_and_node(_path.back().node.pos - getc.offset);
    }
    while (!_path.back().node.payload_bits) {
        _path.back().child_idx += 1;
        SCYLLA_ASSERT(_path.back().child_idx < int(_path.back().node.n_children));
        getc = _path.back().node.get_child(_path.back().child_idx, true, _pages.back());
        if (!try_push(_path.back().node.pos - getc.offset)) {
            co_await push_maybe_page_and_node(_path.back().node.pos - getc.offset);
        }
    }
    co_return set_result::definitely_not_a_match;
}

// Documented near the declaration.
template <trie_reader_source Input>
future<> trie_cursor<Input>::step_back() {
    check_invariants();
    SCYLLA_ASSERT(points_at_eof() || points_at_payloaded_node());
    auto post_check = defer([this] {
        check_invariants();
        SCYLLA_ASSERT(points_at_eof() || points_at_payloaded_node());
    });
    // Note: this function is not supposed to have any side effects if there is no previous key,
    // so we first do a read-only walk, and we only modify _path after we are sure that a previous key exists.
    size_t i = _path.size() - 1;
    if (i > 0) {
        i -= 1;
    }
    // We look for the leafmost ancestor which has some child left of _path, or a payload.
    while (true) {
        SCYLLA_ASSERT(i == 0 || _path[i].child_idx >= 0);
        if (_path[i].child_idx > 0) {
            // Ancestor has a child left of _path. Our direct predecessor is the rightmost descendant of that child.
            _path.resize(i + 1);
            _path.back().child_idx -= 1;
            break;
        } else if (_path[i].child_idx == 0 && _path[i].node.payload_bits) {
            // Ancestor has no children left of _path, but it has a payload. So this ancestor itself is our direct predecessor.
            _path.resize(i + 1);
            _path.back().child_idx -= 1;
            co_return;
        } else if (i == 0) {
            // If we looked over all ancestors, and none fits the previous `if`s, then a previous key doesn't exist. (We are the first key).
            // Bail without modifying anything.
            co_return;
        }
        // Go up.
        SCYLLA_ASSERT(i > 0);
        i -= 1;
    }
    // Descend along the rightmost path.
    co_await push_maybe_page_and_node(_path.back().node.pos -_path.back().node.get_child(_path.back().child_idx, false, _pages.back()).offset);
    while (_path.back().node.n_children) {
        _path.back().child_idx = _path.back().node.n_children - 1;
        co_await push_maybe_page_and_node(_path.back().node.pos -_path.back().node.get_child(_path.back().child_idx, false, _pages.back()).offset);
    }
    SCYLLA_ASSERT(_path.back().child_idx == -1);
    SCYLLA_ASSERT(_path.back().node.payload_bits);
    co_return;
}

template <trie_reader_source Input>
payload_result trie_cursor<Input>::payload() const {
    check_invariants();
    SCYLLA_ASSERT(points_at_payloaded_node());
    return _path.back().node.payload(_pages.back());
}

template <trie_reader_source Input>
bool trie_cursor<Input>::eof() const {
    check_invariants();
    SCYLLA_ASSERT(initialized());
    return points_at_eof();
}

template <trie_reader_source Input>
bool trie_cursor<Input>::initialized() const {
    check_invariants();
    return is_initialized();
}

template <trie_reader_source Input>
void trie_cursor<Input>::reset() {
    check_invariants();
    _path.clear();
    _pages.clear();
    check_invariants();
}

template <trie_reader_source Input>
index_cursor<Input>::index_cursor(Input& par, Input& row, reader_permit permit)
    : _partition_cursor(par)
    , _row_cursor(row)
    , _in_row(row)
    , _permit(permit)
{}

template <trie_reader_source Input>
future<> index_cursor<Input>::init(uint64_t root_offset) {
    LOG_TRACE("index_cursor::init this={} root={}", fmt::ptr(this), root_offset);
    _row_cursor.reset();
    _partition_metadata.reset();
    return _partition_cursor.init(root_offset);
}


template <trie_reader_source Input>
bool index_cursor<Input>::row_cursor_set() const {
    return _row_cursor.initialized();
}

static int64_t payload_to_offset(const payload_result& p) {
    uint64_t bits = p.bits & 0x7;
    SCYLLA_ASSERT(p.bytes.size() >= bits + 1);
    uint64_t be_result = 0;
    std::memcpy(&be_result, p.bytes.data() + 1, bits);
    auto result = int64_t(seastar::be_to_cpu(be_result)) >> 8*(8 - bits);
    LOG_TRACE("payload_to_offset: be_result={:016x} bits={}, bytes={}, result={:016x}", be_result, bits, format_byte_span(p.bytes), uint64_t(result));
    return result;
}

template <trie_reader_source Input>
uint64_t index_cursor<Input>::data_file_pos(uint64_t file_size) const {
    // LOG_TRACE("index_cursor::data_file_pos this={}", fmt::ptr(this));
    SCYLLA_ASSERT(_partition_cursor.initialized());
    if (_partition_metadata) {
        if (!_row_cursor.initialized()) {
            LOG_TRACE("index_cursor::data_file_pos this={} from empty row cursor: {}", fmt::ptr(this), _partition_metadata->data_file_offset);
            return _partition_metadata->data_file_offset;
        }
        const auto p = _row_cursor.payload();
        uint64_t bits = p.bits & 0x7;
        SCYLLA_ASSERT(p.bytes.size() >= size_t(bits + 12));
        uint64_t be_result = 0;
        std::memcpy(&be_result, p.bytes.data(), bits);
        auto result = seastar::be_to_cpu(be_result) >> 8*(8 - bits);
        auto res = _partition_metadata->data_file_offset + result;
        LOG_TRACE("index_cursor::data_file_pos this={} from row cursor: {} bytes={} bits={}", fmt::ptr(this), res, format_byte_span(p.bytes), bits);
        return res;
    }
    if (!_partition_cursor.eof()) {
        auto res = payload_to_offset(_partition_cursor.payload());
        SCYLLA_ASSERT(res >= 0);
        // LOG_TRACE("index_cursor::data_file_pos this={} from partition cursor: {}", fmt::ptr(this), res);
        return res;
    }
    // LOG_TRACE("index_cursor::data_file_pos this={} from eof: {}", fmt::ptr(this), file_size);
    return file_size;
}

template <trie_reader_source Input>
tombstone index_cursor<Input>::open_tombstone() const {
    LOG_TRACE("index_cursor::open_tombstone this={}", fmt::ptr(this));
    SCYLLA_ASSERT(_partition_cursor.initialized());
    SCYLLA_ASSERT(_partition_metadata);
    if (!_row_cursor.initialized() || _row_cursor.eof()) {
        auto res = tombstone();
        LOG_TRACE("index_cursor::open_tombstone this={} from eof: {}", fmt::ptr(this), tombstone());
        return res;
    } else {
        const auto p = _row_cursor.payload();
        if (p.bits >= 8) {
            uint64_t bits = p.bits & 0x7;
            SCYLLA_ASSERT(p.bytes.size() >= size_t(bits + 12));
            auto marked = seastar::be_to_cpu(read_unaligned<uint64_t>(p.bytes.data() + bits));
            auto deletion_time = seastar::be_to_cpu(read_unaligned<int32_t>(p.bytes.data() + bits + 8));
            return tombstone(marked, gc_clock::time_point(gc_clock::duration(deletion_time)));
        } else {
            return tombstone();
        }
    }
}

template <trie_reader_source Input>
const std::optional<row_index_header>& index_cursor<Input>::partition_metadata() const {
    return _partition_metadata;
}

template <trie_reader_source Input>
[[gnu::always_inline]]
future<> index_cursor<Input>::maybe_read_metadata() {
    LOG_TRACE("index_cursor::maybe_read_metadata this={}", fmt::ptr(this));
    if (_partition_cursor.eof()) {
        return make_ready_future<>();
    }
    if (auto res = payload_to_offset(_partition_cursor.payload()); res < 0) {
        return _in_row.get().read_row_index_header(-res, _permit).then([this] (auto result) {
            _partition_metadata = result;
        });
    }
    return make_ready_future<>();
}
template <trie_reader_source Input>
future<set_result> index_cursor<Input>::set_before_partition(const_bytes key) {
    LOG_TRACE("index_cursor::set_before_partition this={} key={}", fmt::ptr(this), format_byte_span(key));
    _row_cursor.reset();
    _partition_metadata.reset();
    return _partition_cursor.set_before(key).then([this] (auto res) -> future<set_result> {
        return maybe_read_metadata().then([res] { return res; });
    });
}
template <trie_reader_source Input>
future<set_result> index_cursor<Input>::set_after_partition(const_bytes key) {
    LOG_TRACE("index_cursor::set_after_partition this={} key={}", fmt::ptr(this), format_byte_span(key));
    _row_cursor.reset();
    _partition_metadata.reset();
    auto res = co_await _partition_cursor.set_after(key);
    co_await maybe_read_metadata();
    co_return res;
}
template <trie_reader_source Input>
future<set_result> index_cursor<Input>::next_partition() {
    LOG_TRACE("index_cursor::next_partition() this={}", fmt::ptr(this));
    _row_cursor.reset();
    _partition_metadata.reset();
    return _partition_cursor.step().then([this] (auto res) -> future<set_result> {
        return maybe_read_metadata().then([res] { return res; });
    });
}
template <trie_reader_source Input>
future<set_result> index_cursor<Input>::set_before_row(const_bytes key) {
    LOG_TRACE("index_cursor::set_before_row this={} key={}", fmt::ptr(this), format_byte_span(key));
    if (!_partition_metadata) {
        co_return set_result::possible_match;
    }
    co_await _row_cursor.init(_partition_metadata->trie_root);
    auto res = co_await _row_cursor.set_before(key);
    if (res != set_result::possible_match) {
        co_await _row_cursor.step_back();
        co_return set_result::definitely_not_a_match;
    }
    co_return res;
}

template <trie_reader_source Input>
future<set_result> index_cursor<Input>::set_after_row_cold(const_bytes key) {
    return _row_cursor.init(_partition_metadata->trie_root).then([this, key] () -> future<set_result> {
        return _row_cursor.set_after(key).then([this] (auto rowres) {
            if (rowres == set_result::eof) {
                return next_partition();
            }
            return make_ready_future<set_result>(rowres);
        });
    }
    );
}

template <trie_reader_source Input>
[[gnu::always_inline]]
future<set_result> index_cursor<Input>::set_after_row(const_bytes key) {
    LOG_TRACE("index_cursor::set_after_row this={} key={}", fmt::ptr(this), format_byte_span(key));
    if (!_partition_metadata) {
        return next_partition();
    }
    return set_after_row_cold(key);
}

future<row_index_header> trie_index_reader::read_row_index_header(uint64_t pos) {
    auto hdr = co_await _in_row._impl->read_row_index_header(pos, _permit);
    LOG_TRACE("trie_index_reader::read_row_index_header this={} pos={} result={}", fmt::ptr(this), pos, hdr.data_file_offset);
    co_return hdr;
}
std::vector<std::byte> trie_index_reader::translate_key(dht::ring_position_view key) {
    auto trie_key = std::vector<std::byte>();
    trie_key.reserve(256);
    trie_key.push_back(std::byte(0x40));
    auto token = key.token().is_maximum() ? std::numeric_limits<uint64_t>::max() : key.token().unbias();
    append_to_vector(trie_key, object_representation(seastar::cpu_to_be<uint64_t>(token)));
    if (auto k = key.key()) {
        trie_key.reserve(k->representation().size() + 64);
        _s->partition_key_type()->memcmp_comparable_form(*k, trie_key);
    }
    std::byte ending;
    if (key.weight() < 0) {
        ending = std::byte(0x20);
    } else if (key.weight() == 0) {
        ending = std::byte(0x38);
    } else {
        ending = std::byte(0x60);
    }
    trie_key.push_back(ending);
    LOG_TRACE("translate_key({}) = {}", key, format_byte_span(trie_key));
    return trie_key;
}
sstables::data_file_positions_range trie_index_reader::data_file_positions() const {
    auto lo = _lower.data_file_pos(_total_file_size);
    auto hi =_upper.data_file_pos(_total_file_size);
    trie_logger.debug("trie_index_reader::data_file_positions this={} result=({}, {})", fmt::ptr(this), lo, hi);
    return {lo, hi};
}
future<std::optional<uint64_t>> trie_index_reader::last_block_offset() {
    trie_logger.debug("trie_index_reader::last_block_offset this={}", fmt::ptr(this));
    return make_ready_future<std::optional<uint64_t>>(std::optional<uint64_t>());
}
future<> trie_index_reader::close() noexcept {
    trie_logger.debug("trie_index_reader::close this={}", fmt::ptr(this));
    return make_ready_future<>();
}
trie_index_reader::trie_index_reader(
    bti_trie_source in,
    bti_trie_source row_in,
    uint64_t root_offset,
    uint64_t total_file_size,
    schema_ptr s,
    reader_permit rp
)
    : _in_par(std::move(in))
    , _in_row(std::move(row_in))
    , _s(s)
    , _permit(std::move(rp))
    , _lower(*_in_par._impl, *_in_row._impl, _permit)
    , _upper(*_in_par._impl, *_in_row._impl, _permit)
    , _root(root_offset)
    , _total_file_size(total_file_size)
{
    trie_logger.debug("trie_index_reader::constructor: this={} root_offset={} total_file_size={} table={}.{}",
        fmt::ptr(this), root_offset, total_file_size, _s->ks_name(), _s->cf_name());
}
future<> trie_index_reader::maybe_init() {
    trie_logger.debug("trie_index_reader::constructor: this={} initialized={}", fmt::ptr(this), _initialized);
    if (!_initialized) {
        return when_all(_lower.init(_root)).then([this] (const auto&) { _upper = _lower; _initialized = true; });
    }
    return make_ready_future<>();
}
std::byte bound_weight_to_terminator(bound_weight b) {
    switch (b) {
        case bound_weight::after_all_prefixed: return std::byte(0x60);
        case bound_weight::before_all_prefixed: return std::byte(0x20);
        case bound_weight::equal: return std::byte(0x40);
    }
}
std::vector<std::byte> byte_comparable(const schema& s, position_in_partition_view pipv) {
    std::vector<std::byte> res;
    if (pipv.has_key()) {
        res.reserve(pipv.key().representation().size() + 64);
        s.clustering_key_type()->memcmp_comparable_form(pipv.key(), res);
    }
    res.push_back(bound_weight_to_terminator(pipv.get_bound_weight()));
    return res;
}
future<bool> trie_index_reader::advance_lower_and_check_if_present(dht::ring_position_view key, std::optional<position_in_partition_view> pos) {
    trie_logger.debug("trie_index_reader::advance_lower_and_check_if_present: this={} key={} pos={}", fmt::ptr(this), key, pos);
    co_await maybe_init();
    auto trie_key = translate_key(key);
    auto res = co_await _lower.set_before_partition(trie_key);
    _upper = _lower;
    if (res != set_result::possible_match) {
        co_return false;
    }
    if (!pos) {
        co_await _upper.next_partition();
    } else {
        co_await _upper.set_after_row(byte_comparable(*_s, *pos));
    }
    co_return true;
}
future<> trie_index_reader::advance_to_next_partition() {
    trie_logger.debug("trie_index_reader::advance_to_next_partition this={}", fmt::ptr(this));
    co_await _lower.next_partition();
}
sstables::indexable_element trie_index_reader::element_kind() const {
    trie_logger.debug("trie_index_reader::element_kind");
    return _lower.row_cursor_set() ? sstables::indexable_element::cell : sstables::indexable_element::partition;
}
future<> trie_index_reader::advance_to(dht::ring_position_view pos) {
    trie_logger.debug("trie_index_reader::advance_to(partition) this={} pos={}", fmt::ptr(this), pos);
    co_await _lower.set_before_partition(translate_key(pos));
}
future<> trie_index_reader::advance_to(position_in_partition_view pos) {
    trie_logger.debug("trie_index_reader::advance_to(row) this={} pos={}", fmt::ptr(this), pos);
    co_await _lower.set_before_row(byte_comparable(*_s, pos));
}
std::optional<sstables::deletion_time> trie_index_reader::partition_tombstone() {
    std::optional<sstables::deletion_time> res;
    if (const auto& hdr = _lower.partition_metadata()) {
        res = sstables::deletion_time{
            hdr->partition_tombstone.deletion_time.time_since_epoch().count(),
            hdr->partition_tombstone.timestamp};
        trie_logger.debug("trie_index_reader::partition_tombstone this={} res={}", fmt::ptr(this), hdr->partition_tombstone);
    } else {
        trie_logger.debug("trie_index_reader::partition_tombstone this={} res=none", fmt::ptr(this));
    }
    return res;
}
std::optional<partition_key> trie_index_reader::get_partition_key() {
    std::optional<partition_key> res;
    if (const auto& hdr = _lower.partition_metadata()) {
        res = hdr->partition_key.to_partition_key(*_s);
    }
    trie_logger.debug("trie_index_reader::get_partition_key this={} res={}", fmt::ptr(this), res);
    return res;
}
partition_key trie_index_reader::get_partition_key_prefix() {
    trie_logger.debug("trie_index_reader::get_partition_key_prefix this={}", fmt::ptr(this));
    abort();
}
bool trie_index_reader::partition_data_ready() const {
    trie_logger.debug("trie_index_reader::partition_data_ready this={}", fmt::ptr(this));
    return _lower.partition_metadata().has_value();
}
future<> trie_index_reader::advance_reverse(position_in_partition_view pos) {
    trie_logger.debug("trie_index_reader::advance_reverse this={} pos={}", fmt::ptr(this), pos);
    _upper = _lower;
    co_await _upper.set_after_row(byte_comparable(*_s, pos));
}
future<> trie_index_reader::read_partition_data() {
    trie_logger.debug("trie_index_reader::read_partition_data this={}", fmt::ptr(this));
    return make_ready_future<>();
}
future<> trie_index_reader::advance_to(const dht::partition_range& range) {
    trie_logger.debug("trie_index_reader::advance_to(range) this={} range={}", fmt::ptr(this), range);
    co_await maybe_init();
    if (const auto s = range.start()) {
        co_await _lower.set_before_partition(translate_key(s.value().value()));
    } else {
        co_await _lower.set_before_partition(const_bytes());
    }
    if (const auto e = range.end()) {
        auto k = translate_key(e.value().value());
        if (e->value().has_key()) {
            k.back() = std::byte(0x60);
        }
        co_await _upper.set_after_partition(k);
    } else {
        std::byte top[1] = {std::byte(0x60)};
        co_await _upper.set_after_partition(top);
    }
}
future<> trie_index_reader::advance_reverse_to_next_partition() {
    trie_logger.debug("trie_index_reader::advance_reverse_to_next_partition() this={}", fmt::ptr(this));
    _upper = _lower;
    return _upper.next_partition().discard_result();
}
std::optional<sstables::open_rt_marker> trie_index_reader::end_open_marker() const {
    trie_logger.debug("trie_index_reader::end_open_marker() this={}", fmt::ptr(this));
    std::optional<sstables::open_rt_marker> res;
    if (const auto& hdr = _lower.partition_metadata()) {
        res = sstables::open_rt_marker{.pos = {position_in_partition::after_static_row_tag_t()}, .tomb = _lower.open_tombstone()};
    }
    trie_logger.debug("trie_index_reader::end_open_marker this={} res={}", fmt::ptr(this), res ? res->tomb : tombstone());
    return res;
}
std::optional<sstables::open_rt_marker> trie_index_reader::reverse_end_open_marker() const {
    trie_logger.debug("trie_index_reader::reverse_end_open_marker() this={}", fmt::ptr(this));
    std::optional<sstables::open_rt_marker> res;
    if (const auto& hdr = _upper.partition_metadata()) {
        res = sstables::open_rt_marker{.pos = {position_in_partition::after_static_row_tag_t()}, .tomb = _upper.open_tombstone()};
    }
    trie_logger.debug("trie_index_reader::reverse_end_open_marker this={} res={}", fmt::ptr(this), res ? res->tomb : tombstone());
    return res;
}
sstables::clustered_index_cursor* trie_index_reader::current_clustered_cursor() {
    trie_logger.debug("trie_index_reader::current_clustered_cursor() this={}", fmt::ptr(this));
    abort();
}
uint64_t trie_index_reader::get_data_file_position() {
    return data_file_positions().start;
}
uint64_t trie_index_reader::get_promoted_index_size() {
    trie_logger.debug("trie_index_reader::get_promoted_index_size() this={}", fmt::ptr(this));
    return 0;
}
bool trie_index_reader::eof() const {
    trie_logger.debug("trie_index_reader::eof() this={}", fmt::ptr(this));
    return _lower.data_file_pos(_total_file_size) >= _total_file_size;
}

template <trie_writer_sink Output>
class row_index_trie_writer_impl {
public:
    row_index_trie_writer_impl(Output&);
    ~row_index_trie_writer_impl();
    void add(const_bytes first_ck, const_bytes last_ck, uint64_t data_file_pos, sstables::deletion_time);
    ssize_t finish();
    using buf = std::vector<std::byte>;

    struct row_index_payload {
        uint64_t data_file_pos;
        sstables::deletion_time dt;
    };

private:
    trie_writer<Output> _wr;
    size_t _added_blocks = 0;
    size_t _last_sep_mismatch = 0;
    size_t _first_key_mismatch = 0;
    buf _first_key;
    buf _last_key;
    row_index_payload _last_payload;
};

template <trie_writer_sink Output>
row_index_trie_writer_impl<Output>::row_index_trie_writer_impl(Output& out)
    : _wr(out)
{}
template <trie_writer_sink Output>
row_index_trie_writer_impl<Output>::~row_index_trie_writer_impl() {
}

template <trie_writer_sink Output>
void row_index_trie_writer_impl<Output>::add(
    const_bytes first_ck,
    const_bytes last_ck,
    uint64_t data_file_pos,
    sstables::deletion_time dt
) {
    LOG_TRACE("row_index_trie_writer::add() this={} first_ck={} last_ck={} data_file_pos={} dt={} _last_sep={}, _last_sep_mismatch={}, _first_key_mismatch={}",
        fmt::ptr(this),
        format_byte_span(first_ck),
        format_byte_span(last_ck),
        data_file_pos,
        dt,
        format_byte_span(_first_key),
        _last_sep_mismatch,
        _first_key_mismatch
    );
    if (_added_blocks > 0) {
        size_t separator_mismatch = std::ranges::mismatch(first_ck, _last_key).in2 - _last_key.begin();
        SCYLLA_ASSERT(separator_mismatch < first_ck.size());
        SCYLLA_ASSERT(separator_mismatch < _last_key.size());

        size_t mismatch = std::ranges::mismatch(first_ck, _first_key).in2 - _first_key.begin();
        // size_t needed_prefix = std::min(std::max(_last_sep_mismatch, mismatch) + 1, _last_separator.size());
        size_t needed_prefix = std::max(_first_key_mismatch, mismatch) + 1;
        SCYLLA_ASSERT(needed_prefix <= _first_key.size());
        auto tail = std::span(_first_key).subspan(_last_sep_mismatch, needed_prefix - _last_sep_mismatch);

        std::array<std::byte, 20> payload_bytes;
        auto payload_bits = div_ceil(std::bit_width<uint64_t>(_last_payload.data_file_pos), 8);
        std::byte* p = payload_bytes.data();
        uint64_t offset_be = seastar::cpu_to_be(_last_payload.data_file_pos);
        std::memcpy(p, (const char*)(&offset_be) + 8 - payload_bits, payload_bits);
        p += payload_bits;
        uint8_t has_tombstone_flag = 0;
        if (!dt.live()) {
            has_tombstone_flag = 0x8;
            p = write_unaligned(p, seastar::cpu_to_be(_last_payload.dt.marked_for_delete_at));
            p = write_unaligned(p, seastar::cpu_to_be(_last_payload.dt.local_deletion_time));
        }

        LOG_TRACE("row_index_trie_writer::add(): _wr.add({}, {}, {}, {}, {:016x})", _last_sep_mismatch, format_byte_span(tail), format_byte_span(payload_bytes), payload_bits, _last_payload.data_file_pos);
        _wr.add(_last_sep_mismatch, tail, payload(has_tombstone_flag | payload_bits, {payload_bytes.data(), p}));

        _first_key.assign(first_ck.begin(), first_ck.end());
        _first_key_mismatch = separator_mismatch;
        _last_sep_mismatch = mismatch;
    } else {
        _first_key.assign(first_ck.begin(), first_ck.end());
    }
    _added_blocks += 1;
    _last_key.assign(last_ck.begin(), last_ck.end());
    _last_payload = row_index_payload{.data_file_pos = data_file_pos, .dt = dt};
}

template <trie_writer_sink Output>
ssize_t row_index_trie_writer_impl<Output>::finish() {
    if (_added_blocks > 0) {
        size_t needed_prefix = std::min(_last_sep_mismatch + 1, _first_key.size());
        auto tail = std::span(_first_key).subspan(_last_sep_mismatch, needed_prefix - _last_sep_mismatch);

        std::array<std::byte, 20> payload_bytes;
        std::byte* p = payload_bytes.data();

        auto payload_bits = div_ceil(std::bit_width<uint64_t>(_last_payload.data_file_pos), 8);
        uint64_t offset_be = seastar::cpu_to_be(_last_payload.data_file_pos);
        std::memcpy(p, (const char*)(&offset_be) + 8 - payload_bits, payload_bits);

        p += payload_bits;
        p = write_unaligned(p, seastar::cpu_to_be(_last_payload.dt.marked_for_delete_at));
        p = write_unaligned(p, seastar::cpu_to_be(_last_payload.dt.local_deletion_time));

        LOG_TRACE("row_index_trie_writer::finish(): _wr.add({}, {}, {})", _last_sep_mismatch, format_byte_span(tail), format_byte_span(payload_bytes));
        _wr.add(_last_sep_mismatch, tail, payload(8 | payload_bits, {payload_bytes.data(), p}));
    }
    return _wr.finish();
}

class row_trie_writer::impl : public row_index_trie_writer_impl<bti_trie_sink::impl> {
    using row_index_trie_writer_impl::row_index_trie_writer_impl;
};

// Pimpl boilerplate
row_trie_writer::row_trie_writer() = default;
row_trie_writer::~row_trie_writer() = default;
row_trie_writer& row_trie_writer::operator=(row_trie_writer&&) = default;
row_trie_writer::row_trie_writer(bti_trie_sink& out)
    : _impl(std::make_unique<impl>(*out._impl)) {
}
void row_trie_writer::add(const_bytes first_ck, const_bytes last_ck, uint64_t data_file_pos, sstables::deletion_time dt) {
    return _impl->add(first_ck, last_ck, data_file_pos, dt);
}
ssize_t row_trie_writer::finish() {
    return _impl->finish();
}

seastar::logger cached_file_logger("cached_file");

bti_trie_sink make_bti_trie_sink(sstables::file_writer& w, size_t page_size) {
    return bti_trie_sink(std::make_unique<bti_trie_sink::impl>(w, page_size));
}

partition_trie_writer make_partition_trie_writer(bti_trie_sink& out) {
    return partition_trie_writer(out);
}

row_trie_writer make_row_trie_writer(bti_trie_sink& out) {
    return row_trie_writer(out);
}

void memcmp_comparable_form_inner(bytes_view linearized, std::vector<std::byte>& out, const data_type& type) {
    if (type == bytes_type) {
        for (size_t i = 0; i < linearized.size(); ++i) {
            if (linearized[i] != 0) {
                out.push_back(std::byte(linearized[i]));
            } else {
                out.push_back(std::byte(0));
                ++i;
                while (true) {
                    if (i == linearized.size()) {
                        out.push_back(std::byte(0xfe));
                        out.push_back(std::byte(0x0));
                        return;
                    } else if (linearized[i] == 0) {
                        out.push_back(std::byte(0xfe));
                        ++i;
                    } else {
                        out.push_back(std::byte(0xff));
                        --i;
                        break;
                    }
                }
            }
        }
        out.push_back(std::byte(0x0));
    } else if (type == ascii_type || type == utf8_type) {
        append_to_vector(out, const_bytes{reinterpret_cast<const std::byte*>(linearized.data()), linearized.size()});
        out.push_back(std::byte(0x0));
    } else if (type == long_type) {
        append_to_vector(out, const_bytes{reinterpret_cast<const std::byte*>(linearized.data()), linearized.size()});
    }
}

std::unique_ptr<sstables::index_reader> make_bti_index_reader(
    bti_trie_source in,
    bti_trie_source in_row,
    uint64_t root_offset,
    uint64_t total_file_size,
    schema_ptr s,
    reader_permit rp
) {
    return std::make_unique<trie_index_reader>(std::move(in), std::move(in_row), root_offset, total_file_size, std::move(s), std::move(rp));
}