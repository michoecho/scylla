#pragma clang optimize off
#include "trie.hh"
#include <algorithm>
#include <cassert>
#include <set>
#include <capnp/message.h>
#include <capnp/serialize-packed.h>
#include "node.capnp.h"
#include "seastar/util/closeable.hh"

static seastar::logger trie_logger("trie");

inline constexpr size_t round_down(size_t a, size_t factor) {
    return a - a % factor;
}
inline constexpr size_t round_up(size_t a, size_t factor) {
    return round_down(a + factor - 1, factor);
}

static constexpr bool implies(bool a, bool b) {
    return !a || b;
}

fmt_hex fmt_hex_cb(const_bytes cb) {
    return {{reinterpret_cast<const bytes::value_type*>(cb.data()), cb.size()}};
}

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
};

constexpr const uint8_t bits_per_pointer_arr[] = {
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

fmt_hex fmt_hex_cb(const_bytes cb);

size_t max_offset_from_child(const node& x, size_t pos) {
    size_t result = 0;
    size_t offset = 0;
    for (auto it = x._children.rbegin(); it != x._children.rend(); ++it) {
        offset += it->get()->_node_size;
        if (it->get()->_output_pos >= 0) {
            assert(size_t(it->get()->_output_pos) < pos);
            result = std::max<size_t>(result, pos - it->get()->_output_pos);
        } else {
            result = std::max<size_t>(result, offset);
        }
        offset += it->get()->_branch_size;
    }
    trie_logger.trace("max_offset_from_child: node={}, pos={}, result={}", fmt::ptr(&x), pos, result);
    return result;
}

node_type choose_node_type_impl(const node& x, size_t pos) {
    const auto n_children = x._children.size();
    if (n_children == 0) {
        return PAYLOAD_ONLY;
    }
    auto max_offset = max_offset_from_child(x, pos);
    constexpr int widths[] = {4, 8, 12, 16, 24, 32, 40, 64};
    constexpr node_type singles[] = {SINGLE_NOPAYLOAD_4, SINGLE_8, SINGLE_NOPAYLOAD_12, SINGLE_16, DENSE_24, DENSE_32, DENSE_40, LONG_DENSE};
    constexpr node_type sparses[] = {SPARSE_8, SPARSE_8, SPARSE_12, SPARSE_16, SPARSE_24, SPARSE_40, SPARSE_40, LONG_DENSE};
    constexpr node_type denses[] = {DENSE_12, DENSE_12, DENSE_12, DENSE_16, DENSE_24, DENSE_32, DENSE_40, LONG_DENSE};
    auto width_idx = std::ranges::lower_bound(widths, std::bit_width(max_offset)) - widths;
    if (n_children == 1) {
        const auto has_payload = x._payload._payload_bits;
        if (has_payload && (width_idx == 0 || width_idx == 2)) {
            return singles[width_idx + 1];
        }
        return singles[width_idx];
    }
    const size_t dense_span = 1 + size_t(x._children.back()->_transition) - size_t(x._children.front()->_transition);
    auto sparse_size = 16 + div_ceil((bits_per_pointer_arr[sparses[width_idx]] + 8) * n_children, 8);
    auto dense_size = 24 + div_ceil(bits_per_pointer_arr[denses[width_idx]] * dense_span, 8);
    if (sparse_size < dense_size) {
        return sparses[width_idx];
    } else {
        return denses[width_idx];
    }
}

node_type choose_node_type(const node& x, size_t pos) {
    auto res = choose_node_type_impl(x, pos);
    trie_logger.trace("choose_node_type: node={}, pos={}, result={}", fmt::ptr(&x), pos, int(res));
    return res;
}

class trie_serializer final : public trie_writer_output {
    sstables::file_writer& _w;
    size_t _page_size;
    constexpr static size_t max_page_size = 64 * 1024;
public:
    trie_serializer(sstables::file_writer& w, size_t page_size) : _w(w), _page_size(page_size) {
        assert(_page_size <= max_page_size);
    }
    void write_int(uint64_t x, size_t bytes) {
        uint64_t be = cpu_to_be(x);
        _w.write(reinterpret_cast<const char*>(&be) + 8 - bytes, bytes);
    }
    void write_bytes(const_bytes x) {
        _w.write(reinterpret_cast<const char*>(x.data()), x.size());
    }
    size_t write_sparse(const node& x, node_type type, int bytes_per_pointer, size_t pos) {
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
    size_t size_sparse(const node& x, int bits_per_pointer) const {
        return 2 + div_ceil(x._children.size() * (8+bits_per_pointer), 8) + x._payload.blob().size();
    }
    size_t write_dense(const node& x, node_type type, int bytes_per_pointer, size_t pos) {
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
    size_t size_dense(const node& x, int bits_per_pointer) const  {
        return 3 + div_ceil(bits_per_pointer * (1 + int(x._children.back()->_transition) - int(x._children.front()->_transition)), 8) + x._payload.blob().size();
    }
    virtual size_t write(const node& x) override {
        auto s = serialized_size(x, x._output_pos);
        auto r = write_impl(x);
        assert(s == r);
        return r;
    }
    size_t write_impl(const node& x) {
        const auto my_pos = x._output_pos;
        auto type = choose_node_type(x, my_pos);
        switch (type) {
        case PAYLOAD_ONLY: {
            write_int(type << 4 | x._payload._payload_bits, 1);
            write_bytes(x._payload.blob());
            return 1 + x._payload.blob().size();
        }
        case SINGLE_NOPAYLOAD_4: {
            size_t offset = my_pos - x._children.front()->_output_pos;
            uint8_t transition = uint8_t(x._children.front()->_transition);
            write_int((type << 4) | offset, 1);
            write_int(transition, 1);
            return 2;
        }
        case SINGLE_8: {
            size_t offset = my_pos - x._children.front()->_output_pos;
            uint8_t transition = uint8_t(x._children.front()->_transition);
            write_int((type << 4) | x._payload._payload_bits, 1);
            write_int(transition, 1);
            write_int(offset, 1);
            write_bytes(x._payload.blob());
            return 3 + x._payload.blob().size();
        }
        case SINGLE_NOPAYLOAD_12: {
            size_t offset = my_pos - x._children.front()->_output_pos;
            uint8_t transition = uint8_t(x._children.front()->_transition);
            write_int((type << 4) | (offset >> 8), 1);
            write_int(offset & 0xff, 1);
            write_int(transition, 1);
            return 3;
        }
        case SINGLE_16: {
            size_t offset = my_pos - x._children.front()->_output_pos;
            uint8_t transition = uint8_t(x._children.front()->_transition);
            write_int((type << 4) | x._payload._payload_bits, 1);
            write_int(transition, 1);
            write_int(offset, 2);
            write_bytes(x._payload.blob());
            return 4 + x._payload.blob().size() + x._payload.blob().size();
        }
        case SPARSE_8: {
            return write_sparse(x, type, 1, my_pos);
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
            return 2 + div_ceil(x._children.size() * 20, 8) + x._payload.blob().size();
        }
        case SPARSE_16: {
            return write_sparse(x, type, 2, my_pos);
        }
        case SPARSE_24: {
            return write_sparse(x, type, 3, my_pos);
        }
        case SPARSE_40: {
            return write_sparse(x, type, 5, my_pos);
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
            return 3 + div_ceil((dense_span) * 12, 8) + x._payload.blob().size();
        }
        case DENSE_16: {
            return write_dense(x, type, 2, my_pos);
        }
        case DENSE_24: {
            return write_dense(x, type, 3, my_pos);
        }
        case DENSE_32: {
            return write_dense(x, type, 4, my_pos);
        }
        case DENSE_40: {
            return write_dense(x, type, 5, my_pos);
        }
        case LONG_DENSE: {
            return write_dense(x, type, 8, my_pos);
        }
        default: abort();
        }
    }
    virtual size_t serialized_size(const node& x, size_t start_pos) const override {
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
    virtual size_t page_size() const override {
        return _page_size;
    }
    virtual size_t bytes_left_in_page() override {
        return round_up(pos() + 1, page_size()) - pos();
    };
    virtual size_t pad_to_page_boundary() override {
        const static std::array<std::byte, max_page_size> zero_page = {};
        size_t pad = bytes_left_in_page();
        _w.write(reinterpret_cast<const char*>(zero_page.data()), pad);
        return pad;
    }
    virtual size_t pos() const override {
        return _w.offset();
    }
};

uint64_t read_offset(const_bytes sp, int idx, int bpp) {
    uint64_t off = 0;
    for (int i = 0; i < div_ceil(bpp, 8); ++i) {
        off = off << 8 | uint64_t(sp[(idx * bpp) / 8 + i]);
    }
    off >>= (8 - (bpp * (idx + 1) % 8)) % 8;
    off &= uint64_t(-1) >> (64 - bpp);
    return off;
}

node_parser my_parser {
    .lookup = [] (const_bytes sp, std::byte transition) -> node_parser::lookup_result {
        auto type = uint8_t(sp[0]) >> 4;
        switch (type) {
        case PAYLOAD_ONLY:
            abort();
        case SINGLE_NOPAYLOAD_4:
            if (transition <= sp[1]) {
                return {0, sp[1], uint64_t(sp[0]) & 0xf};
            }
            return {1, std::byte(0), 0};
        case SINGLE_8:
            if (transition <= sp[1]) {
                return {0, sp[1], uint64_t(sp[2])};
            }
            return {1, std::byte(0), 0};
        case SINGLE_NOPAYLOAD_12:
            if (transition <= sp[2]) {
                return {0, sp[2], (uint64_t(sp[0]) & 0xf) << 8 | uint64_t(sp[1])};
            }
            return {1, std::byte(0), 0};
        case SINGLE_16:
            if (transition <= sp[1]) {
                return {0, sp[1], uint64_t(sp[1]) << 8 | uint64_t(sp[2])};
            }
            return {1, std::byte(0), 0};
        case SPARSE_8:
        case SPARSE_12:
        case SPARSE_16:
        case SPARSE_24:
        case SPARSE_40: {
            auto bpp = bits_per_pointer_arr[type];
            auto n_children = uint8_t(sp[1]);
            auto idx = std::lower_bound(&sp[2], &sp[2 + n_children], transition) - &sp[2];
            if (idx < n_children) {
                return {idx, sp[2 + idx], read_offset(sp.subspan(2+n_children), idx, bpp)};
            } else {
                return {idx, std::byte(0), 0};
            }
        }
        case DENSE_12:
        case DENSE_16:
        case DENSE_24:
        case DENSE_32:
        case DENSE_40:
        case LONG_DENSE: {
            auto start = int(sp[1]);
            auto idx = std::max<int>(0, int(transition) - start);
            auto dense_span = uint64_t(sp[2]) + 1;
            auto bpp = bits_per_pointer_arr[type];
            while (idx < int(dense_span)) {
                if (auto off = read_offset(sp.subspan(3), idx, bpp)) {
                    return {idx, std::byte(start + idx), off};
                } else {
                    ++idx;
                }
            }
            return {dense_span, std::byte(0), 0};
        }
        default: abort();
        }
    },
    .get_child = [] (const_bytes sp, int idx, bool forward) -> node_parser::lookup_result {
        auto type = uint8_t(sp[0]) >> 4;
        switch (type) {
        case PAYLOAD_ONLY:
            abort();
        case SINGLE_NOPAYLOAD_4:
            assert(idx == 0);
            return {idx, sp[1], uint64_t(sp[0]) & 0xf};
        case SINGLE_8:
            assert(idx == 0);
            return {idx, sp[1], uint64_t(sp[2])};
        case SINGLE_NOPAYLOAD_12:
            assert(idx == 0);
            return {idx, sp[2], (uint64_t(sp[0]) & 0xf) << 8 | uint64_t(sp[1])};
        case SINGLE_16:
            assert(idx == 0);
            return {idx, sp[1], uint64_t(sp[2]) << 8 | uint64_t(sp[3])};
        case SPARSE_8:
        case SPARSE_12:
        case SPARSE_16:
        case SPARSE_24:
        case SPARSE_40: {
            auto bpp = bits_per_pointer_arr[type];
            auto n_children = uint8_t(sp[1]);
            assert(idx < n_children);
            return {idx, sp[2 + idx], read_offset(sp.subspan(2+n_children), idx, bpp)};
        }
        case DENSE_12:
        case DENSE_16:
        case DENSE_24:
        case DENSE_32:
        case DENSE_40:
        case LONG_DENSE: {
            auto dense_span = uint64_t(sp[2]) + 1;
            auto bpp = bits_per_pointer_arr[type];
            assert(idx < int(dense_span));
            while (idx < int(dense_span) && idx >= 0) {
                if (auto off = read_offset(sp.subspan(3), idx, bpp)) {
                    auto transition = std::byte(uint8_t(sp[1]) + idx);
                    return {idx, transition, off};
                } else {
                    idx += forward ? 1 : -1;
                }
            }
            return {dense_span, std::byte(0), 0};
        }
        default: abort();
        }
    },
};

payload::payload() noexcept {
}
payload::payload(uint8_t payload_bits, const_bytes payload) noexcept {
    assert(payload.size() <= _payload_buf.size());
    assert(payload_bits < 16);
    _payload_bits = payload_bits;
    _payload_size = payload.size();
    assert(bool(_payload_size) == bool(_payload_bits));
    std::ranges::copy(payload, _payload_buf.data());
}
const_bytes payload::blob() const noexcept {
    return {_payload_buf.data(), _payload_size};
}

node::node(std::byte b) noexcept
    : _transition(b) { }
node* node::add_child(std::byte b) {
    assert(_children.empty() || b > _children.back()->_transition);
    _children.push_back(std::make_unique<node>(b));
    return _children.back().get();
}
void node::set_payload(const payload& p) noexcept {
    assert(_output_pos < 0);
    _payload = p;
}
size_t node::recalc_sizes(const trie_writer_output& out, size_t global_pos) {
    trie_logger.trace("node::recalc_sizes(): this={}", fmt::ptr(this));
    struct local_state {
        node* _node;
        size_t _pos;
        int _stage;
    };
    std::vector<local_state> stack;
    stack.push_back({this, global_pos, 0});
    while (!stack.empty()) {
        auto& [node, pos, stage] = stack.back();
        if (stage < static_cast<int>(node->_children.size()) && node->_has_out_of_page_descendants) {
            stage += 1;
            stack.push_back({node->_children[stage - 1].get(), global_pos, 0});
            continue;
        }
        if (!node->_has_out_of_page_descendants) {
            global_pos += node->_branch_size;
        }
        node->_branch_size = global_pos - pos;
        if (node->_has_out_of_page_children || node->_has_out_of_page_descendants) {
            node->_node_size = out.serialized_size(*node, global_pos);
        }
        global_pos += node->_node_size;
        stack.pop_back();
    }
    return _branch_size + _node_size;
}
void node::write(trie_writer_output& out) {
    trie_logger.trace("node::write(): this={}", fmt::ptr(this));
    size_t starting_pos = out.pos();
    assert(_node_size > 0);
    assert(round_down(out.pos(), out.page_size()) == round_down(out.pos() + _branch_size + _node_size - 1, out.page_size()));
    assert(_output_pos < 0);
    struct local_state {
        node* _node;
        int _stage;
        int _startpos;
    };
    std::vector<local_state> stack;
    stack.push_back({this, 0, out.pos()});
    while (!stack.empty()) {
        auto& [node, stage, startpos] = stack.back();
        if (stage < static_cast<int>(node->_children.size())) {
            stage += 1;
            if (node->_children[stage - 1]->_output_pos < 0) {
                stack.push_back({node->_children[stage - 1].get(), 0, out.pos()});
            }
            continue;
        }
        node->_output_pos = out.pos();
        trie_logger.trace("node::write(): writing at {}, children={}", out.pos(), node->_children.size());
        assert(node->_payload._payload_bits || node->_children.size());
        auto sz = out.write(*node);
        if (!(static_cast<ssize_t>(sz) == node->_node_size)) {
            trie_logger.error("diff: {}, ns={}", sz, node->_node_size);
            abort();
        }
        assert(static_cast<ssize_t>(out.pos() - startpos) == node->_branch_size + node->_node_size);
        stack.pop_back();
    }
    if (!(static_cast<ssize_t>(out.pos() - starting_pos) == _branch_size + _node_size)) {
        trie_logger.error("diff: {}, bs={}, ns={}", out.pos() - starting_pos, _branch_size, _node_size);
    }
    assert(static_cast<ssize_t>(out.pos() - starting_pos) == _branch_size + _node_size);
    _children.clear();
    _branch_size = 0;
    _node_size = 0;
    _has_out_of_page_children = 0;
    _has_out_of_page_descendants = 0;
}

class trie_writer::impl {
public:
    impl(trie_writer_output&);
    void add(size_t depth, const_bytes key_tail, const payload&);
    ssize_t finish();

private:
    void complete(node* x);
    void write(node* x);
    void lay_out_children(node* x);
    size_t recalc_total_size(node* x, size_t start_pos) const noexcept;

private:
    std::vector<node*> _stack;
    std::unique_ptr<node> _root;
    trie_writer_output& _out;
};
trie_writer::impl::impl(trie_writer_output& out)
    : _out(out) {
    _root = std::make_unique<node>(std::byte(0));
    _stack.push_back(_root.get());
}
void trie_writer::impl::complete(node* x) {
    trie_logger.trace("complete: x={}", fmt::ptr(x));
    assert(x->_branch_size < 0);
    assert(x->_node_size < 0);
    assert(x->_has_out_of_page_children == false);
    assert(x->_has_out_of_page_descendants == false);
    assert(x->_output_pos < 0);
    bool has_out_of_page_children = false;
    bool has_out_of_page_descendants = false;
    size_t branch_size = 0;
    for (const auto& c : x->_children) {
        branch_size += c->_branch_size + c->_node_size;
        has_out_of_page_children |= c->_output_pos >= 0;
        has_out_of_page_descendants |= c->_has_out_of_page_descendants || c->_has_out_of_page_children;
    }
    size_t node_size = _out.serialized_size(*x, _out.pos() + branch_size);
    if (branch_size + node_size <= _out.page_size()) {
        x->_branch_size = branch_size;
        x->_node_size = node_size;
        x->_has_out_of_page_children = has_out_of_page_children;
        x->_has_out_of_page_descendants = has_out_of_page_descendants;
    } else {
        lay_out_children(x);
    }
}
void trie_writer::impl::lay_out_children(node* x) {
    trie_logger.trace("layout_children: x={}", fmt::ptr(x));
    assert(x->_output_pos < 0);
    auto cmp = [](node* a, node* b) { return std::make_pair(a->_branch_size + a->_node_size, a->_transition) < std::make_pair(b->_branch_size + b->_node_size, b->_transition); };
    auto unwritten_children = std::set<node*, decltype(cmp)>(cmp);
    for (const auto& c : x->_children) {
        if (c->_output_pos < 0) {
            unwritten_children.insert(c.get());
        }
    }
    while (unwritten_children.size()) {
        node* candidate;

        node selection_key(std::byte(255));
        selection_key._branch_size = _out.bytes_left_in_page();
        selection_key._node_size = 0;
        auto choice_it = unwritten_children.upper_bound(&selection_key);
        if (choice_it == unwritten_children.begin()) {
            _out.pad_to_page_boundary();
            assert(_out.bytes_left_in_page() == _out.page_size());
            choice_it = std::end(unwritten_children);
        }
        choice_it = std::prev(choice_it);
        candidate = *choice_it;
        unwritten_children.erase(choice_it);

        if (candidate->_has_out_of_page_children || candidate->_has_out_of_page_descendants) {
            size_t true_size = recalc_total_size(candidate, _out.pos());
            if (true_size > _out.bytes_left_in_page()) {
                if (true_size > _out.page_size()) {
                    lay_out_children(candidate);
                }
                unwritten_children.insert(candidate);
                continue;
            }
        }
        write(candidate);
    }
    x->_branch_size = 0;
    x->_has_out_of_page_children = true;
    x->_has_out_of_page_descendants = false;
    x->_node_size = _out.serialized_size(*x, _out.pos());
    if (!(x->_node_size <= static_cast<ssize_t>(_out.page_size()))) {
        trie_logger.error("_node_size: {}, page_size: {}", x->_node_size, _out.page_size());
        assert(x->_node_size <= static_cast<ssize_t>(_out.page_size()));
    }
}
size_t trie_writer::impl::recalc_total_size(node* branch, size_t global_pos) const noexcept {
    return branch->recalc_sizes(_out, global_pos);
}
void trie_writer::impl::write(node* branch) {
    branch->write(_out);
}
void trie_writer::impl::add(size_t depth, const_bytes key_tail, const payload& p) {
    assert(_stack.size() >= 1);
    assert(_stack.size() - 1 >= depth);
    assert(implies(key_tail.empty(), depth == 0));

    while (_stack.size() - 1 > depth) {
        complete(_stack.back());
        _stack.pop_back();
    }
    for (size_t i = 0; i < key_tail.size(); ++i) {
        _stack.push_back(_stack.back()->add_child(key_tail[i]));
    }
    _stack.back()->set_payload(p);

    assert(_stack.size() == 1 + depth + key_tail.size());
}
ssize_t trie_writer::impl::finish() {
    while (_stack.size()) {
        complete(_stack.back());
        _stack.pop_back();
    }
    ssize_t res = -1;
    if (_root->_children.size() || _root->_payload._payload_bits) {
        node superroot(std::byte(0));
        superroot._children.push_back(std::move(_root));
        lay_out_children(&superroot);
        res = superroot._children[0]->_output_pos;
    }
    _root = std::make_unique<node>(std::byte(0));
    _stack.push_back(_root.get());
    return res;
}

trie_writer::trie_writer(trie_writer_output& out)
    : _pimpl(std::make_unique<impl>(out)) { }
trie_writer::~trie_writer() { }
void trie_writer::add(size_t depth, const_bytes key_tail, const payload& p) { _pimpl->add(depth, key_tail, p); }
ssize_t trie_writer::finish() { return _pimpl->finish(); }

partition_index_trie_writer::partition_index_trie_writer(trie_writer_output& out)
    : _out(out) {
}

partition_index_trie_writer::~partition_index_trie_writer() {
}

void partition_index_trie_writer::add(const_bytes key, int64_t offset) {
    trie_logger.trace("partition_index_trie_writer::add({}, ...)", fmt_hex_cb(key));
    if (_added_keys > 0) {
        size_t mismatch = std::ranges::mismatch(key, _last_key).in2 - _last_key.begin();
        size_t needed_prefix = std::min(std::max(_last_key_mismatch, mismatch) + 1, _last_key.size());
        auto payload_bytes = std::bit_cast<std::array<std::byte, sizeof(_last_payload)>>(seastar::cpu_to_be(_last_payload));
        auto tail = std::span(_last_key).subspan(_last_key_mismatch, needed_prefix - _last_key_mismatch);
        _wr.add(_last_key_mismatch, tail, payload(1, payload_bytes));
        _last_key_mismatch = mismatch;
    }
    _added_keys += 1;
    _last_key.assign(key.begin(), key.end());
    _last_payload = offset;
}
ssize_t partition_index_trie_writer::finish() {
    if (_added_keys > 0) {
        size_t needed_prefix = std::min(_last_key_mismatch + 1, _last_key.size());
        auto payload_bytes = std::bit_cast<std::array<std::byte, sizeof(_last_payload)>>(seastar::cpu_to_be(_last_payload));
        auto tail = std::span(_last_key).subspan(_last_key_mismatch, needed_prefix - _last_key_mismatch);
        _wr.add(_last_key_mismatch, tail, payload(1, payload_bytes));
    }
    return _wr.finish();
}

payload_result reader_node::payload() const {
    auto p = raw_bytes.get_view();
    auto tail = p.subspan(payload_offset);
    trie_logger.trace("(reader_node::payload: bits={} offset={} result={})", payload_bits, payload_offset, fmt_hex_cb(tail.subspan(0, std::min<int>(20, tail.size()))));;
    return {payload_bits, raw_bytes.get_view().subspan(payload_offset)};
}

auto with_deser(const_bytes p, const std::invocable<const Node::Reader&, void*> auto& f) {
    capnp::word src[cached_file::page_size/sizeof(capnp::word)];
    assert(sizeof(src) >= p.size());
    memcpy(src, p.data(), p.size());
    ::capnp::FlatArrayMessageReader message({std::data(src), std::size(src)});
    Node::Reader node = message.getRoot<Node>();
    return f(node, src);
}

// constinit const static node_parser capnp_node_parser {
//     .lookup = [] (const_bytes p, std::byte transition) -> node_parser::lookup_result {
//         return with_deser(p, [transition] (const Node::Reader& node, void*) {
//             std::vector<std::byte> children;
//             for (const auto& x : node.getChildren()) {
//                 children.emplace_back(std::byte(x.getTransition()));
//             }
//             auto idx = std::ranges::lower_bound(children, transition) - children.begin();
//             return node_parser::lookup_result{
//                 .idx = idx,
//                 .byte = children[idx],
//                 .offset = node.getChildren()[idx].getOffset(),
//             };
//         });
//     },
//     .get_child = [] (const_bytes p, int idx) -> node_parser::lookup_result {
//         return with_deser(p, [idx] (const Node::Reader& node, void*) -> node_parser::lookup_result {
//             return node_parser::lookup_result{
//                 .idx = idx,
//                 .byte = std::byte(node.getChildren()[idx].getTransition()),
//                 .offset = node.getChildren()[idx].getOffset(),
//             };
//         });
//     },
// };

// reader_node::reader_node(cached_file::page_view pv) {
//     _raw_bytes = std::move(pv);
//     _parser = &capnp_node_parser;
//     with_deser(_raw_bytes.get_view(), [this] (const Node::Reader& node) {
//         _payload_bits = node.getPayload().getBits();
//         _payload_offset = reinterpret_cast<const std::byte*>(node.getPayload().getBytes().begin()) - _raw_bytes.get_view().data();
//         _n_children = node.getChildren().size();
//     });
// }

node_parser::lookup_result reader_node::lookup(std::byte transition) {
    return parser->lookup(raw_bytes.get_view(), transition);
}

node_parser::lookup_result reader_node::get_child(int idx, bool forward) {
    return parser->get_child(raw_bytes.get_view(), idx, forward);
}

trie_cursor::trie_cursor(trie_reader_input& in)
    : _in(in)
{
}

future<void> trie_cursor::init(uint64_t root_offset) {
    reset();
    _path.push_back({co_await _in.get().read(root_offset), -1});
}

future<set_result> trie_cursor::set_before(const_bytes key) {
    assert(initialized());
    assert(_path.back().child_idx == -1 || eof());
    _path.resize(1);
    _path.back().child_idx = -1;
    size_t i = 0;
    while (i < key.size()) {
        if (!_path[i].node.n_children) {
            break;
        }
        auto it = _path[i].node.lookup(key[i]);
        _path[i].child_idx = it.idx;
        trie_logger.trace("set_before, lookup: key={:x} children={} idx={}, offset={} pos={}", uint8_t(key[i]),_path[i].node.n_children, it.idx, it.offset, _path.back().node.pos);
        if (size_t(_path[i].child_idx) == _path[i].node.n_children
            || it.byte != key[i]) {
            _path[i].child_idx -= 1;
            co_return co_await step();
        }
        _path.push_back({co_await _in.get().read(_path.back().node.pos - it.offset), -1});
        i += 1;
    }
    if (_path.back().node.payload_bits) {
        co_return set_result::match;
    } else {
        co_return co_await step();
    }
}

future<set_result> trie_cursor::set_after(const_bytes key) {
    assert(initialized());
    auto res = co_await set_before(key);
    if (res == set_result::match) {
        co_return co_await step();
    }
    co_return res;
}

future<set_result> trie_cursor::step() {
    assert(initialized() && !eof());
    _path.back().child_idx += 1;
    while (size_t(_path.back().child_idx) == _path.back().node.n_children) {
        if (_path.size() == 1) {
            co_return set_result::eof;
        }
        _path.pop_back();
        _path.back().child_idx += 1;
    }
    _path.push_back({co_await _in.get().read(_path.back().node.pos - _path.back().node.get_child(_path.back().child_idx, true).offset), -1});
    while (!_path.back().node.payload_bits) {
        _path.back().child_idx += 1;
        assert(_path.back().child_idx < int(_path.back().node.n_children));
        _path.push_back({co_await _in.get().read(_path.back().node.pos - _path.back().node.get_child(_path.back().child_idx, true).offset), -1});
    }
    co_return set_result::no_match;
}

future<set_result> trie_cursor::step_back() {
    assert(initialized());
    assert(eof() || _path.back().child_idx == -1);
    size_t i = _path.size() - 1;
    if (i > 0) {
        i -= 1;
    }
    while (true) {
        assert(i == 0 || _path[i].child_idx >= 0);
        if (_path[i].child_idx > 0) {
            _path.resize(i + 1);
            _path.back().child_idx -= 1;
            break;
        } else if (_path[i].child_idx == 0 && _path[i].node.payload_bits) {
            _path.resize(i + 1);
            _path.back().child_idx -= 1;
            co_return set_result::match;
        } else if (i == 0) {
            co_return set_result::eof;
        } else {
            assert(i > 0);
            i -= 1;
            continue;
        }
    }
    _path.push_back({co_await _in.get().read(_path.back().node.pos -_path.back().node.get_child(_path.back().child_idx, false).offset), -1});
    while (_path.back().node.n_children) {
        _path.back().child_idx = _path.back().node.n_children - 1;
        _path.push_back({co_await _in.get().read(_path.back().node.pos - _path.back().node.get_child(_path.back().child_idx, false).offset), -1});
    }
    assert(_path.back().child_idx == -1);
    assert(_path.back().node.payload_bits);
    co_return set_result::match;
}

payload_result trie_cursor::payload() const {
    assert(initialized());
    assert(_path.back().child_idx == -1);
    return _path.back().node.payload();
}

bool trie_cursor::eof() const {
    assert(initialized());
    return size_t(_path.begin()->child_idx) == _path.begin()->node.n_children;
}

bool trie_cursor::initialized() const {
    return !_path.empty();
}

void trie_cursor::reset() {
    _path.clear();
}

index_cursor::index_cursor(trie_reader_input& par, trie_reader_input& row, reader_permit permit)
    : _partition_cursor(par)
    , _row_cursor(row)
    , _in_row(row)
    , _permit(permit)
{}

future<> index_cursor::init(uint64_t root_offset) {
    trie_logger.trace("index_cursor::init this={} root={}", fmt::ptr(this), root_offset);
    _row_cursor.reset();
    _partition_metadata.reset();
    return _partition_cursor.init(root_offset);
}


bool index_cursor::row_cursor_set() const {
    return _row_cursor.initialized();
}

uint64_t index_cursor::data_file_pos(uint64_t file_size) const {
    trie_logger.trace("index_cursor::data_file_pos this={}", fmt::ptr(this));
    assert(_partition_cursor.initialized());
    if (_partition_metadata) {
        if (!_row_cursor.initialized()) {
            trie_logger.trace("index_cursor::data_file_pos this={} from empty row cursor: {}", fmt::ptr(this), _partition_metadata->data_offset);
            return _partition_metadata->data_offset;
        }
        const auto p = _row_cursor.payload();
        assert(p.bytes.size() >= 8);
        auto res = _partition_metadata->data_offset + seastar::le_to_cpu(read_unaligned<uint64_t>(p.bytes.data()));
        trie_logger.trace("index_cursor::data_file_pos this={} from row cursor: {}", fmt::ptr(this), res);
        return res;
    }
    if (!_partition_cursor.eof()) {
        auto res = payload_to_offset(_partition_cursor.payload().bytes);
        assert(res >= 0);
        trie_logger.trace("index_cursor::data_file_pos this={} from partition cursor: {}", fmt::ptr(this), res);
        return res;
    }
    trie_logger.trace("index_cursor::data_file_pos this={} from eof: {}", fmt::ptr(this), file_size);
    return file_size;
}

tombstone index_cursor::open_tombstone() const {
    trie_logger.trace("index_cursor::open_tombstone this={}", fmt::ptr(this));
    assert(_partition_cursor.initialized());
    assert(_partition_metadata);
    if (!_row_cursor.initialized() || _row_cursor.eof()) {
        auto res = tombstone();
        trie_logger.trace("index_cursor::open_tombstone this={} from eof: {}", fmt::ptr(this), tombstone());
        return res;
    } else {
        const auto p = _row_cursor.payload();
        assert(p.bytes.size() >= 20);
        auto marked = seastar::le_to_cpu(read_unaligned<uint64_t>(p.bytes.data() + 8));
        auto deletion_time = seastar::le_to_cpu(read_unaligned<int32_t>(p.bytes.data() + 16));
        auto res = tombstone(marked, gc_clock::time_point(gc_clock::duration(deletion_time)));
        trie_logger.trace("index_cursor::open_tombstone this={} from payload: {}", fmt::ptr(this), res);
        return res;
    }
}

const std::optional<row_index_header>& index_cursor::partition_metadata() const {
    return _partition_metadata;
}
future<> index_cursor::maybe_read_metadata() {
    trie_logger.trace("index_cursor::maybe_read_metadata this={}", fmt::ptr(this));
    if (_partition_cursor.eof()) {
        co_return;
    }
    if (auto res = payload_to_offset(_partition_cursor.payload().bytes); res < 0) {
        _partition_metadata = co_await _in_row.get().read_row_index_header(-res, _permit);
    }
}
future<set_result> index_cursor::set_before_partition(const_bytes key) {
    trie_logger.trace("index_cursor::set_before_partition this={} key={}", fmt::ptr(this), fmt_hex_cb(key));
    _row_cursor.reset();
    _partition_metadata.reset();
    auto res = co_await _partition_cursor.set_before(key);
    co_await maybe_read_metadata();
    co_return res;
}
future<set_result> index_cursor::set_after_partition(const_bytes key) {
    trie_logger.trace("index_cursor::set_after_partition this={} key={}", fmt::ptr(this), fmt_hex_cb(key));
    _row_cursor.reset();
    _partition_metadata.reset();
    auto res = co_await _partition_cursor.set_after(key);
    co_await maybe_read_metadata();
    co_return res;
}
future<set_result> index_cursor::next_partition() {
    trie_logger.trace("index_cursor::next_partition() this={}", fmt::ptr(this));
    _row_cursor.reset();
    _partition_metadata.reset();
    auto res = co_await _partition_cursor.step();
    co_await maybe_read_metadata();
    co_return res;
}
future<set_result> index_cursor::set_before_row(const_bytes key) {
    trie_logger.trace("index_cursor::set_before_row this={} key={}", fmt::ptr(this), fmt_hex_cb(key));
    if (!_partition_metadata) {
        co_return set_result::match;
    }
    co_await _row_cursor.init(_partition_metadata->trie_root);
    auto res = co_await _row_cursor.set_before(key);
    if (res != set_result::match) {
        co_return co_await _row_cursor.step_back();
    }
    co_return res;
}
future<set_result> index_cursor::set_after_row(const_bytes key) {
    trie_logger.trace("index_cursor::set_after_row this={} key={}", fmt::ptr(this), fmt_hex_cb(key));
    if (!_partition_metadata) {
        co_return co_await next_partition();
    }
    co_await _row_cursor.init(_partition_metadata->trie_root);
    auto rowres = co_await _row_cursor.set_after(key);
    if (rowres == set_result::eof) {
        co_return co_await next_partition();
    }
    co_return rowres;
}

int64_t payload_to_offset(const_bytes p) {
    assert(p.size() >= sizeof(int64_t));
    return seastar::be_to_cpu(read_unaligned<int64_t>(p.data()));
}
future<row_index_header> trie_index_reader::read_row_index_header(uint64_t pos) {
    auto hdr = co_await _in_row->read_row_index_header(pos, _permit);
    trie_logger.trace("trie_index_reader::read_row_index_header this={} pos={} result={}", fmt::ptr(this), pos, hdr.data_offset);
    co_return hdr;
}
std::vector<std::byte> trie_index_reader::translate_key(dht::ring_position_view key) {
    auto trie_key = std::vector<std::byte>();
    trie_key.push_back(std::byte(0x40));
    auto token = key.token().is_maximum() ? std::numeric_limits<uint64_t>::max() : key.token().unbias();
    append_to_vector(trie_key, object_representation(seastar::cpu_to_be<uint64_t>(token)));
    if (auto k = key.key()) {
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
    trie_logger.trace("translate_key({}) = {}", key, fmt_hex_cb(trie_key));
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
    std::unique_ptr<trie_reader_input> in,
    std::unique_ptr<trie_reader_input> row_in,
    uint64_t root_offset,
    uint64_t total_file_size,
    schema_ptr s,
    reader_permit rp
)
    : _in(std::move(in))
    , _in_row(std::move(row_in))
    , _s(s)
    , _permit(std::move(rp))
    , _lower(*_in, *_in_row, _permit)
    , _upper(*_in, *_in_row, _permit)
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
std::vector<std::byte> translate_pipv(const schema& s, position_in_partition_view pipv) {
    std::vector<std::byte> res;
    if (pipv.has_key()) {
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
    if (res != set_result::match) {
        co_return false;
    }
    if (!pos) {
        co_await _upper.next_partition();
    } else {
        co_await _upper.set_after_row(translate_pipv(*_s, *pos));
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
    co_await _lower.set_before_row(translate_pipv(*_s, pos));
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
    co_await _upper.set_after_row(translate_pipv(*_s, pos));
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

// future<reader_node> seastar_file_trie_reader_input::read(uint64_t offset) {
//     trie_logger.trace("seastar_file_trie_reader_input::read(): reading at {}", offset);
//     return _f.get_page_view(offset, _permit, nullptr).then([offset] (cached_file::page_view pv) -> reader_node {
//         return with_deser(pv.get_view(), [offset, pv = std::move(pv)] (const Node::Reader& node, void* b) -> reader_node {
//             auto payload_offset = reinterpret_cast<const std::byte*>(node.getPayload().getBytes().begin()) - reinterpret_cast<const std::byte*>(b);
//             return reader_node {
//                 .raw_bytes = std::move(pv),
//                 .parser = &capnp_node_parser,
//                 .payload_offset = payload_offset,
//                 .n_children = node.getChildren().size(),
//                 .payload_bits = node.getPayload().getBits(),
//                 .pos = offset,
//             };
//         });
//     });
// }

enum class blabla_state {
    START,
    KEY_SIZE,
    KEY_BYTES,
    POSITION,
    ROOT_OFFSET,
    LOCAL_DELETION_TIME,
    MARKED_FOR_DELETE_AT,
    END,
};

inline std::string_view format_as(blabla_state s) {
    using enum blabla_state;
    switch (s) {
    case START: return "START";
    case KEY_SIZE: return "KEY_SIZE";
    case KEY_BYTES: return "KEY_BYTES";
    case POSITION: return "POSITION";
    case ROOT_OFFSET: return "ROOT_OFFSET";
    case LOCAL_DELETION_TIME: return "LOCAL_DELETION_TIME";
    case MARKED_FOR_DELETE_AT: return "MARKED_FOR_DELETE_AT";
    case END: return "END";
    default: abort();
    }
}

struct blabla_context : public data_consumer::continuous_data_consumer<blabla_context> {
    using processing_result = data_consumer::processing_result;
    using proceed = data_consumer::proceed;
    using state = blabla_state;
    state _state = state::START;
    row_index_header _result;
    uint64_t _position_offset;
    temporary_buffer<char> _key;
    void verify_end_state() {
        if (_state != state::END) {
            throw sstables::malformed_sstable_exception(fmt::format("blabla_context: {}", _state));
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
            trie_logger.trace("{}: pos {} state {} - data.size()={}", fmt::ptr(this), current_pos(), state::START, data.size());
            _state = state::KEY_SIZE;
            break;
        case state::KEY_SIZE:
            trie_logger.trace("{}: pos {} state {}", fmt::ptr(this), current_pos(), state::KEY_SIZE);
            if (this->read_16(data) != continuous_data_consumer::read_status::ready) {
                _state = state::KEY_BYTES;
                break;
            }
            [[fallthrough]];
        case state::KEY_BYTES:
            trie_logger.trace("{}: pos {} state {} - size={}", fmt::ptr(this), current_pos(), state::KEY_BYTES, this->_u16);
            if (this->read_bytes_contiguous(data, this->_u16, _key) != continuous_data_consumer::read_status::ready) {
                _state = state::POSITION;
                break;
            }
            [[fallthrough]];
        case state::POSITION:
            _result.partition_key = sstables::key(to_bytes(to_bytes_view(_key)));
            _position_offset = current_pos();
            trie_logger.trace("{}: pos {} state {}", fmt::ptr(this), current_pos(), state::POSITION);
            if (read_unsigned_vint(data) != continuous_data_consumer::read_status::ready) {
                _state = state::ROOT_OFFSET;
                break;
            }
            [[fallthrough]];
        case state::ROOT_OFFSET:
            trie_logger.trace("{}: pos {} state {}", fmt::ptr(this), current_pos(), state::ROOT_OFFSET);
            _result.data_offset = this->_u64;
            if (read_unsigned_vint(data) != continuous_data_consumer::read_status::ready) {
                _state = state::LOCAL_DELETION_TIME;
                break;
            }
            [[fallthrough]];
        case state::LOCAL_DELETION_TIME: {
            _result.trie_root = _position_offset - this->_u64;
            trie_logger.trace("{}: pos {} state {}", fmt::ptr(this), current_pos(), state::LOCAL_DELETION_TIME);
            if (this->read_32(data) != continuous_data_consumer::read_status::ready) {
                _state = state::MARKED_FOR_DELETE_AT;
                break;
            }
        }
            [[fallthrough]];
        case state::MARKED_FOR_DELETE_AT:
            trie_logger.trace("{}: pos {} state {}", fmt::ptr(this), current_pos(), state::MARKED_FOR_DELETE_AT);
            _result.partition_tombstone.deletion_time = gc_clock::time_point(gc_clock::duration(this->_u32));
            if (this->read_64(data) != continuous_data_consumer::read_status::ready) {
                _state = state::END;
                break;
            }
            [[fallthrough]];
        case state::END: {
            _state = blabla_state::END;
            _result.partition_tombstone.timestamp = this->_u64;
        }
        }
        trie_logger.trace("{}: exit pos {} state {}", fmt::ptr(this), current_pos(), _state);
        return _state == state::END ? proceed::no : proceed::yes;
    }
public:
    blabla_context(reader_permit rp, input_stream<char>&& input, uint64_t start, uint64_t maxlen)
        : continuous_data_consumer(std::move(rp), std::move(input), start, maxlen)
    {}
};

// future<row_index_header> seastar_file_trie_reader_input::read_row_index_header(uint64_t offset, reader_permit rp) {
//     trie_logger.trace("seastar_file_trie_reader_input::read_row_index_header: this={}, f.size={} offset={}", fmt::ptr(this), _f.size(), offset);
//     auto ctx = blabla_context(std::move(rp), make_file_input_stream(_f_file, offset, _f.size() - offset), offset, _f.size() - offset);
//     auto close = deferred_close(ctx);
//     co_await ctx.consume_input();
//     co_return std::move(ctx._result);
// }

trie_reader_input::~trie_reader_input() {
}

// seastar_file_trie_reader_input::seastar_file_trie_reader_input(cached_file& f, reader_permit permit)
//     : _f(f)
//     , _f_file(make_cached_seastar_file(_f))
//     , _permit(std::move(permit))
// {}

// future<> seastar_file_trie_reader_input::close() {
//     return _f_file.close();
// }

row_index_trie_writer::row_index_trie_writer(trie_writer_output& out) :_out(out) {
}
row_index_trie_writer::~row_index_trie_writer() {
}

void row_index_trie_writer::add(
    const_bytes first_ck,
    const_bytes last_ck,
    uint64_t data_file_offset,
    sstables::deletion_time dt
) {
    trie_logger.trace("row_index_trie_writer::add() this={} first_ck={} last_ck={} data_file_offset={} dt={} _last_sep={}, _last_sep_mismatch={}, _first_key_mismatch={}",
        fmt::ptr(this),
        fmt_hex_cb(first_ck),
        fmt_hex_cb(last_ck),
        data_file_offset,
        dt,
        fmt_hex_cb(_first_key),
        _last_sep_mismatch,
        _first_key_mismatch
    );
    if (_added_blocks > 0) {
        size_t separator_mismatch = std::ranges::mismatch(first_ck, _last_key).in2 - _last_key.begin();
        assert(separator_mismatch < first_ck.size());
        assert(separator_mismatch < _last_key.size());

        size_t mismatch = std::ranges::mismatch(first_ck, _first_key).in2 - _first_key.begin();
        // size_t needed_prefix = std::min(std::max(_last_sep_mismatch, mismatch) + 1, _last_separator.size());
        size_t needed_prefix = std::max(_first_key_mismatch, mismatch) + 1;
        assert(needed_prefix <= _first_key.size());
        auto tail = std::span(_first_key).subspan(_last_sep_mismatch, needed_prefix - _last_sep_mismatch);

        std::array<std::byte, 20> payload_bytes;
        void* p = payload_bytes.data();
        p = write_unaligned(p, seastar::cpu_to_le(_last_payload.data_file_offset));
        p = write_unaligned(p, seastar::cpu_to_le(_last_payload.dt.marked_for_delete_at));
        p = write_unaligned(p, seastar::cpu_to_le(_last_payload.dt.local_deletion_time));
        assert(p == payload_bytes.data() + payload_bytes.size());

        trie_logger.trace("row_index_trie_writer::add(): _wr.add({}, {}, {})", _last_sep_mismatch, fmt_hex_cb(tail), fmt_hex_cb(payload_bytes));
        _wr.add(_last_sep_mismatch, tail, payload(1, payload_bytes));

        _first_key.assign(first_ck.begin(), first_ck.end());
        _first_key_mismatch = separator_mismatch;
        _last_sep_mismatch = mismatch;
    } else {
        _first_key.assign(first_ck.begin(), first_ck.end());
    }
    _added_blocks += 1;
    _last_key.assign(last_ck.begin(), last_ck.end());
    _last_payload = row_index_payload{.data_file_offset = data_file_offset, .dt = dt};
}
ssize_t row_index_trie_writer::finish() {
    if (_added_blocks > 0) {
        size_t needed_prefix = std::min(_last_sep_mismatch + 1, _first_key.size());
        auto tail = std::span(_first_key).subspan(_last_sep_mismatch, needed_prefix - _last_sep_mismatch);

        std::array<std::byte, 20> payload_bytes;
        void* p = payload_bytes.data();
        p = write_unaligned(p, seastar::cpu_to_le(_last_payload.data_file_offset));
        p = write_unaligned(p, seastar::cpu_to_le(_last_payload.dt.marked_for_delete_at));
        p = write_unaligned(p, seastar::cpu_to_le(_last_payload.dt.local_deletion_time));
        assert(p == payload_bytes.data() + payload_bytes.size());

        trie_logger.trace("row_index_trie_writer::finish(): _wr.add({}, {}, {})", _last_sep_mismatch, fmt_hex_cb(tail), fmt_hex_cb(payload_bytes));
        _wr.add(_last_sep_mismatch, tail, payload(1, payload_bytes));
    }
    return _wr.finish();
}

seastar::logger cached_file_logger("cached_file");

struct my_trie_reader_input : trie_reader_input {
    cached_file& _f;
    reader_permit _permit;
    my_trie_reader_input(cached_file& f, reader_permit p) : _f(f), _permit(p) {}
    future<reader_node> read(uint64_t offset) override {
        auto pv = co_await _f.get_page_view(offset, _permit, nullptr);
        auto sp = pv.get_view();
        trie_logger.trace("my_trie_reader_input::read(): reading at {} {}", offset, fmt_hex_cb(sp.subspan(0, 32)));

        auto type = uint8_t(sp[0]) >> 4;
        switch (type) {
        case PAYLOAD_ONLY:
            co_return reader_node{std::move(pv), &my_parser, 1, 0, uint8_t(sp[0]) & 0xf, offset};
        case SINGLE_NOPAYLOAD_4:
        case SINGLE_NOPAYLOAD_12:
            co_return reader_node{std::move(pv), &my_parser, 1 + div_ceil(bits_per_pointer_arr[type], 8), 1, 0, offset};
        case SINGLE_8:
        case SINGLE_16:
            co_return reader_node{std::move(pv), &my_parser, 2 + div_ceil(bits_per_pointer_arr[type], 8), 1, uint8_t(sp[0]) & 0xf, offset};
        case SPARSE_8:
        case SPARSE_12:
        case SPARSE_16:
        case SPARSE_24:
        case SPARSE_40: {
            auto n_children = uint8_t(sp[1]);
            auto payload_offset = 2 + div_ceil(n_children * (8 + bits_per_pointer_arr[type]), 8);
            co_return reader_node{std::move(pv), &my_parser, payload_offset, n_children, uint8_t(sp[0]) & 0xf, offset};
        }
        case DENSE_12:
        case DENSE_16:
        case DENSE_24:
        case DENSE_32:
        case DENSE_40:
        case LONG_DENSE: {
            auto dense_span = uint8_t(sp[2]) + 1;
            auto payload_offset = 3 + div_ceil(dense_span * bits_per_pointer_arr[type], 8);
            co_return reader_node{std::move(pv), &my_parser, payload_offset, dense_span, uint8_t(sp[0]) & 0xf, offset};
        }
        default: abort();
        }
    }
    future<row_index_header> read_row_index_header(uint64_t offset, reader_permit rp) override {
        trie_logger.trace("my_trie_reader_input::read_row_index_header: this={}, f.size={} offset={}", fmt::ptr(this), _f.size(), offset);
        auto ctx = blabla_context(std::move(rp), make_file_input_stream(file(make_shared<cached_file_impl>(_f)), offset, _f.size() - offset), offset, _f.size() - offset);
        auto close = deferred_close(ctx);
        co_await ctx.consume_input();
        co_return std::move(ctx._result);
    }
};

std::unique_ptr<trie_reader_input> make_trie_reader_input(cached_file& f, reader_permit p) {
    return std::make_unique<my_trie_reader_input>(f, p);
}

std::unique_ptr<trie_writer_output> make_trie_writer_output(sstables::file_writer& w, size_t page_size) {
    return std::make_unique<trie_serializer>(w, page_size);
}

