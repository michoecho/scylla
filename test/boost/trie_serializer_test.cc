#pragma clang optimize off

#include "seastar/testing/thread_test_case.hh"
#include "sstables/trie.hh"
#include "test/lib/log.hh"
#include "test/lib/reader_concurrency_semaphore.hh"
#include "test/lib/tmpdir.hh"

namespace mytest {

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
    for (const auto& c : x._children) {
        if (c->_output_pos >= 0) {
            assert(size_t(c->_output_pos) < pos);
            result = std::max<size_t>(result, pos - c->_output_pos);
        }
    }
    return result;
}

node_type choose_node_type(const node& x, size_t pos) {
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
    if (sparse_size < dense_size || true) {
        return sparses[width_idx];
    } else {
        return denses[width_idx];
    }
}

class trie_serializer final : public trie_writer_output {
    sstables::file_writer& _w;
    size_t _page_size;
    constexpr static size_t max_page_size = 64 * 1024;
    constexpr static size_t round_down(size_t a, size_t factor) {
        return a - a % factor;
    }
    constexpr static size_t round_up(size_t a, size_t factor) {
        return round_down(a + factor - 1, factor);
    }
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
        return 3 + div_ceil(bits_per_pointer * (int(x._children.back()->_transition) - int(x._children.front()->_transition)), 8) + x._payload.blob().size();
    }
    virtual size_t write(const node& x) override {
        auto before = pos();
        auto res = write_impl(x);
        assert(res == serialized_size(x, x._output_pos));
        assert(pos() - before == res);
        return res;
    }
    size_t write_impl(const node& x) {
        const auto my_pos = x._output_pos;
        auto type = choose_node_type(x, my_pos);
        testlog.trace("write: {}", int(type));
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

inline constexpr size_t round_down(size_t a, size_t factor) {
    return a - a % factor;
}
inline constexpr size_t round_up(size_t a, size_t factor) {
    return round_down(a + factor - 1, factor);
}

uint64_t read_offset(const_bytes sp, int idx, int bpp) {
    // testlog.trace("read_offset: sp={}, idx={}, bpp={}", fmt_hex_cb(sp.subspan(0, 20)), idx, bpp);
    uint64_t off = 0;
    for (int i = 0; i < div_ceil(bpp, 8); ++i) {
        off = off << 8 | uint64_t(sp[(idx * bpp) / 8 + i]);
    }
    testlog.trace("off={:x}", off);
    off >>= (8 - (bpp * (idx + 1) % 8)) % 8;
    testlog.trace("off={:x}", off);
    off &= uint64_t(-1) >> (64 - bpp);
    testlog.trace("off={:x}", off);
    return off;
}

node_parser my_parser {
    .lookup = [] (const_bytes, std::byte transition) -> node_parser::lookup_result {
        abort();
    },
    .get_child = [] (const_bytes sp, int idx) -> node_parser::lookup_result {
        auto type = uint8_t(sp[0]) >> 4;
        switch (type) {
        case PAYLOAD_ONLY:
            abort();
        case SINGLE_NOPAYLOAD_4:
            assert(idx == 0);
            return {idx, sp[1], uint64_t(sp[0]) & 0xf};
        case SINGLE_NOPAYLOAD_12:
            assert(idx == 0);
            return {idx, sp[2], (uint64_t(sp[0]) & 0xf) << 8 | uint64_t(sp[1])};
        case SINGLE_8:
            assert(idx == 0);
            return {idx, sp[1], uint64_t(sp[2])};
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
            auto transition = std::byte(uint8_t(sp[1]) + idx);
            auto dense_span = uint64_t(sp[2]) + 1;
            auto bpp = bits_per_pointer_arr[type];
            assert(idx < int(dense_span));
            return {idx, transition, read_offset(sp.subspan(3), idx, bpp)};
        }
        default: abort();
        }
    },
};

struct my_trie_reader_input : trie_reader_input {
    cached_file& _f;
    reader_permit _permit;
    my_trie_reader_input(cached_file& f, reader_permit p) : _f(f), _permit(p) {}
    future<reader_node> read(uint64_t offset) override {
        auto pv = co_await _f.get_page_view(offset, _permit, nullptr);
        auto sp = pv.get_view();

        auto type = uint8_t(sp[0]) >> 4;
        testlog.trace("read: {} {}", offset, fmt_hex_cb(sp.subspan(0, 80)));
        switch (type) {
        case PAYLOAD_ONLY:
            co_return reader_node{std::move(pv), &my_parser, 1, 0, uint8_t(sp[0]) & 0xf};
        case SINGLE_NOPAYLOAD_4:
        case SINGLE_NOPAYLOAD_12:
            co_return reader_node{std::move(pv), &my_parser, 1 + div_ceil(bits_per_pointer_arr[type], 8), 1, 0};
        case SINGLE_8:
        case SINGLE_16:
            co_return reader_node{std::move(pv), &my_parser, 2 + div_ceil(bits_per_pointer_arr[type], 8), 1, uint8_t(sp[0]) & 0xf};
        case SPARSE_8:
        case SPARSE_12:
        case SPARSE_16:
        case SPARSE_24:
        case SPARSE_40: {
            auto n_children = uint8_t(sp[1]);
            auto payload_offset = 2 + div_ceil(n_children * (8 + bits_per_pointer_arr[type]), 8);
            co_return reader_node{std::move(pv), &my_parser, payload_offset, n_children, uint8_t(sp[0]) & 0xf};
        }
        case DENSE_12:
        case DENSE_16:
        case DENSE_24:
        case DENSE_32:
        case DENSE_40:
        case LONG_DENSE: {
            auto dense_span = uint8_t(sp[2]) + 1;
            auto payload_offset = 3 + div_ceil(dense_span * bits_per_pointer_arr[type], 8);
            co_return reader_node{std::move(pv), &my_parser, payload_offset, dense_span, uint8_t(sp[0]) & 0xf};
        }
        default: abort();
        }
    }
    future<row_index_header> read_row_index_header(uint64_t offset, reader_permit) override {
        abort();
    }
};

} // namespace mytest

SEASTAR_THREAD_TEST_CASE(test_serde) {
    using namespace mytest;
    tmpdir tmp;
    const auto file_path = tmp.path() / "test";
    file f = open_file_dma(file_path.native(), open_flags::create | open_flags::truncate | open_flags::wo).get();
    sstables::file_writer w(make_file_output_stream(std::move(f)).get());
    auto ser = trie_serializer(w, 4096);

    std::vector<std::byte> empty_payload;
    std::vector<std::byte> full_payload;
    for (auto c : std::string_view("abcd")) {
        full_payload.push_back(std::byte(c));
    }

    std::vector<uint64_t> offsets;

    for (uint64_t max_offset_log_2 = 0; max_offset_log_2 < 42; ++max_offset_log_2)
    for (uint64_t n_children : {0, 1, 2, 255})
    for (auto payl : {std::cref(empty_payload), std::cref(full_payload)}) {
        testlog.trace("writing max_offset_log_2={}, n_children={} payload=", max_offset_log_2, n_children, !payl.get().empty());
        node root(std::byte(0));
        for (uint64_t k = 0; k < n_children; ++k) {
            uint64_t i = n_children - k - 1;
            root.add_child(std::byte(255 - i))->_output_pos = (uint64_t(1) << 50) - ((uint64_t(1) << max_offset_log_2) + i);
        }
        root.set_payload(payload(payl.get().empty() ? 0 : 5, payl.get()));
        root._output_pos = uint64_t(1) << 50;

        if (ser.bytes_left_in_page() < ser.serialized_size(root, root._output_pos)) {
            ser.pad_to_page_boundary();
        }
        offsets.push_back(ser.pos());
        ser.write(root);
    }

    w.close();

    f = open_file_dma(file_path.native(), open_flags::ro).get();
    logalloc::region r;
    cached_file_stats cfs;
    lru lrulist;
    cached_file cf(f, cfs, lrulist, r, f.size().get(), file_path.native());

    tests::reader_concurrency_semaphore_wrapper semaphore;

    auto mtri = my_trie_reader_input(cf, semaphore.make_permit());

    size_t off_idx = 0;
    for (uint64_t max_offset_log_2 = 0; max_offset_log_2 < 42; ++max_offset_log_2)
    for (uint64_t n_children : {0, 1, 2, 255})
    for (auto payl : {std::cref(empty_payload), std::cref(full_payload)}) {
        testlog.trace("reading max_offset_log_2={}, n_children={} payload={}", max_offset_log_2, n_children, !payl.get().empty());
        auto rn = mtri.read(offsets[off_idx++]).get();
        BOOST_REQUIRE_EQUAL(rn.n_children, n_children);
        for (uint64_t k = 0; k < n_children; ++k) {
            uint64_t i = n_children - k - 1;
            auto child = rn.get_child(k);
            BOOST_REQUIRE_EQUAL(uint8_t(child.byte), 255 - i);
            BOOST_REQUIRE_EQUAL(child.offset, ((uint64_t(1) << max_offset_log_2) + i));
        }
        BOOST_REQUIRE_EQUAL(rn.payload_bits, payl.get().empty() ? 0 : 5);
        if (rn.payload_bits) {
            BOOST_REQUIRE_GE(rn.payload().bytes.size(), full_payload.size());
            BOOST_REQUIRE(std::ranges::equal(full_payload, rn.payload().bytes.subspan(0, full_payload.size())));
        }
    }
}
