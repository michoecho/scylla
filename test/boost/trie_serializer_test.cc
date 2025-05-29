/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#pragma clang optimize off

#include "seastar/core/file.hh"
#include "seastar/testing/thread_test_case.hh"
#include "test/lib/log.hh"
#include "test/lib/random_utils.hh"
#include "utils/bit_cast.hh"
#include "utils/cached_file.hh"
#include "utils/memory_data_sink.hh"
#include <ranges>
#include "sstables/trie/trie_serializer.hh"
#include <fmt/ranges.h>
#include "sstables/trie/trie_bti_node_reader.hh"

#undef BOOST_REQUIRE
#define BOOST_REQUIRE SCYLLA_ASSERT

using namespace trie;

inline const_bytes string_as_bytes(std::string_view sv) {
    return std::as_bytes(std::span(sv.data(), sv.size()));
}

inline std::string_view bytes_as_string(const_bytes sv) {
    return {reinterpret_cast<const char*>(sv.data()), sv.size()};
}

static std::vector<std::byte> linearize(const memory_data_sink_buffers& bufs) {
    std::vector<std::byte> retval;
    for (const auto& frag : bufs.buffers()) {
        auto v = std::as_bytes(std::span(frag));
        retval.insert(retval.end(), v.begin(), v.end());
    }
    return retval;
}

std::vector<uint8_t> unpack_bitstring(const_bytes packed) {
    std::vector<uint8_t> unpacked;
    for (const auto byte : packed) {
        for (int i = 7; i >= 0; --i) {
            unpacked.push_back((uint8_t(byte) >> i) & 1);
        }
    }
    return unpacked;
}

// Test read_offset() on a random blob, by unpacking the bits
// of the blob and the bits of read_offset() and checking that
// the relevant bitstrings are equal. 
SEASTAR_THREAD_TEST_CASE(test_read_offset) {
    auto test_blob_buf = tests::random::get_bytes(256);
    auto test_string = std::as_bytes(std::span(test_blob_buf));
    auto test_bitstring = unpack_bitstring(test_string);

    for (const int width : {8, 12, 16, 24, 32, 48, 56, 64}) {
        for (int idx = 0; idx * width + width <= int(test_string.size()); ++idx) {
            uint64_t result = read_offset(test_string, idx, width);
            auto read_blob = unpack_bitstring(object_representation(seastar::cpu_to_be(result)));
            auto actual_bitstring = std::span(read_blob).subspan(64 - width, width);
            auto expected_bitstring = std::span(test_bitstring).subspan(idx * width, width);
            BOOST_REQUIRE(std::ranges::equal(actual_bitstring, expected_bitstring));
        }
    }
}

std::vector<std::byte> serialize_body(const writer_node& node, sink_pos pos, node_type type) {
    memory_data_sink_buffers bufs;
    constexpr size_t page_size = 4096;
    sstables::file_writer fw(data_sink(std::make_unique<memory_data_sink>(bufs)));

    bti_trie_sink_impl serializer(fw, page_size);
    auto sz = serializer.serialized_size_body_type(node, type);
    serializer.write_body(node, pos, type);
    fw.close();
    BOOST_REQUIRE(fw.offset() <= page_size);
    BOOST_REQUIRE(fw.offset() == uint64_t(sz.value));

    return linearize(bufs);
}

struct serialize_chain_result {
    std::vector<std::byte> serialized;
    size_t starting_point;
};
serialize_chain_result serialize_chain(const writer_node& node, node_size body_offset) {
    memory_data_sink_buffers bufs;
    constexpr size_t page_size = 4096;
    sstables::file_writer fw(data_sink(std::make_unique<memory_data_sink>(bufs)));

    bti_trie_sink_impl serializer(fw, page_size);
    auto sz = serializer.serialized_size_chain(node, body_offset);
    auto starting_point = serializer.write_chain(node, body_offset);
    fw.close();
    BOOST_REQUIRE(fw.offset() <= page_size);
    BOOST_REQUIRE(fw.offset() == uint64_t(sz.value));

    return {linearize(bufs), starting_point.value};
}

struct deserialize_node_result {
    std::vector<uint64_t> offsets;
    std::vector<std::byte> transitions;
    payload_result payload;
};

deserialize_node_result deserialize_body(const_bytes raw) {
    deserialize_node_result result;
    auto n_children = get_n_children(raw);
    for (int i = 0; i < n_children; ++i) {
        auto child = get_child(raw, i, true);
        i = child.idx;
        result.offsets.push_back(child.offset);
        result.transitions.push_back(child.byte);
    }
    result.payload = get_payload(raw);
    return result;
}

struct deserialize_chain_result {
    std::vector<std::byte> transition;
    node_size body_offset;
};

deserialize_chain_result deserialize_chain(const_bytes raw, size_t start_point) {
    BOOST_REQUIRE(start_point < raw.size());
    deserialize_chain_result result;
    while (true) {
        auto n_children = get_n_children(raw.subspan(start_point));
        BOOST_REQUIRE(n_children == 1);
        auto child = get_child(raw.subspan(start_point), 0, true);
        BOOST_REQUIRE(child.idx == 0);
        BOOST_REQUIRE(child.offset > 0);
        BOOST_REQUIRE(child.offset < 4096);
        result.transition.push_back(child.byte);
        if (child.offset > start_point) {
            BOOST_REQUIRE(start_point == 0);
            result.body_offset = node_size(child.offset);
            return result;
        }
        start_point -= child.offset;
    }
}

bool eligible(node_type type, const writer_node& node, sink_pos pos) {
    auto max_offset = max_offset_from_child(node, pos);
    BOOST_REQUIRE(max_offset.valid());
    auto width = std::bit_width<uint64_t>(max_offset.value);
    switch (type) {
    case PAYLOAD_ONLY:
        return node.get_children().size() == 0;
    case SINGLE_NOPAYLOAD_4:
        return node.get_children().size() == 1 && width <= 4 && node._payload._payload_bits == 0;
    case SINGLE_8:
        return node.get_children().size() == 1 && width <= 8;
    case SINGLE_NOPAYLOAD_12:
        return node.get_children().size() == 1 && width <= 12 && node._payload._payload_bits == 0;
    case SINGLE_16:
        return node.get_children().size() == 1 && width <= 16;
    case SPARSE_8:
        return node.get_children().size() < 256 && width <= 8;
    case SPARSE_12:
        return node.get_children().size() < 256 && width <= 12;
    case SPARSE_16:
        return node.get_children().size() < 256 && width <= 16;
    case SPARSE_24:
        return node.get_children().size() < 256 && width <= 24;
    case SPARSE_40:
        return node.get_children().size() < 256 && width <= 40;
    case DENSE_12:
        return node.get_children().size() >= 1 && width <= 12;
    case DENSE_16:
        return node.get_children().size() >= 1 && width <= 16;
    case DENSE_24:
        return node.get_children().size() >= 1 && width <= 24;
    case DENSE_32:
        return node.get_children().size() >= 1 && width <= 32;
    case DENSE_40:
        return node.get_children().size() >= 1 && width <= 40;
    case LONG_DENSE:
        return node.get_children().size() >= 1;
    default: abort();
    }
}

// Generates multiple interesting sets of child edges for a trie node.
std::vector<std::vector<uint8_t>> get_some_interesting_transition_sets() {
    std::vector<std::vector<uint8_t>> result;
    // 0 children. Important edge case.
    result.push_back({});
    // 1 child, at both extremes. (Extremes are useful to ensure that there is no weird wrapping). 
    result.push_back({0x00});
    result.push_back({0xff});
    // 2 children, at both extremes, and also with a gap between them (to test unused child slots in DENSE nodes).
    result.push_back({0x00, 0x01});
    result.push_back({0xfe, 0xff});
    result.push_back({0x00, 0xff});
    // 256 children, with all possible transition bytes.
    // Edge case.
    auto full = std::ranges::iota_view(0x00, 0x100) | std::ranges::to<std::vector<uint8_t>>();
    result.push_back(full);
    // 255 children, with all possible transition bytes except one.
    auto almost_full = full;
    almost_full.erase(almost_full.begin() + 100);
    result.push_back(almost_full);
    return result;
}

// For the given number of children and the given max supported integer width,
// generates a few sets of interesting child offsets.
// (Where "interesting" in this case means that they are just close to extremes of the supported integer
// range. This checks that no bits are lost).
std::vector<std::vector<int64_t>> get_some_interesting_child_offsets(int width, int n_children) {
    std::vector<std::vector<int64_t>> result;
    int64_t max = (int64_t(1) << width) - 1;
    auto clamp_to_legal = [=] (int64_t v) {
        return std::clamp<int64_t>(v, 1, max);
    };
    auto clamped_iota = [=] (int64_t a, int64_t b) {
        return std::ranges::iota_view(a, b) | std::views::transform(clamp_to_legal) | std::ranges::to<std::vector>();
    };
    result.emplace_back(clamped_iota(1, n_children + 1));
    result.emplace_back(clamped_iota(max - n_children + 1, max + 1));
    return result;
}

ptr<writer_node> make_node(
    sink_pos pos,
    const_bytes transition,
    std::span<const uint8_t> child_transitions,
    std::span<const int64_t> child_offsets,
    std::optional<trie_payload> payload,
    bump_allocator& alctr
) {
    auto node = writer_node::create(transition, alctr);
    for (size_t i = 0; i < child_transitions.size(); ++i) {
        std::byte transition[] = {std::byte(child_transitions[i])};
        auto child = node->add_child(transition, alctr);
        child->_pos = pos - sink_offset(child_offsets[i]);
        child->_node_size = node_size(1);
        child->_branch_size = sink_offset(0);
        child->_transition_meta = uint8_t(transition[0]);
    }
    if (payload) {
        node->set_payload(*payload);
    }
    return node;
}

SEASTAR_THREAD_TEST_CASE(test_body) {
    std::vector<std::vector<uint8_t>> interesting_transition_sets = get_some_interesting_transition_sets();

    // Arbitrary, but large enough to cover all interesting widths.
    auto pos = sink_pos((uint64_t(1) << 60) + 1);
    auto whatever = string_as_bytes("hahaha");
    const auto custom_payload = trie_payload(0x7, string_as_bytes("lololo"));

    for (int width = 1; width < 50; ++width)
    for (const auto& child_transitions : interesting_transition_sets)
    for (const auto& offsets : get_some_interesting_child_offsets(width, child_transitions.size()))
    for (bool payload : {true, false}) {
        testlog.trace("transitions={} offsets={}", child_transitions, offsets);
        SCYLLA_ASSERT(offsets.size() == child_transitions.size());
        trie::bump_allocator alctr(128 * 1024);
        auto payload_opt = payload ? std::optional<trie_payload>(custom_payload) : std::optional<trie_payload>(); 
        auto node = make_node(pos, whatever, child_transitions, offsets, payload_opt, alctr);
        for (node_type type = node_type(0); type < NODE_TYPE_COUNT; type = node_type(int(type) + 1)) {
            testlog.trace("type={} payload={}", int(type), payload);
            if (eligible(type, *node, pos)) {
                auto serialized = serialize_body(*node, pos, type);
                auto deserialized = deserialize_body(serialized);
                testlog.trace("serialized={:x} deserialized_transitions={:x}",
                    fmt::join(serialized, ", "),
                    fmt::join(deserialized.transitions, ", "));
                BOOST_REQUIRE(deserialized.offsets.size() == child_transitions.size());

                for (size_t i = 0; i < child_transitions.size(); ++i) {
                    BOOST_REQUIRE(std::byte(child_transitions[i]) == deserialized.transitions[i]);
                    BOOST_REQUIRE(uint64_t(offsets[i]) == deserialized.offsets[i]);
                }

                if (payload) {
                    BOOST_REQUIRE(deserialized.payload.bits == custom_payload._payload_bits);
                    BOOST_REQUIRE(std::ranges::equal(
                        deserialized.payload.bytes.subspan(0, custom_payload.blob().size()),
                        custom_payload.blob()));
                } else {
                    BOOST_REQUIRE(deserialized.payload.bits == 0);
                }
            }
        }
    }
}

SEASTAR_THREAD_TEST_CASE(test_chain) {
    std::vector<bytes> interesting_transitions;
    for (int i = 2; i < 65; ++i) {
        interesting_transitions.push_back(tests::random::get_bytes(i));
    }

    std::vector<node_size> interesting_body_offsets;
    interesting_body_offsets.emplace_back(1);
    interesting_body_offsets.emplace_back(2);
    interesting_body_offsets.emplace_back(15);
    interesting_body_offsets.emplace_back(16);
    interesting_body_offsets.emplace_back(17);

    auto test_one = [] (const_bytes transition, node_size body_offset) {
        trie::bump_allocator alctr(128 * 1024);
        auto node = writer_node::create(transition, alctr);
        auto [serialized, starting_point] = serialize_chain(*node, body_offset);
        auto deserialized = deserialize_chain(serialized, starting_point);
        BOOST_REQUIRE(deserialized.body_offset.value == body_offset.value);
        BOOST_REQUIRE(std::ranges::equal(deserialized.transition, transition.subspan(1)));
    };

    for (const auto& off : interesting_body_offsets) {
        auto some_transition = std::as_bytes(std::span(interesting_transitions.back()));
        test_one(some_transition, off);
    }
    for (const auto& transition : interesting_transitions) {
        auto transition_view = std::as_bytes(std::span(transition)); 
        auto some_offset = interesting_body_offsets.back();
        test_one(transition_view, some_offset);
    }
}

class in_memory_file_impl : public file_impl {
    const_bytes _view;
    uint64_t _fake_start_pos;
private:
    [[noreturn]] void unsupported() {
        throw_with_backtrace<std::logic_error>("unsupported operation");
    }
public:
    in_memory_file_impl(const_bytes buf, uint64_t fake_start_pos)
        : _view(buf)
        , _fake_start_pos(fake_start_pos)
    { }

    // unsupported
    virtual future<size_t> write_dma(uint64_t pos, const void* buffer, size_t len, io_intent*) override { unsupported(); }
    virtual future<size_t> write_dma(uint64_t pos, std::vector<iovec> iov, io_intent*) override { unsupported(); }
    virtual future<> flush(void) override { unsupported(); }
    virtual future<> truncate(uint64_t length) override { unsupported(); }
    virtual future<> discard(uint64_t offset, uint64_t length) override { unsupported(); }
    virtual future<> allocate(uint64_t position, uint64_t length) override { unsupported(); }
    virtual subscription<directory_entry> list_directory(std::function<future<>(directory_entry)>) override { unsupported(); }
    virtual future<struct stat> stat(void) override { unsupported(); }
    virtual std::unique_ptr<seastar::file_handle_impl> dup() override { unsupported(); }

    // delegating
    virtual future<uint64_t> size(void) override { return make_ready_future<uint64_t>(_view.size() + _fake_start_pos); }
    virtual future<> close() override { return make_ready_future<>(); }
    virtual future<temporary_buffer<uint8_t>> dma_read_bulk(uint64_t offset, size_t size, io_intent* intent) override {
        SCYLLA_ASSERT(offset >= _fake_start_pos && offset <= _fake_start_pos + _view.size());
        auto p = _view.subspan(offset - _fake_start_pos);
        p = p.first(std::min(p.size(), size));
        return make_ready_future<temporary_buffer<uint8_t>>(reinterpret_cast<const uint8_t*>(p.data()), p.size());
    }

    virtual future<size_t> read_dma(uint64_t pos, void* buffer, size_t len, io_intent*) override {
        unsupported(); // FIXME
    }

    virtual future<size_t> read_dma(uint64_t pos, std::vector<iovec> iov, io_intent*) override {
        unsupported(); // FIXME
    }
};

struct cached_file_in_memory {
    // cached_file has no default constructor, so it's a pain to initialize without optional<>
    std::optional<cached_file> _cf;
    std::vector<std::byte> _backing;
    logalloc::region _region;
    lru _cachelist;
    cached_file_stats _stats;
    cached_file_in_memory(const_bytes backing, uint64_t fake_start) {
        uint64_t padding = fake_start % cached_file::page_size;
        _backing.resize(padding);
        _backing.insert(_backing.end(), backing.begin(), backing.end());
        auto fake_file_impl = seastar::make_shared<in_memory_file_impl>(_backing, fake_start - padding);
        auto fake_file = seastar::file(seastar::static_pointer_cast<file_impl>(fake_file_impl));
        _cf.emplace(fake_file, _stats, _cachelist, _region, fake_start + backing.size());
    }
    cached_file_in_memory(cached_file_in_memory&&) = delete;
    cached_file& get_cached_file() {
        return *_cf;
    }
};

SEASTAR_THREAD_TEST_CASE(test_bti_node_reader) {
    std::vector<std::vector<uint8_t>> interesting_transition_sets = get_some_interesting_transition_sets();

    // Arbitrary, but large enough to cover all interesting widths.
    auto pos = sink_pos((uint64_t(1) << 60) + 1);
    auto whatever = string_as_bytes("hahaha");
    const auto custom_payload = trie_payload(0x7, string_as_bytes("lololo"));

    for (int width = 1; width < 50; ++width)
    for (const auto& child_transitions : interesting_transition_sets)
    for (const auto& offsets : get_some_interesting_child_offsets(width, child_transitions.size()))
    for (bool payload : {true, false}) {
        testlog.trace("transitions={} offsets={}", child_transitions, offsets);
        SCYLLA_ASSERT(offsets.size() == child_transitions.size());
        trie::bump_allocator alctr(128 * 1024);
        auto payload_opt = payload ? std::optional<trie_payload>(custom_payload) : std::optional<trie_payload>(); 
        auto node = make_node(pos, whatever, child_transitions, offsets, payload_opt, alctr);
    
        for (node_type type = node_type(0); type < NODE_TYPE_COUNT; type = node_type(int(type) + 1)) {
            testlog.trace("type={} payload={}", int(type), payload);
            if (eligible(type, *node, pos)) {
                auto serialized = serialize_body(*node, pos, type);
                auto fake_file = cached_file_in_memory(serialized, pos.value);
                bti_node_reader bnr(fake_file.get_cached_file());
                bnr.load(pos.value).get();

                auto basic_metadata = bnr.read_final_node(pos.value); 
                BOOST_REQUIRE(basic_metadata.n_children >= int(child_transitions.size()));
                BOOST_REQUIRE(basic_metadata.payload_bits == (payload ? custom_payload._payload_bits : 0));

                if (payload) {
                    auto p = bnr.get_payload(pos.value);
                    SCYLLA_ASSERT(std::ranges::equal(custom_payload.blob(), p));
                }

                std::vector<int> padded_child_indices;

                for (int b = 0; b < 256; ++b) {
                    std::byte key[] = {std::byte(b)};
                    auto result = bnr.traverse_node_by_key(pos.value, key);
                    BOOST_REQUIRE(result.traversed_key_bytes == 0);
                    BOOST_REQUIRE(result.body_pos == pos.value);
                    BOOST_REQUIRE(result.payload_bits == basic_metadata.payload_bits);
                    BOOST_REQUIRE(result.n_children == basic_metadata.n_children);
                    auto it = std::ranges::lower_bound(child_transitions, b);
                    if (it == child_transitions.end()) {
                        BOOST_REQUIRE(result.found_byte == -1);
                        BOOST_REQUIRE(result.found_idx == result.n_children);
                    } else if (*it == b) {
                        BOOST_REQUIRE(result.found_byte == b);
                        BOOST_REQUIRE(result.found_idx < result.n_children);
                        BOOST_REQUIRE(result.child_offset == offsets.at(it - child_transitions.begin()));
                        padded_child_indices.push_back(result.found_idx);
                    } else {
                        BOOST_REQUIRE(result.found_byte > b);
                        BOOST_REQUIRE(result.found_idx < result.n_children);
                    }
                }

                {
                    auto result = bnr.traverse_node_leftmost(pos.value);
                    BOOST_REQUIRE(result.n_children == basic_metadata.n_children);
                    BOOST_REQUIRE(result.payload_bits == basic_metadata.payload_bits);
                    BOOST_REQUIRE(result.body_pos == pos.value);
                    if (result.n_children > 0) {
                        BOOST_REQUIRE(result.child_offset == offsets.front());
                    }
                }
                {
                    auto result = bnr.traverse_node_rightmost(pos.value);
                    BOOST_REQUIRE(result.n_children == basic_metadata.n_children);
                    BOOST_REQUIRE(result.payload_bits == basic_metadata.payload_bits);
                    BOOST_REQUIRE(result.body_pos == pos.value);
                    if (result.n_children > 0) {
                        BOOST_REQUIRE(result.child_offset == offsets.back());
                    }
                }
                {
                    for (int i = 0; i < basic_metadata.n_children; ++i) {
                        int next_child_idx, prev_child_idx;
                        auto it = std::ranges::lower_bound(padded_child_indices, i);
                        BOOST_REQUIRE(it != padded_child_indices.end());
                        if (i == *it) {
                            next_child_idx = it - padded_child_indices.begin();
                            prev_child_idx = next_child_idx;
                        } else {
                            BOOST_REQUIRE(it != padded_child_indices.begin());
                            next_child_idx = it - padded_child_indices.begin();
                            prev_child_idx = next_child_idx - 1;
                        }
                        {
                            auto result = bnr.get_child(pos.value, i, true);
                            BOOST_REQUIRE(result.idx == padded_child_indices[next_child_idx]);
                            BOOST_REQUIRE(result.offset == static_cast<uint64_t>(offsets[next_child_idx]));
                        }
                        {
                            auto result = bnr.get_child(pos.value, i, false);
                            BOOST_REQUIRE(result.idx == padded_child_indices[prev_child_idx]);
                            BOOST_REQUIRE(result.offset == static_cast<uint64_t>(offsets[prev_child_idx]));
                        }
                    }
                }
            }
        }
    }
}