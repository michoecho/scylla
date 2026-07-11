/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "sstables/decompressing_input_stream.hh"

#include <cstring>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

#include <boost/test/unit_test.hpp>
#include <seastar/core/byteorder.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/memory-data-source.hh>

#include "sstables/checksum_utils.hh"
#include "sstables/compress.hh"
#include "sstables/compressor.hh"

namespace {

class noop_compressor final : public compressor {
public:
    size_t uncompress(const char* input, size_t input_len, char* output, size_t output_len) const override {
        if (input_len > output_len) {
            throw std::runtime_error("output buffer too small");
        }
        std::memcpy(output, input, input_len);
        return input_len;
    }

    size_t compress(const char* input, size_t input_len, char* output, size_t output_len) const override {
        return uncompress(input, input_len, output, output_len);
    }

    size_t compress_max_size(size_t input_len) const override {
        return input_len;
    }

    algorithm get_algorithm() const override {
        return algorithm::none;
    }
};

std::vector<char> make_chunk(std::string_view payload, uint32_t uncompressed_chunk_length) {
    // On-disk layout: [header][compressed data][checksum][footer], with the
    // header and footer both holding this chunk's compressed length.
    const auto frame_size = sstables::chunk_length_field_size(uncompressed_chunk_length);
    std::vector<char> chunk(frame_size + payload.size() + sizeof(uint32_t) + frame_size);
    sstables::write_chunk_length_field(chunk.data(), uncompressed_chunk_length, payload.size());
    std::copy(payload.begin(), payload.end(), chunk.begin() + frame_size);
    // The checksum covers the header and the compressed data, not the footer.
    const auto checksum = crc32_utils::checksum(chunk.data(), frame_size + payload.size());
    write_be<uint32_t>(chunk.data() + frame_size + payload.size(), checksum);
    sstables::write_chunk_length_field(chunk.data() + frame_size + payload.size() + sizeof(uint32_t),
            uncompressed_chunk_length, payload.size());
    return chunk;
}

temporary_buffer<char> make_file(std::span<const std::string_view> payloads, uint32_t uncompressed_chunk_length) {
    std::vector<std::vector<char>> chunks;
    size_t size = 0;
    for (auto payload : payloads) {
        chunks.push_back(make_chunk(payload, uncompressed_chunk_length));
        size += chunks.back().size();
    }

    temporary_buffer<char> file(size);
    size_t pos = 0;
    for (const auto& chunk : chunks) {
        std::copy(chunk.begin(), chunk.end(), file.get_write() + pos);
        pos += chunk.size();
    }
    return file;
}

std::string to_string(const temporary_buffer<char>& buf) {
    return std::string(buf.get(), buf.size());
}

} // anonymous namespace

SEASTAR_THREAD_TEST_CASE(test_decompressing_input_stream_reads_forward_without_trusting_chunk_length_hint) {
    constexpr uint32_t uncompressed_chunk_length = 8;
    const std::string_view payloads[] = {"abcde", "fghijk"};
    auto file = make_file(payloads, uncompressed_chunk_length);
    const auto file_size = file.size();
    const auto first_exact_extent = 2 * sstables::chunk_length_field_size(uncompressed_chunk_length) + payloads[0].size() + sizeof(uint32_t);
    // const auto second_exact_extent = 2 * sstables::chunk_length_field_size(uncompressed_chunk_length) + payloads[1].size() + sizeof(uint32_t);
    noop_compressor compressor;

    auto input = sstables::make_decompressing_input_stream(
            sstables::decompressing_input_stream_source_opener([file = std::move(file)] () mutable {
                return make_ready_future<data_source>(data_source(std::make_unique<seastar::util::temporary_buffer_data_source>(std::move(file))));
            }),
            sstables::sstable_position::from_physical(0, first_exact_extent - 4, 1),
            compressor,
            uncompressed_chunk_length,
            file_size);
    input->init_stream_position(sstables::sstable_position::from_physical(0, first_exact_extent - 4, 1));

    auto first_result = input->consume_one(std::nullopt, [&] (temporary_buffer<char> data) {
        BOOST_REQUIRE_EQUAL(to_string(data), "bcde");
        auto pos = input->compute_relative_position(-2).as_physical();
        BOOST_REQUIRE_EQUAL(pos.chunk_position, 0);
        // BOOST_REQUIRE_EQUAL(pos.chunk_length, first_exact_extent);
        BOOST_REQUIRE_EQUAL(pos.offset_within_chunk, 3);
        return continue_consuming{};
    }).get();
    BOOST_REQUIRE(std::holds_alternative<continue_consuming>(first_result.get()));
    auto pos = input->stream_position().position.as_physical();
    BOOST_REQUIRE_EQUAL(pos.chunk_position, first_exact_extent);
    BOOST_REQUIRE_EQUAL(pos.offset_within_chunk, 0);

    input->skip(2).get();
    pos = input->stream_position().position.as_physical();
    BOOST_REQUIRE_EQUAL(pos.chunk_position, first_exact_extent);
    // BOOST_REQUIRE_EQUAL(pos.chunk_length, second_exact_extent);
    BOOST_REQUIRE_EQUAL(pos.offset_within_chunk, 2);

    auto second_result = input->consume_one(std::nullopt, [] (temporary_buffer<char> data) {
        BOOST_REQUIRE_EQUAL(to_string(data), "hijk");
        return stop_consuming<char>{std::move(data)};
    }).get();
    BOOST_REQUIRE(std::holds_alternative<stop_consuming<char>>(second_result.get()));
    pos = input->stream_position().position.as_physical();
    BOOST_REQUIRE_EQUAL(pos.chunk_position, first_exact_extent);
    BOOST_REQUIRE_EQUAL(pos.offset_within_chunk, 2);

    input->close().get();
}

SEASTAR_THREAD_TEST_CASE(test_decompressing_input_stream_caps_hint_at_file_end) {
    constexpr uint32_t uncompressed_chunk_length = 8;
    const std::string_view payloads[] = {"abcde", "fghijk"};
    auto file = make_file(payloads, uncompressed_chunk_length);
    const auto file_size = file.size();
    const auto first_exact_extent = 2 * sstables::chunk_length_field_size(uncompressed_chunk_length) + payloads[0].size() + sizeof(uint32_t);
    noop_compressor compressor;

    auto input = sstables::make_decompressing_input_stream(
            sstables::decompressing_input_stream_source_opener([file = std::move(file)] () mutable {
                return make_ready_future<data_source>(data_source(std::make_unique<seastar::util::temporary_buffer_data_source>(std::move(file))));
            }),
            sstables::sstable_position::from_physical(first_exact_extent, 512, 0),
            compressor,
            uncompressed_chunk_length,
            file_size);
    input->init_stream_position(sstables::sstable_position::from_physical(first_exact_extent, 512, 0));

    auto result = input->consume_one(std::nullopt, [] (temporary_buffer<char> data) {
        BOOST_REQUIRE_EQUAL(to_string(data), "fghijk");
        return continue_consuming{};
    }).get();
    BOOST_REQUIRE(std::holds_alternative<continue_consuming>(result.get()));
    auto pos = input->stream_position().position.as_physical();
    BOOST_REQUIRE_EQUAL(pos.chunk_position, file_size);
    // BOOST_REQUIRE_EQUAL(pos.chunk_length, 0);
    BOOST_REQUIRE_EQUAL(pos.offset_within_chunk, 0);

    input->close().get();
}

SEASTAR_THREAD_TEST_CASE(test_decompressing_input_stream_skip_to_preserves_cached_chunk) {
    constexpr uint32_t uncompressed_chunk_length = 8;
    const std::string_view payloads[] = {"abcdefgh"};
    auto file = make_file(payloads, uncompressed_chunk_length);
    const auto file_size = file.size();
    const auto exact_extent = 2 * sstables::chunk_length_field_size(uncompressed_chunk_length) + payloads[0].size() + sizeof(uint32_t);
    noop_compressor compressor;

    auto input = sstables::make_decompressing_input_stream(
            sstables::decompressing_input_stream_source_opener([file = std::move(file)] () mutable {
                return make_ready_future<data_source>(data_source(std::make_unique<seastar::util::temporary_buffer_data_source>(std::move(file))));
            }),
            sstables::sstable_position::from_physical(0, exact_extent, 0),
            compressor,
            uncompressed_chunk_length,
            file_size);
    input->init_stream_position(sstables::sstable_position::from_physical(0, exact_extent, 0));

    auto first_result = input->consume_one(
            sstables::sstable_position::from_physical(0, exact_extent, 2),
            [] (temporary_buffer<char> data) {
        BOOST_REQUIRE_EQUAL(to_string(data), "ab");
        return continue_consuming{};
    }).get();
    BOOST_REQUIRE(std::holds_alternative<continue_consuming>(first_result.get()));

    input->skip_to(
            sstables::sstable_position::from_physical(0, exact_extent, 5)).get();

    auto second_result = input->consume_one(std::nullopt, [] (temporary_buffer<char> data) {
        BOOST_REQUIRE_EQUAL(to_string(data), "fgh");
        return continue_consuming{};
    }).get();
    BOOST_REQUIRE(std::holds_alternative<continue_consuming>(second_result.get()));
    auto pos = input->stream_position().position.as_physical();
    BOOST_REQUIRE_EQUAL(pos.chunk_position, file_size);
    BOOST_REQUIRE_EQUAL(pos.offset_within_chunk, 0);

    input->close().get();
}

SEASTAR_THREAD_TEST_CASE(test_decompressing_input_stream_reuses_chunk_after_remainder_at_chunk_end) {
    constexpr uint32_t uncompressed_chunk_length = 8;
    const std::string_view payloads[] = {"abcdefgh"};
    auto file = make_file(payloads, uncompressed_chunk_length);
    const auto file_size = file.size();
    const auto exact_extent = 2 * sstables::chunk_length_field_size(uncompressed_chunk_length) + payloads[0].size() + sizeof(uint32_t);
    noop_compressor compressor;

    auto input = sstables::make_decompressing_input_stream(
            sstables::decompressing_input_stream_source_opener([file = std::move(file)] () mutable {
                return make_ready_future<data_source>(data_source(std::make_unique<seastar::util::temporary_buffer_data_source>(std::move(file))));
            }),
            sstables::sstable_position::from_physical(0, exact_extent, 5),
            compressor,
            uncompressed_chunk_length,
            file_size);
    input->init_stream_position(sstables::sstable_position::from_physical(0, exact_extent, 5));

    auto first_result = input->consume_one(std::nullopt, [] (temporary_buffer<char> data) {
        BOOST_REQUIRE_EQUAL(to_string(data), "fgh");
        return stop_consuming<char>{std::move(data)};
    }).get();
    BOOST_REQUIRE(std::holds_alternative<stop_consuming<char>>(first_result.get()));
    auto pos = input->stream_position().position.as_physical();
    BOOST_REQUIRE_EQUAL(pos.chunk_position, 0);
    BOOST_REQUIRE_EQUAL(pos.offset_within_chunk, 5);

    input->skip_to(
            sstables::sstable_position::from_physical(0, exact_extent, 6)).get();

    auto second_result = input->consume_one(std::nullopt, [] (temporary_buffer<char> data) {
        BOOST_REQUIRE_EQUAL(to_string(data), "gh");
        return continue_consuming{};
    }).get();
    BOOST_REQUIRE(std::holds_alternative<continue_consuming>(second_result.get()));
    pos = input->stream_position().position.as_physical();
    BOOST_REQUIRE_EQUAL(pos.chunk_position, file_size);
    BOOST_REQUIRE_EQUAL(pos.offset_within_chunk, 0);

    input->close().get();
}
