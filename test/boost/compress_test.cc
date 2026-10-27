/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <string>
#include <string_view>
#include <vector>

#include "sstables/compress.hh"
#include "sstables/exceptions.hh"
#include "sstables/framed_chunk.hh"
#include "vint-serialization.hh"

BOOST_AUTO_TEST_CASE(segmented_offsets_basic_functionality) {
    sstables::compression::segmented_offsets offsets;

    // f = 20, c = 10, n = 8
    offsets.init(1 << 10);

    sstables::compression::segmented_offsets::writer writer = offsets.get_writer();
    sstables::compression::segmented_offsets::accessor accessor = offsets.get_accessor();

    writer.push_back(0);
    writer.push_back(100);
    writer.push_back(200);
    writer.push_back(300);
    writer.push_back(400);
    writer.push_back(500);
    writer.push_back(600);
    writer.push_back(700);

    BOOST_REQUIRE_EQUAL(accessor.at(0), 0);
    BOOST_REQUIRE_EQUAL(accessor.at(1), 100);
    BOOST_REQUIRE_EQUAL(accessor.at(2), 200);
    BOOST_REQUIRE_EQUAL(accessor.at(3), 300);
    BOOST_REQUIRE_EQUAL(accessor.at(4), 400);
    BOOST_REQUIRE_EQUAL(accessor.at(5), 500);
    BOOST_REQUIRE_EQUAL(accessor.at(6), 600);
    BOOST_REQUIRE_EQUAL(accessor.at(7), 700);

    const uint64_t largest_base{0x00000000000fe000};
    const uint64_t trailing_zeroes{0x00000000000fff00};
    const uint64_t all_ones{0x00000000000fffff};

    writer.push_back(largest_base);
    writer.push_back(trailing_zeroes);
    writer.push_back(all_ones);

    BOOST_REQUIRE_EQUAL(accessor.at(0), 0);
    BOOST_REQUIRE_EQUAL(accessor.at(1), 100);
    BOOST_REQUIRE_EQUAL(accessor.at(2), 200);
    BOOST_REQUIRE_EQUAL(accessor.at(3), 300);
    BOOST_REQUIRE_EQUAL(accessor.at(4), 400);
    BOOST_REQUIRE_EQUAL(accessor.at(5), 500);
    BOOST_REQUIRE_EQUAL(accessor.at(6), 600);
    BOOST_REQUIRE_EQUAL(accessor.at(7), 700);
    BOOST_REQUIRE_EQUAL(accessor.at(8), largest_base);
    BOOST_REQUIRE_EQUAL(accessor.at(9), trailing_zeroes);
    BOOST_REQUIRE_EQUAL(accessor.at(10), all_ones);
}

BOOST_AUTO_TEST_CASE(segmented_offsets_more_buckets) {
    sstables::compression::segmented_offsets offsets;
    offsets.init(1 << 9);

    sstables::compression::segmented_offsets::writer writer = offsets.get_writer();
    sstables::compression::segmented_offsets::accessor accessor = offsets.get_accessor();

    const std::size_t size = 0x0000000000100000;

    for (std::size_t i = 0; i < size; ++i) {
        writer.push_back(i);
    }

    BOOST_REQUIRE_EQUAL(offsets.size(), size);

    for (std::size_t i = 0; i < size; ++i) {
        BOOST_REQUIRE_EQUAL(accessor.at(i), i);
    }
}

BOOST_AUTO_TEST_CASE(segmented_offsets_iterator) {
    sstables::compression::segmented_offsets offsets;
    offsets.init(1 << 14);

    sstables::compression::segmented_offsets::writer writer = offsets.get_writer();
    sstables::compression::segmented_offsets::accessor accessor = offsets.get_accessor();

    const std::size_t size = 0x0000000000100000;

    for (std::size_t i = 0; i < size; ++i) {
        writer.push_back(i);
    }

    BOOST_REQUIRE_EQUAL(offsets.size(), size);

    std::size_t i{0};
    for (auto offset : offsets) {
        BOOST_REQUIRE_EQUAL(offset, i);
        ++i;
    }

    for (std::size_t i = 0; i < size; i += 1024) {
        BOOST_REQUIRE_EQUAL(accessor.at(i), i);
    }
}

BOOST_AUTO_TEST_CASE(segmented_offsets_overflow_detection) {
    sstables::compression::segmented_offsets offsets;
    offsets.init(1 << 8);

    sstables::compression::segmented_offsets::writer writer = offsets.get_writer();

    const uint64_t overflown_base_offset{0x0000000000100000};
    BOOST_REQUIRE_THROW(writer.push_back(overflown_base_offset), std::invalid_argument);

    const uint64_t good_base_offset{0x00000000000f0000};
    BOOST_REQUIRE_NO_THROW(writer.push_back(good_base_offset));

    const uint64_t overflown_segment_offset{0x00000000000fffff};
    BOOST_REQUIRE_THROW(writer.push_back(overflown_segment_offset), std::invalid_argument);

    const uint64_t good_segment_offset{0x00000000000f0001};
    BOOST_REQUIRE_NO_THROW(writer.push_back(good_segment_offset));
}

BOOST_AUTO_TEST_CASE(segmented_offsets_corner_cases) {
    sstables::compression::segmented_offsets offsets;
    offsets.init(1 << 12);

    sstables::compression::segmented_offsets::writer writer = offsets.get_writer();
    sstables::compression::segmented_offsets::accessor accessor = offsets.get_accessor();

    const std::size_t size = 0x0000000000100000;

    for (std::size_t i = 0; i < size; ++i) {
        writer.push_back(i);
    }

    // Random at() to a position just before a bucket boundary, then do an
    // incremental at() to read the next offset.
    BOOST_REQUIRE(accessor.at(4079) == 4079);
    BOOST_REQUIRE(accessor.at(4080) == 4080);
}

BOOST_AUTO_TEST_CASE(chunk_frame_len) {
    // A frame is the compressed length as an unsigned vint, so the widest frame
    // is the vint of compressed_chunk_length_limit. The 10922/10923 pair
    // straddles the boundary where the limit crosses 2^14, which is where the
    // vint grows from 2 to 3 bytes.
    for (uint32_t uncompressed_chunk_length : {1u << 10, 1u << 12, 10922u, 10923u, 1u << 16, 1u << 17}) {
        BOOST_TEST_CONTEXT("uncompressed_chunk_length=" << uncompressed_chunk_length) {
            const uint64_t limit = sstables::compressed_chunk_length_limit(uncompressed_chunk_length);
            const auto codec = sstables::framed_chunk_codec::for_framed_chunks(uncompressed_chunk_length);
            BOOST_CHECK_EQUAL(codec.max_frame_len(), unsigned_vint::serialized_size(limit));
            BOOST_CHECK_EQUAL(codec.frame_len(limit), codec.max_frame_len());
            BOOST_CHECK_EQUAL(codec.extent(limit), limit + 2 * codec.max_frame_len() + sizeof(uint32_t));
        }
    }
    BOOST_CHECK_EQUAL(sstables::framed_chunk_codec::for_framed_chunks(10922).max_frame_len(), 2);
    BOOST_CHECK_EQUAL(sstables::framed_chunk_codec::for_framed_chunks(10923).max_frame_len(), 3);

    const auto codec = sstables::framed_chunk_codec::for_framed_chunks(1u << 12);
    BOOST_CHECK_EQUAL(codec.frame_len(0), 1);
    BOOST_CHECK_EQUAL(codec.frame_len(127), 1);
    BOOST_CHECK_EQUAL(codec.frame_len(128), 2);
    BOOST_CHECK_EQUAL(codec.extent(127), 127 + 2 + sizeof(uint32_t));
    BOOST_CHECK_EQUAL(codec.extent(128), 128 + 4 + sizeof(uint32_t));

    // Unframed chunks have no frames.
    const auto unframed = sstables::framed_chunk_codec(sstables::sstable_version_types::me, 1u << 12);
    BOOST_CHECK(!unframed.framed());
    BOOST_CHECK_EQUAL(unframed.frame_len(128), 0);
    BOOST_CHECK_EQUAL(unframed.extent(128), 128 + sizeof(uint32_t));
}

BOOST_AUTO_TEST_CASE(chunk_frames_roundtrip) {
    const uint32_t uncompressed_chunk_length = 1u << 12; // 4096
    const uint32_t limit = sstables::compressed_chunk_length_limit(uncompressed_chunk_length); // 6144
    const auto codec = sstables::framed_chunk_codec::for_framed_chunks(uncompressed_chunk_length);

    for (uint32_t len : {0u, 1u, 127u, 128u, uncompressed_chunk_length, limit}) {
        BOOST_TEST_CONTEXT("len=" << len) {
            const auto frame = codec.frame_len(len);
            const auto extent = codec.extent(len);
            std::vector<char> buf(extent + 4, char(0xAA)); // extra bytes must stay untouched
            codec.write_frames(buf.data(), len);

            const auto header = std::string_view(buf.data(), frame);
            const auto footer = std::string_view(buf.data() + extent - frame, frame);
            BOOST_CHECK_EQUAL(codec.frame_len_from_first_byte(header.front()), frame);
            BOOST_CHECK_EQUAL(codec.frame_len_from_first_byte(footer.back()), frame);
            BOOST_CHECK_EQUAL(codec.extent_from_header(header), extent);
            BOOST_CHECK_EQUAL(codec.extent_from_footer(footer), extent);
            // The footer is the header, reversed.
            BOOST_CHECK(std::equal(header.begin(), header.end(), footer.rbegin()));
            // The frames must not touch the compressed data, the checksum, or
            // the bytes past the chunk.
            for (size_t i = frame; i < extent - frame; ++i) {
                BOOST_CHECK_EQUAL(static_cast<unsigned char>(buf[i]), 0xAAu);
            }
            for (size_t i = extent; i < buf.size(); ++i) {
                BOOST_CHECK_EQUAL(static_cast<unsigned char>(buf[i]), 0xAAu);
            }
        }
    }
}

BOOST_AUTO_TEST_CASE(chunk_frames_reject_corrupt_lengths) {
    // The writer never emits these frames, so craft them by hand.
    const uint32_t uncompressed_chunk_length = 1u << 12; // limit 6144, at most 2-byte frames
    const uint64_t limit = sstables::compressed_chunk_length_limit(uncompressed_chunk_length);
    const auto codec = sstables::framed_chunk_codec::for_framed_chunks(uncompressed_chunk_length);
    auto encode = [] (uint64_t v) {
        bytes b(bytes::initialized_later(), unsigned_vint::serialized_size(v));
        unsigned_vint::serialize(v, b.begin());
        return std::string(reinterpret_cast<const char*>(b.data()), b.size());
    };

    // A length exceeding compressed_chunk_length_limit signals corruption; the
    // reader must throw rather than size a read from a bogus length.
    const auto too_long = encode(limit + 1);
    BOOST_REQUIRE_EQUAL(too_long.size(), codec.max_frame_len());
    BOOST_CHECK_THROW(codec.extent_from_header(too_long), sstables::malformed_sstable_exception);
    BOOST_CHECK_THROW(codec.extent_from_footer(std::string(too_long.rbegin(), too_long.rend())), sstables::malformed_sstable_exception);

    // A frame wider than any valid frame is rejected from its first byte.
    const auto too_wide = encode(uint64_t(1) << 14);
    BOOST_REQUIRE_GT(too_wide.size(), codec.max_frame_len());
    BOOST_CHECK_THROW(codec.frame_len_from_first_byte(too_wide.front()), sstables::malformed_sstable_exception);

    // 5 in two bytes is a valid vint, but not the shortest encoding, so the
    // width of the frame doesn't follow from the length.
    const auto non_minimal = std::string{char(0x80), char(0x05)};
    BOOST_REQUIRE_EQUAL(codec.frame_len_from_first_byte(non_minimal.front()), 2);
    BOOST_CHECK_THROW(codec.extent_from_header(non_minimal), sstables::malformed_sstable_exception);
}
