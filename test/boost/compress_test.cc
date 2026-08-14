/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <array>
#include <cstdint>
#include <random>
#include <vector>

#include <seastar/core/bitops.hh>
#include <seastar/core/format.hh>

#include "utils/xx_hasher.hh"
#include "sstables/compress.hh"
#include "sstables/segmented_compress_params.hh"

namespace sstables {
// Defined in compress.cc, and not declared in segmented_compress_params.hh.
std::pair<bucket_info, segment_info> params_for_chunk_size(uint32_t chunk_size);
}

namespace {

class digest {
    xx_hasher _hasher;

public:
    void feed_byte(uint8_t b) {
        feed(reinterpret_cast<const char*>(&b), 1);
    }

    void feed(const char* data, size_t size) {
        _hasher.update(data, size);
    }

    void feed_u64(uint64_t value) {
        // Fed byte by byte, in a fixed order, so that the digest doesn't depend
        // on the host's endianness.
        std::array<char, 8> bytes;
        for (int i = 0; i < 8; ++i) {
            bytes[i] = static_cast<char>(value >> (8 * i));
        }
        feed(bytes.data(), bytes.size());
    }

    uint64_t value() {
        return _hasher.finalize_uint64();
    }
};

// The bit layout segmented_offsets::init() derives for a given chunk size.
// Recomputed here (rather than read out of the container) on purpose: the point
// of the test below is to pin down the encoding, so it should describe the
// encoding independently of the code which produces it.
struct layout {
    uint8_t base_bits;
    uint8_t relative_bits;
    uint8_t grouped_offsets;
    uint16_t segment_bits;
    uint32_t segments_per_bucket;

    uint64_t offsets_per_bucket() const {
        return uint64_t(grouped_offsets) * segments_per_bucket;
    }

    // The number of bits a bucket holding `offsets` offsets has had written to.
    // The rest of its storage is untouched, and hence not safe to hash.
    uint64_t written_bits(uint64_t offsets) const {
        const uint64_t full_segments = offsets / grouped_offsets;
        const uint64_t rest = offsets % grouped_offsets;
        uint64_t bits = full_segments * segment_bits;
        if (rest != 0) {
            // A partial segment always has its base written, plus one relative
            // offset for each entry past the first.
            bits += base_bits + (rest - 1) * relative_bits;
        }
        return bits;
    }
};

layout layout_for(uint32_t chunk_size) {
    const auto params = sstables::params_for_chunk_size(chunk_size);
    layout l;
    l.grouped_offsets = params.second.grouped_offsets;
    l.base_bits = params.second.data_size_log2;
    // With a single offset per segment there are no relative offsets at all,
    // and the width the container computes for them is meaningless.
    l.relative_bits = l.grouped_offsets > 1
            ? uint8_t(seastar::log2ceil(uint32_t((chunk_size + 64) * (l.grouped_offsets - 1))))
            : uint8_t(0);
    l.segment_bits = l.base_bits + (l.grouped_offsets - 1) * l.relative_bits;
    l.segments_per_bucket = params.first.segments_per_bucket;
    return l;
}

// Hashes the packed form of `offsets`, bucket by bucket.
void feed_encoding(digest& d, const sstables::compression::segmented_offsets& offsets) {
    const auto l = layout_for(offsets.chunk_size());
    const auto buckets = offsets.encoded_buckets();

    const uint64_t expected_buckets =
            (offsets.size() + l.offsets_per_bucket() - 1) / l.offsets_per_bucket();
    BOOST_REQUIRE_EQUAL(buckets.size(), expected_buckets);

    d.feed_u64(offsets.chunk_size());
    d.feed_u64(offsets.size());
    d.feed_u64(buckets.size());

    uint64_t remaining = offsets.size();
    for (const auto& [base_offset, storage] : buckets) {
        const uint64_t in_bucket = std::min<uint64_t>(remaining, l.offsets_per_bucket());
        remaining -= in_bucket;

        const uint64_t bits = l.written_bits(in_bucket);
        // The storage of a bucket is `bucket_size` bytes, and the encoding must
        // not run past it (read_bits()/write_bits() touch 8 bytes at a time).
        BOOST_REQUIRE_LE(bits / 8 + sizeof(uint64_t), sstables::bucket_size);

        d.feed_u64(base_offset);
        d.feed_u64(bits);
        d.feed(storage, bits / 8);
        if (bits % 8 != 0) {
            // The last byte is shared with the untouched tail; hash only the
            // bits which were written.
            const uint8_t mask = uint8_t((1u << (bits % 8)) - 1);
            d.feed_byte(uint8_t(storage[bits / 8]) & mask);
        }
    }
    BOOST_REQUIRE_EQUAL(remaining, 0u);
}

} // anonymous namespace

// Feeds randomly generated offsets to segmented_offsets, checks that they read
// back through every accessor the container has, and hashes the packed form
// they were stored in. The hashes of all runs are combined into one digest,
// which is compared against a golden value.
//
// The golden value is not meaningful in itself: it just pins down the encoding.
// segmented_offsets holds the offsets of every compressed sstable in memory, so
// a change to how densely it packs them is a change in memory footprint, and it
// should be a deliberate one. If this test starts failing, either the encoding
// changed (update the constant, and say why in the commit message) or the
// layout parameters did (layout_for() above has to be updated to match).
BOOST_AUTO_TEST_CASE(segmented_offsets_randomized) {
    // Every power of two the parameter tables cover up to 1 MiB, plus a few
    // sizes which aren't powers of two - those round up to the parameters of
    // the next power of two, but size their relative offsets from the real
    // chunk size.
    //
    // Chunk sizes below 16 are left out: they don't hit the tables at all, and
    // the fallback in params_for_chunk_size() fills segment_info in field
    // order, so it ends up with a base offset field of 0 bits and can't store
    // anything. Chunk sizes that small don't occur in practice.
    const std::vector<uint32_t> chunk_sizes{
        15,
        1 << 4, 1 << 5, 1 << 6, 1 << 7, 1 << 8, 1 << 9, 1 << 10, 1 << 11,
        1 << 12, 1 << 13, 1 << 14, 1 << 15, 1 << 16, 1 << 17, 1 << 18,
        1 << 19, 1 << 20,
        1000, 5000, 100000,
    };

    // Fixed seed, and hand-rolled bounded draws: the digest has to come out the
    // same on every run and every platform, and std::uniform_int_distribution
    // is not specified to.
    std::mt19937_64 rng(0x5eed5eed);
    auto below = [&rng] (uint64_t n) { return rng() % n; };

    digest total;
    constexpr int runs = 300;

    for (int run = 0; run < runs; ++run) {
        const uint32_t chunk_size = chunk_sizes[below(chunk_sizes.size())];
        // Enough to span several buckets (a bucket holds 1360-3268 offsets,
        // depending on the chunk size), and occasionally empty.
        const std::size_t count = below(8000);

        // The container is only required to cope with offsets which grow by at
        // most a chunk at a time - that is what the field widths are sized for.
        // Within that, cover the extremes as well as the middle.
        const int pattern = int(below(4));
        auto next_delta = [&] () -> uint64_t {
            switch (pattern) {
            case 0: return below(uint64_t(chunk_size) + 1); // anything legal
            case 1: return chunk_size;                      // widest fields
            case 2: return 0;                               // repeated offsets
            // Tightly packed, i.e. the values stay well inside their fields.
            default: return below(std::min<uint64_t>(chunk_size, 63) + 1);
            }
        };

        sstables::compression::segmented_offsets offsets;
        offsets.init(chunk_size);
        BOOST_REQUIRE_EQUAL(offsets.chunk_size(), chunk_size);

        std::vector<uint64_t> expected;
        expected.reserve(count);
        auto writer = offsets.get_writer();
        uint64_t offset = 0;
        for (std::size_t i = 0; i < count; ++i) {
            expected.push_back(offset);
            writer.push_back(offset);
            offset += next_delta();
        }

        BOOST_REQUIRE_EQUAL(offsets.size(), count);

        // One accessor used for everything: it carries a cursor which is only
        // updated incrementally when the reads are sequential, so the order the
        // reads come in in is part of what's being tested.
        auto accessor = offsets.get_accessor();

        for (std::size_t i = 0; i < count; ++i) {
            BOOST_REQUIRE_EQUAL(accessor.at(i), expected[i]);
        }
        for (std::size_t i = count; i-- > 0; ) {
            BOOST_REQUIRE_EQUAL(accessor.at(i), expected[i]);
        }
        if (count != 0) {
            for (int i = 0; i < 200; ++i) {
                const std::size_t index = below(count);
                BOOST_REQUIRE_EQUAL(accessor.at(index), expected[index]);
                // A random seek followed by a step forward, which is the access
                // pattern the incremental cursor is there for.
                if (index + 1 < count) {
                    BOOST_REQUIRE_EQUAL(accessor.at(index + 1), expected[index + 1]);
                }
            }
            BOOST_REQUIRE_THROW(accessor.at(count), std::out_of_range);
        }

        // A fresh accessor starts with a cursor pointing at the beginning, so it
        // takes a different path through the position tracking than the one
        // above.
        auto fresh = offsets.get_accessor();
        for (std::size_t i = 0; i < count; i += 97) {
            BOOST_REQUIRE_EQUAL(fresh.at(i), expected[i]);
        }

        std::size_t seen = 0;
        for (auto it = offsets.begin(); it != offsets.end(); ++it, ++seen) {
            BOOST_REQUIRE_EQUAL(*it, expected[seen]);
        }
        BOOST_REQUIRE_EQUAL(seen, count);

        feed_encoding(total, offsets);
    }

    BOOST_TEST_MESSAGE(seastar::format("combined encoding digest: {:#018x}", total.value()));
    BOOST_REQUIRE_EQUAL(total.value(), 0xeea1f5a0021b4b56ull);
}

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
