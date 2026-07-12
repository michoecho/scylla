/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <bit>
#include <utility>
#include "bti_index.hh"
#include "sstables/compress.hh"
#include "utils/div_ceil.hh"

// This file contains some declarations which aren't needed by users of BTI readers/writers,
// but are exposed here for testing, or because they are shared between readers and writers.

namespace sstables::trie {

// The compressed data file is read in aligned blocks of this size. To keep the
// on-disk index compact, a chunk's length is stored not as a byte count but as the
// number of these blocks the chunk spans. The two helpers below are the only place
// that block-size math lives: they convert between the byte length carried in a
// physical position's `chunk_length_hint` and the on-disk block count. This detail
// is hidden from users of the index, who deal only in byte lengths.
inline constexpr int64_t sstable_compressed_block_size = 512;

// Byte length -> on-disk block count, for the writer. A chunk may begin at an
// arbitrary offset within a block, so the count accounts for the leading
// misalignment and rounds up.
inline int64_t bti_chunk_length_to_block_count(int64_t chunk_position, int64_t chunk_length) {
    const int64_t misalignment = chunk_position % sstable_compressed_block_size;
    return div_ceil(misalignment + chunk_length, sstable_compressed_block_size);
}

// On-disk block count -> an upper bound on the chunk's byte length, for the reader.
// This is lossy: the recovered length is >= the original, because it rounds the
// chunk out to whole blocks.
inline int64_t bti_chunk_length_from_block_count(int64_t chunk_position, int64_t block_count) {
    const int64_t misalignment = chunk_position % sstable_compressed_block_size;
    return block_count * sstable_compressed_block_size - misalignment;
}

// The upper bound on the on-disk block count a chunk spans (the value actually
// stored in the index; see bti_chunk_length_to_block_count). A chunk's on-disk
// extent is at most compressed_chunk_length_limit() bytes; since it may start at an
// arbitrary offset within a block, it spans at most one extra block.
inline uint64_t bti_max_chunk_length_blocks(uint32_t uncompressed_chunk_length) {
    return div_ceil(compressed_chunk_length_limit(uncompressed_chunk_length), uint64_t(sstable_compressed_block_size)) + 1;
}

inline unsigned bti_offset_within_chunk_bit_width(uint32_t uncompressed_chunk_length) {
    return std::bit_width(uncompressed_chunk_length);
}

// Number of bits used to store a chunk's on-disk block count. The block count is
// a 1-based value in [1, bti_max_chunk_length_blocks], stored zero-based (so
// callers offset by one on either side), and held exactly in this many bits.
inline unsigned bti_chunk_length_bit_width(uint32_t uncompressed_chunk_length) {
    return std::bit_width(bti_max_chunk_length_blocks(uncompressed_chunk_length) - 1);
}

inline unsigned bti_packed_length_and_offset_bit_width(uint32_t uncompressed_chunk_length) {
    return bti_chunk_length_bit_width(uncompressed_chunk_length)
        + bti_offset_within_chunk_bit_width(uncompressed_chunk_length);
}

inline size_t bti_packed_length_and_offset_bytewidth(uint32_t uncompressed_chunk_length) {
    return div_ceil(bti_packed_length_and_offset_bit_width(uncompressed_chunk_length), 8u);
}

// `chunk_length` here is the chunk_length_hint block count, not a byte length.
inline uint64_t bti_pack_length_and_offset(uint64_t chunk_length, uint64_t offset_within_chunk, uint32_t uncompressed_chunk_length) {
    const auto offset_bits = bti_offset_within_chunk_bit_width(uncompressed_chunk_length);
    const auto encoded_length = chunk_length - 1;
    return (encoded_length << offset_bits) | offset_within_chunk;
}

inline std::pair<uint64_t, uint64_t> bti_unpack_length_and_offset(uint64_t packed, uint32_t uncompressed_chunk_length) {
    const auto length_bits = bti_chunk_length_bit_width(uncompressed_chunk_length);
    const auto offset_bits = bti_offset_within_chunk_bit_width(uncompressed_chunk_length);
    const auto offset_mask = (uint64_t{1} << offset_bits) - 1;
    const auto encoded_length = (packed >> offset_bits) & ((uint64_t{1} << length_bits) - 1);
    return {
        encoded_length + 1,
        packed & offset_mask,
    };
}

// FIXME: we calculate the murmur hash when inserting or reading keys
// from bloom filters.
// It's a waste of work to hash the key again.
// The hash should be passed to BTI writers and readers from above,
// and then this function should be eliminated.
std::byte hash_byte_from_key(const schema &s, const partition_key& x);

// Each row index trie in Rows.db is followed by a header containing some partition metadata.
// The partition's entry in Partitions.db points into that header.
struct row_index_header {
    // The partiition key, in BIG serialization format (i.e. the same as in Data.db or Index.db).
    sstables::key partition_key = bytes();
    // The global position of the root node of this partition's row index within Rows.db.
    uint64_t trie_root;
    // The position of the partition inside Data.db, in the sstable's position kind:
    // a logical (uncompressed) position, or a physical one carrying the full chunk
    // coordinates that locate the partition start in the compressed (on-disk) file
    // so the physical cursor can navigate to it directly.
    sstables::sstable_position data_file_position;
    // The partition tombstone of this partition.
    sstables::deletion_time partition_tombstone;
    uint64_t number_of_blocks = 0;
};

future<row_index_header> read_row_index_header(
    bool physical,
    uint32_t uncompressed_chunk_length,
    input_stream<char>&& input,
    uint64_t start,
    uint64_t maxlen,
    reader_permit rp
);

void write_row_index_header(
    sstable_version_types sst_ver,
    sstables::file_writer& fw,
    const sstables::key& pk,
    sstable_position partition_data_start,
    uint32_t uncompressed_chunk_length,
    uint64_t added_blocks,
    uint64_t root_pos,
    const sstables::deletion_time& partition_tombstone
);

} // namespace sstables::trie
