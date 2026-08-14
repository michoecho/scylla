/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cstdint>
#include <stdexcept>

#include <seastar/core/byteorder.hh>
#include <seastar/core/format.hh>

namespace sstables {

namespace bit_packing {

enum class mask_type : uint8_t {
    set,
    clear
};

// size_bits cannot be >= 64
inline uint64_t make_mask(uint8_t size_bits, uint8_t offset, mask_type t) noexcept {
    const uint64_t mask = ((1 << size_bits) - 1) << offset;
    return t == mask_type::set ? mask : ~mask;
}

/*
 * ----> memory addresses
 * MSB           LSB
 * | | | | |3|2|1|0| CPU integer (big or little endian byte order)
 *          -------
 *             |
 *       +-----+ << shift = prefix bits
 *       |
 *    -------
 *  7 6 5 4 3 2 1 0  index*
 * | |3|2|1|0| | | | raw storage (unaligned, little endian byte order)
 *  = ------- =====
 *  |           |
 *  |           +-> prefix bits
 *  +-> suffix bits
 *
 * |0|1|1|1|1|0|0|0| read/write mask
 *
 * * On big endian systems the indices in storage are reversed and
 *   run left to right: 0 1 .. 6 7. To avoid differences in the
 *   encoding logic on machines with different native byte orders
 *   reads and writes to storage must be explicitly little endian.
 */
struct bit_displacement {
    uint64_t shift;
    uint64_t mask;
};

inline bit_displacement displacement_for(uint64_t prefix_bits, uint8_t size_bits, mask_type t) {
    return {prefix_bits, make_mask(size_bits, prefix_bits, t)};
}

} // namespace bit_packing

// Reads the `size_bits`-wide bit field which starts at bit `offset_bits` of `storage`.
//
// Touches the 8 bytes at `storage + offset_bits / 8`, so `storage` has to have that
// many bytes past the first byte of the field.
inline uint64_t read_bits(const char* storage, uint64_t offset_bits, uint64_t size_bits) {
    const uint64_t offset_byte = offset_bits / 8;
    uint64_t value = seastar::read_le<uint64_t>(storage + offset_byte);

    const auto displacement = bit_packing::displacement_for(offset_bits % 8, size_bits, bit_packing::mask_type::set);

    value &= displacement.mask;
    value >>= displacement.shift;

    return value;
}

// Writes the `size_bits`-wide bit field which starts at bit `offset_bits` of `storage`.
// The other bits of the touched bytes are left alone.
//
// See read_bits() for the requirements on `storage`.
inline void write_bits(char* storage, uint64_t offset_bits, uint64_t size_bits, uint64_t value) {
    const uint64_t offset_byte = offset_bits / 8;

    uint64_t old_value = seastar::read_le<uint64_t>(storage + offset_byte);

    const auto displacement = bit_packing::displacement_for(offset_bits % 8, size_bits, bit_packing::mask_type::clear);

    value <<= displacement.shift;

    if ((~displacement.mask | value) != ~displacement.mask) {
        throw std::invalid_argument(seastar::format("{}: to-be-written value would overflow the allocated bits", __FUNCTION__));
    }

    old_value &= displacement.mask;
    value |= old_value;

    seastar::write_le(storage + offset_byte, value);
}

// The bit-packed encoding of a monotonically growing run of offsets, shared by
// compression::segmented_offsets (which holds the offsets of a whole
// CompressionInfo.db in memory) and by compression_info_cache (which holds one
// bucketful of them at a time).
//
// The offsets of a bucket are stored relative to the bucket's base offset, and
// grouped into segments of `grouped_offsets` offsets each. A segment stores the
// offset of its first entry relative to the bucket base (in `base_bits` bits),
// followed by the remaining offsets of the segment, each stored relative to the
// first offset of the segment (in `relative_bits` bits each). The segments of a
// bucket are packed one after another, without padding:
//      arrrarrrarrr...
// where `grouped_offsets` is 4, `a` is a segment base offset, and `r` is an
// offset relative to the `a` preceding it.
//
// The choice of the three parameters is up to the user of the encoding; this
// class only knows how to address and access the fields they describe.
class offset_packing {
    uint8_t _base_bits = 0;
    uint8_t _relative_bits = 0;
    uint8_t _grouped_offsets = 1;
    uint16_t _segment_bits = 0;

public:
    offset_packing() = default;

    offset_packing(uint8_t base_bits, uint8_t relative_bits, uint8_t grouped_offsets) noexcept
        : _base_bits(base_bits)
        , _relative_bits(relative_bits)
        , _grouped_offsets(grouped_offsets)
        , _segment_bits(base_bits + (grouped_offsets - 1) * relative_bits)
    { }

    uint8_t base_bits() const noexcept { return _base_bits; }
    uint8_t relative_bits() const noexcept { return _relative_bits; }
    uint8_t grouped_offsets() const noexcept { return _grouped_offsets; }
    uint16_t segment_bits() const noexcept { return _segment_bits; }

    // The bit offset, within the storage of a bucket, of the segment holding the
    // offset with the given index within that bucket.
    uint64_t segment_bit_offset(uint64_t index_in_bucket) const noexcept {
        return (index_in_bucket / _grouped_offsets) * uint64_t(_segment_bits);
    }

    // The number of bytes of storage needed by a bucket of `segments` segments,
    // including the slack the accessors below need past the last field.
    uint64_t storage_size(uint64_t segments) const noexcept {
        return (segments * uint64_t(_segment_bits) + 7) / 8 + sizeof(uint64_t);
    }

    // The base offset of the segment at `segment_bit`, relative to the base
    // offset of the bucket.
    uint64_t read_base(const char* storage, uint64_t segment_bit) const {
        return read_bits(storage, segment_bit, _base_bits);
    }

    void write_base(char* storage, uint64_t segment_bit, uint64_t value) const {
        write_bits(storage, segment_bit, _base_bits, value);
    }

    // The offset with the index `relative_index` (which must be in
    // [1, grouped_offsets)) within the segment at `segment_bit`, relative to the
    // base offset of that segment.
    uint64_t read_relative(const char* storage, uint64_t segment_bit, uint64_t relative_index) const {
        return read_bits(storage, relative_bit_offset(segment_bit, relative_index), _relative_bits);
    }

    void write_relative(char* storage, uint64_t segment_bit, uint64_t relative_index, uint64_t value) const {
        write_bits(storage, relative_bit_offset(segment_bit, relative_index), _relative_bits, value);
    }

private:
    uint64_t relative_bit_offset(uint64_t segment_bit, uint64_t relative_index) const noexcept {
        return segment_bit + _base_bits + (relative_index - 1) * uint64_t(_relative_bits);
    }
};

} // namespace sstables
