/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cstdint>
#include <string_view>

#include <seastar/core/byteorder.hh>
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/temporary_buffer.hh>

#include "seastarx.hh"
#include "sstables/checksum_utils.hh"
#include "sstables/version.hh"

class compressor;

namespace sstables {

// The parts of one compressed chunk of Data.db. The views point into a buffer
// that the caller owns.
//
// Unframed chunk (every format except `mu`):
//
//     [ compressed data ][ 4-byte checksum ]
//
// Framed chunk (`mu`):
//
//     [ header ][ compressed data ][ 4-byte checksum ][ footer ]
//
// The header is the length of the compressed data, as an unsigned vint in its
// shortest encoding. The footer is the bytes of the header in reverse order.
// The first byte of a vint gives its width, so a reader at the start of a chunk
// learns the width of the header from its first byte, and a reader at the end
// of a chunk learns the width of the footer from its last byte.
//
// A reader at the start of a chunk uses the header to find the end of the chunk.
// It uses the footer of the previous chunk (the bytes just before its position)
// to find the start of the previous chunk. Thus, a reader can move in both
// directions without the chunk offsets from CompressionInfo.db.
//
// The per-chunk checksum covers the header and the compressed data. The
// whole-file digest covers every byte of the file.
//
// The checksum doesn't cover the footer. A corrupt footer is detected
// indirectly: the reader computes a wrong start for the previous chunk, and
// the data it reads there fails the checksum. Thus, a reader must not trust a
// length until it has verified the checksum of the chunk.
// framed_chunk_codec also rejects lengths that are larger than
// compressed_chunk_length_limit(), so a reader never sizes a read from an
// obviously corrupt length.
struct framed_chunk {
    // Empty for an unframed chunk.
    std::string_view header;
    std::string_view compressed;
    uint32_t stored_checksum;
    // Empty for an unframed chunk.
    std::string_view footer;

    // The size of the chunk in Data.db.
    uint64_t extent() const noexcept {
        return header.size() + compressed.size() + sizeof(uint32_t) + footer.size();
    }

    template <ChecksumUtils Checksum>
    uint32_t compute_checksum() const {
        if (header.empty()) {
            return Checksum::checksum(compressed.data(), compressed.size());
        }
        auto c = Checksum::checksum(header.data(), header.size());
        return Checksum::checksum(c, compressed.data(), compressed.size());
    }

    // Returns the checksum of the chunk. Throws malformed_sstable_exception if
    // it differs from the stored checksum. `file_offset` is the position of the
    // chunk in Data.db; it is used only in the error message.
    template <ChecksumUtils Checksum>
    uint32_t verify_checksum(uint64_t file_offset) const {
        auto actual = compute_checksum<Checksum>();
        if (actual != stored_checksum) {
            throw_checksum_mismatch(file_offset, actual);
        }
        return actual;
    }

    // Adds the chunk to the whole-file digest `digest`, in on-disk order.
    // `checksum` is the chunk's checksum, as returned by compute_checksum().
    // `include_checksum` tells whether the digest covers the per-chunk
    // checksums. It is true for `mc` and later formats.
    template <ChecksumUtils Checksum>
    uint32_t fold_into_digest(uint32_t digest, uint32_t checksum, bool include_checksum) const {
        if constexpr (Checksum::prefer_combine()) {
            digest = Checksum::checksum_combine(digest, checksum, header.size() + compressed.size());
        } else {
            // zlib's adler32() returns its initial value for a null buffer, so
            // never pass it the empty header.
            if (!header.empty()) {
                digest = Checksum::checksum(digest, header.data(), header.size());
            }
            digest = Checksum::checksum(digest, compressed.data(), compressed.size());
        }
        if (include_checksum) {
            uint32_t be_checksum = cpu_to_be(checksum);
            digest = Checksum::checksum(digest, reinterpret_cast<const char*>(&be_checksum), sizeof(be_checksum));
        }
        if (!footer.empty()) {
            digest = Checksum::checksum(digest, footer.data(), footer.size());
        }
        return digest;
    }

private:
    [[noreturn]] void throw_checksum_mismatch(uint64_t file_offset, uint32_t actual) const;
};

// Encodes and decodes the compressed chunks of one Data.db. It knows whether the
// chunks are framed. It is the only place that knows the chunk layout; see
// framed_chunk.
class framed_chunk_codec {
    uint32_t _uncompressed_chunk_length;
    // The width of the widest valid header or footer. 0 for unframed chunks.
    uint32_t _max_frame_len;

    framed_chunk_codec(uint32_t uncompressed_chunk_length, uint32_t max_frame_len) noexcept
        : _uncompressed_chunk_length(uncompressed_chunk_length)
        , _max_frame_len(max_frame_len)
    {}

    // Decodes the compressed length from `vint`, which holds exactly one
    // unsigned vint. Throws malformed_sstable_exception if the length is too
    // large, or if its encoding is not the shortest one.
    uint64_t read_compressed_length(std::string_view vint) const;
public:
    // The width of most headers and footers. A header is 1 byte wide only if
    // the compressed data is shorter than 128 bytes, and 3 bytes wide only if
    // it is at least 16 KiB long. So a reader of a header or a footer reads
    // this many bytes first, and reads more only for a wider one.
    //
    // A chunk is at least 6 bytes long, so this many bytes never extend past
    // the chunk.
    static constexpr uint32_t typical_frame_len = 2;

    // The codec of the chunks of an sstable of version `v`.
    framed_chunk_codec(sstable_version_types v, uint32_t uncompressed_chunk_length);

    // The codec of framed chunks, whatever the format version. For tests only.
    // Code that reads an sstable uses sstable::get_chunk_codec(), which follows
    // the version.
    static framed_chunk_codec for_framed_chunks(uint32_t uncompressed_chunk_length);

    bool framed() const noexcept {
        return _max_frame_len != 0;
    }

    // The width of the widest valid header or footer. 0 for unframed chunks.
    uint32_t max_frame_len() const noexcept {
        return _max_frame_len;
    }

    // The width of the header and of the footer of a chunk with
    // `compressed_len` bytes of compressed data. 0 for unframed chunks.
    uint32_t frame_len(uint64_t compressed_len) const noexcept;

    // Framed chunks only. The width of a header that starts with `first_byte`,
    // or of a footer that ends with it. Throws malformed_sstable_exception if
    // it is wider than max_frame_len().
    uint32_t frame_len_from_first_byte(char first_byte) const;

    uint32_t uncompressed_chunk_length() const noexcept {
        return _uncompressed_chunk_length;
    }

    // The size in Data.db of a chunk with `compressed_len` bytes of compressed
    // data.
    uint64_t extent(uint64_t compressed_len) const noexcept {
        return 2 * frame_len(compressed_len) + compressed_len + sizeof(uint32_t);
    }

    // Framed chunks only. The extent of the largest valid chunk.
    uint64_t max_extent() const noexcept;

    // Framed chunks only. Reads a header, and returns the extent of its chunk.
    // `header` holds exactly the header. Throws malformed_sstable_exception if
    // the length is too large.
    uint64_t extent_from_header(std::string_view header) const;

    // Framed chunks only. Like extent_from_header(), but reads a footer.
    uint64_t extent_from_footer(std::string_view footer) const;

    // Splits a chunk. `chunk` holds exactly one chunk. Throws
    // malformed_sstable_exception if `chunk` is too short, or if a framed
    // chunk's header doesn't match the size of `chunk`.
    framed_chunk parse(std::string_view chunk) const;

    // Framed chunks only. Splits a chunk whose header is `header` and whose
    // other bytes are at the start of `rest`. `rest` can extend past the end of
    // the chunk. Throws malformed_sstable_exception if `rest` is too short.
    framed_chunk parse(std::string_view header, std::string_view rest) const;

    // Decompresses the chunk. Doesn't verify its checksum.
    temporary_buffer<char> decompress(const compressor& c, const framed_chunk& chunk) const;

    // Framed chunks only. Writes the header and the footer of a chunk that
    // starts at `chunk` and has `compressed_len` bytes of compressed data.
    void write_frames(char* chunk, uint32_t compressed_len) const;
};

// One framed chunk read from a stream, as two buffers. They aren't copied
// into one, because `rest` is usually shared from the stream's buffer.
struct framed_chunk_buffers {
    // Exactly the header.
    temporary_buffer<char> header;
    // Exactly the rest of the chunk.
    temporary_buffer<char> rest;
};

// Reads the framed chunk that starts at `file_offset` from `in`. `file_end` is
// the end of the data in the file. Doesn't verify the checksum. Throws
// malformed_sstable_exception if the header is corrupt, if it puts the end of
// the chunk past `file_end`, or if `in` ends before the end of the chunk.
future<framed_chunk_buffers> read_framed_chunk(input_stream<char>& in, const framed_chunk_codec& codec,
        uint64_t file_offset, uint64_t file_end);

} // namespace sstables
