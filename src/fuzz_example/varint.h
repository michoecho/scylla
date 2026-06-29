#pragma once

#include <cstddef>
#include <cstdint>
#include <span>

// A tiny LEB128-style varint decoder. This exists ONLY as a worked example of
// fuzzing a function with AFL++ (see skills/afl-fuzz). It is not used by the
// rest of the program. It decodes a single unsigned varint from the front of a
// byte buffer: each byte contributes 7 bits of payload, and the high bit
// (0x80) means "more bytes follow".
//
// When built with -DDELIBERATE_BUGS_FOR_FUZZING the decoder contains an
// intentional out-of-bounds read so the fuzz harness has something to find.
// Without the flag (the default) it is correct. Do not copy the buggy variant.

namespace varint {

struct DecodeResult {
    std::uint64_t value;     // The decoded integer.
    std::size_t bytes_read;  // How many input bytes the varint consumed.
};

// Decode one varint from the front of `input`. Returns the value and the number
// of bytes consumed.
DecodeResult decode(std::span<const std::uint8_t> input);

}  // namespace varint
