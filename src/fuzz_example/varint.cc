#include "varint.h"

namespace varint {

DecodeResult decode(std::span<const std::uint8_t> input) {
    std::uint64_t value = 0;
    std::size_t i = 0;
    int shift = 0;

#ifdef DELIBERATE_BUGS_FOR_FUZZING
    // BUG (only compiled in for the fuzzing demo): the loop never checks `i`
    // against `input.size()`. As long as each byte has its continuation bit
    // (0x80) set, we keep stepping `i` forward and dereferencing `input[i]`
    // past the end of the buffer. Any all-continuation input (e.g. {0x80})
    // walks straight off the end; under ASAN that's a heap-buffer-overflow
    // read, which AFL++ finds in milliseconds. The #else branch is correct.
    while (true) {
        std::uint8_t byte = input[i];
        value |= static_cast<std::uint64_t>(byte & 0x7f) << shift;
        ++i;
        if ((byte & 0x80) == 0)
            break;
        shift += 7;
    }
#else
    // Stop at the end of the buffer. If the last byte still has its
    // continuation bit set the varint is truncated; we return what we have so
    // far with bytes_read == input.size(). Callers can detect truncation by
    // checking that the last consumed byte had its high bit clear.
    while (i < input.size()) {
        std::uint8_t byte = input[i];
        value |= static_cast<std::uint64_t>(byte & 0x7f) << shift;
        ++i;
        if ((byte & 0x80) == 0)
            break;
        shift += 7;
    }
#endif

    return {value, i};
}

}  // namespace varint
