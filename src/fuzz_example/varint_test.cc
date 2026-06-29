#include "varint.h"

#include <cstdint>
#include <vector>

#include "doctest/doctest.h"

// Well-formed inputs the decoder is expected to handle. These pass regardless
// of the DELIBERATE_BUGS_FOR_FUZZING flag.
TEST_CASE("varint decodes well-formed values") {
    // 0 -> single byte 0x00.
    {
        std::vector<std::uint8_t> in = {0x00};
        auto r = varint::decode(in);
        CHECK(r.value == 0);
        CHECK(r.bytes_read == 1);
    }
    // 300 -> 0xAC 0x02 (classic LEB128 example).
    {
        std::vector<std::uint8_t> in = {0xAC, 0x02};
        auto r = varint::decode(in);
        CHECK(r.value == 300);
        CHECK(r.bytes_read == 2);
    }
}

// Regression test for the out-of-bounds read the fuzzer found: a continuation
// byte with nothing after it. The decoder must NOT read past the buffer; it
// must stop at input.size() bytes.
//
// With the default (fixed) build this passes. Built with
// -DDELIBERATE_BUGS_FOR_FUZZING under the Sanitize preset, the buggy decoder
// instead aborts here (libstdc++ hardened span bounds check / ASAN
// heap-buffer-overflow) — that's the bug being exposed. See skills/afl-fuzz for
// how AFL produced this input.
TEST_CASE("varint does not read past a truncated buffer") {
    std::vector<std::uint8_t> truncated = {0x80};
    auto r = varint::decode(truncated);
    // Whatever the decoder returns for truncated input, it must not have
    // consumed more bytes than it was given.
    CHECK(r.bytes_read <= truncated.size());
}
