#include "fuzz_targets.h"

#include <cstring>
#include <memory>
#include <span>

#include "varint.h"

namespace fuzz {

void varint(const std::uint8_t* data, std::size_t size) {
    // Copy the input into a freshly-allocated, exactly-`size`-sized heap buffer
    // before handing it to decode(). AFL's persistent-mode testcase buffer is a
    // large (~1 MB) shared-memory region, so a one-past-the-end read of the raw
    // AFL buffer lands in still-valid memory and ASAN sees nothing — the fuzzer
    // would run forever finding no crash. A tight `new[]` puts an ASAN redzone
    // right after the last byte, so any over-read is caught. This copy is the
    // difference between the fuzzer working and silently doing nothing.
    std::unique_ptr<std::uint8_t[]> tight(new std::uint8_t[size]);
    std::memcpy(tight.get(), data, size);
    ::varint::decode(std::span<const std::uint8_t>(tight.get(), size));
}

}  // namespace fuzz
