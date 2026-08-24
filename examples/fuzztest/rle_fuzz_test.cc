#include <string>

#include "fuzztest/fuzztest_core.h"
#include "rle.h"

// `fuzztest_core.h` rather than `fuzztest.h`: the core header carries the
// FUZZ_TEST macro and the built-in domains without pulling the protobuf domain,
// and -- unlike the gtest-integrated headers -- needs no GoogleTest.
//
// With no .WithDomains(), each parameter defaults to Arbitrary<T>().
void DecodeRleNeverOverflows(const std::string& data) { DecodeRle(data); }
FUZZ_TEST(RleDecoder, DecodeRleNeverOverflows);
