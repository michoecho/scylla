#ifndef EXAMPLES_FUZZTEST_RLE_H_
#define EXAMPLES_FUZZTEST_RLE_H_

#include <string>

// Decodes a toy run-length encoding: a four byte "RLE1" magic, then a sequence
// of (count, byte) pairs. Anything that does not start with the magic decodes to
// the empty string.
//
// This function contains a deliberate bug -- it is the thing being demonstrated.
std::string DecodeRle(const std::string& data);

#endif  // EXAMPLES_FUZZTEST_RLE_H_
