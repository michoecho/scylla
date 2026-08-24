#include "rle.h"

#include <cstddef>
#include <string>
#include <vector>

std::string DecodeRle(const std::string& data) {
  if (data.size() < 4) return {};
  if (data[0] != 'R' || data[1] != 'L' || data[2] != 'E' || data[3] != '1') {
    return {};
  }

  // The bug: the output buffer is sized from the *number of pairs* rather than
  // from the sum of the counts, so any run longer than one byte writes past the
  // end of the allocation. Reaching it requires getting the magic right first,
  // which is what makes this a fuzzing problem rather than an arithmetic one.
  std::vector<char> buffer((data.size() - 4) / 2);
  std::size_t written = 0;
  for (std::size_t i = 4; i + 1 < data.size(); i += 2) {
    const auto count = static_cast<unsigned char>(data[i]);
    for (unsigned char n = 0; n < count; ++n) {
      buffer[written++] = data[i + 1];
    }
  }
  return std::string(buffer.data(), written);
}
