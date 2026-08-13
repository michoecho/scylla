// Minimal zstd round-trip: compress a string, decompress it, check we got the
// original back. Enough to prove the toolchain, the headers and the shared
// library all came from Nix and actually link.

#include <zstd.h>

#include <cstdio>
#include <string>
#include <vector>

int main() {
  const std::string input =
      "hello from buck2 + nativelink + nix, "
      "repeated to give the compressor something to chew on. "
      "hello from buck2 + nativelink + nix, "
      "repeated to give the compressor something to chew on.";

  const size_t bound = ZSTD_compressBound(input.size());
  std::vector<char> compressed(bound);

  const size_t compressed_size = ZSTD_compress(
      compressed.data(), bound, input.data(), input.size(), /*level=*/3);
  if (ZSTD_isError(compressed_size)) {
    std::fprintf(stderr, "compression failed: %s\n",
                 ZSTD_getErrorName(compressed_size));
    return 1;
  }

  std::vector<char> decompressed(input.size());
  const size_t decompressed_size =
      ZSTD_decompress(decompressed.data(), decompressed.size(),
                      compressed.data(), compressed_size);
  if (ZSTD_isError(decompressed_size)) {
    std::fprintf(stderr, "decompression failed: %s\n",
                 ZSTD_getErrorName(decompressed_size));
    return 1;
  }

  const std::string output(decompressed.data(), decompressed_size);
  if (output != input) {
    std::fprintf(stderr, "round-trip mismatch\n");
    return 1;
  }

  std::printf("zstd version:  %s\n", ZSTD_versionString());
  std::printf("original:      %zu bytes\n", input.size());
  std::printf("compressed:    %zu bytes\n", compressed_size);
  std::printf("round-tripped: ok\n");
  return 0;
}
