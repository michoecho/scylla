// The part of llvm-symbolizer's JSON response that this module consumes.
#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace addrdec::json {

struct symbol {
    std::string function;
    std::string file;
    std::uint32_t line = 0;
    std::uint32_t column = 0;
};

using document = std::vector<symbol>;

// Parse one complete llvm-symbolizer JSON response. Fields not represented by
// symbol are ignored, while fields missing from a symbol keep their defaults.
// Returns nullopt for malformed input, including trailing non-whitespace.
std::optional<document> parse(std::string_view text);

}  // namespace addrdec::json
