#include "address_decoder/json.h"

#include <glaze/glaze.hpp>

#include <utility>

namespace addrdec::json {

namespace detail {

// These names mirror llvm-symbolizer's JSON keys. Keeping this wire type
// private means the rest of the module can use its normal lower-case names.
struct glaze_symbol {
    std::string FunctionName;
    std::string FileName;
    std::uint32_t Line = 0;
    std::uint32_t Column = 0;
};

struct glaze_document {
    std::vector<glaze_symbol> Symbol;
};

// llvm-symbolizer adds fields over time (for example Discriminator), so the
// decoder should only require the fields it consumes. The explicit trailing
// check preserves the one-line protocol's requirement that a whole JSON
// document, rather than a valid prefix, was received.
struct parse_options : glz::opts {
    bool null_terminated = false;
    bool error_on_unknown_keys = false;
    bool validate_skipped = true;
    bool validate_trailing_whitespace = true;
};

}  // namespace detail

std::optional<document> parse(std::string_view text) {
    detail::glaze_document input;
    if (glz::read<detail::parse_options{}>(input, text)) {
        return std::nullopt;
    }

    document output;
    output.reserve(input.Symbol.size());
    for (detail::glaze_symbol& symbol : input.Symbol) {
        output.push_back({
            .function = std::move(symbol.FunctionName),
            .file = std::move(symbol.FileName),
            .line = symbol.Line,
            .column = symbol.Column,
        });
    }
    return output;
}

}  // namespace addrdec::json
