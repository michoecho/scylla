#include "exponential_histogram/integer_to_float.h"

#include <bit>
#include <format>
#include <limits>
#include <stdexcept>
#include <string>
#include <type_traits>

#include <doctest/doctest.h>
#include <hegel/hegel.h>

#include "snapshot/check.h"

namespace exponential_histogram {
namespace {

constexpr unsigned kIntegerBits = std::numeric_limits<std::uint64_t>::digits;

void check_widths(unsigned exponent_bits, unsigned significand_bits) {
    if (exponent_bits > kIntegerBits || significand_bits > kIntegerBits ||
        exponent_bits + significand_bits > kIntegerBits) {
        throw std::invalid_argument(
            "exponent and significand must fit in a uint64_t bit pattern");
    }
}

std::uint64_t low_mask(unsigned bits) {
    return bits == kIntegerBits
               ? std::numeric_limits<std::uint64_t>::max()
               : (std::uint64_t{1} << bits) - 1;
}


}  // namespace

encoded_float integer_to_float(std::uint64_t value,
                               unsigned exponent_bits,
                               unsigned significand_bits) {
    check_widths(exponent_bits, significand_bits);

    const std::uint64_t max_exponent = low_mask(exponent_bits);
    const unsigned width = std::bit_width(value);
    if (width <= significand_bits) {
        return encoded_float(value);
    }

    const unsigned shift = width - significand_bits - 1;
    const std::uint64_t exponent = static_cast<std::uint64_t>(shift) + 1;
    if (exponent > max_exponent) {
        return encoded_float(low_mask(exponent_bits + significand_bits));
    }

    const std::uint64_t significand =
        (value >> shift) & low_mask(significand_bits);
    return encoded_float((exponent << significand_bits) | significand);
}

}  // namespace exponential_histogram

namespace {

using snapshot_testing::check_snapshot;
using snapshot_testing::operator""_snap;
namespace gs = hegel::generators;


static_assert(!std::is_convertible_v<exponential_histogram::encoded_float,
                                     std::uint64_t>);
static_assert(!std::is_convertible_v<std::uint64_t,
                                     exponential_histogram::encoded_float>);

std::string conversion_table(unsigned exponent_bits, unsigned significand_bits,
                             std::uint64_t end) {
    std::string result;
    for (std::uint64_t value = 0; value <= end; ++value) {
        result += std::format("{} -> {}\n", value,
                              exponential_histogram::integer_to_float(
                                  value, exponent_bits, significand_bits)
                                  .representation());
    }
    return result;
}

hegel::Settings hegel_settings() {
    hegel::Settings result;
    result.test_cases = 1000;
    result.verbosity = hegel::Verbosity::Normal;
    result.derandomize = true;
    result.print_blob = true;
    return result;
}

std::uint64_t decode(std::uint64_t code, unsigned significand_bits) {
    if (significand_bits == exponential_histogram::kIntegerBits) {
        return code;
    }
    const std::uint64_t mask = (std::uint64_t{1} << significand_bits) - 1;
    const std::uint64_t fraction = code & mask;
    const std::uint64_t exponent = code >> significand_bits;
    if (exponent == 0) {
        return fraction;
    }
    return ((std::uint64_t{1} << significand_bits) | fraction)
           << (exponent - 1);
}

}  // namespace

TEST_CASE("integer_to_float exposes subnormals and rounded normal values") {
    check_snapshot(conversion_table(2, 2, 31), R"snap(
        |0 -> 0
        |1 -> 1
        |2 -> 2
        |3 -> 3
        |4 -> 4
        |5 -> 5
        |6 -> 6
        |7 -> 7
        |8 -> 8
        |9 -> 8
        |10 -> 9
        |11 -> 9
        |12 -> 10
        |13 -> 10
        |14 -> 11
        |15 -> 11
        |16 -> 12
        |17 -> 12
        |18 -> 12
        |19 -> 12
        |20 -> 13
        |21 -> 13
        |22 -> 13
        |23 -> 13
        |24 -> 14
        |25 -> 14
        |26 -> 14
        |27 -> 14
        |28 -> 15
        |29 -> 15
        |30 -> 15
        |31 -> 15
        )snap"_snap);
}

TEST_SUITE("hegel") {

TEST_CASE("integer_to_float is the greatest representable value not above input") {
    hegel::test(
        [](hegel::TestCase& tc) {
            HEGEL_DRAW(tc, exponent_bits,
                       gs::integers<unsigned>({.min_value = 0, .max_value = exponential_histogram::kIntegerBits}));
            HEGEL_DRAW(tc, significand_bits,
                       gs::integers<unsigned>({.min_value = 0, .max_value = exponential_histogram::kIntegerBits - exponent_bits}));
            HEGEL_DRAW(tc, value,
                       gs::integers<std::uint64_t>());

            const std::uint64_t code =
                exponential_histogram::integer_to_float(
                    value, exponent_bits, significand_bits)
                    .representation();
            const std::uint64_t rounded = decode(code, significand_bits);
            REQUIRE(rounded <= value);

            std::uint64_t max_code_1 = exponential_histogram::low_mask(significand_bits) + ((exponential_histogram::kIntegerBits - significand_bits) << significand_bits);
            std::uint64_t max_code_2 = exponential_histogram::low_mask(significand_bits + exponent_bits);
            std::uint64_t max_code = std::min(max_code_1, max_code_2);
            if (code < max_code) {
                auto x = decode(code + 1, significand_bits);
                REQUIRE(x > value);
            }
        },
        {"integer_to_float_rounds_down", __FILE__, __LINE__}, hegel_settings()
    );
}

}  // TEST_SUITE("hegel")
