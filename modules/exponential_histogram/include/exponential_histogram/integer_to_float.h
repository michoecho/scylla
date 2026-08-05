#pragma once

#include <cstdint>

namespace exponential_histogram {

class encoded_float {
public:
    explicit constexpr encoded_float(std::uint64_t representation) noexcept
        : representation_(representation) {}

    [[nodiscard]] constexpr std::uint64_t representation() const noexcept {
        return representation_;
    }

    friend constexpr bool operator==(encoded_float, encoded_float) = default;

private:
    std::uint64_t representation_;
};

// Returns the strongly typed representation of value rounded down to the
// unsigned floating-point format [exponent | significand]. Exponent zero
// denotes subnormals, whose decoded integer values are the significand itself;
// normal values have an implicit leading significand bit.
encoded_float integer_to_float(std::uint64_t value,
                               unsigned exponent_bits,
                               unsigned significand_bits);

}  // namespace exponential_histogram
