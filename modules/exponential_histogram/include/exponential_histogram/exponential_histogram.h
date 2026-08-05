#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include "exponential_histogram/integer_to_float.h"

namespace exponential_histogram {
struct bucket {
    std::uint64_t lower_bound;
    std::uint64_t upper_bound;
    std::uint64_t number_of_elements;
};

class exponential_histogram {
public:
    exponential_histogram(std::uint64_t end,
                          unsigned exponent_bits,
                          unsigned significand_bits);

    void insert(std::uint64_t value);
    [[nodiscard]] std::span<const bucket> buckets() const noexcept;

private:
    std::uint64_t end_;
    unsigned exponent_bits_;
    unsigned significand_bits_;
    std::vector<bucket> buckets_;
};

}  // namespace exponential_histogram
