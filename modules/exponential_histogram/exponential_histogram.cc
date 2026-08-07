#include "exponential_histogram/exponential_histogram.h"

#include <algorithm>
#include <format>
#include <limits>
#include <stdexcept>
#include <string>

#include <doctest/doctest.h>
#include <hegel/hegel.h>

#include "snapshot/check.h"

namespace exponential_histogram {
namespace {

constexpr unsigned kIntegerBits = std::numeric_limits<std::uint64_t>::digits;

std::uint64_t low_mask(unsigned bits) {
    return bits == kIntegerBits
               ? std::numeric_limits<std::uint64_t>::max()
               : (std::uint64_t{1} << bits) - 1;
}

std::uint64_t float_to_integer(std::uint64_t value, unsigned significand_bits) {
    const std::uint64_t significand = value & low_mask(significand_bits);
    const std::uint64_t exponent = value >> significand_bits;
    if (exponent == 0) {
        return significand;
    }

    const unsigned shift = static_cast<unsigned>(exponent - 1);
    const unsigned __int128 decoded =
        ((static_cast<unsigned __int128>(1) << significand_bits) | significand)
        << shift;
    return decoded > std::numeric_limits<std::uint64_t>::max()
               ? std::numeric_limits<std::uint64_t>::max()
               : static_cast<std::uint64_t>(decoded);
}


}  // namespace

exponential_histogram::exponential_histogram(std::uint64_t end,
                                             unsigned exponent_bits,
                                             unsigned significand_bits)
    : end_(0),
      exponent_bits_(exponent_bits),
      significand_bits_(significand_bits) {

    const std::uint64_t greatest_code =
        integer_to_float(std::numeric_limits<std::uint64_t>::max(),
                         exponent_bits, significand_bits)
            .representation();
    const std::uint64_t greatest =
        float_to_integer(greatest_code, significand_bits);
    end_ = std::min(end, greatest);

    const std::uint64_t last =
        integer_to_float(end_, exponent_bits, significand_bits).representation();
    if (last >= std::numeric_limits<std::size_t>::max()) {
        throw std::length_error("histogram has too many buckets");
    }
    buckets_.reserve(static_cast<std::size_t>(last + 1));
    for (std::uint64_t code = 0; code <= last; ++code) {
        const std::uint64_t lower = float_to_integer(code, significand_bits);
        const std::uint64_t upper = code == last
                                        ? end_ + 1
                                        : float_to_integer(code + 1,
                                                           significand_bits);
        buckets_.push_back({lower, upper, 0});
    }
}

void exponential_histogram::insert(std::uint64_t value) {
    value = std::min(value, end_);
    const auto index = integer_to_float(value, exponent_bits_, significand_bits_)
                           .representation();
    ++buckets_[static_cast<std::size_t>(index)].number_of_elements;
}

std::span<const bucket> exponential_histogram::buckets() const noexcept {
    return buckets_;
}

}  // namespace exponential_histogram

namespace {

using Histogram = exponential_histogram::exponential_histogram;
using snapshot_testing::check_snapshot;
using snapshot_testing::operator""_snap;
namespace gs = hegel::generators;


std::string render(const Histogram& histogram) {
    std::string result;
    for (const auto& bucket : histogram.buckets()) {
        result += std::format("[{}, {}) {}\n", bucket.lower_bound,
                              bucket.upper_bound,
                              bucket.number_of_elements);
    }
    return result;
}

hegel::Settings hegel_settings() {
    hegel::Settings result;
    result.verbosity = hegel::Verbosity::Quiet;
    result.derandomize = true;
    result.print_blob = false;
    return result;
}

}  // namespace

TEST_CASE("exponential_histogram groups values and clamps at end") {
    Histogram histogram(21, 3, 2);
    for (const std::uint64_t value : {0, 1, 4, 5, 7, 8, 9, 15, 16, 20,
                                      21, 22, 100}) {
        histogram.insert(value);
    }
    check_snapshot(render(histogram), R"snap(
        |[0, 1) 1
        |[1, 2) 1
        |[2, 3) 0
        |[3, 4) 0
        |[4, 5) 1
        |[5, 6) 1
        |[6, 7) 0
        |[7, 8) 1
        |[8, 10) 2
        |[10, 12) 0
        |[12, 14) 0
        |[14, 16) 1
        |[16, 20) 1
        |[20, 22) 4
        )snap"_snap);
}

TEST_CASE("zero exponent and significand bits form a counter") {
    Histogram histogram(100, 0, 0);
    histogram.insert(0);
    histogram.insert(42);
    histogram.insert(1000);
    check_snapshot(render(histogram), "[0, 1) 3\n"_snap);
}

TEST_CASE("either floating-point field may have zero bits") {
    Histogram no_exponent(100, 0, 2);
    Histogram no_significand(20, 3, 0);
    for (const std::uint64_t value : {0, 1, 2, 3, 4, 7, 8, 20, 100}) {
        no_exponent.insert(value);
        no_significand.insert(value);
    }

    check_snapshot("exponent=0\n" + render(no_exponent) +
                       "significand=0\n" + render(no_significand),
                   R"snap(
                       |exponent=0
                       |[0, 1) 1
                       |[1, 2) 1
                       |[2, 3) 1
                       |[3, 4) 6
                       |significand=0
                       |[0, 1) 1
                       |[1, 2) 1
                       |[2, 4) 2
                       |[4, 8) 2
                       |[8, 16) 1
                       |[16, 21) 2
                       )snap"_snap);
}

TEST_SUITE("hegel") {

TEST_CASE("histogram buckets partition inserted values and preserve their count") {
    hegel::test(
        [](hegel::TestCase& tc) {
            HEGEL_DRAW(tc, exponent_bits,
                       gs::integers<unsigned>({.min_value = 0, .max_value = 3}));
            HEGEL_DRAW(tc, significand_bits,
                       gs::integers<unsigned>({.min_value = 0, .max_value = 3}));
            HEGEL_DRAW(tc, end,
                       gs::integers<std::uint64_t>(
                           {.min_value = 0, .max_value = 200}));
            HEGEL_DRAW(tc, a, gs::integers<std::uint64_t>(
                                  {.min_value = 0, .max_value = 400}));
            HEGEL_DRAW(tc, b, gs::integers<std::uint64_t>(
                                  {.min_value = 0, .max_value = 400}));
            HEGEL_DRAW(tc, c, gs::integers<std::uint64_t>(
                                  {.min_value = 0, .max_value = 400}));
            HEGEL_DRAW(tc, d, gs::integers<std::uint64_t>(
                                  {.min_value = 0, .max_value = 400}));

            Histogram histogram(end, exponent_bits, significand_bits);
            for (const auto value : {a, b, c, d}) {
                histogram.insert(value);
            }

            std::uint64_t count = 0;
            std::uint64_t previous_upper = 0;
            for (const auto& bucket : histogram.buckets()) {
                if (bucket.lower_bound != previous_upper ||
                    bucket.lower_bound >= bucket.upper_bound) {
                    throw std::runtime_error("buckets do not form a partition");
                }
                previous_upper = bucket.upper_bound;
                count += bucket.number_of_elements;
            }
            if (count != 4) {
                throw std::runtime_error("histogram lost inserted elements");
            }
        },
        {"histogram_partition_and_count", __FILE__, __LINE__}, hegel_settings());
}

}  // TEST_SUITE("hegel")
