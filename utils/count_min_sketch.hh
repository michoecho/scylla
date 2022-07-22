/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <vector>
#include <bit>
#include <algorithm>
#include "utils/chunked_vector.hh"

namespace utils {

class count_min_sketch {
private:
    // Random large odd integers.
    constexpr static uint64_t ROWS = 4;
#if 0
    constexpr static uint64_t BITS_PER_COUNTER = 4;
    constexpr static uint64_t HALVING_MASK = 0x7777'7777'7777'7777ull;
    constexpr static uint64_t COUNTER_MASK = 0xfull;
#else
    constexpr static uint64_t BITS_PER_COUNTER = 8;
    constexpr static uint64_t HALVING_MASK = 0x7f7f'7f7f'7f7f'7f7full;
    constexpr static uint64_t COUNTER_MASK = 0xffull;
#endif
    constexpr static uint64_t COUNTERS_PER_WORD = 64 / BITS_PER_COUNTER;
    constexpr static uint64_t COUNTERS_PER_ENTRY = 4;
    constexpr static uint64_t SEEDS[ROWS] = {0xc3a5c85c97cb3127ull, 0xb492b66fbe98f273ull, 0x9ae16a3b2f90404full, 0xcbf29ce484222325ull};
    utils::chunked_vector<uint64_t> _table;
    uint64_t _columns;
    uint64_t _lower_bits;

    static uint64_t div_ceil(uint64_t a, uint64_t b) {
        return (a / b) + !!(a % b);
    }
    void increment_counter(uint64_t total_index) {
        uint64_t global_index = total_index / COUNTERS_PER_WORD;
        uint64_t shift = (total_index % COUNTERS_PER_WORD) * BITS_PER_COUNTER;
        uint64_t shifted_mask = COUNTER_MASK << shift;
        uint64_t shifted_one = uint64_t(1) << shift;
        if ((_table[global_index] & shifted_mask) != shifted_mask) {
            _table[global_index] += shifted_one;
        }
    }
    uint64_t read_counter(uint64_t total_index) const {
        uint64_t global_index = total_index / COUNTERS_PER_WORD;
        uint64_t shift = (total_index % COUNTERS_PER_WORD) * BITS_PER_COUNTER;
        uint64_t result = (_table[global_index] >> shift) & COUNTER_MASK;
        return (_table[global_index] >> shift) & COUNTER_MASK;
    }
public:
    count_min_sketch(uint64_t expected_num_of_entries) {
        expected_num_of_entries = std::max(expected_num_of_entries, uint64_t(1));
        _columns = std::bit_ceil(expected_num_of_entries * COUNTERS_PER_ENTRY);
        _lower_bits = 64 - std::bit_width(_columns - 1);
        _table.resize(div_ceil(ROWS * _columns, COUNTERS_PER_WORD));
    }
    void increment(uint64_t key) {
        for (int i = 0; i < ROWS; ++i) {
            size_t index = (i * _columns) + (key * SEEDS[i] >> _lower_bits);
            increment_counter(index);
        }
    }
    uint64_t estimate(uint64_t key) const {
        uint64_t result = -1;
        for (int i = 0; i < ROWS; ++i) {
            size_t index = (i * _columns) + (key * SEEDS[i] >> _lower_bits);
            result = std::min(result, read_counter(index));
        }
        return result;
    }
    void halve() {
        for (uint64_t& v : _table) {
            v = (v >> 1) & HALVING_MASK;
        }
    }
};

} // namespace utils
