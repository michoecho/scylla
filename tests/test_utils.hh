#pragma once
#include <string_view>
#include <vector>
#include <iostream>
#include "../src/trie.hh"

std::vector<std::string> generate_all_strings(std::string_view chars, size_t max_len);
std::vector<std::vector<size_t>> generate_all_subsets(size_t n, size_t k);

inline static void log(std::string_view sv) {
    std::cout << sv << "\n";
}

inline static constexpr const_bytes operator""_b(const char* ptr, size_t sz) {
    return const_bytes{static_cast<const std::byte*>(static_cast<const void*>(ptr)), sz};
}
