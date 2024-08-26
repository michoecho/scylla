#include "test_utils.hh"
#include <algorithm>
#include <string>
#include <cassert>

std::vector<std::string> generate_all_strings(std::string_view chars_raw, size_t max_len) {
    std::string chars(chars_raw);
    std::ranges::sort(chars);
    std::ranges::unique(chars);

    std::vector<std::string> all_strings;
    all_strings.push_back("");
    size_t prev_old_n = 0;
    for (size_t i = 0; i < max_len; ++i) {
        size_t old_n = all_strings.size();
        for (size_t k = prev_old_n; k < old_n; ++k) {
            for (auto c : chars) {
                all_strings.push_back(all_strings[k]);
                all_strings.back().push_back(c);
            }
        }
        prev_old_n = old_n;
    }
    std::ranges::sort(all_strings);
    return all_strings;
}

std::vector<std::vector<size_t>> generate_all_subsets(size_t n, size_t k) {
    using sample = std::vector<size_t>;
    std::vector<size_t> wksp(k);
    auto first = wksp.begin();
    auto last = wksp.end();
    // Fill wksp with first possible sample.
    std::ranges::iota(wksp, 0);
    size_t n_samples = 0;
    std::vector<sample> samples;
    while (true) {
        samples.push_back(wksp);
        // Advance wksp to next possible sample.
        auto mt = last;
        --mt;
        while (mt > first && *mt == n - (last - mt)) {
            --mt;
        }
        if (mt == first && *mt == n - (last - mt)) {
            break;
        }
        ++(*mt);
        while (++mt != last) {
            *mt = *(mt - 1) + 1;
        }
    }
    return samples;
}