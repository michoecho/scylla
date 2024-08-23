#include "../src/trie.hh"
#include <algorithm>
#include <fmt/ranges.h>
#include <format>
#include <gtest/gtest.h>
#include <iostream>

static void log(std::string_view sv) {
    std::cout << sv << "\n";
}

static constexpr const_bytes operator""_b(const char* ptr, size_t sz) {
    return const_bytes{static_cast<const std::byte*>(static_cast<const void*>(ptr)), sz};
}

void test_one(const std::vector<std::string>& inputs) {
    assert(std::ranges::is_sorted(inputs));
    struct out final : trie_writer_output {
        size_t _pos = 0;
        struct serialized {
            bool payload;
            std::vector<std::pair<std::byte, size_t>> children;
        };
        std::map<size_t, serialized> _output;
        virtual size_t serialized_size(const node& x, size_t start_pos) const override {
            size_t penalty = 0;
            for (const auto& z : x._children) {
                if (z->_output_pos >= 0 && start_pos - z->_output_pos > 1) {
                    penalty = 1;
                }
            }
            return 1 + penalty;
        }
        virtual size_t write(const node& x, size_t depth) override {
            serialized s;
            for (const auto& c : x._children) {
                EXPECT_GE(c->_output_pos, 0);
                s.children.push_back({c->_transition, c->_output_pos});
            }
            s.payload = x._payload._payload_bits;
            // log(std::format("{:{}}: {} {} ", "", depth, char(x._transition), bytes_as_string(x._payload.blob())));
            _output.insert({_pos, std::move(s)});
            auto sz = serialized_size(x, _pos);
            _pos += sz;
            return sz;
        }
        virtual size_t page_size() const override {
            return 2;
        };
        virtual size_t pad_to_page_boundary() override {
            size_t pad = bytes_left_in_page();
            _pos += pad;
            return pad;
        };
        virtual size_t bytes_left_in_page() override {
            return round_up(_pos + 1, page_size()) - _pos;
        };
        virtual size_t pos() const override {
            return _pos;
        }
    };
    out o;
    auto wr = trie_writer(o);
    if (inputs.size() == 0) {
        ASSERT_EQ(wr.finish(), -1);
        ASSERT_EQ(o.pos(), 0);
        return;
    }
    payload p(1, ""_b);
    wr.add(0, string_as_bytes(inputs[0]), p);
    for (size_t i = 1; i < inputs.size(); ++i) {
        auto depth = std::ranges::mismatch(inputs[i], inputs[i - 1]).in1 - inputs[i].begin();
        assert(depth < inputs[i].size());
        wr.add(depth, string_as_bytes(inputs[i]).subspan(depth), p);
    }
    size_t root_pos = wr.finish();

    log("Outputs");
    for (const auto& [x, y] : o._output) {
        log(std::format("{} {}", x, y.payload ? "T" : ""));
        for (const auto& [b, ptr] : y.children) {
            log(std::format("    {} {}", char(b), ptr));
        }
    }

    std::vector<std::string> reconstructed;
    struct local_state {
        size_t _idx;
        int _stage;
    };
    std::string charstack;
    std::vector<local_state> stack;
    stack.push_back({root_pos, -1});
    while (stack.size()) {
        auto& [idx, stage] = stack.back();
        const auto& node = o._output[idx];
        log(std::format("Visit {} {}", idx, stage));
        if (stage < 0) {
            if (node.payload) {
                reconstructed.push_back(charstack);
            }
            stage += 1;
            continue;
        }
        if (stage < node.children.size()) {
            log(std::format("Visit2 {} {}", idx, stage));
            stage += 1;
            charstack.push_back(char(node.children[stage - 1].first));
            stack.push_back({node.children[stage - 1].second, -1});
            continue;
        }
        log(std::format("Pop {} {}", idx, stage));
        stack.pop_back();
        if (charstack.size()) {
            charstack.pop_back();
        }
    }
    log(std::format("Reconstructed: {}", reconstructed.size()));
    for (const auto& c : reconstructed) {
        log(c);
    }
    ASSERT_TRUE(std::ranges::equal(inputs, reconstructed));
}

TEST(TrieWriter, Exhaustive) {
    size_t max_len = 4;
    char chars[] = {'a', 'b'};
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
    log(fmt::format("{}", fmt::join(all_strings, "\n")));

    using sample = std::array<size_t, 4>;
    sample wksp;
    auto first = wksp.begin();
    auto last = wksp.end();
    // Fill wksp with first possible sample.
    std::ranges::iota(wksp, 0);
    size_t n_samples = 0;
    std::vector<sample> samples;
    size_t N = all_strings.size();
    while (true) {
        samples.push_back(wksp);
        // Advance wksp to next possible sample.
        auto mt = last;
        --mt;
        while (mt > first && *mt == N - (last - mt)) {
            --mt;
        }
        if (mt == first && *mt == N - (last - mt)) {
            break;
        }
        ++(*mt);
        while (++mt != last) {
            *mt = *(mt - 1) + 1;
        }
    }

    for (const auto& s : samples) {
        std::vector<std::string> inputs;
        for (auto z : s) {
            inputs.push_back(all_strings[z]);
        }
        log(fmt::format("[{}]", fmt::join(inputs, ", ")));
        ASSERT_NO_FATAL_FAILURE(test_one(inputs));
    }
    ASSERT_NO_FATAL_FAILURE(test_one({}));
}
