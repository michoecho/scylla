#include "../src/trie.hh"
#include "node.capnp.h"
#include "test_utils.hh"
#include <algorithm>
#include <capnp/message.h>
#include <capnp/serialize-packed.h>
#include <fcntl.h>
#include <fmt/ranges.h>
#include <format>
#include <gtest/gtest.h>
#include <unistd.h>

struct simple_file_writer final : trie_writer_output {
    size_t _pos = 0;
    int _fd;
    simple_file_writer(int fd)
        : _fd(fd) { }
    void serialize(const node& x, ::capnp::MessageBuilder& mb) const {
        Node::Builder node = mb.initRoot<Node>();
        ::capnp::List<Child>::Builder children = node.initChildren(x._children.size());
        for (size_t i = 0; i < x._children.size(); ++i) {
            Child::Builder cb = children[i];
            cb.setOffset(x._children[i]->_output_pos);
            cb.setTransition(uint8_t(x._children[i]->_transition));
        }
        Payload::Builder payload = node.initPayload();
        payload.setBits(x._payload._payload_bits);
        auto payload_bytes = x._payload.blob();
        payload.setBytes({(const kj::byte*)payload_bytes.data(), payload_bytes.size()});
    }
    virtual size_t serialized_size(const node& x, size_t start_pos) const override {
        ::capnp::MallocMessageBuilder message;
        serialize(x, message);
        return ::capnp::computeSerializedSizeInWords(message) * 8;
    }
    virtual size_t write(const node& x, size_t depth) override {
        for (size_t i = 0; i < x._children.size(); ++i) {
            assert(x._children[i]->_output_pos >= 0);
        }
        ::capnp::MallocMessageBuilder message;
        serialize(x, message);
        size_t sz = ::capnp::computeSerializedSizeInWords(message) * 8;
        EXPECT_EQ(lseek(_fd, 0, SEEK_CUR), _pos);
        ::capnp::writeMessageToFd(_fd, message);
        _pos += sz;
        EXPECT_EQ(lseek(_fd, 0, SEEK_CUR), _pos);
        return sz;
    }
    virtual size_t page_size() const override {
        return 4096;
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

void test_one(const std::vector<std::string>& inputs) {
    assert(std::ranges::is_sorted(inputs));

    auto muh_fd = open("/tmp/trie_test_file", O_TRUNC | O_RDWR | O_CREAT, 0600);
    auto closer = kj::defer([muh_fd] { close(muh_fd); });
    if (muh_fd < 0) {
        perror("open");
        abort();
    }

    auto o = simple_file_writer(muh_fd);
    auto wr = partition_index_trie_writer(o);
    if (inputs.size() == 0) {
        ASSERT_EQ(wr.finish(), -1);
        ASSERT_EQ(o.pos(), 0);
        return;
    }
    payload p(1, ""_b);
    wr.add(string_as_bytes(inputs[0]), 64);
    for (size_t i = 1; i < inputs.size(); ++i) {
        wr.add(string_as_bytes(inputs[i]), 64);
    }
    size_t root_pos = wr.finish();

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
        lseek(muh_fd, idx, SEEK_SET);
        EXPECT_EQ(lseek(muh_fd, 0, SEEK_CUR), idx);
        ::capnp::StreamFdMessageReader message(muh_fd);
        Node::Reader node = message.getRoot<Node>();
        if (stage < 0) {
            if (node.getPayload().getBits()) {
                reconstructed.push_back(charstack);
            }
            stage += 1;
            continue;
        }
        if (stage < node.getChildren().size()) {
            stage += 1;
            charstack.push_back(char(node.getChildren()[stage - 1].getTransition()));
            stack.push_back({node.getChildren()[stage - 1].getOffset(), -1});
            continue;
        }
        stack.pop_back();
        if (charstack.size()) {
            charstack.pop_back();
        }
    }
    log(std::format("Reconstructed: {}", reconstructed.size()));
    for (const auto& c : reconstructed) {
        log(c);
    }
    auto is_prefix = [](const auto& a, const auto& b) {
        return std::ranges::mismatch(a, b).in1 == a.end();
    };
    ASSERT_TRUE(reconstructed.size() == inputs.size());
    ASSERT_TRUE(std::ranges::is_sorted(reconstructed));
    for (size_t i = 0; i < inputs.size(); ++i) {
        ASSERT_TRUE(is_prefix(reconstructed[i], inputs[i]));
        if (i + 1 < inputs.size()) {
            ASSERT_TRUE(equiv(is_prefix(reconstructed[i], inputs[i + 1]), is_prefix(inputs[i], inputs[i + 1])));
        }
    }
}

TEST(TriePartitionIndexWriter, Exhaustive) {
    size_t max_len = 4;
    std::vector<std::string> all_strings = generate_all_strings("ab", max_len);
    log(fmt::format("{}", fmt::join(all_strings, "\n")));

    std::vector<std::vector<size_t>> samples = generate_all_subsets(all_strings.size(), 4);

    for (const auto& s : samples) {
        std::vector<std::string> inputs;
        for (auto i : s) {
            inputs.push_back(all_strings[i]);
        }
        log(fmt::format("[{}]", fmt::join(inputs, ", ")));
        ASSERT_NO_FATAL_FAILURE(test_one(inputs));
    }
    ASSERT_NO_FATAL_FAILURE(test_one({}));
}
