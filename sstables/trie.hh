#pragma once

#include <array>
#include <memory>
#include <span>
#include <vector>

using const_bytes = std::span<const std::byte>;

struct node;

struct trie_writer_output {
    virtual size_t serialized_size(const node&, size_t pos) const = 0;
    virtual size_t write(const node&, size_t depth) = 0;
    virtual size_t page_size() const = 0;
    virtual size_t bytes_left_in_page() = 0;
    virtual size_t pad_to_page_boundary() = 0;
    virtual size_t pos() const = 0;
};

struct payload {
    std::array<std::byte, 19> _payload_buf = {};
    uint8_t _payload_bits = {};
    uint8_t _payload_size = {};
    payload() noexcept;
    payload(uint8_t bits, const_bytes blob) noexcept;
    const_bytes blob() const noexcept;
};

struct node {
    payload _payload;
    std::byte _transition = {};
    bool _has_out_of_page_children = false;
    bool _has_out_of_page_descendants = false;
    std::vector<std::unique_ptr<node>> _children;
    ssize_t _node_size = -1;
    ssize_t _branch_size = -1;
    ssize_t _output_pos = -1;

    node(std::byte b) noexcept;
    node* add_child(std::byte b);
    void set_payload(const payload&) noexcept;
    const_bytes get_payload() const noexcept;
    size_t recalc_sizes(const trie_writer_output&, size_t starting_pos);
    void write(trie_writer_output&);
};

class trie_writer {
public:
    trie_writer(trie_writer_output&);
    ~trie_writer();
    void add(size_t depth, const_bytes key_tail, const payload&);
    ssize_t finish();

private:
    class impl;
    std::unique_ptr<impl> _pimpl;
};

class partition_index_trie_writer {
public:
    partition_index_trie_writer(trie_writer_output&);
    ~partition_index_trie_writer();
    void add(const_bytes key, uint64_t data_file_offset);
    ssize_t finish();
    using buf = std::vector<std::byte>;

private:
    trie_writer_output& _out;
    trie_writer _wr = {_out};
    size_t _added_keys = 0;
    size_t _last_key_mismatch = 0;
    buf _last_key;
    uint64_t _last_payload;
};