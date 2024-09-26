#pragma once

#include "sstables/index_reader.hh"
#include <array>
#include <memory>
#include <span>
#include <vector>
#include "utils/cached_file.hh"

using const_bytes = std::span<const std::byte>;

struct node;

struct trie_writer_output {
    virtual ~trie_writer_output() = default;
    virtual size_t serialized_size(const node&, size_t pos) const = 0;
    virtual size_t write(const node&) = 0;
    virtual size_t page_size() const = 0;
    virtual size_t bytes_left_in_page() = 0;
    virtual size_t pad_to_page_boundary() = 0;
    virtual size_t pos() const = 0;
};

struct payload {
    std::array<std::byte, 20> _payload_buf = {};
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
    void add(const_bytes key, int64_t data_file_offset);
    ssize_t finish();
    using buf = std::vector<std::byte>;

private:
    trie_writer_output& _out;
    trie_writer _wr = {_out};
    size_t _added_keys = 0;
    size_t _last_key_mismatch = 0;
    buf _last_key;
    int64_t _last_payload;
};

class row_index_trie_writer {
public:
    row_index_trie_writer(trie_writer_output&);
    ~row_index_trie_writer();
    void add(const_bytes first_ck, const_bytes last_ck, uint64_t data_file_offset, sstables::deletion_time);
    ssize_t finish();
    using buf = std::vector<std::byte>;

    struct row_index_payload {
        uint64_t data_file_offset;
        sstables::deletion_time dt;
    };

private:
    trie_writer_output& _out;
    trie_writer _wr = {_out};
    size_t _added_blocks = 0;
    size_t _last_sep_mismatch = 0;
    size_t _first_key_mismatch = 0;
    buf _first_key;
    buf _last_key;
    row_index_payload _last_payload;
};

struct node_parser {
    struct lookup_result {
        int idx;
        std::byte byte;
        uint64_t offset;
    };
    lookup_result (*lookup)(const_bytes, std::byte transition);
    lookup_result (*get_child)(const_bytes, int idx, bool forward);
};


struct payload_result {
    uint8_t bits;
    const_bytes bytes;
};


struct reader_node {
    struct page_ptr : cached_file::ptr_type {
        using parent = cached_file::ptr_type;
        page_ptr() noexcept = default;
        page_ptr(parent&& x) noexcept : parent(std::move(x)) {}
        page_ptr(const page_ptr& other) noexcept : parent(other->share()) {}
        page_ptr(page_ptr&&) noexcept = default;
        page_ptr& operator=(page_ptr&&) noexcept = default;
        page_ptr& operator=(const page_ptr& other) noexcept {
            parent::operator=(other->share());
            return *this;
        }
    };
    size_t pos;
    uint16_t n_children;
    uint8_t payload_bits;
    std::byte transition;

    node_parser::lookup_result lookup(std::byte transition, const page_ptr& ptr);
    node_parser::lookup_result get_child(int idx, bool forward, const page_ptr& ptr);
    payload_result payload(const page_ptr& ptr) const;
    const_bytes raw(const page_ptr& ptr) const;
};

struct row_index_header {
    sstables::key partition_key = bytes();
    uint64_t trie_root;
    uint64_t data_offset;
    tombstone partition_tombstone;
};

struct trie_reader_input {
    virtual ~trie_reader_input();
    virtual future<reader_node> read(uint64_t offset) = 0;
    virtual future<row_index_header> read_row_index_header(uint64_t offset, reader_permit) = 0;
};

struct node_cursor {
    reader_node node;
    int child_idx;
};

enum class set_result {
    eof,
    no_match,
    match,
};

class my_trie_reader_input;

class trie_cursor {
    std::reference_wrapper<my_trie_reader_input> _in;
    utils::small_vector<node_cursor, 8> _path;
    utils::small_vector<reader_node::page_ptr, 8> _pages;
public:
    trie_cursor(trie_reader_input&);
    trie_cursor& operator=(const trie_cursor&) = default;
    future<void> init(uint64_t root_offset);
    future<set_result> set_before(const_bytes key);
    future<set_result> fast_traverse(const_bytes key);
    future<set_result> set_before2(const_bytes key);
    future<set_result> set_after(const_bytes key);
    future<set_result> step();
    future<set_result> step_back();
    payload_result payload() const;
    bool eof() const;
    bool initialized() const;
    void reset();
    bool push(uint64_t offset);
    future<> push_page_and_node(uint64_t offset);
    future<> push_maybe_page_and_node(uint64_t offset);
    void pop();
};

int64_t payload_to_offset(const_bytes p);

class index_cursor {
    trie_cursor _partition_cursor;
    trie_cursor _row_cursor;
    std::reference_wrapper<trie_reader_input> _in_row;
    reader_permit _permit;
    std::optional<row_index_header> _partition_metadata;
    future<> maybe_read_metadata();
public:
    index_cursor(trie_reader_input& par, trie_reader_input& row, reader_permit);
    index_cursor& operator=(const index_cursor&) = default;
    future<> init(uint64_t root_offset);
    uint64_t data_file_pos(uint64_t file_size) const;
    tombstone open_tombstone() const;
    const std::optional<row_index_header>& partition_metadata() const;
    future<set_result> set_before_partition(const_bytes);
    future<set_result> set_after_partition(const_bytes);
    future<set_result> next_partition();
    future<set_result> set_before_row(const_bytes);
    future<set_result> set_after_row(const_bytes);
    future<set_result> set_after_row_cold(const_bytes);
    bool row_cursor_set() const;
};

class trie_index_reader : public sstables::index_reader {
    std::unique_ptr<trie_reader_input> _in;
    std::unique_ptr<trie_reader_input> _in_row;
    schema_ptr _s;
    reader_permit _permit;
    index_cursor _lower;
    index_cursor _upper;
    uint64_t _root;
    uint64_t _total_file_size;
    bool _initialized = false;

    future<row_index_header> read_row_index_header(uint64_t);
    std::vector<std::byte> translate_key(dht::ring_position_view key);
    future<> maybe_init();
public:
    trie_index_reader(std::unique_ptr<trie_reader_input> in, std::unique_ptr<trie_reader_input> in_row, uint64_t root_offset, uint64_t total_file_size, schema_ptr s, reader_permit);
    virtual future<> close() noexcept override;
    virtual sstables::data_file_positions_range data_file_positions() const override;
    virtual future<std::optional<uint64_t>> last_block_offset() override;
    virtual future<bool> advance_lower_and_check_if_present(dht::ring_position_view key, std::optional<position_in_partition_view> pos = {}) override;
    virtual future<> advance_to_next_partition() override;
    virtual sstables::indexable_element element_kind() const override;
    virtual future<> advance_to(dht::ring_position_view pos) override;
    virtual future<> advance_to(position_in_partition_view pos) override;
    virtual std::optional<sstables::deletion_time> partition_tombstone() override;
    virtual std::optional<partition_key> get_partition_key() override;
    virtual partition_key get_partition_key_prefix() override;
    virtual bool partition_data_ready() const override;
    virtual future<> read_partition_data() override;
    virtual future<> advance_reverse(position_in_partition_view pos) override;
    virtual future<> advance_to(const dht::partition_range& range) override;
    virtual future<> advance_reverse_to_next_partition() override;
    virtual std::optional<sstables::open_rt_marker> end_open_marker() const override;
    virtual std::optional<sstables::open_rt_marker> reverse_end_open_marker() const override;
    virtual sstables::clustered_index_cursor* current_clustered_cursor() override;
    virtual uint64_t get_data_file_position() override;
    virtual uint64_t get_promoted_index_size() override;
    virtual bool eof() const override;
};

class seastar_file_trie_reader_input : public trie_reader_input {
    cached_file& _f;
    seastar::file _f_file;
    reader_permit _permit;
public:
    seastar_file_trie_reader_input(cached_file&, reader_permit);
    future<reader_node> read(uint64_t offset) override;
    future<row_index_header> read_row_index_header(uint64_t offset, reader_permit rp) override;
    future<> close();
};

std::unique_ptr<trie_writer_output> make_trie_writer_output(sstables::file_writer&, size_t page_size);
std::unique_ptr<trie_reader_input> make_trie_reader_input(cached_file&, reader_permit);