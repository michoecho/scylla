#pragma once

#include "sstables/index_reader.hh"
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
    buf _last_separator;
    buf _last_key;
    row_index_payload _last_payload;
};

struct reader_node {
    struct child {
        std::byte transition;
        uint64_t offset;
    };
    std::vector<child> children;
    std::vector<std::byte> payload;
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

class trie_cursor {
    std::reference_wrapper<trie_reader_input> _in;
    std::vector<node_cursor> _path;
public:
    trie_cursor(trie_reader_input&);
    trie_cursor& operator=(const trie_cursor&) = default;
    future<void> init(uint64_t root_offset);
    future<set_result> set_before(const_bytes key);
    future<set_result> set_after(const_bytes key);
    future<set_result> step();
    future<set_result> step_back();
    const_bytes payload() const;
    bool eof() const;
    bool initialized() const;
    void reset();
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
    bool row_cursor_set() const;
};

class trie_index_reader : public sstables::index_reader {
    trie_reader_input& _in;
    trie_reader_input& _in_row;
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
    trie_index_reader(trie_reader_input& in, trie_reader_input& in_row, uint64_t root_offset, uint64_t total_file_size, schema_ptr s, reader_permit);
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

namespace capnp {
    class MessageBuilder;
}
class seastar_file_trie_reader_input : public trie_reader_input {
    seastar::file _f;
public:
    seastar_file_trie_reader_input(seastar::file);
    future<reader_node> read(uint64_t offset) override;
    future<row_index_header> read_row_index_header(uint64_t offset, reader_permit rp) override;
};