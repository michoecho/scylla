/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

//
// This implementation of a trie-based index follows Cassandra's BTI implementation. 
//
// For an overview of the format, see:
// https://github.com/apache/cassandra/blob/9dfcfaee6585a3443282f56d54e90446dc4ff012/src/java/org/apache/cassandra/io/sstable/format/bti/BtiFormat.md
//
// For the writer logic, and it's arbitrary heuristics for splitting the trie into pages, see:
// https://github.com/apache/cassandra/blob/9dfcfaee6585a3443282f56d54e90446dc4ff012/src/java/org/apache/cassandra/io/tries/IncrementalTrieWriterPageAware.java#L32
//
// (The reader logic doesn't have much in the way of design -- the design of readers must follow the format).

#pragma clang optimize off

#include "trie.hh"
#include <algorithm>
#include <cassert>
#include <immintrin.h>
#include "sstables/index_reader.hh"
#include "utils/small_vector.hh"
#include "trie_writer.hh"
#include "trie_serializer.hh"
#include "trie_bti_node_reader.hh"
#include "types/comparable_bytes.hh"

namespace trie {

using namespace trie;

// Pimpl boilerplate
bti_trie_sink::bti_trie_sink() = default;
bti_trie_sink::~bti_trie_sink() = default;
bti_trie_sink& bti_trie_sink::operator=(bti_trie_sink&&) = default;
bti_trie_sink::bti_trie_sink(std::unique_ptr<bti_trie_sink_impl> x) : _impl(std::move(x)) {}

static_assert(trie_writer_sink<bti_trie_sink_impl>);

// The job of this class is to take a stream of partition keys and their index payloads
// (Data.db or Rows.db position, and hash bits for filtering false positives out)
// and translate it to a minimal trie which still allows for efficient queries.
//
// For that, each key in the trie is trimmed to the shortest length which still results
// in a unique leaf.
// For example, assume that keys ABCXYZ, ABDXYZ, ACXYZ are added to the trie.
// A "naive" trie would look like this:
//     A
//     B---C
//     C-D X
//     X X Y
//     Y Y Z
//     Z Z
//
// This writer creates:
//     A
//     B---C
//     C-D
// 
// Note: the trie-serialization format of partition keys is prefix-free,
// which guarantees that only leaf nodes carry a payload. 
// 
// In practice, Output == trie_serializer.
// But for testability reasons, we don't hardcode the output type.
// (So that tests can test just the logic of this writer, without
// roping in file I/O and the serialization format)
// For performance reasons we don't want to make the output virtual,
// so we instead make it a template parameter.
template <trie_writer_sink Output>
class partition_index_writer_impl {
public:
    partition_index_writer_impl(Output&);
    void add(const_bytes key, int64_t data_file_pos, uint8_t hash_bits);
    sink_pos finish();
private:
    // The lower trie-writing layer, oblivious to the semantics of the partition index.
    trie_writer<Output> _wr;
    // Counter of added keys.
    size_t _added_keys = 0;

    // We "buffer" one key in the writer. To write an entry to the trie_writer,
    // we want to trim the key to the shortest prefix which is still enough
    // to differentiate the key from its neighbours. For that, we need to see
    // the *next* entry first.
    //
    // The first position in _last_key where it differs from its predecessor.
    // After we see the next entry, we will also know the first position where it differs
    // from the successor, and we will be able to determine the shortest unique prefix. 
    size_t _last_key_mismatch = 0;
    // The key added most recently.
    std::vector<std::byte> _last_key;
    // The payload of _last_key: the Data.db position and the hash bits. 
    int64_t _last_data_file_pos;
    uint8_t _last_hash_bits;
};

template <trie_writer_sink Output>
partition_index_writer_impl<Output>::partition_index_writer_impl(Output& out)
    : _wr(out) {
}

template <trie_writer_sink Output>
void partition_index_writer_impl<Output>::add(const_bytes key, int64_t data_file_pos, uint8_t hash_bits) {
    expensive_log("partition_index_writer_impl::add: this={} key={}, data_file_pos={}", fmt::ptr(this), fmt_hex(key), data_file_pos); 
    if (_added_keys > 0) {
        // First position where the new key differs from the last key.
        size_t mismatch = std::ranges::mismatch(key, _last_key).in2 - _last_key.begin();
        // From `_last_key_mismatch` (mismatch position between `_last_key` and its predecessor)
        // and `mismatch` (mismatch position between `_last_key` and its successor),
        // compute the minimal needed prefix of `_last_key`.
        // FIXME: the std::min(..., _last_key.size()) isn't really needed, since a key can't be a prefix of another key.
        size_t needed_prefix = std::min(std::max(_last_key_mismatch, mismatch) + 1, _last_key.size());
        // The new nodes/characters we have to add to the trie.
        auto tail = std::span(_last_key).subspan(_last_key_mismatch, needed_prefix - _last_key_mismatch);
        // Serialize the payload.
        std::array<std::byte, 9> payload_bytes;
        payload_bytes[0] = std::byte(_last_hash_bits);
        auto payload_bits = div_ceil(std::bit_width<uint64_t>(std::abs(_last_data_file_pos)) + 1, 8);
        uint64_t pos_be = seastar::cpu_to_be(_last_data_file_pos << 8*(8 - payload_bits));
        memcpy(&payload_bytes[1], &pos_be, 8);
        // Pass the new node chain and its payload to the lower layer.
        // Note: we pass (payload_bits | 0x8) because the additional 0x8 bit indicates that hash bits are present.
        // (Even though currently they are always present).
        _wr.add(_last_key_mismatch, tail, trie_payload(payload_bits | 0x8, std::span(payload_bytes).subspan(0, payload_bits + 1)));
        // Update _last_* variables with the new key.
        _last_key_mismatch = mismatch;
    }
    _added_keys += 1;
    // Update _last_* variables with the new key.
    _last_key.assign(key.begin(), key.end());
    _last_data_file_pos = data_file_pos;
    _last_hash_bits = hash_bits;
}
template <trie_writer_sink Output>
sink_pos partition_index_writer_impl<Output>::finish() {
    if (_added_keys > 0) {
        // Mostly duplicates the code from add(), except there is only one mismatch position to care about,
        // not two which we have to take a max() of.
        //
        // FIXME: the std::min(..., _last_key.size()) isn't really needed, since a key can't be a prefix of another key.
        //
        // FIXME: maybe somehow get rid of the code duplication between here and add().
        size_t needed_prefix = std::min(_last_key_mismatch + 1, _last_key.size());
        // The new nodes/characters we have to add to the trie.
        auto tail = std::span(_last_key).subspan(_last_key_mismatch, needed_prefix - _last_key_mismatch);
        // Serialize the payload.
        std::array<std::byte, 9> payload_bytes;
        uint64_t pos_be = seastar::cpu_to_be(_last_data_file_pos);
        payload_bytes[0] = std::byte(_last_hash_bits);
        auto payload_bits = div_ceil(std::bit_width<uint64_t>(std::abs(_last_data_file_pos)) + 1, 8);
        std::memcpy(&payload_bytes[1], (const char*)(&pos_be) + 8 - payload_bits, payload_bits);
        // Pass the new node chain and its payload to the lower layer.
        // Note: we pass (payload_bits | 0x8) because the additional 0x8 bit indicates that hash bits are present.
        // (Even though currently they are always present, and the reader assumes so).
        _wr.add(_last_key_mismatch, tail, trie_payload(payload_bits | 0x8, std::span(payload_bytes).subspan(0, payload_bits + 1)));
    }
    return _wr.finish();
}

// Instantiation of partition_index_writer_impl with `Output` == `bti_trie_sink_impl`.
// This is the partition trie writer which is actually used in practice.
// Other instantiations of partition_index_writer_impl are only for testing.
class partition_trie_writer::impl : public partition_index_writer_impl<bti_trie_sink_impl> {
    using partition_index_writer_impl::partition_index_writer_impl;
};

// Pimpl boilerplate
partition_trie_writer::partition_trie_writer() = default;
partition_trie_writer::~partition_trie_writer() = default;
partition_trie_writer& partition_trie_writer::operator=(partition_trie_writer&&) = default;
partition_trie_writer::partition_trie_writer(bti_trie_sink& out)
    : _impl(std::make_unique<impl>(*out._impl)){
}
void partition_trie_writer::add(const_bytes key, int64_t data_file_pos, uint8_t hash_bits) {
    _impl->add(key, data_file_pos, hash_bits);
}
int64_t partition_trie_writer::finish() {
    return _impl->finish().value;
}

template <typename T>
concept trie_reader_source = requires(T& o, uint64_t pos, reader_permit rp) {
    { o.read_page(pos) } -> std::same_as<future<std::pair<cached_file::ptr_type, bool>>>;
    { o.read_row_index_header(pos, rp) } -> std::same_as<future<row_index_header>>;
    { o.read_row_index_header(pos, rp) } -> std::same_as<future<row_index_header>>;
};

enum class row_index_header_parser_state {
    START,
    KEY_SIZE,
    KEY_BYTES,
    DATA_FILE_POSITION,
    OFFSET_FROM_TRIE_ROOT,
    LOCAL_DELETION_TIME,
    MARKED_FOR_DELETE_AT,
    END,
};

inline std::string_view state_name(row_index_header_parser_state s) {
    using enum row_index_header_parser_state;
    switch (s) {
    case START: return "START";
    case KEY_SIZE: return "KEY_SIZE";
    case KEY_BYTES: return "KEY_BYTES";
    case DATA_FILE_POSITION: return "DATA_FILE_POSITION";
    case OFFSET_FROM_TRIE_ROOT: return "OFFSET_TO_TRIE_ROOT";
    case LOCAL_DELETION_TIME: return "LOCAL_DELETION_TIME";
    case MARKED_FOR_DELETE_AT: return "MARKED_FOR_DELETE_AT";
    case END: return "END";
    default: abort();
    }
}

} // namespace trie

template <>
struct fmt::formatter<trie::row_index_header_parser_state> : fmt::formatter<string_view> {
    auto format(const trie::row_index_header_parser_state& r, fmt::format_context& ctx) const
            -> decltype(ctx.out()) {
        return fmt::format_to(ctx.out(), "{}", state_name(r));
    }
};

namespace trie {

// We use this to parse the row_index_header.
// FIXME: I didn't look at the performance of this at all yet. Maybe it's very inefficient.
struct row_index_header_parser : public data_consumer::continuous_data_consumer<row_index_header_parser> {
    using processing_result = data_consumer::processing_result;
    using proceed = data_consumer::proceed;
    using state = row_index_header_parser_state;
    state _state = state::START;
    row_index_header _result;
    uint64_t _position_offset;
    temporary_buffer<char> _key;
    void verify_end_state() {
        if (_state != state::END) {
            throw sstables::malformed_sstable_exception(fmt::format("row_index_header_parser: verify_end_state: expected END, got {}", _state));
        }
    }
    bool non_consuming() const {
        return ((_state == state::END) || (_state == state::START));
    }
    processing_result process_state(temporary_buffer<char>& data) {
        auto current_pos = [&] { return this->position() - data.size(); };
        switch (_state) {
        // START comes first, to make the handling of the 0-quantity case simpler
        case state::START:
            expensive_log("{}: pos {} state {} - data.size()={}", fmt::ptr(this), current_pos(), state::START, data.size());
            _state = state::KEY_SIZE;
            break;
        case state::KEY_SIZE:
            expensive_log("{}: pos {} state {}", fmt::ptr(this), current_pos(), state::KEY_SIZE);
            if (this->read_16(data) != continuous_data_consumer::read_status::ready) {
                _state = state::KEY_BYTES;
                break;
            }
            [[fallthrough]];
        case state::KEY_BYTES:
            expensive_log("{}: pos {} state {} - size={}", fmt::ptr(this), current_pos(), state::KEY_BYTES, this->_u16);
            if (this->read_bytes_contiguous(data, this->_u16, _key) != continuous_data_consumer::read_status::ready) {
                _state = state::DATA_FILE_POSITION;
                break;
            }
            [[fallthrough]];
        case state::DATA_FILE_POSITION:
            _result.partition_key = sstables::key(to_bytes(to_bytes_view(_key)));
            _position_offset = current_pos();
            expensive_log("{}: pos {} state {}", fmt::ptr(this), current_pos(), state::DATA_FILE_POSITION);
            if (read_unsigned_vint(data) != continuous_data_consumer::read_status::ready) {
                _state = state::OFFSET_FROM_TRIE_ROOT;
                break;
            }
            [[fallthrough]];
        case state::OFFSET_FROM_TRIE_ROOT:
            expensive_log("{}: pos {} state {}", fmt::ptr(this), current_pos(), state::OFFSET_FROM_TRIE_ROOT);
            _result.data_file_offset = this->_u64;
            if (read_unsigned_vint(data) != continuous_data_consumer::read_status::ready) {
                _state = state::LOCAL_DELETION_TIME;
                break;
            }
            [[fallthrough]];
        case state::LOCAL_DELETION_TIME: {
            _result.trie_root = _position_offset - this->_u64;
            expensive_log("{}: pos {} state {}", fmt::ptr(this), current_pos(), state::LOCAL_DELETION_TIME);
            if (this->read_32(data) != continuous_data_consumer::read_status::ready) {
                _state = state::MARKED_FOR_DELETE_AT;
                break;
            }
        }
            [[fallthrough]];
        case state::MARKED_FOR_DELETE_AT:
            expensive_log("{}: pos {} state {}", fmt::ptr(this), current_pos(), state::MARKED_FOR_DELETE_AT);
            _result.partition_tombstone.deletion_time = gc_clock::time_point(gc_clock::duration(this->_u32));
            if (this->read_64(data) != continuous_data_consumer::read_status::ready) {
                _state = state::END;
                break;
            }
            [[fallthrough]];
        case state::END: {
            _state = row_index_header_parser_state::END;
            _result.partition_tombstone.timestamp = this->_u64;
        }
        }
        expensive_log("{}: exit pos {} state {}", fmt::ptr(this), current_pos(), _state);
        return _state == state::END ? proceed::no : proceed::yes;
    }
public:
    row_index_header_parser(reader_permit rp, input_stream<char>&& input, uint64_t start, uint64_t maxlen)
        : continuous_data_consumer(std::move(rp), std::move(input), start, maxlen)
    {}
};

future<row_index_header> bti_node_reader::read_row_index_header(uint64_t pos, reader_permit rp) {
    auto ctx = row_index_header_parser(
        std::move(rp),
        make_file_input_stream(file(make_shared<cached_file_impl>(_file.get())), pos, _file.get().size() - pos, {.buffer_size = 4096, .read_ahead = 0}),
        pos,
        _file.get().size() - pos);
    std::exception_ptr ex;
    try {
        co_await ctx.consume_input();
        co_await ctx.close();
        expensive_log("read_row_index_header this={} result={}", fmt::ptr(this), ctx._result.data_file_offset);
        co_return std::move(ctx._result);
    } catch (...) {
        ex = std::current_exception();
    }
    co_await ctx.close();
    std::rethrow_exception(ex);
}

enum class set_result {
    eof,
    definitely_not_a_match,
    possible_match,
};

} // namespace trie

template <>
struct fmt::formatter<trie::trail_entry> : fmt::formatter<string_view> {
    auto format(const trie::trail_entry& r, fmt::format_context& ctx) const
            -> decltype(ctx.out()) {
        return fmt::format_to(ctx.out(), "trail_entry(id={} child_idx={} payload_bits={})", r.pos, r.child_idx, r.payload_bits);
    }
};

namespace trie {

template <node_reader Input>
class trie_cursor {
    // Reference wrapper to allow copying the cursor.
    Input _in;
    // A stack holding the path from the root to the currently visited node.
    // When initialized, _path[0] is root.
    //
    // FIXME: To support stepping, the iterator has to keep
    // a stack of visited nodes (the path from root to the current node).
    // But single-partition reads don't need to support stepping, and
    // it this case maintaining the stack only adds overhead.
    // Consider adding a separate cursor type for single-partition reads,
    // or a special single-partititon variant of `set_before()` which won't bother maintaining the stack.
public:
    decltype(traversal_state::trail) _trail;
public:
    trie_cursor(Input);
    ~trie_cursor() = default;
    trie_cursor& operator=(const trie_cursor&) = default;
    // Checks whether the cursor is initialized.
    // Preconditions: none.
    bool initialized() const;
    future<> set_to(uint64_t root, const_bytes key);
    // Moves the cursor to the next key (or EOF).
    //
    // step() returns a set_result, but it can only return `eof` (when it steps beyond all keys),
    // or `definitely_not_a_match` otherwise.
    // Returning a "not EOF" result as "definitely_not_a_match" isn't clean,
    // but we can't easily return a more meaningful enum because that would
    // require adding futures to the continuation chains.
    //
    // Preconditions: points at a node or transition.
    // Postconditions: points at eof or a payloaded node.
    future<> step();
    // Moves the cursor to the previous key.
    // If there is no previous key, doesn't do anything. 
    //
    // Preconditions: points at eof or a payloaded (sic!) node.
    // Postconditions: points at eof or a payloaded node.
    future<> step_back();
    // Preconditions: points at a payloaded node.
    payload_result payload() const;
    // Checks whether the cursor in the EOF position.
    // 
    // Preconditions: points at eof or a node.
    bool eof() const;
    // Preconditions: none.
    // Postconditions: uninitialized.
    void reset();
    bool at_leaf() const;
    bool at_key() const;
    void snap_to_leaf();
};

// An index cursor which can be used to obtain the Data.db bounds
// (and tombstones, where appropriate) for the given partition and row ranges.
template <node_reader Input>
class index_cursor {
    // A cursor into Partitions.db.
    trie_cursor<Input> _partition_cursor;
    // A cursor into Rows.db. Only initialized when the pointed-to partition has an entry in Rows.db.
    trie_cursor<Input> _row_cursor;
    // Holds the row index header for the current partition, iff the current partition has a row index.  
    std::optional<row_index_header> _partition_metadata;
    // _row_cursor reads the tries written in Rows.db.
    // Rerefence wrapper to allow for copying the cursor.
    Input _in_row;
    // Accounts the memory consumed by the cursor.
    // FIXME: it actually doesn't do that, yet.
    reader_permit _permit;
    uint64_t _par_root;
private:
    // If the current partition has a row index, reads its header.
    future<> maybe_read_metadata();
    // The colder part of set_after_row, just to hint at inlining the hotter part.
    future<> set_after_row_cold(const_bytes);
public:
    index_cursor(uint64_t par_root, Input par, Input row, reader_permit);
    index_cursor& operator=(const index_cursor&) = default;
    // Returns the data file position of the cursor. Can only be called on a set cursor.
    //
    // If the row cursor is set, returns the position of the pointed-to clustering key block.
    // Otherwise, if the partition cursor is set to a partition, returns the position of the partition.
    // Otherwise, if the partition cursor is set to EOF, returns `file_size`. (This part of the API is somewhat awkward)
    uint64_t data_file_pos(uint64_t file_size) const;
    // If the row index is set to a clustering key block, returns the range tombstone active at the start of the block.
    // Otherwise returns an empty tombstone.
    tombstone open_tombstone() const;
    // Returns the current partition's row index header, if it has one.
    const std::optional<row_index_header>& partition_metadata() const;
    // Sets the partition cursor to *some* position smaller-or-equal to all entries greater-or-equal to K.
    // See the comments at trie_cursor::set_before for more elaboration.
    //
    // Resets the row cursor.
    future<set_result> set_before_partition(const_bytes K);
    // Sets the partition cursor to *some* position strictly greater than all entries smaller-or-equal to K. 
    // See the comments at trie_cursor::set_after for details.
    //
    // Resets the row cursor.
    future<> set_after_partition(const_bytes K);
    // Moves the partition cursor to the next position (next partition or eof).
    // Resets the row cursor.
    future<> next_partition();
    // Sets the row cursor to *some* position (within the current partition)
    // smaller-or-equal to all entries greater-or-equal to K.
    // See the comments at trie_cursor::set_before for more elaboration.
    future<> set_before_row(const_bytes);
    // Sets the row cursor to *some* position (within the current partition)
    // smaller-or-equal to all entries greater-or-equal to K.
    // See the comments at trie_cursor::set_before for more elaboration.
    //
    // If the row cursor would go beyond the end of the partition,
    // it instead resets moves the partition cursor to the next partition
    // and resets the row cursor.
    future<> set_after_row(const_bytes);
    // Checks if the row cursor is set.
    bool row_cursor_set() const;
    future<std::optional<uint64_t>> last_block_offset() const;
    std::optional<uint8_t> partition_hash() const;
};

class bti_index_reader : public sstables::index_reader {
    bti_node_reader _in_row;
    // We need the schema solely to parse the partition keys serialized in row index headers.
    schema_ptr _s;
    // Supposed to account the memory usage of the index reader.
    // FIXME: it doesn't actually do that yet.
    // FIXME: it should also mark the permit as blocked on disk?
    reader_permit _permit;
    // The index is, in essence, a pair of cursors.
    index_cursor<bti_node_reader> _lower;
    index_cursor<bti_node_reader> _upper;
    // Partitions.db doesn't store the size of Data.db, so the cursor doesn't know by itself
    // what Data.db position to return when its position is EOF. We need to pass the file size
    // from the above.
    uint64_t _total_file_size;

    // Helper which reads a row index header from the given position in Rows.db.
    future<row_index_header> read_row_index_header(uint64_t);
    // Helper which translates a ring_position_view to a BTI byte-comparable form.
    std::vector<std::byte> translate_key(dht::ring_position_view key);
public:
    // FIXME: our semantics are different from the semantics of the old (BIG) index reader.
    //
    // This new index reader is *unset* after creation, and initially doesn't point at anything.
    // The old index reader points at the start of the sstable immediately after creation.
    //
    // So in our case, some operations (like advance_to_next_partition()) can only be legal
    // after the cursor is explicitly set to something, while in the old reader's case
    // they were always legal.
    //
    // In particular, we arbitrarily decide that the only legal operations for the new index reader
    // immediately after creation are advance_to(partition_range) and advance_lower_and_check_if_present().
    // That's why we only call maybe_init() from those functions -- other calls are assumed to be used
    // only after the index reader was initialized via one of those 2 chosen functions.
    //
    // In practice, the MX reader always starts by setting the index reader with advance_to(partition_range)
    // (for multi-partition reads), or with advance_lower_and_check_if_present().
    // But it's not documented that it has to stay this way, and it's dangerous to rely on that.
    // We should harden the API, via better docs and/or asserts and/or changing the semantics of this index
    // reader to match the old index reader.
    bti_index_reader(bti_node_reader partitions_db, bti_node_reader rows_db, uint64_t root_pos, uint64_t total_file_size, schema_ptr s, reader_permit);
    virtual future<> close() noexcept override;
    virtual sstables::data_file_positions_range data_file_positions() const override;
    virtual future<std::optional<uint64_t>> last_block_offset() override;
    virtual future<bool> advance_lower_and_check_if_present(dht::ring_position_view key) override;
    virtual future<> advance_to_next_partition() override;
    virtual sstables::indexable_element element_kind() const override;
    virtual future<> advance_to(dht::ring_position_view pos) override;
    virtual future<> advance_after_existing(const dht::decorated_key& dk) override;
    virtual future<> advance_to(position_in_partition_view pos) override;
    virtual std::optional<sstables::deletion_time> partition_tombstone() override;
    virtual std::optional<partition_key> get_partition_key() override;
    virtual partition_key get_partition_key_prefix() override;
    virtual bool partition_data_ready() const override;
    virtual future<> read_partition_data() override;
    virtual future<> advance_reverse(position_in_partition_view pos) override;
    virtual future<> advance_to(const dht::partition_range& range) override;
    virtual future<> advance_reverse_to_next_partition() override;
    virtual future<> advance_upper_past(position_in_partition_view pos) override;
    virtual std::optional<sstables::open_rt_marker> end_open_marker() const override;
    virtual std::optional<sstables::open_rt_marker> reverse_end_open_marker() const override;
    virtual sstables::clustered_index_cursor* current_clustered_cursor() override;
    future<> reset_clustered_cursor() override;
    virtual uint64_t get_data_file_position() override;
    virtual uint64_t get_promoted_index_size() override;
    virtual bool eof() const override;
};

template <node_reader Input>
trie_cursor<Input>::trie_cursor(Input in)
    : _in(std::move(in))
{
}

// Documented near the declaration.
template <node_reader Input>
future<> trie_cursor<Input>::set_to(uint64_t root, const_bytes key) {
    traversal_state ts{.next_pos = root, .edges_traversed = 0};
    co_await trie::traverse(_in, key, ts);
    _trail = std::move(ts.trail);
}

// Documented near the declaration.
template <node_reader Input>
future<> trie_cursor<Input>::step() {
    traversal_state ts{.trail = std::move(_trail)};
    co_await trie::step(_in, ts);
    _trail = std::move(ts.trail);
}

// Documented near the declaration.
template <node_reader Input>
future<> trie_cursor<Input>::step_back() {
    traversal_state ts{.trail = std::move(_trail)};
    co_await trie::step_back(_in, ts);
    _trail = std::move(ts.trail);
}

template <node_reader Input>
payload_result trie_cursor<Input>::payload() const {
    return payload_result{
        .bits = _trail.back().payload_bits,
        .bytes = _in.get_payload(_trail.back().pos),
    };
}

template <node_reader Input>
bool trie_cursor<Input>::eof() const {
    return _trail[0].child_idx == _trail[0].n_children;
}

template <node_reader Input>
bool trie_cursor<Input>::initialized() const {
    return !_trail.empty();
}

template <node_reader Input>
void trie_cursor<Input>::reset() {
    _trail.clear();
}

template <node_reader Input>
void trie_cursor<Input>::snap_to_leaf() {
    _trail.back().child_idx = -1;
}

template <node_reader Input>
bool trie_cursor<Input>::at_leaf() const {
    return _trail.back().n_children == 0;
}

template <node_reader Input>
bool trie_cursor<Input>::at_key() const {
    return _trail.back().payload_bits && _trail.back().child_idx == -1;
}

template <node_reader Input>
index_cursor<Input>::index_cursor(uint64_t par_root, Input par, Input row, reader_permit permit)
    : _partition_cursor(par)
    , _row_cursor(row)
    , _in_row(row)
    , _permit(permit)
    , _par_root(par_root)
{}

template <node_reader Input>
bool index_cursor<Input>::row_cursor_set() const {
    return _row_cursor.initialized();
}

static int64_t partition_payload_to_pos(const payload_result& p) {
    uint64_t bits = p.bits & 0x7;
    expensive_assert(p.bytes.size() >= bits + 1);
    uint64_t be_result = 0;
    std::memcpy(&be_result, p.bytes.data() + 1, bits);
    auto result = int64_t(seastar::be_to_cpu(be_result)) >> 8*(8 - bits);
    expensive_log("payload_to_offset: be_result={:016x} bits={}, bytes={}, result={:016x}", be_result, bits, fmt_hex(p.bytes), uint64_t(result));
    return result;
}

static int64_t row_payload_to_offset(const payload_result& p) {
    uint64_t bits = p.bits & 0x7;
    expensive_assert(p.bytes.size() >= size_t(bits + 12));
    uint64_t be_result = 0;
    std::memcpy(&be_result, p.bytes.data(), bits);
    auto result = seastar::be_to_cpu(be_result) >> 8*(8 - bits);
    return result;
}

template <node_reader Input>
future<std::optional<uint64_t>> index_cursor<Input>::last_block_offset() const {
    if (!_partition_metadata) {
        expensive_log("last_block_offset: no partition metadata");
        co_return std::nullopt;
    }

    //abort();
    auto cur = _row_cursor;
    const std::byte key[] = {std::byte(0x60)};
    co_await cur.set_to(_partition_metadata->trie_root, key);
    co_await cur.step_back();
    if (cur.eof()) {
        co_return std::nullopt; 
    }

    auto result = row_payload_to_offset(cur.payload());
    expensive_log("last_block_offset: {}", result);
    co_return result;
}

template <node_reader Input>
std::optional<uint8_t> index_cursor<Input>::partition_hash() const {
    auto p = _partition_cursor.payload();
    if (p.bits & 0x8) {
        return static_cast<uint8_t>(p.bytes[0]);
    }
    return std::nullopt;
}

template <node_reader Input>
uint64_t index_cursor<Input>::data_file_pos(uint64_t file_size) const {
    expensive_log("index_cursor::data_file_pos this={} initialized={}", fmt::ptr(this), _partition_cursor.initialized());
    expensive_assert(_partition_cursor.initialized());
    if (_partition_metadata) {
        if (!_row_cursor.initialized()) {
            expensive_log("index_cursor::data_file_pos this={} from empty row cursor: {}", fmt::ptr(this), _partition_metadata->data_file_offset);
            return _partition_metadata->data_file_offset;
        }
        const auto p = _row_cursor.payload();
        auto res = _partition_metadata->data_file_offset + row_payload_to_offset(p);
        expensive_log("index_cursor::data_file_pos this={} from row cursor: {} bytes={} bits={}", fmt::ptr(this), res, fmt_hex(p.bytes), p.bits & 0x7);
        return res;
    }
    if (!_partition_cursor.eof()) {
        auto res = partition_payload_to_pos(_partition_cursor.payload());
        expensive_assert(res >= 0);
        // expensive_log("index_cursor::data_file_pos this={} from partition cursor: {}", fmt::ptr(this), res);
        return res;
    }
    // expensive_log("index_cursor::data_file_pos this={} from eof: {}", fmt::ptr(this), file_size);
    return file_size;
}

template <node_reader Input>
tombstone index_cursor<Input>::open_tombstone() const {
    expensive_log("index_cursor::open_tombstone this={}", fmt::ptr(this));
    expensive_assert(_partition_cursor.initialized());
    expensive_assert(_partition_metadata.has_value());
    if (!_row_cursor.initialized() || _row_cursor.eof()) {
        auto res = tombstone();
        expensive_log("index_cursor::open_tombstone this={} from eof: {}", fmt::ptr(this), tombstone());
        return res;
    } else {
        const auto p = _row_cursor.payload();
        if (p.bits >= 8) {
            uint64_t bits = p.bits & 0x7;
            expensive_assert(p.bytes.size() >= size_t(bits + 12));
            auto marked = seastar::be_to_cpu(read_unaligned<uint64_t>(p.bytes.data() + bits));
            auto deletion_time = seastar::be_to_cpu(read_unaligned<int32_t>(p.bytes.data() + bits + 8));
            return tombstone(marked, gc_clock::time_point(gc_clock::duration(deletion_time)));
        } else {
            return tombstone();
        }
    }
}

template <node_reader Input>
const std::optional<row_index_header>& index_cursor<Input>::partition_metadata() const {
    return _partition_metadata;
}

template <node_reader Input>
[[gnu::always_inline]]
future<> index_cursor<Input>::maybe_read_metadata() {
    expensive_log("index_cursor::maybe_read_metadata this={}", fmt::ptr(this));
    if (_partition_cursor.eof()) {
        return make_ready_future<>();
    }
    if (auto res = partition_payload_to_pos(_partition_cursor.payload()); res < 0) {
        return _in_row.read_row_index_header(-res, _permit).then([this] (auto result) {
            _partition_metadata = result;
        });
    }
    return make_ready_future<>();
}
template <node_reader Input>
future<set_result> index_cursor<Input>::set_before_partition(const_bytes key) {
    expensive_log("index_cursor::set_before_partition this={} key={}", fmt::ptr(this), fmt_hex(key));
    _row_cursor.reset();
    _partition_metadata.reset();
    co_await _partition_cursor.set_to(_par_root, key);
    if (_partition_cursor.at_leaf()) {
        _partition_cursor.snap_to_leaf();
        expensive_log("index_cursor::set_before_partition, points at key, trail={}", fmt::join(_partition_cursor._trail, ", "));
        co_await maybe_read_metadata();
        co_return set_result::possible_match;
    } else {
        expensive_log("index_cursor::set_before_partition, doesn't point at key, trail={}", fmt::join(_partition_cursor._trail, ", "));
        co_await _partition_cursor.step();
        co_await maybe_read_metadata();
        co_return _partition_cursor.eof() ? set_result::eof : set_result::definitely_not_a_match;
    }
}

template <node_reader Input>
future<> index_cursor<Input>::set_after_partition(const_bytes key) {
    expensive_log("index_cursor::set_after_partition this={} key={}", fmt::ptr(this), fmt_hex(key));
    _row_cursor.reset();
    _partition_metadata.reset();
    co_await _partition_cursor.set_to(_par_root, key);
    co_await _partition_cursor.step();
    co_await maybe_read_metadata();
}
template <node_reader Input>
future<> index_cursor<Input>::next_partition() {
    expensive_log("index_cursor::next_partition() this={}", fmt::ptr(this));
    _row_cursor.reset();
    _partition_metadata.reset();
    return _partition_cursor.step().then([this] {
        return maybe_read_metadata();
    });
}
template <node_reader Input>
future<> index_cursor<Input>::set_before_row(const_bytes key) {
    expensive_log("index_cursor::set_before_row this={} key={}", fmt::ptr(this), fmt_hex(key));
    if (!_partition_metadata) {
        co_return;
    }
    _row_cursor.reset();
    co_await _row_cursor.set_to(_partition_metadata->trie_root, key);
    if (!_row_cursor.at_key()) {
        co_await _row_cursor.step_back();
    }
}

template <node_reader Input>
future<> index_cursor<Input>::set_after_row_cold(const_bytes key) {
    co_await _row_cursor.set_to(_partition_metadata->trie_root, key);
    co_await _row_cursor.step();
    if (_row_cursor.eof()) {
        co_await next_partition();
    }
}

template <node_reader Input>
[[gnu::always_inline]]
future<> index_cursor<Input>::set_after_row(const_bytes key) {
    expensive_log("index_cursor::set_after_row this={} key={}", fmt::ptr(this), fmt_hex(key));
    if (!_partition_metadata) {
        return next_partition();
    }
    return set_after_row_cold(key);
}

future<row_index_header> bti_index_reader::read_row_index_header(uint64_t pos) {
    auto hdr = co_await _in_row.read_row_index_header(pos, _permit);
    expensive_log("bti_index_reader::read_row_index_header this={} pos={} result={}", fmt::ptr(this), pos, hdr.data_file_offset);
    co_return hdr;
}
std::vector<std::byte> bti_index_reader::translate_key(dht::ring_position_view key) {
    auto trie_key = std::vector<std::byte>();
    trie_key.reserve(256);
    trie_key.push_back(std::byte(0x40));
    auto token = key.token().is_maximum() ? std::numeric_limits<uint64_t>::max() : key.token().unbias();
    append_to_vector(trie_key, object_representation(seastar::cpu_to_be<uint64_t>(token)));
    if (auto k = key.key()) {
        trie_key.reserve(k->representation().size() + 64);
        _s->partition_key_type()->memcmp_comparable_form(*k, trie_key);
    }
    std::byte ending;
    if (key.weight() < 0) {
        ending = std::byte(0x20);
    } else if (key.weight() == 0) {
        ending = std::byte(0x38);
    } else {
        ending = std::byte(0x60);
    }
    trie_key.push_back(ending);
    expensive_log("translate_key({}) = {}", key, fmt_hex(trie_key));
    return trie_key;
}
sstables::data_file_positions_range bti_index_reader::data_file_positions() const {
    auto lo = _lower.data_file_pos(_total_file_size);
    auto hi =_upper.data_file_pos(_total_file_size);
    trie_logger.debug("bti_index_reader::data_file_positions this={} result=({}, {})", fmt::ptr(this), lo, hi);
    return {lo, hi};
}
future<std::optional<uint64_t>> bti_index_reader::last_block_offset() {
    trie_logger.trace("bti_index_reader::last_block_offset this={}", fmt::ptr(this));
    return _lower.last_block_offset();
}
future<> bti_index_reader::close() noexcept {
    trie_logger.debug("bti_index_reader::close this={}", fmt::ptr(this));
    return make_ready_future<>();
}
bti_index_reader::bti_index_reader(
    bti_node_reader partitions_db,
    bti_node_reader rows_db,
    uint64_t root_offset,
    uint64_t total_file_size,
    schema_ptr s,
    reader_permit rp
)
    : _in_row(rows_db)
    , _s(s)
    , _permit(std::move(rp))
    , _lower(root_offset, partitions_db, rows_db, _permit)
    , _upper(root_offset, partitions_db, rows_db, _permit)
    , _total_file_size(total_file_size)
{
    trie_logger.debug("bti_index_reader::constructor: this={} root_offset={} total_file_size={} table={}.{}",
        fmt::ptr(this), root_offset, total_file_size, _s->ks_name(), _s->cf_name());
}

std::byte bound_weight_to_terminator(bound_weight b) {
    switch (b) {
        case bound_weight::after_all_prefixed: return std::byte(0x60);
        case bound_weight::before_all_prefixed: return std::byte(0x20);
        case bound_weight::equal: return std::byte(0x40);
    }
}
std::vector<std::byte> byte_comparable(const schema& s, position_in_partition_view pipv) {
    std::vector<std::byte> res;
    if (pipv.has_key()) {
        res.reserve(pipv.key().representation().size() + 64);
        s.clustering_key_type()->memcmp_comparable_form(pipv.key(), res);
    }
    res.push_back(bound_weight_to_terminator(pipv.get_bound_weight()));
    return res;
}
future<bool> bti_index_reader::advance_lower_and_check_if_present(dht::ring_position_view key) {
    trie_logger.debug("bti_index_reader::advance_lower_and_check_if_present: this={} key={}", fmt::ptr(this), key);
    auto trie_key = translate_key(key);
    auto res = co_await _lower.set_before_partition(trie_key);
    _upper = _lower;
    if (res != set_result::possible_match) {
        co_return false;
    }
    if (std::optional<uint8_t> hash_byte = _lower.partition_hash()) {
        uint8_t expected = hash_bits_from_token(key.token());
        if (*hash_byte != expected) {
            co_return false;
        }
    }
    co_await _upper.next_partition();
    co_return true;
}
future<> bti_index_reader::advance_to_next_partition() {
    trie_logger.debug("bti_index_reader::advance_to_next_partition this={}", fmt::ptr(this));
    co_await _lower.next_partition();
}
sstables::indexable_element bti_index_reader::element_kind() const {
    trie_logger.debug("bti_index_reader::element_kind");
    return _lower.row_cursor_set() ? sstables::indexable_element::cell : sstables::indexable_element::partition;
}
future<> bti_index_reader::advance_to(dht::ring_position_view pos) {
    trie_logger.debug("bti_index_reader::advance_to(partition) this={} pos={}", fmt::ptr(this), pos);
    co_await _lower.set_before_partition(translate_key(pos));
}
future<> bti_index_reader::advance_after_existing(const dht::decorated_key& dk) {
    trie_logger.debug("bti_index_reader::advance_after_existing(partition) this={} pos={}", fmt::ptr(this), dk);
    co_await _lower.set_after_partition(translate_key(dht::ring_position_view(dk.token(), &dk.key(), 0)));
}
future<> bti_index_reader::advance_to(position_in_partition_view pos) {
    trie_logger.debug("bti_index_reader::advance_to(row) this={} pos={}", fmt::ptr(this), pos);
    co_await _lower.set_before_row(byte_comparable(*_s, pos));
}
std::optional<sstables::deletion_time> bti_index_reader::partition_tombstone() {
    std::optional<sstables::deletion_time> res;
    if (const auto& hdr = _lower.partition_metadata()) {
        res = sstables::deletion_time{
            hdr->partition_tombstone.deletion_time.time_since_epoch().count(),
            hdr->partition_tombstone.timestamp};
        trie_logger.debug("bti_index_reader::partition_tombstone this={} res={}", fmt::ptr(this), hdr->partition_tombstone);
    } else {
        trie_logger.debug("bti_index_reader::partition_tombstone this={} res=none", fmt::ptr(this));
    }
    return res;
}
std::optional<partition_key> bti_index_reader::get_partition_key() {
    std::optional<partition_key> res;
    if (const auto& hdr = _lower.partition_metadata()) {
        res = hdr->partition_key.to_partition_key(*_s);
    }
    trie_logger.debug("bti_index_reader::get_partition_key this={} res={}", fmt::ptr(this), res);
    return res;
}
partition_key bti_index_reader::get_partition_key_prefix() {
    trie_logger.debug("bti_index_reader::get_partition_key_prefix this={}", fmt::ptr(this));
    abort();
}
bool bti_index_reader::partition_data_ready() const {
    trie_logger.debug("bti_index_reader::partition_data_ready this={}", fmt::ptr(this));
    return _lower.partition_metadata().has_value();
}
future<> bti_index_reader::advance_reverse(position_in_partition_view pos) {
    trie_logger.debug("bti_index_reader::advance_reverse this={} pos={}", fmt::ptr(this), pos);
    _upper = _lower;
    co_await _upper.set_after_row(byte_comparable(*_s, pos));
}
future<> bti_index_reader::read_partition_data() {
    trie_logger.debug("bti_index_reader::read_partition_data this={}", fmt::ptr(this));
    return make_ready_future<>();
}
future<> bti_index_reader::advance_to(const dht::partition_range& range) {
    trie_logger.debug("bti_index_reader::advance_to(range) this={} range={}", fmt::ptr(this), range);
    if (const auto s = range.start()) {
        co_await _lower.set_before_partition(translate_key(s.value().value()));
    } else {
        co_await _lower.set_before_partition(const_bytes());
    }
    if (const auto e = range.end()) {
        auto k = translate_key(e.value().value());
        if (e->value().has_key()) {
            k.back() = std::byte(0x60);
        }
        co_await _upper.set_after_partition(k);
    } else {
        std::byte top[1] = {std::byte(0x60)};
        co_await _upper.set_after_partition(top);
    }
}
future<> bti_index_reader::advance_reverse_to_next_partition() {
    trie_logger.debug("bti_index_reader::advance_reverse_to_next_partition() this={}", fmt::ptr(this));
    _upper = _lower;
    return _upper.next_partition().discard_result();
}
future<> bti_index_reader::advance_upper_past(position_in_partition_view pos) {
    trie_logger.debug("bti_index_reader::advance_upper_past() this={}", fmt::ptr(this));
    co_await _upper.set_after_row(byte_comparable(*_s, pos));
}
std::optional<sstables::open_rt_marker> bti_index_reader::end_open_marker() const {
    trie_logger.debug("bti_index_reader::end_open_marker() this={}", fmt::ptr(this));
    std::optional<sstables::open_rt_marker> res;
    if (const auto& hdr = _lower.partition_metadata()) {
        res = sstables::open_rt_marker{.pos = {position_in_partition::after_static_row_tag_t()}, .tomb = _lower.open_tombstone()};
    }
    trie_logger.debug("bti_index_reader::end_open_marker this={} res={}", fmt::ptr(this), res ? res->tomb : tombstone());
    return res;
}
std::optional<sstables::open_rt_marker> bti_index_reader::reverse_end_open_marker() const {
    trie_logger.debug("bti_index_reader::reverse_end_open_marker() this={}", fmt::ptr(this));
    std::optional<sstables::open_rt_marker> res;
    if (const auto& hdr = _upper.partition_metadata()) {
        res = sstables::open_rt_marker{.pos = {position_in_partition::after_static_row_tag_t()}, .tomb = _upper.open_tombstone()};
    }
    trie_logger.debug("bti_index_reader::reverse_end_open_marker this={} res={}", fmt::ptr(this), res ? res->tomb : tombstone());
    return res;
}
sstables::clustered_index_cursor* bti_index_reader::current_clustered_cursor() {
    trie_logger.debug("bti_index_reader::current_clustered_cursor() this={}", fmt::ptr(this));
    return nullptr;
    abort();
}
future<> bti_index_reader::reset_clustered_cursor() {
    trie_logger.debug("bti_index_reader::reset_clustered_cursor() this={}", fmt::ptr(this));
    abort();
}
uint64_t bti_index_reader::get_data_file_position() {
    return data_file_positions().start;
}
uint64_t bti_index_reader::get_promoted_index_size() {
    trie_logger.debug("bti_index_reader::get_promoted_index_size() this={}", fmt::ptr(this));
    return 0;
}
bool bti_index_reader::eof() const {
    trie_logger.debug("bti_index_reader::eof() this={}", fmt::ptr(this));
    return _lower.data_file_pos(_total_file_size) >= _total_file_size;
}

template <trie_writer_sink Output>
class row_index_writer_impl {
public:
    row_index_writer_impl(Output&);
    ~row_index_writer_impl();
    void add(const_bytes first_ck, const_bytes last_ck, uint64_t data_file_pos, sstables::deletion_time);
    sink_pos finish();
    using buf = std::vector<std::byte>;

    struct row_index_payload {
        uint64_t data_file_pos;
        sstables::deletion_time dt;
    };

private:
    trie_writer<Output> _wr;
    size_t _added_blocks = 0;
    buf _last_separator;
    buf _last_key;
};

template <trie_writer_sink Output>
row_index_writer_impl<Output>::row_index_writer_impl(Output& out)
    : _wr(out)
{}
template <trie_writer_sink Output>
row_index_writer_impl<Output>::~row_index_writer_impl() {
}

[[gnu::target("avx2")]]
size_t mismatch_idx(const std::byte* __restrict__ a, const std::byte* __restrict__ b, size_t n) {
    size_t i;
    for (i = 0; i + 32 <= n; i += 32) {
        __m256i av, bv;
        memcpy(&av, &a[i], 32);
        memcpy(&bv, &b[i], 32);
        __m256i pcmp = _mm256_cmpeq_epi32(av, bv);
        unsigned bitmask = _mm256_movemask_epi8(pcmp);
        if (bitmask != 0xffffffffU) {
            break;
        }
    }
    for (; i < n; ++i) {
        if (a[i] != b[i]) {
            break;
        }
    }
    return i;
}

template <trie_writer_sink Output>
void row_index_writer_impl<Output>::add(
    const_bytes first_ck,
    const_bytes last_ck,
    uint64_t data_file_pos,
    sstables::deletion_time dt
) {
    expensive_log("row_index_writer_impl::add() this={} first_ck={} last_ck={} data_file_pos={} dt={} _last_key={} _last_sep={}",
        fmt::ptr(this),
        fmt_hex(first_ck),
        fmt_hex(last_ck),
        data_file_pos,
        dt,
        fmt_hex(_last_key),
        fmt_hex(_last_separator)
    );
    size_t n = std::min(first_ck.size(), _last_key.size());
    size_t separator_mismatch = mismatch_idx(first_ck.data(), _last_key.data(), n);

    // size_t separator_mismatch = std::ranges::mismatch(first_ck, _last_key).in2 - _last_key.begin();
    expensive_assert(separator_mismatch < first_ck.size());
    expensive_assert(separator_mismatch <= _last_key.size());
    if (_added_blocks > 0) {
        // For the first block, _last_key is empty.
        // We leave it that way. The key we insert into the trie to represent the first block is empty.
        //
        // For later blocks, we need to insert some separator S which is greater than the last key (A) of the previous
        // block and not smaller than the first key (B) of the current block.
        //
        // The choice of this separator affects the efficiency of lookups for range queries starting at any X within the keyless range (A, B).
        // Such queries lie entirely after the previous block, so the optimal answer from the index is the current block.
        // But whether the index returns the previous or the current block, can depend only on whether X is smaller or not smaller than the separator.
        //
        // For example, assume that A=0 and B=9.
        // Imagine a query for the range (5, +∞). If S=1, then index will return the current block, which is optimal.
        // If S=9, then index will return the previous block, and the reader will waste time scanning through it.
        // 
        // Therefore it is good to construct S to be as close as possible to A (not to B) as possible.
        // In this regard, the optimal separator is A concatenated with a zero byte.
        //
        // But at the same time, we don't want to use a separator as long as a full key if much shorter possible separators exist.
        //
        // Therefore, as an arbitrary compromise, we use the optimal-distance separator in the set
        // of optimal-length separators. Which means we just nudge the byte at the point of mismatch by 1.
        _last_key.resize(separator_mismatch + 1);
        // The byte at the point of mismatch must be greater in the next key than in the previous key.
        // So the byte in the previous key can't possibly be 0xff.
        expensive_assert(_last_key[separator_mismatch] != std::byte(0xff));
        // Sanity check. Encoding of keys is supposed to be prefix-free.
        expensive_assert(separator_mismatch < _last_key.size());
        // The condition of this `if` is always true iff key encoding is prefix-free.
        // (And in the BTI format, it is. The previous line asserts that).
        // But with this `if` in place, the writer doesn't depend on a prefix-free encoding,
        // so I left it in the code for posterity. 
        if (separator_mismatch < _last_key.size()) {
            _last_key[separator_mismatch] = std::byte(uint8_t(_last_key[separator_mismatch]) + 1);
        }
    }

    // size_t needed_prefix = std::min(std::max(_last_sep_mismatch, mismatch) + 1, _last_separator.size());
    size_t mismatch = std::ranges::mismatch(_last_key, _last_separator).in2 - _last_separator.begin();
    auto tail = std::span(_last_key).subspan(mismatch, _last_key.size() - mismatch);

    std::array<std::byte, 20> payload_bytes;
    auto payload_bits = div_ceil(std::bit_width<uint64_t>(data_file_pos), 8);
    std::byte* p = payload_bytes.data();
    uint64_t offset_be = seastar::cpu_to_be(data_file_pos);
    std::memcpy(p, (const char*)(&offset_be) + 8 - payload_bits, payload_bits);
    p += payload_bits;
    uint8_t has_tombstone_flag = 0;
    if (!dt.live()) {
        has_tombstone_flag = 0x8;
        p = write_unaligned(p, seastar::cpu_to_be(dt.marked_for_delete_at));
        p = write_unaligned(p, seastar::cpu_to_be(dt.local_deletion_time));
    }

    expensive_log("row_index_trie_writer::add(): _wr.add({}, {}, {}, {}, {:016x})", mismatch, fmt_hex(tail), fmt_hex(payload_bytes), payload_bits, data_file_pos);
    _wr.add(mismatch, tail, trie_payload(has_tombstone_flag | payload_bits, {payload_bytes.data(), p}));

    _added_blocks += 1;
    std::swap(_last_separator, _last_key);
    _last_key.assign(last_ck.begin(), last_ck.end());
}

template <trie_writer_sink Output>
sink_pos row_index_writer_impl<Output>::finish() {
    expensive_log("row_index_writer_impl::finish() this={}", fmt::ptr(this));
    auto result = _wr.finish();
    _added_blocks = 0;
    _last_key.clear();
    _last_separator.clear();
    return result;
}

class row_trie_writer::impl : public row_index_writer_impl<bti_trie_sink_impl> {
    using row_index_writer_impl::row_index_writer_impl;
};

// Pimpl boilerplate
row_trie_writer::row_trie_writer() = default;
row_trie_writer::~row_trie_writer() = default;
row_trie_writer& row_trie_writer::operator=(row_trie_writer&&) = default;
row_trie_writer::row_trie_writer(bti_trie_sink& out)
    : _impl(std::make_unique<impl>(*out._impl)) {
}
void row_trie_writer::add(const_bytes first_ck, const_bytes last_ck, uint64_t data_file_pos, sstables::deletion_time dt) {
    return _impl->add(first_ck, last_ck, data_file_pos, dt);
}
int64_t row_trie_writer::finish() {
    return _impl->finish().value;
}

bti_trie_sink make_bti_trie_sink(sstables::file_writer& w, size_t page_size) {
    return bti_trie_sink(std::make_unique<bti_trie_sink_impl>(w, page_size));
}

partition_trie_writer make_partition_trie_writer(bti_trie_sink& out) {
    return partition_trie_writer(out);
}

row_trie_writer make_row_trie_writer(bti_trie_sink& out) {
    return row_trie_writer(out);
}

std::unique_ptr<sstables::index_reader> make_bti_index_reader(
    cached_file& in,
    cached_file& in_row,
    uint64_t root_offset,
    uint64_t total_file_size,
    schema_ptr s,
    reader_permit rp
) {
    return std::make_unique<bti_index_reader>(
        bti_node_reader(in),
        bti_node_reader(in_row),
        root_offset,
        total_file_size,
        std::move(s),
        std::move(rp));
}

uint8_t hash_bits_from_token(const dht::token& x) {
    return x.unbias();
}

} // namespace trie
