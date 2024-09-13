#include "trie.hh"
#include <algorithm>
#include <cassert>
#include <set>
#include <capnp/message.h>
#include <capnp/serialize-packed.h>
#include "node.capnp.h"
#include "seastar/util/closeable.hh"

static seastar::logger trie_logger("trie");

inline constexpr size_t round_down(size_t a, size_t factor) {
    return a - a % factor;
}
inline constexpr size_t round_up(size_t a, size_t factor) {
    return round_down(a + factor - 1, factor);
}

static constexpr bool implies(bool a, bool b) {
    return !a || b;
}

fmt_hex fmt_hex_cb(const_bytes cb) {
    return {{reinterpret_cast<const bytes::value_type*>(cb.data()), cb.size()}};
}

payload::payload() noexcept {
}
payload::payload(uint8_t payload_bits, const_bytes payload) noexcept {
    assert(payload.size() <= _payload_buf.size());
    assert(payload_bits < 16);
    _payload_bits = payload_bits;
    _payload_size = payload.size();
    std::ranges::copy(payload, _payload_buf.data());
}
const_bytes payload::blob() const noexcept {
    return {_payload_buf.data(), _payload_size};
}

node::node(std::byte b) noexcept
    : _transition(b) { }
node* node::add_child(std::byte b) {
    assert(_children.empty() || b > _children.back()->_transition);
    _children.push_back(std::make_unique<node>(b));
    return _children.back().get();
}
void node::set_payload(const payload& p) noexcept {
    assert(_output_pos < 0);
    _payload = p;
}
size_t node::recalc_sizes(const trie_writer_output& out, size_t global_pos) {
    struct local_state {
        node* _node;
        size_t _pos;
        int _stage;
    };
    std::vector<local_state> stack;
    stack.push_back({this, global_pos, 0});
    while (!stack.empty()) {
        auto& [node, pos, stage] = stack.back();
        if (stage < static_cast<int>(node->_children.size()) && node->_has_out_of_page_descendants) {
            stage += 1;
            stack.push_back({node->_children[stage - 1].get(), global_pos, 0});
            continue;
        }
        node->_branch_size = global_pos - pos;
        if (node->_has_out_of_page_children || node->_has_out_of_page_descendants) {
            node->_node_size = out.serialized_size(*node, global_pos);
        }
        global_pos += node->_node_size;
        stack.pop_back();
    }
    return _branch_size + _node_size;
}
void node::write(trie_writer_output& out) {
    size_t starting_pos = out.pos();
    assert(_node_size > 0);
    assert(round_down(out.pos(), out.page_size()) == round_down(out.pos() + _branch_size + _node_size - 1, out.page_size()));
    assert(_output_pos < 0);
    struct local_state {
        node* _node;
        int _stage;
    };
    std::vector<local_state> stack;
    stack.push_back({this, 0});
    while (!stack.empty()) {
        auto& [node, stage] = stack.back();
        if (stage < static_cast<int>(node->_children.size())) {
            stage += 1;
            if (node->_children[stage - 1]->_output_pos < 0) {
                stack.push_back({node->_children[stage - 1].get(), 0});
            }
            continue;
        }
        node->_output_pos = out.pos();
        auto sz = out.write(*node, 0);
        if (!(static_cast<ssize_t>(sz) == node->_node_size)) {
            trie_logger.error("diff: {}, ns={}", sz, node->_node_size);
            abort();
        }
        stack.pop_back();
    }
    if (!(static_cast<ssize_t>(out.pos() - starting_pos) == _branch_size + _node_size)) {
        trie_logger.error("diff: {}, bs={}, ns={}", out.pos() - starting_pos, _branch_size, _node_size);
    }
    assert(static_cast<ssize_t>(out.pos() - starting_pos) == _branch_size + _node_size);
    _children.clear();
    _branch_size = 0;
    _node_size = 0;
    _has_out_of_page_children = 0;
    _has_out_of_page_descendants = 0;
}

class trie_writer::impl {
public:
    impl(trie_writer_output&);
    void add(size_t depth, const_bytes key_tail, const payload&);
    ssize_t finish();

private:
    void complete(node* x);
    void write(node* x);
    void lay_out_children(node* x);
    size_t recalc_total_size(node* x, size_t start_pos) const noexcept;

private:
    std::vector<node*> _stack;
    std::unique_ptr<node> _root;
    trie_writer_output& _out;
};
trie_writer::impl::impl(trie_writer_output& out)
    : _out(out) {
    _root = std::make_unique<node>(std::byte(0));
    _stack.push_back(_root.get());
}
void trie_writer::impl::complete(node* x) {
    assert(x->_branch_size < 0);
    assert(x->_node_size < 0);
    assert(x->_has_out_of_page_children == false);
    assert(x->_has_out_of_page_descendants == false);
    assert(x->_output_pos < 0);
    bool has_out_of_page_children = false;
    bool has_out_of_page_descendants = false;
    size_t node_size = _out.serialized_size(*x, _out.pos());
    size_t branch_size = 0;
    for (const auto& c : x->_children) {
        branch_size += c->_branch_size + c->_node_size;
        has_out_of_page_children |= c->_output_pos >= 0;
        has_out_of_page_descendants |= c->_has_out_of_page_descendants || c->_has_out_of_page_children;
    }
    if (branch_size + node_size <= _out.page_size()) {
        x->_branch_size = branch_size;
        x->_node_size = node_size;
        x->_has_out_of_page_children = has_out_of_page_children;
        x->_has_out_of_page_descendants = has_out_of_page_descendants;
    } else {
        lay_out_children(x);
    }
}
void trie_writer::impl::lay_out_children(node* x) {
    assert(x->_output_pos < 0);
    auto cmp = [](node* a, node* b) { return std::make_pair(a->_branch_size + a->_node_size, a->_transition) < std::make_pair(b->_branch_size + b->_node_size, b->_transition); };
    auto unwritten_children = std::set<node*, decltype(cmp)>(cmp);
    for (const auto& c : x->_children) {
        if (c->_output_pos < 0) {
            unwritten_children.insert(c.get());
        }
    }
    while (unwritten_children.size()) {
        node* candidate;

        node selection_key(std::byte(255));
        selection_key._branch_size = _out.bytes_left_in_page();
        selection_key._node_size = 0;
        auto choice_it = unwritten_children.upper_bound(&selection_key);
        if (choice_it == unwritten_children.begin()) {
            _out.pad_to_page_boundary();
            assert(_out.bytes_left_in_page() == _out.page_size());
            choice_it = std::end(unwritten_children);
        }
        choice_it = std::prev(choice_it);
        candidate = *choice_it;
        unwritten_children.erase(choice_it);

        if (candidate->_has_out_of_page_children || candidate->_has_out_of_page_descendants) {
            size_t true_size = recalc_total_size(candidate, _out.pos());
            if (true_size > _out.bytes_left_in_page()) {
                if (true_size > _out.page_size()) {
                    lay_out_children(candidate);
                }
                unwritten_children.insert(candidate);
                continue;
            }
        }
        write(candidate);
    }
    x->_branch_size = 0;
    x->_has_out_of_page_children = true;
    x->_has_out_of_page_descendants = false;
    x->_node_size = _out.serialized_size(*x, _out.pos());
    if (!(x->_node_size <= static_cast<ssize_t>(_out.page_size()))) {
        trie_logger.error("_node_size: {}, page_size: {}", x->_node_size, _out.page_size());
        assert(x->_node_size <= static_cast<ssize_t>(_out.page_size()));
    }
}
size_t trie_writer::impl::recalc_total_size(node* branch, size_t global_pos) const noexcept {
    return branch->recalc_sizes(_out, global_pos);
}
void trie_writer::impl::write(node* branch) {
    branch->write(_out);
}
void trie_writer::impl::add(size_t depth, const_bytes key_tail, const payload& p) {
    assert(_stack.size() >= 1);
    assert(_stack.size() - 1 >= depth);
    assert(implies(key_tail.empty(), depth == 0));

    while (_stack.size() - 1 > depth) {
        complete(_stack.back());
        _stack.pop_back();
    }
    for (size_t i = 0; i < key_tail.size(); ++i) {
        _stack.push_back(_stack.back()->add_child(key_tail[i]));
    }
    _stack.back()->set_payload(p);

    assert(_stack.size() == 1 + depth + key_tail.size());
}
ssize_t trie_writer::impl::finish() {
    while (_stack.size()) {
        complete(_stack.back());
        _stack.pop_back();
    }
    if (_root->_children.empty() && !_root->_payload._payload_bits) {
        return -1;
    }
    node superroot(std::byte(0));
    superroot._children.push_back(std::move(_root));
    lay_out_children(&superroot);
    return superroot._children[0]->_output_pos;
}

trie_writer::trie_writer(trie_writer_output& out)
    : _pimpl(std::make_unique<impl>(out)) { }
trie_writer::~trie_writer() { }
void trie_writer::add(size_t depth, const_bytes key_tail, const payload& p) { _pimpl->add(depth, key_tail, p); }
ssize_t trie_writer::finish() { return _pimpl->finish(); }

partition_index_trie_writer::partition_index_trie_writer(trie_writer_output& out)
    : _out(out) {
}

partition_index_trie_writer::~partition_index_trie_writer() {
}

void partition_index_trie_writer::add(const_bytes key, int64_t offset) {
    trie_logger.trace("partition_index_trie_writer::add({}, ...)", fmt_hex_cb(key));
    if (_added_keys > 0) {
        size_t mismatch = std::ranges::mismatch(key, _last_key).in2 - _last_key.begin();
        size_t needed_prefix = std::min(std::max(_last_key_mismatch, mismatch) + 1, _last_key.size());
        auto payload_bytes = std::bit_cast<std::array<std::byte, sizeof(_last_payload)>>(seastar::cpu_to_be(_last_payload));
        auto tail = std::span(_last_key).subspan(_last_key_mismatch, needed_prefix - _last_key_mismatch);
        _wr.add(_last_key_mismatch, tail, payload(1, payload_bytes));
        _last_key_mismatch = mismatch;
    }
    _added_keys += 1;
    _last_key.assign(key.begin(), key.end());
    _last_payload = offset;
}
ssize_t partition_index_trie_writer::finish() {
    if (_added_keys > 0) {
        size_t needed_prefix = std::min(_last_key_mismatch + 1, _last_key.size());
        auto payload_bytes = std::bit_cast<std::array<std::byte, sizeof(_last_payload)>>(_last_payload);
        auto tail = std::span(_last_key).subspan(_last_key_mismatch, needed_prefix - _last_key_mismatch);
        _wr.add(_last_key_mismatch, tail, payload(1, payload_bytes));
    }
    return _wr.finish();
}

trie_cursor::trie_cursor(trie_reader_input& in)
    : _in(in)
{
}

future<void> trie_cursor::init(uint64_t root_offset) {
    reset();
    _path.push_back({co_await _in.get().read(root_offset), -1});
}

future<set_result> trie_cursor::set_before(const_bytes key) {
    assert(initialized());
    assert(_path.back().child_idx == -1);
    _path.resize(1);
    _path.back().child_idx = -1;
    size_t i = 0;
    while (i < key.size()) {
        if (!_path[i].node.children.size()) {
            break;
        }
        auto it = std::ranges::lower_bound(_path[i].node.children, key[i], std::less(), &reader_node::child::transition);
        _path[i].child_idx = it - _path[i].node.children.begin();
        if (size_t(_path[i].child_idx) == _path[i].node.children.size()
            || _path.back().node.children[_path.back().child_idx].transition != key[i]) {
            _path[i].child_idx -= 1;
            co_return co_await step();
        }
        _path.push_back({co_await _in.get().read(_path.back().node.children[_path.back().child_idx].offset), -1});
        i += 1;
    }
    if (_path.back().node.payload.size()) {
        co_return set_result::match;
    } else {
        co_return co_await step();
    }
}

future<set_result> trie_cursor::set_after(const_bytes key) {
    assert(initialized());
    auto res = co_await set_before(key);
    if (res == set_result::match) {
        co_return co_await step();
    }
    co_return res;
}

future<set_result> trie_cursor::step() {
    assert(initialized() && !eof());
    _path.back().child_idx += 1;
    while (size_t(_path.back().child_idx) == _path.back().node.children.size()) {
        if (_path.size() == 1) {
            co_return set_result::eof;
        }
        _path.pop_back();
        _path.back().child_idx += 1;
    }
    _path.push_back({co_await _in.get().read(_path.back().node.children[_path.back().child_idx].offset), -1});
    while (!_path.back().node.payload.size()) {
        _path.back().child_idx += 1;
        assert(_path.back().child_idx < int(_path.back().node.children.size()));
        _path.push_back({co_await _in.get().read(_path.back().node.children[_path.back().child_idx].offset), -1});
    }
    co_return set_result::no_match;
}

const_bytes trie_cursor::payload() const {
    assert(initialized());
    assert(_path.back().child_idx == -1);
    return _path.back().node.payload;
}

bool trie_cursor::eof() const {
    assert(initialized());
    return size_t(_path.begin()->child_idx) == _path.begin()->node.children.size();
}

bool trie_cursor::initialized() const {
    return !_path.empty();
}

void trie_cursor::reset() {
    _path.clear();
}

index_cursor::index_cursor(trie_reader_input& par, trie_reader_input& row)
    : _partition_cursor(par)
    , _row_cursor(row)
{}

future<> index_cursor::init(uint64_t root_offset) {
    _row_cursor.reset();
    _partition_metadata.reset();
    return _partition_cursor.init(root_offset);
}

uint64_t index_cursor::data_file_pos(uint64_t file_size) const {
    assert(_partition_cursor.initialized());
    if (_partition_metadata) {
        return _partition_metadata->data_offset;
    }
    if (!_partition_cursor.eof()) {
        auto res = payload_to_offset(_partition_cursor.payload());
        assert(res >= 0);
        return res;
    }
    return file_size;
}

int64_t payload_to_offset(const_bytes p) {
    assert(p.size() == sizeof(int64_t));
    return seastar::le_to_cpu(read_unaligned<int64_t>(p.data()));
}
future<row_index_header> trie_index_reader::read_row_index_header(uint64_t pos) {
    auto hdr = co_await _in_row.read_row_index_header(pos, _permit);
    trie_logger.trace("trie_index_reader::payload_to_offset this={} pos={} result={}", fmt::ptr(this), pos, hdr.data_offset);
    co_return hdr;
}
std::vector<std::byte> trie_index_reader::translate_key(dht::ring_position_view key) {
    auto trie_key = std::vector<std::byte>();
    trie_key.push_back(std::byte(0x40));
    auto token = key.token().is_maximum() ? std::numeric_limits<uint64_t>::max() : key.token().unbias();
    append_to_vector(trie_key, object_representation(seastar::cpu_to_be<uint64_t>(token)));
    if (auto k = key.key()) {
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
    return trie_key;
}
sstables::data_file_positions_range trie_index_reader::data_file_positions() const {
    trie_logger.debug("trie_index_reader::data_file_positions this={} result=({}, {})", fmt::ptr(this), _lower_offset, _upper_offset);
    return {_lower_offset, _upper_offset};
}
future<std::optional<uint64_t>> trie_index_reader::last_block_offset() {
    trie_logger.debug("trie_index_reader::last_block_offset this={}", fmt::ptr(this));
    return make_ready_future<std::optional<uint64_t>>(std::optional<uint64_t>());
}
future<> trie_index_reader::close() noexcept {
    trie_logger.debug("trie_index_reader::close this={}", fmt::ptr(this));
    return make_ready_future<>();
}
trie_index_reader::trie_index_reader(
    trie_reader_input& in,
    trie_reader_input& row_in,
    uint64_t root_offset,
    uint64_t total_file_size,
    schema_ptr s,
    reader_permit rp
)
    : _in(in), _in_row(row_in), _root(root_offset), _total_file_size(total_file_size), _s(s), _permit(std::move(rp)) {
    trie_logger.debug("trie_index_reader::constructor: this={} root_offset={} total_file_size={} table={}.{}",
        fmt::ptr(this), root_offset, total_file_size, _s->ks_name(), _s->cf_name());
}
future<bool> trie_index_reader::advance_lower_and_check_if_present(dht::ring_position_view key, std::optional<position_in_partition_view> pos) {
    trie_logger.debug("trie_index_reader::advance_lower_and_check_if_present: this={} key={} pos={}", fmt::ptr(this), key, pos);
    // assert(!pos);
    auto trie_key = translate_key(key);
    _lower.emplace(_in);
    co_await _lower->init(_root);
    auto res = co_await _lower->set_before(trie_key);
    if (res == set_result::eof) {
        _lower.reset();
        _upper.reset();
    } else {
        _upper = _lower;
        if (res == set_result::match) {
            if (co_await _upper->step() == set_result::eof) {
                _upper.reset();
            }
        }
    }
    co_await refresh_offsets();
    co_return true;
}
future<> trie_index_reader::advance_to_next_partition() {
    trie_logger.debug("trie_index_reader::advance_to_next_partition this={}", fmt::ptr(this));
    auto res = co_await _lower.value().step();
    if (res == set_result::eof) {
        _lower.reset();
    }
    co_await refresh_offsets();
    co_return;
}
sstables::indexable_element trie_index_reader::element_kind() const {
    trie_logger.debug("trie_index_reader::element_kind");
    return sstables::indexable_element::partition;
}
future<> trie_index_reader::advance_to(dht::ring_position_view pos) {
    trie_logger.debug("trie_index_reader::advance_to(partition) this={} pos={}", fmt::ptr(this), pos);
    co_await _lower->set_before(translate_key(pos));
    co_await refresh_offsets();
}
future<> trie_index_reader::advance_to(position_in_partition_view pos) {
    trie_logger.debug("trie_index_reader::advance_to(row) this={} pos={}", fmt::ptr(this), pos);
    return make_ready_future<>();
}
std::optional<sstables::deletion_time> trie_index_reader::partition_tombstone() {
    trie_logger.debug("trie_index_reader::partition_tombstone this={}", fmt::ptr(this));
    return {};
}
std::optional<partition_key> trie_index_reader::get_partition_key() {
    trie_logger.debug("trie_index_reader::get_partition_key this={}", fmt::ptr(this));
    return {};
}
partition_key trie_index_reader::get_partition_key_prefix() {
    trie_logger.debug("trie_index_reader::get_partition_key_prefix this={}", fmt::ptr(this));
    abort();
}
bool trie_index_reader::partition_data_ready() const {
    trie_logger.debug("trie_index_reader::partition_data_ready this={}", fmt::ptr(this));
    return false;
}
future<> trie_index_reader::advance_reverse(position_in_partition_view pos) {
    trie_logger.debug("trie_index_reader::advance_reverse this={} pos={}", fmt::ptr(this), pos);
    return make_ready_future<>();
}
future<> trie_index_reader::read_partition_data() {
    trie_logger.debug("trie_index_reader::read_partition_data this={}", fmt::ptr(this));
    return make_ready_future<>();
}
future<> trie_index_reader::refresh_offsets() {
    _lower_offset = _total_file_size;
    _upper_offset = _total_file_size;
    _lower_header.reset();
    _upper_header.reset();
    _lower_row.reset();
    _upper_row.reset();
    if (_lower && !_lower->eof()) {
        auto off = payload_to_offset(_lower->payload());
        if (off >= 0) {
            _lower_offset = off;
        } else {
            _lower_header = co_await read_row_index_header(-off);
            _lower_offset = _lower_header->data_offset;
        }
    }
    if (_upper && !_upper->eof()) {
        auto off = payload_to_offset(_upper->payload());
        if (off >= 0) {
            _upper_offset = off;
        } else {
            _upper_header = co_await read_row_index_header(-off);
            _upper_offset = _upper_header->data_offset;
        }
    }
}
future<> trie_index_reader::advance_to(const dht::partition_range& range) {
    trie_logger.debug("trie_index_reader::advance_to(range) this={} range={}", fmt::ptr(this), range);
    if (const auto s = range.start()) {
        _lower.emplace(_in);
        co_await _lower->init(_root);
        co_await _lower->set_before(translate_key(s.value().value()));
    } else {
        _lower.emplace(_in);
        co_await _lower->init(_root);
        co_await _lower->set_before(const_bytes());
    }
    if (const auto e = range.end()) {
        _upper.emplace(_in);
        co_await _upper->init(_root);
        co_await _upper->set_after(translate_key(e.value().value()));
    } else {
        _upper.reset();
    }
    co_await refresh_offsets();
}
future<> trie_index_reader::advance_reverse_to_next_partition() {
    trie_logger.debug("trie_index_reader::advance_reverse_to_next_partition() this={}", fmt::ptr(this));
    return make_ready_future<>();
}
std::optional<sstables::open_rt_marker> trie_index_reader::end_open_marker() const {
    trie_logger.debug("trie_index_reader::end_open_marker() this={}", fmt::ptr(this));
    abort();
}
std::optional<sstables::open_rt_marker> trie_index_reader::reverse_end_open_marker() const {
    trie_logger.debug("trie_index_reader::reverse_end_open_marker() this={}", fmt::ptr(this));
    abort();
}
sstables::clustered_index_cursor* trie_index_reader::current_clustered_cursor() {
    trie_logger.debug("trie_index_reader::current_clustered_cursor() this={}", fmt::ptr(this));
    abort();
}
uint64_t trie_index_reader::get_data_file_position() {
    trie_logger.debug("trie_index_reader::current_clustered_cursor() this={} result={}", fmt::ptr(this), _lower_offset);
    return _lower_offset;
}
uint64_t trie_index_reader::get_promoted_index_size() {
    trie_logger.debug("trie_index_reader::get_promoted_index_size() this={}", fmt::ptr(this));
    return 0;
}
bool trie_index_reader::eof() const {
    trie_logger.debug("trie_index_reader::eof() this={}", fmt::ptr(this));
    return !_lower;
}

future<reader_node> seastar_file_trie_reader_input::read(uint64_t offset) {
    constexpr size_t page_size = 16*1024;
    auto p = co_await _f.dma_read<char>(round_down(offset, page_size), page_size);
    capnp::word src[page_size/sizeof(capnp::word)];
    auto off = offset % page_size;
    memcpy(src, p.get() + off, page_size - off);
    ::capnp::FlatArrayMessageReader message({std::data(src), std::size(src)});
    Node::Reader node = message.getRoot<Node>();
    std::vector<reader_node::child> children;
    reader_node rn;
    for (const auto& x : node.getChildren()) {
        rn.children.emplace_back(reader_node::child{std::byte(x.getTransition()), x.getOffset()});
    }
    auto x = std::as_bytes(std::span(node.getPayload().getBytes().asBytes()));
    rn.payload = std::vector(x.begin(), x.end());
    co_return rn;
}

enum class blabla_state {
    START,
    KEY_SIZE,
    KEY_BYTES,
    POSITION,
    ROOT_OFFSET,
    LOCAL_DELETION_TIME,
    MARKED_FOR_DELETE_AT,
    END,
};

inline std::string_view format_as(blabla_state s) {
    using enum blabla_state;
    switch (s) {
    case START: return "START";
    case KEY_SIZE: return "KEY_SIZE";
    case KEY_BYTES: return "KEY_BYTES";
    case POSITION: return "POSITION";
    case ROOT_OFFSET: return "ROOT_OFFSET";
    case LOCAL_DELETION_TIME: return "LOCAL_DELETION_TIME";
    case MARKED_FOR_DELETE_AT: return "MARKED_FOR_DELETE_AT";
    case END: return "END";
    default: abort();
    }
}

struct blabla_context : public data_consumer::continuous_data_consumer<blabla_context> {
    using processing_result = data_consumer::processing_result;
    using proceed = data_consumer::proceed;
    using state = blabla_state;
    state _state = state::START;
    row_index_header _result;
    uint64_t _position_offset;
    temporary_buffer<char> _key;
    void verify_end_state() {
        if (_state != state::END) {
            throw sstables::malformed_sstable_exception(fmt::format("blabla_context: {}", _state));
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
            trie_logger.trace("{}: pos {} state {} - data.size()={}", fmt::ptr(this), current_pos(), state::START, data.size());
            _state = state::KEY_SIZE;
            break;
        case state::KEY_SIZE:
            trie_logger.trace("{}: pos {} state {}", fmt::ptr(this), current_pos(), state::KEY_SIZE);
            if (this->read_16(data) != continuous_data_consumer::read_status::ready) {
                _state = state::KEY_BYTES;
                break;
            }
            [[fallthrough]];
        case state::KEY_BYTES:
            trie_logger.trace("{}: pos {} state {} - size={}", fmt::ptr(this), current_pos(), state::KEY_BYTES, this->_u16);
            if (this->read_bytes_contiguous(data, this->_u16, _key) != continuous_data_consumer::read_status::ready) {
                _state = state::POSITION;
                break;
            }
            [[fallthrough]];
        case state::POSITION:
            _result.partition_key = sstables::key(to_bytes(to_bytes_view(_key)));
            _position_offset = current_pos();
            trie_logger.trace("{}: pos {} state {}", fmt::ptr(this), current_pos(), state::POSITION);
            if (read_unsigned_vint(data) != continuous_data_consumer::read_status::ready) {
                _state = state::ROOT_OFFSET;
                break;
            }
            [[fallthrough]];
        case state::ROOT_OFFSET:
            trie_logger.trace("{}: pos {} state {}", fmt::ptr(this), current_pos(), state::ROOT_OFFSET);
            _result.data_offset = this->_u64;
            if (read_unsigned_vint(data) != continuous_data_consumer::read_status::ready) {
                _state = state::LOCAL_DELETION_TIME;
                break;
            }
            [[fallthrough]];
        case state::LOCAL_DELETION_TIME: {
            _result.trie_root = _position_offset - this->_u64;
            trie_logger.trace("{}: pos {} state {}", fmt::ptr(this), current_pos(), state::LOCAL_DELETION_TIME);
            if (this->read_32(data) != continuous_data_consumer::read_status::ready) {
                _state = state::MARKED_FOR_DELETE_AT;
                break;
            }
        }
            [[fallthrough]];
        case state::MARKED_FOR_DELETE_AT:
            trie_logger.trace("{}: pos {} state {}", fmt::ptr(this), current_pos(), state::MARKED_FOR_DELETE_AT);
            _result.partition_tombstone.deletion_time = gc_clock::time_point(gc_clock::duration(this->_u32));
            if (this->read_64(data) != continuous_data_consumer::read_status::ready) {
                _state = state::END;
                break;
            }
            [[fallthrough]];
        case state::END: {
            _state = blabla_state::END;
            _result.partition_tombstone.timestamp = this->_u64;
        }
        }
        trie_logger.trace("{}: exit pos {} state {}", fmt::ptr(this), current_pos(), _state);
        return _state == state::END ? proceed::no : proceed::yes;
    }
public:
    blabla_context(reader_permit rp, input_stream<char>&& input, uint64_t start, uint64_t maxlen)
        : continuous_data_consumer(std::move(rp), std::move(input), start, maxlen)
    {}
};

future<row_index_header> seastar_file_trie_reader_input::read_row_index_header(uint64_t offset, reader_permit rp) {
    auto ctx = blabla_context(std::move(rp), make_file_input_stream(_f, offset), offset, co_await _f.size() - offset);
    auto close = deferred_close(ctx);
    co_await ctx.consume_input();
    co_return std::move(ctx._result);
}

trie_reader_input::~trie_reader_input() {
}

seastar_file_trie_reader_input::seastar_file_trie_reader_input(seastar::file f) : _f(f) {
}

row_index_trie_writer::row_index_trie_writer(trie_writer_output& out) :_out(out) {
}
row_index_trie_writer::~row_index_trie_writer() {
}

void row_index_trie_writer::add(
    const_bytes first_ck,
    const_bytes last_ck,
    uint64_t data_file_offset,
    sstables::deletion_time dt
) {
    trie_logger.trace("row_index_trie_writer::add() this={} first_ck={} last_ck={} data_file_offset={} dt={} _last_sep={}, _last_sep_mismatch={}",
        fmt::ptr(this),
        fmt_hex_cb(first_ck),
        fmt_hex_cb(last_ck),
        data_file_offset,
        dt,
        fmt_hex_cb(_last_separator),
        _last_sep_mismatch
    );
    if (_added_blocks > 0) {
        size_t separator_mismatch = std::ranges::mismatch(first_ck, _last_key).in2 - _last_key.begin();
        size_t needed_pref = std::min(separator_mismatch + 1, first_ck.size());
        auto sp = std::span(first_ck).subspan(0, needed_pref);

        size_t mismatch = std::ranges::mismatch(sp, _last_separator).in2 - _last_separator.begin();
        size_t needed_prefix = std::min(std::max(_last_sep_mismatch, mismatch) + 1, _last_separator.size());
        auto tail = std::span(_last_separator).subspan(_last_sep_mismatch, needed_prefix - _last_sep_mismatch);

        std::array<std::byte, 20> payload_bytes;
        void* p = payload_bytes.data();
        p = write_unaligned(p, seastar::cpu_to_le(_last_payload.data_file_offset));
        p = write_unaligned(p, seastar::cpu_to_le(_last_payload.dt.marked_for_delete_at));
        p = write_unaligned(p, seastar::cpu_to_le(_last_payload.dt.local_deletion_time));
        assert(p == payload_bytes.data() + payload_bytes.size());

        trie_logger.trace("row_index_trie_writer::add(): _wr.add({}, {}, {})", _last_sep_mismatch, fmt_hex_cb(tail), fmt_hex_cb(payload_bytes));
        _wr.add(_last_sep_mismatch, tail, payload(1, payload_bytes));

        _last_separator.assign(sp.begin(), sp.end());
        _last_sep_mismatch = mismatch;
    }
    _added_blocks += 1;
    _last_key.assign(last_ck.begin(), last_ck.end());
    _last_payload = row_index_payload{.data_file_offset = data_file_offset, .dt = dt};
}
ssize_t row_index_trie_writer::finish() {
    if (_added_blocks > 0) {
        size_t needed_prefix = std::min(_last_sep_mismatch + 1, _last_separator.size());
        auto tail = std::span(_last_separator).subspan(_last_sep_mismatch, needed_prefix - _last_sep_mismatch);

        std::array<std::byte, 20> payload_bytes;
        void* p = payload_bytes.data();
        p = write_unaligned(p, seastar::cpu_to_le(_last_payload.data_file_offset));
        p = write_unaligned(p, seastar::cpu_to_le(_last_payload.dt.marked_for_delete_at));
        p = write_unaligned(p, seastar::cpu_to_le(_last_payload.dt.local_deletion_time));
        assert(p == payload_bytes.data() + payload_bytes.size());

        trie_logger.trace("row_index_trie_writer::finish(): _wr.add({}, {}, {})", _last_sep_mismatch, fmt_hex_cb(tail), fmt_hex_cb(payload_bytes));
        _wr.add(_last_sep_mismatch, tail, payload(1, payload_bytes));
    }
    return _wr.finish();
}
