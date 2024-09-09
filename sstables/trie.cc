#include "trie.hh"
#include <algorithm>
#include <cassert>
#include <set>
#include <capnp/message.h>
#include <capnp/serialize-packed.h>
#include "node.capnp.h"

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
        out.write(*node, 0);
        stack.pop_back();
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
    assert(x->_node_size <= static_cast<ssize_t>(_out.page_size()));
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

void partition_index_trie_writer::add(const_bytes key, uint64_t offset) {
    if (_added_keys > 0) {
        size_t mismatch = std::ranges::mismatch(key, _last_key).in2 - _last_key.begin();
        size_t needed_prefix = std::min(std::max(_last_key_mismatch, mismatch) + 1, _last_key.size());
        auto payload_bytes = std::bit_cast<std::array<std::byte, sizeof(_last_payload)>>(seastar::cpu_to_le(_last_payload));
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
    _path.push_back({co_await _in.get().read(root_offset), -1});
}

future<set_result> trie_cursor::set_before(const_bytes key) {
    assert(_path.size() > 0);
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
    assert(_path.size() > 0);
    auto res = co_await set_before(key);
    if (res == set_result::match) {
        co_return co_await step();
    }
    co_return res;
}

future<set_result> trie_cursor::step() {
    assert(_path.size() > 0);
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
    assert(_path.size() > 0);
    assert(_path.back().child_idx == -1);
    return _path.back().node.payload;
}

bool trie_cursor::eof() const {
    return size_t(_path.begin()->child_idx) == _path.begin()->node.children.size();
}

uint64_t trie_index_reader::payload_to_offset(const_bytes p) {
    assert(p.size() == sizeof(uint64_t));
    return seastar::le_to_cpu(read_unaligned<uint64_t>(p.data()));
}
std::vector<std::byte> trie_index_reader::translate_key(dht::ring_position_view key) {
    auto trie_key = std::vector<std::byte>();
    uint64_t token = seastar::cpu_to_be<uint64_t>(key.token().unbias());
    if (auto k = key.key()) {
        auto byte_comparable_form = _s->partition_key_type()->memcmp_comparable_form(*k);
        trie_key.resize(byte_comparable_form.size() + sizeof(token));
        std::memcpy(trie_key.data(), &token, sizeof(token));
        std::memcpy(trie_key.data() + sizeof(token), byte_comparable_form.data(), byte_comparable_form.size());
    } else {
        trie_key.resize(sizeof(token));
        std::memcpy(trie_key.data(), &token, sizeof(token));
    }
    return trie_key;
}
sstables::data_file_positions_range trie_index_reader::data_file_positions() const {
    trie_logger.debug("trie_index_reader::data_file_positions this={}", fmt::ptr(this));
    auto lo = _lower && !_lower->eof() ? payload_to_offset(_lower->payload()) : _total_file_size;
    auto hi = _upper && !_upper->eof() ? payload_to_offset(_upper->payload()) : _total_file_size;
    trie_logger.debug("trie_index_reader::data_file_positions this={} result=({}, {})", fmt::ptr(this), lo, hi);
    return {lo, hi};
}
future<std::optional<uint64_t>> trie_index_reader::last_block_offset() {
    trie_logger.debug("trie_index_reader::last_block_offset this={}", fmt::ptr(this));
    return make_ready_future<std::optional<uint64_t>>(std::optional<uint64_t>());
}
future<> trie_index_reader::close() noexcept {
    trie_logger.debug("trie_index_reader::close this={}", fmt::ptr(this));
    return make_ready_future<>();
}
trie_index_reader::trie_index_reader(trie_reader_input& in, uint64_t root_offset, uint64_t total_file_size, schema_ptr s)
    : _in(in), _root(root_offset), _total_file_size(total_file_size), _s(s) {
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
    co_return true;
}
future<> trie_index_reader::advance_to_next_partition() {
    trie_logger.debug("trie_index_reader::advance_to_next_partition this={}", fmt::ptr(this));
    auto res = co_await _lower.value().step();
    if (res == set_result::eof) {
        _lower.reset();
    }
    co_return;
}
sstables::indexable_element trie_index_reader::element_kind() const {
    trie_logger.debug("trie_index_reader::element_kind");
    return sstables::indexable_element::partition;
}
future<> trie_index_reader::advance_to(dht::ring_position_view pos) {
    trie_logger.debug("trie_index_reader::advance_to(partition) this={} pos={}", fmt::ptr(this), pos);
    co_await _lower->set_before(translate_key(pos));
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
        co_await _upper->set_before(translate_key(e.value().value()));
    }
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
    auto retval = payload_to_offset(_lower->payload());
    trie_logger.debug("trie_index_reader::current_clustered_cursor() this={} result={}", fmt::ptr(this), retval);
    return retval;
}
uint64_t trie_index_reader::get_promoted_index_size() {
    trie_logger.debug("trie_index_reader::get_promoted_index_size() this={}", fmt::ptr(this));
    return 0;
}
bool trie_index_reader::eof() const {
    trie_logger.debug("trie_index_reader::eof() this={}", fmt::ptr(this));
    return !_lower;
}

seastar_file_trie_writer_output::seastar_file_trie_writer_output(seastar::output_stream<char>& f) : _f(f) {
}
size_t seastar_file_trie_writer_output::serialized_size(const node& x, size_t pos) const {
    ::capnp::MallocMessageBuilder message;
    serialize(x, message);
    return ::capnp::computeSerializedSizeInWords(message) * 8;
}
size_t seastar_file_trie_writer_output::write(const node& x, size_t depth) {
    for (size_t i = 0; i < x._children.size(); ++i) {
        assert(x._children[i]->_output_pos >= 0);
    }
    ::capnp::MallocMessageBuilder message;
    serialize(x, message);
    struct out : kj::OutputStream {
        size_t sz = 0;
        seastar::output_stream<char>& _f;
        out(seastar::output_stream<char>& f) : _f(f) {}
        void write(const void* buffer, size_t size) override {
            sz += size;
            _f.write((char*)buffer, size).get();
        }
    };
    auto o = out(_f);
    ::capnp::writeMessage(o, message);
    _pos += o.sz;
    return o.sz;
}
size_t seastar_file_trie_writer_output::page_size() const {
    return 4096;
}
size_t seastar_file_trie_writer_output::bytes_left_in_page() {
    return round_up(_pos + 1, page_size()) - _pos;
}
size_t seastar_file_trie_writer_output::pad_to_page_boundary() {
    size_t pad = bytes_left_in_page();
    _pos += pad;
    return pad;
}
size_t seastar_file_trie_writer_output::pos() const {
    return _pos;
}
void seastar_file_trie_writer_output::serialize(const node& x, ::capnp::MessageBuilder& mb) const {
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

future<reader_node> seastar_file_trie_reader_input::read(uint64_t offset) {
    constexpr size_t page_size = 4096;
    auto p = co_await _f.dma_read<char>(offset / page_size, page_size);
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
trie_reader_input::~trie_reader_input() {
}
seastar_file_trie_reader_input::seastar_file_trie_reader_input(seastar::file f) : _f(f) {
}
