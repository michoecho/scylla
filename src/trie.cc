#include "trie.hh"
#include <algorithm>
#include <cassert>
#include <set>

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
        if (stage < node->_children.size() && node->_has_out_of_page_descendants) {
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
        if (stage < node->_children.size()) {
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
    assert(out.pos() - starting_pos == _branch_size + _node_size);
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
    assert(x->_node_size <= _out.page_size());
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
        auto payload_bytes = std::bit_cast<std::array<std::byte, sizeof(_last_payload)>>(_last_payload);
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