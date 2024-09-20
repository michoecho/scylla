#include "sstables/file_writer.hh"
#include "sstables/trie.hh"
#include "sstables/node.capnp.h"
#include <capnp/message.h>
#include <capnp/serialize-packed.h>

namespace sstables {

class trie_writer_output_file_writer final : public trie_writer_output {
    file_writer& _w;
    size_t _page_size;
    constexpr static size_t round_down(size_t a, size_t factor) {
        return a - a % factor;
    }
    constexpr static size_t round_up(size_t a, size_t factor) {
        return round_down(a + factor - 1, factor);
    }
public:
    trie_writer_output_file_writer(file_writer& w, size_t ps) : _w(w), _page_size(ps) {
        assert(_page_size >= 4096);
    }
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
    virtual size_t write(const node& x) override {
        for (size_t i = 0; i < x._children.size(); ++i) {
            assert(x._children[i]->_output_pos >= 0);
        }
        ::capnp::MallocMessageBuilder message;
        serialize(x, message);
        auto expected_sz = ::capnp::computeSerializedSizeInWords(message) * 8;
        struct out : kj::OutputStream {
            size_t sz = 0;
            file_writer& _w;
            out(file_writer& f) : _w(f) {}
            void write(const void* buffer, size_t size) override {
                sz += size;
                _w.write((char*)buffer, size);
            }
        };
        auto o = out(_w);
        ::capnp::writeMessage(o, message);
        assert(expected_sz == o.sz);
        return o.sz;
    }
    virtual size_t page_size() const override {
        return _page_size;
    }
    virtual size_t bytes_left_in_page() override {
        return round_up(pos() + 1, page_size()) - pos();
    };
    virtual size_t pad_to_page_boundary() override {
        size_t pad = bytes_left_in_page();
        auto buf = std::vector<std::byte>(pad);
        _w.write(reinterpret_cast<const char*>(buf.data()), buf.size());
        return pad;
    }
    virtual size_t pos() const override {
        return _w.offset();
    }
};

} // namespace sstables