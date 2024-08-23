#include <span>
#include <memory>
#include <array>
#include <vector>

using const_bytes = std::span<const std::byte>;

inline const_bytes string_as_bytes(std::string_view sv) {
    return std::as_bytes(std::span(sv.data(), sv.size()));
}

inline std::string_view bytes_as_string(const_bytes sv) {
    return {reinterpret_cast<const char*>(sv.data()), sv.size()};
}

inline constexpr size_t round_down(size_t a, size_t factor) {
    return a - a % factor;
}
inline constexpr size_t round_up(size_t a, size_t factor) {
    return round_down(a + factor - 1, factor);
}


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
    int _node_size = -1;
    int _branch_size = -1;
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