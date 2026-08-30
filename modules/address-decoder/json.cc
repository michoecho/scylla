#include "address_decoder/json.h"

#include <charconv>
#include <cstdlib>

namespace addrdec::json {

std::optional<bool> value::as_bool() const {
    if (kind_ != kind::boolean) {
        return std::nullopt;
    }
    return boolean_;
}

std::optional<double> value::as_number() const {
    if (kind_ != kind::number) {
        return std::nullopt;
    }
    return number_;
}

std::optional<std::int64_t> value::as_int() const {
    if (kind_ != kind::number) {
        return std::nullopt;
    }
    return static_cast<std::int64_t>(number_);
}

const std::string* value::as_string() const {
    return kind_ == kind::string ? &string_ : nullptr;
}

const json::array* value::as_array() const {
    return kind_ == kind::array ? &array_ : nullptr;
}

const json::object* value::as_object() const {
    return kind_ == kind::object ? &object_ : nullptr;
}

const value& value::operator[](std::string_view key) const {
    static const value nothing;
    if (kind_ != kind::object) {
        return nothing;
    }
    const auto found = object_.find(key);
    return found == object_.end() ? nothing : found->second;
}

std::string value::string_or(std::string_view key, std::string fallback) const {
    const std::string* const s = (*this)[key].as_string();
    return s == nullptr ? std::move(fallback) : *s;
}

std::int64_t value::int_or(std::string_view key, std::int64_t fallback) const {
    return (*this)[key].as_int().value_or(fallback);
}

namespace {

// A cursor over the text. Every step either consumes what it recognised and
// returns a value, or leaves the cursor wherever it failed and returns nullopt
// -- the caller unwinds rather than trying to resynchronise, because a
// half-read document has nothing worth recovering.
class parser {
public:
    explicit parser(std::string_view text) : text_(text) {}

    std::optional<value> document() {
        skip_space();
        std::optional<value> v = parse_value(0);
        if (!v) {
            return std::nullopt;
        }
        skip_space();
        if (pos_ != text_.size()) {
            return std::nullopt;
        }
        return v;
    }

private:
    // Depth is bounded so that a pathological input -- ten thousand open
    // brackets -- fails instead of overflowing the stack of whatever thread is
    // reading the symbolizer.
    static constexpr int max_depth = 64;

    bool at_end() const { return pos_ >= text_.size(); }
    char peek() const { return text_[pos_]; }

    void skip_space() {
        while (!at_end()) {
            const char c = peek();
            if (c != ' ' && c != '\t' && c != '\n' && c != '\r') {
                break;
            }
            ++pos_;
        }
    }

    bool literal(std::string_view what) {
        if (text_.compare(pos_, what.size(), what) != 0) {
            return false;
        }
        pos_ += what.size();
        return true;
    }

    std::optional<value> parse_value(int depth) {
        if (depth > max_depth || at_end()) {
            return std::nullopt;
        }
        switch (peek()) {
        case '{': return parse_object(depth);
        case '[': return parse_array(depth);
        case '"': {
            std::optional<std::string> s = parse_string();
            if (!s) {
                return std::nullopt;
            }
            return value(std::move(*s));
        }
        case 't': return literal("true") ? std::optional(value(true)) : std::nullopt;
        case 'f': return literal("false") ? std::optional(value(false)) : std::nullopt;
        case 'n': return literal("null") ? std::optional(value()) : std::nullopt;
        default: return parse_number();
        }
    }

    std::optional<value> parse_object(int depth) {
        ++pos_;  // '{'
        json::object out;
        skip_space();
        if (!at_end() && peek() == '}') {
            ++pos_;
            return value(std::move(out));
        }
        for (;;) {
            skip_space();
            if (at_end() || peek() != '"') {
                return std::nullopt;
            }
            std::optional<std::string> key = parse_string();
            if (!key) {
                return std::nullopt;
            }
            skip_space();
            if (at_end() || peek() != ':') {
                return std::nullopt;
            }
            ++pos_;
            skip_space();
            std::optional<value> v = parse_value(depth + 1);
            if (!v) {
                return std::nullopt;
            }
            out.insert_or_assign(std::move(*key), std::move(*v));
            skip_space();
            if (at_end()) {
                return std::nullopt;
            }
            if (peek() == ',') {
                ++pos_;
                continue;
            }
            if (peek() == '}') {
                ++pos_;
                return value(std::move(out));
            }
            return std::nullopt;
        }
    }

    std::optional<value> parse_array(int depth) {
        ++pos_;  // '['
        json::array out;
        skip_space();
        if (!at_end() && peek() == ']') {
            ++pos_;
            return value(std::move(out));
        }
        for (;;) {
            skip_space();
            std::optional<value> v = parse_value(depth + 1);
            if (!v) {
                return std::nullopt;
            }
            out.push_back(std::move(*v));
            skip_space();
            if (at_end()) {
                return std::nullopt;
            }
            if (peek() == ',') {
                ++pos_;
                continue;
            }
            if (peek() == ']') {
                ++pos_;
                return value(std::move(out));
            }
            return std::nullopt;
        }
    }

    // UTF-8 encode one code point, which is how a \uXXXX escape (and a
    // surrogate pair) has to come out: the rest of this program deals in
    // std::string and expects it to be UTF-8.
    static void append_utf8(std::string& out, std::uint32_t cp) {
        if (cp <= 0x7f) {
            out.push_back(static_cast<char>(cp));
        } else if (cp <= 0x7ff) {
            out.push_back(static_cast<char>(0xc0 | (cp >> 6)));
            out.push_back(static_cast<char>(0x80 | (cp & 0x3f)));
        } else if (cp <= 0xffff) {
            out.push_back(static_cast<char>(0xe0 | (cp >> 12)));
            out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3f)));
            out.push_back(static_cast<char>(0x80 | (cp & 0x3f)));
        } else {
            out.push_back(static_cast<char>(0xf0 | (cp >> 18)));
            out.push_back(static_cast<char>(0x80 | ((cp >> 12) & 0x3f)));
            out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3f)));
            out.push_back(static_cast<char>(0x80 | (cp & 0x3f)));
        }
    }

    std::optional<std::uint32_t> parse_hex4() {
        if (pos_ + 4 > text_.size()) {
            return std::nullopt;
        }
        std::uint32_t v = 0;
        for (int i = 0; i < 4; ++i) {
            const char c = text_[pos_ + std::size_t(i)];
            v <<= 4;
            if (c >= '0' && c <= '9') {
                v |= std::uint32_t(c - '0');
            } else if (c >= 'a' && c <= 'f') {
                v |= std::uint32_t(c - 'a' + 10);
            } else if (c >= 'A' && c <= 'F') {
                v |= std::uint32_t(c - 'A' + 10);
            } else {
                return std::nullopt;
            }
        }
        pos_ += 4;
        return v;
    }

    std::optional<std::string> parse_string() {
        ++pos_;  // '"'
        std::string out;
        while (!at_end()) {
            const char c = text_[pos_++];
            if (c == '"') {
                return out;
            }
            if (c != '\\') {
                out.push_back(c);
                continue;
            }
            if (at_end()) {
                return std::nullopt;
            }
            const char esc = text_[pos_++];
            switch (esc) {
            case '"': out.push_back('"'); break;
            case '\\': out.push_back('\\'); break;
            case '/': out.push_back('/'); break;
            case 'b': out.push_back('\b'); break;
            case 'f': out.push_back('\f'); break;
            case 'n': out.push_back('\n'); break;
            case 'r': out.push_back('\r'); break;
            case 't': out.push_back('\t'); break;
            case 'u': {
                std::optional<std::uint32_t> cp = parse_hex4();
                if (!cp) {
                    return std::nullopt;
                }
                // A high surrogate is only half a code point; the low half
                // follows as a second escape. An unpaired one is passed
                // through as-is rather than rejected -- it is somebody else's
                // malformed string, not a reason to lose the whole record.
                if (*cp >= 0xd800 && *cp <= 0xdbff && pos_ + 1 < text_.size() &&
                    text_[pos_] == '\\' && text_[pos_ + 1] == 'u') {
                    const std::size_t save = pos_;
                    pos_ += 2;
                    std::optional<std::uint32_t> low = parse_hex4();
                    if (low && *low >= 0xdc00 && *low <= 0xdfff) {
                        cp = 0x10000 + ((*cp - 0xd800) << 10) + (*low - 0xdc00);
                    } else {
                        pos_ = save;
                    }
                }
                append_utf8(out, *cp);
                break;
            }
            default: return std::nullopt;
            }
        }
        return std::nullopt;
    }

    std::optional<value> parse_number() {
        const std::size_t begin = pos_;
        if (!at_end() && peek() == '-') {
            ++pos_;
        }
        const std::size_t digits_begin = pos_;
        while (!at_end() && peek() >= '0' && peek() <= '9') {
            ++pos_;
        }
        if (pos_ == digits_begin) {
            return std::nullopt;
        }
        if (!at_end() && peek() == '.') {
            ++pos_;
            const std::size_t frac_begin = pos_;
            while (!at_end() && peek() >= '0' && peek() <= '9') {
                ++pos_;
            }
            if (pos_ == frac_begin) {
                return std::nullopt;
            }
        }
        if (!at_end() && (peek() == 'e' || peek() == 'E')) {
            ++pos_;
            if (!at_end() && (peek() == '+' || peek() == '-')) {
                ++pos_;
            }
            const std::size_t exp_begin = pos_;
            while (!at_end() && peek() >= '0' && peek() <= '9') {
                ++pos_;
            }
            if (pos_ == exp_begin) {
                return std::nullopt;
            }
        }
        // std::from_chars for double is what the grammar above was validated
        // for; the text is known to be a well-formed number by here.
        double out = 0;
        const char* const first = text_.data() + begin;
        const char* const last = text_.data() + pos_;
        const auto [ptr, ec] = std::from_chars(first, last, out);
        if (ec != std::errc() || ptr != last) {
            return std::nullopt;
        }
        return value(out);
    }

    std::string_view text_;
    std::size_t pos_ = 0;
};

}  // namespace

std::optional<value> parse(std::string_view text) {
    return parser(text).document();
}

}  // namespace addrdec::json
