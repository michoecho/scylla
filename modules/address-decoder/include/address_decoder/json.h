// A minimal JSON reader, for llvm-symbolizer's --output-style=JSON lines.
//
// Not a general-purpose library: it exists because this module has exactly one
// producer to parse and pulling a JSON dependency in for one line format is
// more build than it is worth. It is a complete reader of the grammar all the
// same -- objects, arrays, strings with the usual escapes including \uXXXX,
// numbers, the three literals -- because a parser that handles only the shapes
// seen so far is a parser that misreads the first shape it has not seen.
#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace addrdec::json {

class value;

using object = std::map<std::string, value, std::less<>>;
using array = std::vector<value>;

// One JSON value. Held by variant-ish tag rather than std::variant so that the
// recursive object/array members can be by-value through a pointer without
// dragging incomplete-type rules into the interface.
class value {
public:
    enum class kind { null, boolean, number, string, array, object };

    value() = default;
    explicit value(bool b) : kind_(kind::boolean), boolean_(b) {}
    explicit value(double n) : kind_(kind::number), number_(n) {}
    explicit value(std::string s) : kind_(kind::string), string_(std::move(s)) {}
    explicit value(json::array a) : kind_(kind::array), array_(std::move(a)) {}
    explicit value(json::object o) : kind_(kind::object), object_(std::move(o)) {}

    kind type() const { return kind_; }
    bool is_null() const { return kind_ == kind::null; }

    // Accessors that answer "what is here, if it is what I expect" rather than
    // throwing: every caller in this module is reading a field out of somebody
    // else's output, where absent and wrong-typed are the same recovery.
    std::optional<bool> as_bool() const;
    std::optional<double> as_number() const;
    // Numbers come back through this because JSON has no integers and the
    // fields wanted here (Line, Column) are small counts.
    std::optional<std::int64_t> as_int() const;
    const std::string* as_string() const;
    const json::array* as_array() const;
    const json::object* as_object() const;

    // Object member lookup, null-safe in both directions: a non-object, or a
    // missing key, is a null value rather than an error.
    const value& operator[](std::string_view key) const;

    // Convenience for the two shapes this module reads out of a symbolizer
    // record: a string field, and a small integer field.
    std::string string_or(std::string_view key, std::string fallback = {}) const;
    std::int64_t int_or(std::string_view key, std::int64_t fallback = 0) const;

private:
    kind kind_ = kind::null;
    bool boolean_ = false;
    double number_ = 0;
    std::string string_;
    json::array array_;
    json::object object_;
};

// Parse one complete JSON document. Returns nullopt on any malformed input,
// including trailing garbage after the value.
std::optional<value> parse(std::string_view text);

}  // namespace addrdec::json
