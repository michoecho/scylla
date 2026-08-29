#include "snapshot/regex_text.h"

#include <string>
#include <string_view>

#include <re2/re2.h>

namespace snapshot_testing {

std::string escape_regex(std::string_view text) {
    return RE2::QuoteMeta(text);
}

RegexText& RegexText::literal(std::string_view text) {
    text_ += text;
    pattern_ += escape_regex(text);
    return *this;
}

RegexText& RegexText::variable(std::string_view text, std::string_view pattern) {
    text_ += text;
    pattern_ += "(?:";
    pattern_ += pattern;
    pattern_ += ')';
    return *this;
}

RegexText& RegexText::append(const RegexText& other) {
    text_ += other.text_;
    pattern_ += other.pattern_;
    return *this;
}

}  // namespace snapshot_testing
