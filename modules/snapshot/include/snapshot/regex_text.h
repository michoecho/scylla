// A serialized value that carries a regular expression describing its own
// general form.
//
// Some values are worth snapshotting even though parts of them cannot be
// reproduced from one run to the next: a path under buck-out, an address, a
// line number that moves whenever the file above it is edited. A plain
// snapshot of such a value is either useless (it fails every run) or has to be
// scrubbed down to the parts that do not vary -- and scrubbing throws away
// exactly the fields a reader wants to see when the test breaks.
//
// RegexText keeps both. A serializer builds the value out of two kinds of
// piece:
//
//     RegexText out;
//     out.literal("offset ");
//     out.variable(std::format("{:#x}", offset), "0x[0-9a-f]+");
//     out.literal("\n");
//
// `text()` is then the full serialization, addresses and all, and `pattern()`
// is a regular expression matching every serialization the same code could
// produce: literal pieces contribute themselves, escaped, and variable pieces
// contribute the pattern their serializer declared.
//
// The two halves are used at different moments, which is the whole idea. A
// snapshot is *recorded* from text(), so the file holds a real, readable
// sample -- with the actual path and the actual offset in it. It is
// *compared* against pattern(), by full match, so the recorded sample keeps
// passing on a machine where those fields differ, and still fails the moment
// something structural changes: a key appears, a name changes, a column moves.
//
// The cost is that a variable field's recorded value is a sample and nothing
// more: it is not re-checked, and it goes stale silently. Declare a variable
// piece only for a field that genuinely varies, and give it the tightest
// pattern that admits every value it can take.
//
// Patterns are RE2, not std::regex: matching runs in time linear in the value,
// which matters because a snapshot is routinely thousands of characters long
// and a backtracking engine handles that by recursing once per character.
//
// See snapshot.h for compare()/render_mismatch() over one of these, and
// check.h for the assertion.

#ifndef SNAPSHOT_REGEX_TEXT_H
#define SNAPSHOT_REGEX_TEXT_H

#include <string>
#include <string_view>

namespace snapshot_testing {

// `text` as a pattern matching exactly itself: RE2::QuoteMeta, which escapes
// every character that is not a word character rather than trying to enumerate
// the syntax set.
//
// Exposed because a serializer occasionally has to build a pattern by hand --
// a variable piece whose form depends on a literal prefix, say -- and getting
// the escape set wrong is how a `.` in a path silently starts matching any
// character.
std::string escape_regex(std::string_view text);

class RegexText {
public:
    RegexText() = default;

    // A piece that is the same in every run. Contributes itself to both
    // halves, escaped on the pattern side.
    RegexText& literal(std::string_view text);

    // A piece that varies, with the pattern matching its general form.
    //
    // `pattern` is wrapped in a non-capturing group before being concatenated,
    // so an alternation ("a|b") means what its author meant rather than
    // splitting the whole compound pattern in two.
    RegexText& variable(std::string_view text, std::string_view pattern);

    // Concatenate another RegexText, both halves at once. This is what lets a
    // serializer for a field be written as a function returning a RegexText
    // and composed by the serializer for the record containing it.
    RegexText& append(const RegexText& other);

    // The full serialization: what a snapshot is recorded from.
    [[nodiscard]] const std::string& text() const { return text_; }

    // The compound pattern: what a snapshot is compared against.
    [[nodiscard]] const std::string& pattern() const { return pattern_; }

private:
    std::string text_;
    std::string pattern_;
};

}  // namespace snapshot_testing

#endif  // SNAPSHOT_REGEX_TEXT_H
