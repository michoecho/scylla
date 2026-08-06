#include "snapshot/updater.h"

#include <algorithm>
#include <cstdint>
#include <string>
#include <utility>

namespace snapshot_testing {
namespace {

// --- UTF-8 -------------------------------------------------------------------
//
// Both the file being rewritten and the values going into it are validated.
// The file, because this tool locates edits by byte offset derived from a
// column, and a column in a file that is not what we think it is has no
// meaning. The values, because writing invalid UTF-8 into a source file
// produces a file the compiler may reject and the user's editor may mangle --
// and the resulting corruption would be blamed on the editor, not on us.
//
// Rejects overlong encodings, surrogates and out-of-range code points, not
// just malformed lead/continuation patterns: those are exactly the sequences
// that a lax validator passes and a strict consumer later rejects.
bool valid_utf8(std::string_view text) {
    std::size_t i = 0;
    while (i < text.size()) {
        const auto byte = static_cast<unsigned char>(text[i]);
        std::size_t extra = 0;
        std::uint32_t code_point = 0;

        if (byte < 0x80) {
            i += 1;
            continue;
        } else if ((byte & 0xE0) == 0xC0) {
            extra = 1;
            code_point = byte & 0x1Fu;
        } else if ((byte & 0xF0) == 0xE0) {
            extra = 2;
            code_point = byte & 0x0Fu;
        } else if ((byte & 0xF8) == 0xF0) {
            extra = 3;
            code_point = byte & 0x07u;
        } else {
            return false;  // continuation byte in lead position, or 5+ byte lead
        }

        if (i + extra >= text.size()) return false;
        for (std::size_t k = 1; k <= extra; ++k) {
            const auto cont = static_cast<unsigned char>(text[i + k]);
            if ((cont & 0xC0) != 0x80) return false;
            code_point = (code_point << 6) | (cont & 0x3Fu);
        }

        // Overlong encodings, UTF-16 surrogates, and anything past U+10FFFF.
        if (extra == 1 && code_point < 0x80) return false;
        if (extra == 2 && code_point < 0x800) return false;
        if (extra == 3 && code_point < 0x10000) return false;
        if (code_point > 0x10FFFF) return false;
        if (code_point >= 0xD800 && code_point <= 0xDFFF) return false;

        i += extra + 1;
    }
    return true;
}

// --- line index --------------------------------------------------------------

// Byte offset of the start of each 1-based line. Index 0 is unused so that
// lines[n] is line n.
std::vector<std::size_t> line_offsets(std::string_view source) {
    std::vector<std::size_t> offsets{0, 0};
    for (std::size_t i = 0; i < source.size(); ++i)
        if (source[i] == '\n') offsets.push_back(i + 1);
    return offsets;
}

// --- literal parsing ---------------------------------------------------------
//
// The two pieces of _snap syntax this tool understands:
//
//   * a run of single-line string literals separated by whitespace and
//     comments, with _snap on the last one, decoded as their concatenation; or
//   * one block literal, R"snap(...)snap"_snap, decoded by stripping the
//     margins exactly as the _snap suffix does at compile time.
//
// One or the other, never both: they are two spellings of a whole value, and a
// mixture is not something the writer below can produce, so reading one would
// mean rewriting text nobody wrote.
//
// The block form is the *only* raw string accepted, and only with that exact
// delimiter and that exact suffix. A raw string is otherwise unreadable to a
// decoder this small -- its delimiter is arbitrary, so where it ends is a
// parsing question -- and pinning the delimiter is what turns that back into a
// literal search for a fixed terminator.
//
// Deliberately absent: any other raw string, encoding prefixes (L, u8, u, U),
// and any escape whose meaning depends on how many characters follow it
// (\x, \0-\7, \u). Those are all legal C++ that this refuses to read, because
// each one is a way for the text in the file to mean something other than what
// a naive decoder thinks -- and being wrong about the old value is how an
// updater overwrites the wrong thing. Snapshot values are written by
// render_literals below, which emits none of them.

// The block literal's fixed spelling. Fixed so that finding its end is a search
// for a known string rather than a decision.
constexpr std::string_view kBlockOpen = "R\"snap(";
constexpr std::string_view kBlockClose = ")snap\"_snap";

bool identifier_character(char c) {
    const auto byte = static_cast<unsigned char>(c);
    return c == '_' || (byte >= '0' && byte <= '9') ||
           (byte >= 'A' && byte <= 'Z') || (byte >= 'a' && byte <= 'z');
}

bool suffix_ends_at(std::string_view source, std::size_t end) {
    return end >= source.size() || !identifier_character(source[end]);
}

// Decode a block literal's raw text: drop the newline just past the opening
// delimiter, then from each line the leading spaces and one `|`.
//
// The mirror of detail::strip_margins in snapshot.h, which does this at compile
// time. The two must agree -- this reads back what that produced -- so the rule
// is stated identically in both and tested against a value that round-trips.
std::string strip_margins(std::string_view raw) {
    std::string value;
    std::size_t i = (!raw.empty() && raw[0] == '\n') ? 1 : 0;
    while (i < raw.size()) {
        while (i < raw.size() && raw[i] == ' ') ++i;
        if (i < raw.size() && raw[i] == '|') ++i;
        while (i < raw.size()) {
            const char c = raw[i++];
            value.push_back(c);
            if (c == '\n') break;
        }
    }
    return value;
}

struct ParsedLiterals {
    bool ok = false;
    std::string value;    // decoded concatenation, valid when ok
    std::size_t end = 0;  // offset just past _snap, valid when ok
    std::string error;
};

ParsedLiterals parse_literals(std::string_view source, std::size_t pos) {
    ParsedLiterals result;
    std::string value;

    // A block is a complete literal expression, including its suffix.
    if (source.compare(pos, kBlockOpen.size(), kBlockOpen) == 0) {
        const std::size_t body = pos + kBlockOpen.size();
        const std::size_t close = source.find(kBlockClose, body);
        if (close == std::string_view::npos) {
            result.error = "unterminated block snapshot literal: no )snap\"_snap";
            return result;
        }
        if (!suffix_ends_at(source, close + kBlockClose.size())) {
            result.error = "block snapshot literal has an unsupported suffix";
            return result;
        }
        result.ok = true;
        result.value = strip_margins(source.substr(body, close - body));
        result.end = close + kBlockClose.size();
        return result;
    }

    bool saw_quoted = false;

    while (true) {
        // Whitespace and comments between literals.
        while (pos < source.size()) {
            const char c = source[pos];
            if (c == ' ' || c == '\t' || c == '\r' || c == '\n') {
                ++pos;
            } else if (source.compare(pos, 2, "//") == 0) {
                while (pos < source.size() && source[pos] != '\n') ++pos;
            } else if (source.compare(pos, 2, "/*") == 0) {
                const std::size_t close = source.find("*/", pos + 2);
                if (close == std::string_view::npos) {
                    result.error = "unterminated comment inside snapshot literal";
                    return result;
                }
                pos = close + 2;
            } else {
                break;
            }
        }

        if (pos >= source.size()) {
            result.error = "unterminated snapshot literal: reached end of file";
            return result;
        }

        // The suffix ends the run. Even an empty snapshot is an ordinary empty
        // string literal followed by it: ""_snap.
        if (saw_quoted && source.compare(pos, 5, "_snap") == 0 &&
            suffix_ends_at(source, pos + 5)) {
            result.ok = true;
            result.value = std::move(value);
            result.end = pos + 5;
            return result;
        }

        if (source[pos] != '"') {
            result.error = std::string("expected a string literal or _snap, found '") +
                           source[pos] + "'";
            return result;
        }
        saw_quoted = true;

        ++pos;  // opening quote
        while (true) {
            if (pos >= source.size() || source[pos] == '\n') {
                result.error = "unterminated string snapshot literal";
                return result;
            }
            const char c = source[pos];
            if (c == '"') {
                ++pos;
                break;
            }
            if (c != '\\') {
                value.push_back(c);
                ++pos;
                continue;
            }
            // An escape. Only the fixed-length ones, for the reasons above.
            if (pos + 1 >= source.size()) {
                result.error = "unterminated escape inside snapshot literal";
                return result;
            }
            const char esc = source[pos + 1];
            switch (esc) {
                case 'n': value.push_back('\n'); break;
                case 't': value.push_back('\t'); break;
                case 'r': value.push_back('\r'); break;
                case '"': value.push_back('"'); break;
                case '\\': value.push_back('\\'); break;
                default:
                    result.error = std::string("unsupported escape '\\") + esc +
                                   "' inside snapshot literal; snapshot literals "
                                   "support only \\n \\t \\r \\\" and \\\\";
                    return result;
            }
            pos += 2;
        }
    }
}

}  // namespace

// --- rendering ---------------------------------------------------------------

std::string render_literals(std::string_view value, unsigned indent) {
    // A value occupying a single line stays inline: "foo\n"_snap. Anything
    // genuinely spanning lines becomes a block literal, whose lines are the
    // value's own lines -- so a later change shows up as a line-granular diff
    // rather than one enormous changed line, and the text in the file reads as
    // the program printed it rather than as a run of escapes.
    //
    // The test is for a newline anywhere but the very end: a lone trailing
    // newline is what nearly every line-oriented value ends with, and breaking
    // those across two source lines would be noise.
    const std::size_t first_newline = value.find('\n');
    const bool multiline =
        first_newline != std::string_view::npos && first_newline + 1 < value.size();

    const std::string pad(indent, ' ');
    std::string out;

    // What a block literal cannot carry, in which case the escaped
    // one-literal-per-line form is used instead.
    //
    // A raw string ends at its closing delimiter, so a value containing that
    // exact sequence would end the literal early -- and no choice of margin
    // fixes it, because the terminator is fixed by parse_literals. The other
    // two are legible rather than structural: a tab or a carriage return
    // written raw is invisible in the file, and an invisible character in an
    // expected value is one nobody can review. Escaped, they are at least
    // spelled.
    //
    // Both spellings decode to the same value, and the parser reads either, so
    // this choice costs nothing but the layout.
    const bool block_safe = value.find(kBlockClose) == std::string_view::npos &&
                            value.find('\t') == std::string_view::npos &&
                            value.find('\r') == std::string_view::npos;

    auto emit_literal = [&out](std::string_view line) {
        out.push_back('"');
        for (const char c : line) {
            switch (c) {
                case '\n': out += "\\n"; break;
                case '\t': out += "\\t"; break;
                case '\r': out += "\\r"; break;
                case '"': out += "\\\""; break;
                case '\\': out += "\\\\"; break;
                default: out.push_back(c);
            }
        }
        out.push_back('"');
    };

    if (!multiline) {
        emit_literal(value);
        out += "_snap";
        return out;
    }

    if (block_safe) {
        // The block form. The opening delimiter is followed immediately by a
        // newline -- stripped on the way back in -- so that the first content
        // line starts in column 1 of its own source line and aligns with the
        // rest under the call.
        //
        // Every line gets the margin, so the block is one rectangle the eye can
        // follow. Where the closing delimiter goes is decided below, and is not
        // always the next line.
        out += kBlockOpen;
        // Each source line is a newline, the margin, and the value line
        // *without* its terminator: the newline separating two value lines is
        // the same newline that separates the two source lines carrying them,
        // written once. Emitting the line with its terminator and then
        // starting the next one would write it twice.
        std::size_t start = 0;
        while (start < value.size()) {
            const std::size_t nl = value.find('\n', start);
            const std::size_t end = (nl == std::string_view::npos) ? value.size() : nl;
            out += "\n";
            out += pad;
            out += "|";
            out += value.substr(start, end - start);
            start = (nl == std::string_view::npos) ? value.size() : nl + 1;
        }
        // Where the closing delimiter goes is decided by the value's own last
        // character, and the two cases are not cosmetic.
        //
        // Ending in a newline, the delimiter goes on the next line, indented
        // with the others: the strip pass sees that line as margin with no
        // content and contributes nothing, so the value keeps exactly the
        // newline the loop wrote.
        //
        // Not ending in one, the delimiter must sit immediately after the last
        // character, because any line break before it would be inside the raw
        // string and would come back as a trailing newline the value never had.
        // That line is therefore longer than the rest -- correctness over the
        // rectangle.
        if (value.back() == '\n') {
            out += "\n";
            out += pad;
        }
        out += kBlockClose;
        return out;
    }

    // Split after each newline, keeping the newline on the line it terminates,
    // so concatenating the literals reproduces the value exactly -- including
    // whether it ended with a trailing newline.
    std::size_t start = 0;
    while (start < value.size()) {
        const std::size_t nl = value.find('\n', start);
        const std::size_t end = (nl == std::string_view::npos) ? value.size() : nl + 1;
        out += "\n";
        out += pad;
        emit_literal(value.substr(start, end - start));
        start = end;
    }
    out += "_snap";
    return out;
}

// --- the rewrite -------------------------------------------------------------

namespace {

// How far a value's lines sit in from the indentation of the line holding the
constexpr unsigned kContinuationIndent = 4;

}  // namespace

UpdateResult apply_updates(std::string_view source, std::vector<Update> updates) {
    UpdateResult result;

    if (!valid_utf8(source)) {
        result.error = "source file is not valid UTF-8";
        return result;
    }
    for (const Update& update : updates) {
        if (!valid_utf8(update.new_value)) {
            result.error = "new snapshot value at line " + std::to_string(update.line) +
                           " is not valid UTF-8";
            return result;
        }
    }

    const std::vector<std::size_t> lines = line_offsets(source);

    // --- phase one: resolve every location against the original text ---------
    //
    // Nothing is written until all of these succeed, so a set that cannot be
    // fully resolved changes nothing -- and, because no edit has happened yet,
    // every recorded location still describes the file the test actually saw.
    struct Resolved {
        std::size_t literals_start = 0;  // first byte of the literal expression
        std::size_t literals_end = 0;    // just past its _snap suffix
        unsigned indent = 0;             // leading whitespace of the call's line
        const Update* update = nullptr;
    };

    std::vector<Resolved> resolved;

    for (const Update& update : updates) {
        const std::string at = "line " + std::to_string(update.line) + " column " +
                               std::to_string(update.column);

        if (update.line == 0 || update.line >= lines.size()) {
            result.error = "no such line in source file: line " + std::to_string(update.line);
            return result;
        }

        // The reported point, as a byte offset. Columns are 1-based and count
        // bytes; a column past the end of the line is as wrong as a line past
        // the end of the file.
        const std::size_t line_start = lines[update.line];
        const std::size_t line_end =
            (update.line + 1 < lines.size()) ? lines[update.line + 1] : source.size();
        if (update.column == 0 || line_start + update.column - 1 > line_end) {
            result.error = "no such column in source file: " + at;
            return result;
        }
        const std::size_t point = line_start + update.column - 1;

        // The implicit Snapshot conversion reports the beginning of the UDL
        // expression: `"` for an ordinary literal, or `R` for the block form.
        // This is an exact anchor, not a place from which to search.
        if (source.compare(point, 1, "\"") != 0 &&
            source.compare(point, kBlockOpen.size(), kBlockOpen) != 0) {
            result.error = "no snapshot literal at " + at +
                           "; the file has changed since the test ran";
            return result;
        }

        const ParsedLiterals parsed = parse_literals(source, point);
        if (!parsed.ok) {
            result.error = parsed.error + " (at " + at + ")";
            return result;
        }

        // The staleness guard. Position told us *which* snapshot this is; this
        // proves the file still holds what the test saw, and so that rewriting
        // here cannot clobber something edited in the meantime.
        if (parsed.value != update.old_value) {
            result.error = "the snapshot at " + at +
                           " no longer holds the recorded value; the file has "
                           "changed since the test ran";
            return result;
        }

        const auto same_call = [&](const Resolved& other) {
            return other.literals_start == point;
        };
        if (std::any_of(resolved.begin(), resolved.end(), same_call)) {
            result.error = "two updates resolve to the same snapshot at " + at;
            return result;
        }

        // The indentation of the line the call sits on -- its leading
        // whitespace, not the column of the identifier. A continuation line is
        // laid out relative to the statement, so a call nested deep in an
        // expression does not drag the value out to wherever the identifier
        // happened to land. Measured from the start of the line up to its first
        // non-blank character, which for a line that is nothing but blanks is
        // its whole length.
        std::size_t first_text = line_start;
        while (first_text < line_end && (source[first_text] == ' ' || source[first_text] == '\t'))
            ++first_text;

        resolved.push_back(Resolved{
            .literals_start = point,
            .literals_end = parsed.end,
            .indent = static_cast<unsigned>(first_text - line_start),
            .update = &update,
        });
    }

    // --- phase two: rewrite, bottom-up ---------------------------------------
    //
    // Descending by position, so each replacement only disturbs text that has
    // already been rewritten and every resolved offset stays valid. See
    // updater.h.
    std::sort(resolved.begin(), resolved.end(), [](const Resolved& a, const Resolved& b) {
        return a.literals_start > b.literals_start;
    });

    std::string text(source);
    for (const Resolved& item : resolved) {
        // A multi-line value is laid out with its lines starting on the line
        // after the call, one continuation step in from the statement that owns
        // it: `indent` is the call line's own leading whitespace, and the value
        // sits kContinuationIndent further right.
        //
        // Relative to the line rather than to the identifier, so the layout does
        // not depend on how deep in an expression the call happens to sit. A
        // snapshot passed as the second argument of a nested call would
        // otherwise have its value flung far to the right, and a long enough
        // prefix would push every line past the column limit with nothing the
        // author could do about it.
        text.replace(item.literals_start, item.literals_end - item.literals_start,
                     render_literals(item.update->new_value,
                                     item.indent + kContinuationIndent));
    }

    result.ok = true;
    result.text = std::move(text);
    return result;
}

}  // namespace snapshot_testing
