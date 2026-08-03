#include "snapshot/updater.h"

#include <algorithm>
#include <cctype>
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
// The single piece of syntax this tool understands. Between the snapshot( and
// its closing paren it accepts a run of single-line string literals separated
// by whitespace and comments, and decodes their concatenation.
//
// Deliberately absent: raw string literals, encoding prefixes (L, u8, u, U),
// and any escape whose meaning depends on how many characters follow it
// (\x, \0-\7, \u). Those are all legal C++ that this refuses to read, because
// each one is a way for the text in the file to mean something other than what
// a naive decoder thinks -- and being wrong about the old value is how an
// updater overwrites the wrong thing. Snapshot values are written by
// render_literals below, which emits none of them.

struct ParsedLiterals {
    bool ok = false;
    std::string value;    // decoded concatenation, valid when ok
    std::size_t end = 0;  // offset just past the closing paren, valid when ok
    std::string error;
};

ParsedLiterals parse_literals(std::string_view source, std::size_t pos) {
    ParsedLiterals result;
    std::string value;

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
                    result.error = "unterminated comment inside snapshot(...)";
                    return result;
                }
                pos = close + 2;
            } else {
                break;
            }
        }

        if (pos >= source.size()) {
            result.error = "unterminated snapshot(...): reached end of file";
            return result;
        }

        // The closing paren ends the run. An empty run is legal: snapshot() is
        // how a new snapshot is written before its first update.
        if (source[pos] == ')') {
            result.ok = true;
            result.value = std::move(value);
            result.end = pos + 1;
            return result;
        }

        if (source[pos] != '"') {
            result.error = std::string("expected a string literal or ')' inside "
                                       "snapshot(...), found '") +
                           source[pos] + "'";
            return result;
        }

        ++pos;  // opening quote
        while (true) {
            if (pos >= source.size() || source[pos] == '\n') {
                result.error = "unterminated string literal inside snapshot(...)";
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
                result.error = "unterminated escape inside snapshot(...)";
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
                                   "' inside snapshot(...); snapshot literals "
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
    // A value occupying a single line stays inline: snapshot("foo\n"). Anything
    // genuinely spanning lines gets one literal per line, so that a later change
    // to it shows up as a line-granular diff rather than one enormous changed
    // line -- which is most of what makes a snapshot reviewable.
    //
    // The test is for a newline anywhere but the very end: a lone trailing
    // newline is what nearly every line-oriented value ends with, and breaking
    // those across two source lines would be noise.
    const std::size_t first_newline = value.find('\n');
    const bool multiline =
        first_newline != std::string_view::npos && first_newline + 1 < value.size();

    const std::string pad(indent, ' ');
    std::string out;

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
    return out;
}

// --- the rewrite -------------------------------------------------------------

namespace {

// The identifier this anchors on. Its length is the whole of the gcc/clang
// difference: gcc points at the '(' immediately after it, clang at its first
// character.
constexpr std::string_view kName = "snapshot";

// Whether `offset` starts the identifier `snapshot` and not some longer name
// that merely contains it.
//
// The neighbours are checked on both sides, so `check_snapshot`, `snapshot_of`
// and `mysnapshot` are all rejected. Without this, the anchor rule would
// happily accept a location pointing into the middle of an unrelated
// identifier, which is precisely the sort of near-miss the whole design exists
// to refuse.
bool is_name_at(std::string_view source, std::size_t offset) {
    if (source.compare(offset, kName.size(), kName) != 0) return false;

    auto part_of_identifier = [](char c) {
        return c == '_' || std::isalnum(static_cast<unsigned char>(c)) != 0;
    };
    if (offset > 0 && part_of_identifier(source[offset - 1])) return false;

    const std::size_t after = offset + kName.size();
    return after >= source.size() || !part_of_identifier(source[after]);
}

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
        std::size_t literals_start = 0;  // just past the '('
        std::size_t literals_end = 0;    // the closing paren
        unsigned indent = 0;             // 1-based column of the 's' of snapshot
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

        // The two conventions, and only these two. See updater.h: clang reports
        // the identifier's first character, gcc the '(' just past its end, so
        // the identifier begins either exactly at the reported point or exactly
        // kName.size() bytes before it.
        std::size_t name_start = 0;
        if (is_name_at(source, point)) {
            name_start = point;
        } else if (point >= kName.size() && is_name_at(source, point - kName.size())) {
            name_start = point - kName.size();
        } else {
            result.error = "no `snapshot` identifier at " + at +
                           "; the file has changed since the test ran";
            return result;
        }

        // Verify the rest of the shape. From here on the position is settled,
        // so every failure is a description of what is wrong with the text
        // found there rather than a reason to keep looking.
        std::size_t pos = name_start + kName.size();
        while (pos < source.size() && (source[pos] == ' ' || source[pos] == '\t')) ++pos;
        if (pos >= source.size() || source[pos] != '(') {
            result.error = "`snapshot` at " + at + " is not followed by '('";
            return result;
        }
        ++pos;

        const ParsedLiterals parsed = parse_literals(source, pos);
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
            return other.literals_start == pos;
        };
        if (std::any_of(resolved.begin(), resolved.end(), same_call)) {
            result.error = "two updates resolve to the same snapshot at " + at;
            return result;
        }

        resolved.push_back(Resolved{
            .literals_start = pos,
            .literals_end = parsed.end - 1,  // parsed.end is one past the ')'
            .indent = static_cast<unsigned>(name_start - line_start + 1),
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
        // A multi-line value is laid out with its literals starting on the line
        // after the call, indented to sit directly under the opening
        // parenthesis: `indent` is the 1-based column of the 's', "snapshot" is
        // 8 characters wide, and the '(' takes one more, so the text after it
        // begins at indent + 9. Aligning there rather than at a fixed offset is
        // what keeps a nested snapshot's value visually attached to the call
        // that owns it.
        text.replace(item.literals_start, item.literals_end - item.literals_start,
                     render_literals(item.update->new_value,
                                     item.indent + static_cast<unsigned>(kName.size())));
    }

    result.ok = true;
    result.text = std::move(text);
    return result;
}

}  // namespace snapshot_testing
