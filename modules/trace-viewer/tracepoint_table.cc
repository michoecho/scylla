// Reading a `tracepoints` section back out of an object. See the header.

#include "tracepoint_table.h"

#include <algorithm>
#include <cctype>
#include <set>

#include <fmt/format.h>

#include "trace_wire.h"

namespace tracepoints {
namespace {

// `tracer::tracepoint_entry`, as bytes.
//
// The four strings and the two integers this reads; the static key at offset 40
// is skipped, because what it gates is a fact about the process that wrote the
// trace and not about the trace. tracer.h asserts these offsets, which is the
// other half of the agreement -- see the header comment.
constexpr std::uint64_t entry_size = 64;
constexpr std::uint64_t name_at = 0;
constexpr std::uint64_t file_at = 8;
constexpr std::uint64_t line_at = 16;
constexpr std::uint64_t function_at = 24;
constexpr std::uint64_t signature_at = 32;
constexpr std::uint64_t static_id_at = 48;
constexpr std::uint64_t timestamps_at = 56;

bool is_identifier(std::string_view s) {
    if (s.empty() || std::isdigit(static_cast<unsigned char>(s.front())) != 0) {
        return false;
    }
    return std::ranges::all_of(s, [](char c) {
        return c == '_' || std::isalnum(static_cast<unsigned char>(c)) != 0;
    });
}

// "conn:u32,peer:str" -> two fields, or a reason it is not a signature.
//
// The same rules tracer::generate_decoder_source() applied when it was the one
// reading these tables, minus the part where a bad one is a build failure: this
// runs against an object somebody else built, so what it can do about a table
// it cannot read is decline to read it.
std::string parse_signature(std::string_view signature, std::vector<field>& out) {
    std::set<std::string, std::less<>> seen;
    for (std::size_t pos = 0; pos < signature.size();) {
        const std::size_t comma = std::min(signature.find(',', pos), signature.size());
        const std::string_view token = signature.substr(pos, comma - pos);
        pos = comma + 1;

        const std::size_t colon = token.find(':');
        if (colon == std::string_view::npos) {
            return fmt::format("parameter \"{}\" is not name:type", token);
        }
        const std::string_view name = token.substr(0, colon);
        const std::string_view type = token.substr(colon + 1);
        if (!is_identifier(name)) {
            return fmt::format("parameter name \"{}\" is not an identifier", name);
        }
        if (type.empty()) {
            return fmt::format("parameter \"{}\" has no type", name);
        }
        if (!seen.insert(std::string(name)).second) {
            return fmt::format("parameter \"{}\" appears twice", name);
        }
        out.push_back({std::string(name), std::string(type)});
    }
    return {};
}

// The build ID a file under `.build-id/` is filed as: the directory's two hex
// digits and the file's stem. Read from the path rather than from the object's
// ELF note, because the path is what a trace's build ID is matched against --
// an object filed under the wrong name is one the decode would never find,
// whatever it says about itself.
std::string build_id_of(const std::filesystem::path& file) {
    std::string stem = file.filename().string();
    if (stem.ends_with(".debug")) {
        stem.resize(stem.size() - 6);
    }
    return file.parent_path().filename().string() + stem;
}

// One object's table, or the reason it could not be read.
//
// Every string is a link-time virtual address that has to survive two things:
// being in a shared object, where the pointer is not in the file at all but in
// .rela.dyn (see trace_wire.h), and being read from a file whose section
// headers may have been stripped. Both come out as an empty string here, which
// is what the identifier check below refuses.
std::string read_object(const std::filesystem::path& path, object& out) {
    trace::detail::mapped_file image(path.string());
    if (!image.ok()) {
        return "cannot be opened";
    }
    const std::span<const std::byte> bytes = image.bytes();
    const trace::detail::section table = trace::detail::section_by_name(bytes, "tracepoints");
    if (table.size == 0) {
        return {};  // no tracepoints in this object, which is the usual case
    }
    if (table.size % entry_size != 0) {
        return fmt::format(
            "its tracepoints section is {} bytes, which is not a whole number of {}-byte "
            "entries -- this object's tracer lays an entry out differently from the one this "
            "viewer knows",
            table.size, entry_size);
    }

    const std::vector<std::pair<std::uint64_t, std::uint64_t>> fixups =
        trace::detail::pointer_relocations(bytes);
    // A pointer field, once the loader would have been through it.
    const auto string_at = [&](std::uint64_t at) {
        const std::byte* const p = trace::detail::at_vaddr(bytes, at, 8);
        if (p == nullptr) {
            return std::string{};
        }
        std::uint64_t value = 0;
        std::memcpy(&value, p, sizeof(value));
        value = trace::detail::relocated(fixups, at, value);
        return value == 0 ? std::string{} : trace::detail::string_at_vaddr(bytes, value);
    };
    const auto word_at = [&](std::uint64_t at, std::size_t width) -> std::uint64_t {
        const std::byte* const p = trace::detail::at_vaddr(bytes, at, width);
        std::uint64_t value = 0;
        if (p != nullptr) {
            std::memcpy(&value, p, width);
        }
        return value;
    };

    for (std::uint64_t at = table.vaddr; at < table.vaddr + table.size; at += entry_size) {
        entry e;
        e.name = string_at(at + name_at);
        e.file = string_at(at + file_at);
        e.function = string_at(at + function_at);
        e.signature = string_at(at + signature_at);
        e.line = static_cast<int>(word_at(at + line_at, 4));
        e.static_id = word_at(at + static_id_at, 8);
        e.timestamps = static_cast<std::uint8_t>(word_at(at + timestamps_at, 1));

        // What says the layout was right. A wrong stride, or a section whose
        // strings did not survive, puts something that is not a tracepoint name
        // here on the first entry or two -- so this is the check that a table
        // read as this layout really is one, and not merely that its author
        // named it well.
        if (!is_identifier(e.name)) {
            return fmt::format(
                "entry {} of its tracepoints section has \"{}\" where a tracepoint name should "
                "be -- this object was built by a tracer this viewer does not know, or its "
                "strings did not survive being stripped",
                (at - table.vaddr) / entry_size, e.name);
        }
        if (e.timestamps > 2) {
            return fmt::format("tracepoint \"{}\" carries timestamp encoding {}, which this "
                               "viewer has no reader for",
                               e.name, e.timestamps);
        }
        if (const std::string why = parse_signature(e.signature, e.fields); !why.empty()) {
            return fmt::format("tracepoint \"{}\" at {}:{}: {}", e.name, e.file, e.line, why);
        }
        out.entries.push_back(std::move(e));
    }
    return {};
}

}  // namespace

std::vector<object> read_tables(const std::filesystem::path& root,
                                std::vector<std::string>& notes) {
    std::vector<object> out;
    std::error_code ec;
    const std::filesystem::path by_build_id = root / ".build-id";

    // Two levels: the two-hex-digit directories, and the objects in them.
    for (const auto& prefix : std::filesystem::directory_iterator(by_build_id, ec)) {
        if (!prefix.is_directory()) {
            continue;
        }
        for (const auto& file : std::filesystem::directory_iterator(prefix.path(), ec)) {
            if (!file.is_regular_file()) {
                continue;
            }
            object read;
            read.build_id = build_id_of(file.path());
            read.path = file.path();
            if (const std::string why = read_object(file.path(), read); !why.empty()) {
                notes.push_back(fmt::format("object {}: {}", read.build_id, why));
                continue;
            }
            if (!read.entries.empty()) {
                out.push_back(std::move(read));
            }
        }
    }
    if (ec) {
        notes.push_back(fmt::format("{}: {}", by_build_id.string(), ec.message()));
    }
    std::ranges::sort(out, {}, &object::build_id);
    return out;
}

}  // namespace tracepoints
