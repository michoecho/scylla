#include "tracer/codegen.h"

#include <algorithm>
#include <cctype>
#include <format>
#include <map>
#include <set>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "tracer/tracer.h"

namespace tracer {
namespace {

struct field {
    std::string name;
    std::string type;  // a signature token: "u32", "str", "bytes", ...
};

// Where a complaint about a tracepoint points. The generator runs as a build
// step, so its diagnostics are all the author gets.
std::string at(const tracepoint_entry& entry) {
    return std::format("{}:{} (tracepoint \"{}\")", entry.file, entry.line, entry.name);
}

[[noreturn]] void fail(const tracepoint_entry& entry, std::string_view what) {
    throw std::runtime_error(std::format("{}: {}", at(entry), what));
}

bool is_identifier(std::string_view s) {
    if (s.empty()) return false;
    if (std::isdigit(static_cast<unsigned char>(s.front())) != 0) return false;
    return std::ranges::all_of(s, [](char c) {
        return c == '_' || std::isalnum(static_cast<unsigned char>(c)) != 0;
    });
}

// "conn:u32,peer:str" -> two fields. Everything about the shape of a signature
// that could be wrong is wrong here, where the tracepoint that produced it is
// still in hand.
std::vector<field> parse_signature(const tracepoint_entry& entry) {
    const std::string_view signature = entry.signature;
    std::vector<field> fields;
    std::set<std::string, std::less<>> seen;

    for (std::size_t pos = 0; pos < signature.size();) {
        const std::size_t comma = std::min(signature.find(',', pos), signature.size());
        const std::string_view token = signature.substr(pos, comma - pos);
        pos = comma + 1;

        const std::size_t colon = token.find(':');
        if (colon == std::string_view::npos) {
            fail(entry, std::format("parameter \"{}\" is not name:type", token));
        }
        const std::string_view name = token.substr(0, colon);
        const std::string_view type = token.substr(colon + 1);

        if (!is_identifier(name)) {
            fail(entry, std::format("parameter name \"{}\" is not an identifier", name));
        }
        if (type.empty()) {
            fail(entry, std::format("parameter \"{}\" has no type", name));
        }
        if (!seen.insert(std::string(name)).second) {
            fail(entry, std::format("parameter \"{}\" appears twice", name));
        }
        fields.push_back({std::string(name), std::string(type)});
    }
    return fields;
}

// The C++ type a signature token decodes into, or "" for a token this
// generator does not know -- which is a table it must refuse rather than guess
// at.
std::string_view decoded_type(std::string_view type) {
    if (type == "u64") return "std::uint64_t";
    if (type == "i64") return "std::int64_t";
    if (type == "u32") return "std::uint32_t";
    if (type == "i32") return "std::int32_t";
    if (type == "u16") return "std::uint16_t";
    if (type == "i16") return "std::int16_t";
    if (type == "u8") return "std::uint8_t";
    if (type == "i8") return "std::int8_t";
    if (type == "bool") return "bool";
    if (type == "ptr") return "const void*";
    if (type == "str") return "std::string_view";
    if (type == "srcloc") return "source_location";
    if (type == "bytes") return "std::span<const std::byte>";
    return {};
}

std::string escape(std::string_view s) {
    std::string out;
    for (char c : s) {
        if (c == '\\' || c == '"') out += '\\';
        out += c;
    }
    return out;
}

// The statement that reads one field out of the record.
std::string read_field(const field& f) {
    if (f.type == "srcloc") {
        // Only the address is on the wire. Turning it into a file and a line
        // needs the object it points into, which is a fact about the *moment*
        // the record was written -- so it is left to resolve_*() below, where
        // the mappings are.
        return std::format(
            "    out.{}.address = detail::read_unaligned<std::uint64_t>(p, end);\n", f.name);
    }
    if (f.type == "str") {
        return std::format("    out.{} = detail::read_str(p, end);\n", f.name);
    }
    if (f.type == "bytes") {
        return std::format("    out.{} = detail::read_bytes(p, end);\n", f.name);
    }
    if (f.type == "ptr") {
        return std::format(
            "    out.{} = reinterpret_cast<const void*>(\n"
            "        detail::read_unaligned<std::uintptr_t>(p, end));\n",
            f.name);
    }
    return std::format("    out.{} = detail::read_unaligned<{}>(p, end);\n", f.name,
                       decoded_type(f.type));
}

// --- what the tables say ------------------------------------------------------

// One tracepoint *name*, which is one struct in the generated source.
//
// Several entries may share it: a tracepoint written in a header is compiled
// into every object that includes it, and two call sites may simply be given
// the same name. They agree on a struct or they are an error, so the signature
// that first claimed the name is kept here to hold the rest to it.
struct tracepoint_kind {
    std::string name;
    std::string signature;
    std::vector<field> fields;
    const tracepoint_entry* first;  // for a diagnostic about a later disagreement
};

// One entry of one object: which struct it decodes into, and where it was
// written. Metadata is per entry rather than per kind, so a shared tracepoint
// still reports the file and line of the copy that fired -- which, for a header
// included in two libraries, is the same place anyway.
struct entry_plan {
    const tracepoint_entry* entry;
    std::size_t kind;
};

struct object_plan {
    std::string build_id;
    std::vector<entry_plan> entries;
    std::size_t first_id;  // the id of this object's entry 0
};

// The names of the tracer's own tracepoints, which a generated decoder has to
// read before it can read anything else. See "the metadata stream" in tracer.h.
constexpr std::string_view objects_loaded_kind = "trace_objects_loaded";
constexpr std::string_view object_loaded_kind = "trace_object_loaded";
constexpr std::string_view object_unloaded_kind = "trace_object_unloaded";

struct plan {
    std::vector<tracepoint_kind> kinds;
    std::vector<object_plan> objects;
    std::vector<const tracepoint_entry*> by_id;  // every entry, in id order
    std::size_t fileline_width = 0;

    // Whether the tables carried the tracer's own tracepoints. A table that
    // does not is not an object's -- it is one somebody assembled -- and the
    // decoder generated from it can describe its structs but cannot read a
    // trace, because a trace begins with events of those three shapes.
    [[nodiscard]] bool has_metadata_kinds() const {
        for (std::string_view name : {objects_loaded_kind, object_loaded_kind,
                                      object_unloaded_kind}) {
            if (std::ranges::none_of(kinds, [name](const tracepoint_kind& kind) {
                    return kind.name == name;
                })) {
                return false;
            }
        }
        return true;
    }
};

plan make_plan(std::span<const codegen_object> objects) {
    plan out;
    std::map<std::string, std::size_t, std::less<>> kind_of_name;

    for (const codegen_object& object : objects) {
        object_plan planned{std::string(object.build_id), {}, out.by_id.size()};

        for (const tracepoint_entry& entry : object.table) {
            // A tracepoint's name becomes a struct's name, so what a name has to
            // be is stricter here than at the call site.
            if (!is_identifier(entry.name)) {
                fail(entry, "tracepoint name is not an identifier");
            }

            const auto found = kind_of_name.find(entry.name);
            std::size_t kind = 0;
            if (found != kind_of_name.end()) {
                // Two tracepoints of one name are one struct, and so have to be
                // one shape. Comparing the signatures compares the parameter
                // names and their wire types at once, which is the whole of
                // what the struct is made of.
                const tracepoint_kind& existing = out.kinds[found->second];
                if (existing.signature != entry.signature) {
                    fail(entry,
                         std::format("a tracepoint of this name is defined at {}:{} with a "
                                     "different parameter list (\"{}\" there, \"{}\" here)",
                                     existing.first->file, existing.first->line,
                                     existing.signature, entry.signature));
                }
                kind = found->second;
            } else {
                std::vector<field> fields = parse_signature(entry);
                for (const field& f : fields) {
                    if (decoded_type(f.type).empty()) {
                        fail(entry, std::format("parameter \"{}\" has unknown type \"{}\"", f.name,
                                                f.type));
                    }
                }
                kind = out.kinds.size();
                out.kinds.push_back(
                    {std::string(entry.name), std::string(entry.signature), std::move(fields), &entry});
                kind_of_name.emplace(entry.name, kind);
            }

            planned.entries.push_back({&entry, kind});
            out.by_id.push_back(&entry);

            // One column width for the whole trace, so a caller printing every
            // record does not have to make two passes to line them up.
            out.fileline_width = std::max(
                out.fileline_width, std::format("{}:{}", entry.file, entry.line).size());
        }
        out.objects.push_back(std::move(planned));
    }
    return out;
}

// --- the generated source -----------------------------------------------------

std::string generate_prologue() {
    return R"cpp(// Generated by tracer::generate_decoder_source(). Do not edit.
//
// One struct per tracepoint of the objects this was generated from, and a
// decode() that turns a trace of those objects into those structs.
#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <format>
#include <fstream>
#include <iterator>
#include <map>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace trace {

// Which tracepoint a record came from, and when it was taken. Everything but
// the timestamp is a fact about the call site, fixed at compile time.
struct tracepoint_metadata {
    std::string_view name;
    std::string_view file;
    int line;
    std::string_view function;
    std::uint64_t timestamp;
};

namespace detail {

// Every read is bounded by the end of the trace. A record is not
// self-delimiting, so a truncated one cannot be recognised as short until a
// field runs off the end -- and a field that ran off the end must be an
// exception rather than a view into whatever follows the buffer.
inline void require(const std::byte* p, const std::byte* end, std::size_t n) {
    if (static_cast<std::size_t>(end - p) < n) {
        throw std::runtime_error(
            std::format("truncated trace: {} bytes left, {} wanted", end - p, n));
    }
}

template <typename To>
    requires std::is_trivially_copyable_v<To>
To read_unaligned(const std::byte*& p, const std::byte* end) {
    require(p, end, sizeof(To));
    To dst;
    std::memcpy(&dst, p, sizeof(To));
    p += sizeof(To);
    return dst;
}

// Length-prefixed runs. Both views point into the trace buffer rather than
// copying out of it, which is what keeps decoding allocation-free -- and what
// makes them valid only as long as that buffer is.
inline std::span<const std::byte> read_bytes(const std::byte*& p, const std::byte* end) {
    const auto len = read_unaligned<std::uint16_t>(p, end);
    require(p, end, len);
    const std::span<const std::byte> out(p, len);
    p += len;
    return out;
}

inline std::string_view read_str(const std::byte*& p, const std::byte* end) {
    const std::span<const std::byte> raw = read_bytes(p, end);
    return {reinterpret_cast<const char*>(raw.data()), raw.size()};
}

// How to_string() renders one field. Free functions rather than a formatter
// specialisation, so that a caller printing a field itself can reach for the
// same rendering without adopting it for every std::span in its program.
inline std::string field_to_string(std::span<const std::byte> v) {
    std::string out;
    for (std::byte b : v) {
        out += std::format("{:02x}", std::to_integer<unsigned>(b));
    }
    return out;
}

inline std::string field_to_string(std::string_view v) { return std::string(v); }

inline std::string field_to_string(const void* v) { return std::format("{}", v); }

template <typename T>
std::string field_to_string(const T& v) {
    return std::format("{}", v);
}

}  // namespace detail

// --- resolving a source location ----------------------------------------------
//
// A srcloc::location on the wire is one address, and nothing else: the address
// of a constant the compiler laid down in the object that captured it. Reading
// it back is three steps.
//
//   1. Which object was it in? The metadata stream says where each object was
//      mapped -- its base and how far it reached -- as of the moment the record
//      was written, so this is the same "read the mappings at that timestamp"
//      lookup a tracepoint address goes through, against a different range.
//   2. Subtract the base. What is left is a virtual address in the object's own
//      link-time layout, which is a fact about the *file* rather than the run.
//   3. Open the file and read it. The object is found by build ID, in the
//      directory laid out below; the location is the four fields of a
//      std::source_location, and the two of them that are pointers are more
//      virtual addresses in the same object, read the same way.
//
// A location that cannot be taken through all three -- an object the directory
// does not have, a run recorded before it was mapped -- comes out unresolved
// rather than wrong, with the address it was recorded as. See resolved below.

// One decoded source location. Owning strings rather than views into the object
// file, so that an event outlives the dso_directory it was resolved against;
// everything else a decoded event holds points into the trace, and this is the
// one field that would otherwise point somewhere with a shorter life.
struct source_location {
    std::string file;
    std::string function;
    std::uint32_t line = 0;
    std::uint32_t column = 0;

    std::uint64_t address = 0;  // as recorded, and all there is if !resolved
    std::string object;         // build ID of the object it was found in
    bool resolved = false;

    [[nodiscard]] std::string to_string() const {
        if (resolved) {
            return std::format("{}:{}:{}", file, line, column);
        }
        if (address == 0) {
            return "<none>";  // srcloc::location::none(), recorded as it was
        }
        return std::format("<unresolved {:#x}>", address);
    }
};

namespace detail {

// Where a link-time virtual address lands in the object's file, or nullptr if
// it lands nowhere that has `size` bytes behind it.
//
// The ELF header and program headers are read by offset rather than through
// <elf.h>, so that a decoder built anywhere can read a trace from a Linux
// x86-64 program -- which is the only kind there is. Anything that is not a
// little-endian 64-bit ELF is refused rather than misread.
[[nodiscard]] inline const std::byte* at_vaddr(std::span<const std::byte> image,
                                               std::uint64_t vaddr, std::size_t size) {
    constexpr std::size_t e_phoff_at = 0x20;
    constexpr std::size_t e_phentsize_at = 0x36;
    constexpr std::size_t e_phnum_at = 0x38;
    constexpr std::uint32_t pt_load = 1;

    const auto word = [image](std::size_t at, std::size_t width) -> std::uint64_t {
        std::uint64_t value = 0;
        std::memcpy(&value, image.data() + at, width);
        return value;
    };
    const auto fits = [image](std::size_t at, std::size_t width) {
        return at + width <= image.size();
    };

    if (!fits(0, 0x40) || std::memcmp(image.data(), "\x7f" "ELF\x02\x01", 6) != 0) {
        return nullptr;
    }
    const std::uint64_t phoff = word(e_phoff_at, 8);
    const std::uint64_t phentsize = word(e_phentsize_at, 2);
    const std::uint64_t phnum = word(e_phnum_at, 2);

    for (std::uint64_t i = 0; i < phnum; ++i) {
        const auto at = static_cast<std::size_t>(phoff + i * phentsize);
        if (!fits(at, 56)) {
            return nullptr;
        }
        if (word(at, 4) != pt_load) {
            continue;
        }
        const std::uint64_t p_offset = word(at + 8, 8);
        const std::uint64_t p_vaddr = word(at + 16, 8);
        const std::uint64_t p_filesz = word(at + 32, 8);
        if (vaddr < p_vaddr || vaddr - p_vaddr >= p_filesz) {
            continue;
        }
        // A .bss address is in the segment's memory size but not its file size,
        // and has nothing behind it to read.
        const std::uint64_t offset = p_offset + (vaddr - p_vaddr);
        if (!fits(static_cast<std::size_t>(offset), size)) {
            return nullptr;
        }
        return image.data() + offset;
    }
    return nullptr;
}

// The relative relocations of one object, as (place, value) pairs sorted by
// place -- both link-time virtual addresses.
//
// A pointer stored inside a shared object is not in the object's file. x86-64
// uses RELA, whose addend lives in the relocation entry rather than at the place
// it patches, so the file holds a zero there and the value is only in
// .rela.dyn; the loader writes it as the object is mapped. The two pointers of a
// source location -- its file and its function -- are exactly that, so a decoder
// reading them straight out of the image would get nothing but a zero, and
// resolve every location in every shared library to the same wrong thing.
//
// Only R_X86_64_RELATIVE is collected, which is what a pointer to something in
// the same object is. DT_RELR needs nothing here: it packs offsets and leaves
// the value in place, so the file already holds it.
//
// An object with no dynamic segment -- a non-PIE executable is the usual one --
// has no such relocations and needs none: its pointers are absolute and already
// written down.
[[nodiscard]] inline std::vector<std::pair<std::uint64_t, std::uint64_t>> relative_relocations(
    std::span<const std::byte> image) {
    constexpr std::size_t e_phoff_at = 0x20;
    constexpr std::size_t e_phentsize_at = 0x36;
    constexpr std::size_t e_phnum_at = 0x38;
    constexpr std::uint32_t pt_dynamic = 2;
    constexpr std::uint64_t dt_null = 0;
    constexpr std::uint64_t dt_rela = 7;
    constexpr std::uint64_t dt_relasz = 8;
    constexpr std::uint64_t dt_relaent = 9;
    constexpr std::uint32_t r_x86_64_relative = 8;

    std::vector<std::pair<std::uint64_t, std::uint64_t>> out;
    const auto word = [image](std::size_t at, std::size_t width) -> std::uint64_t {
        std::uint64_t value = 0;
        std::memcpy(&value, image.data() + at, width);
        return value;
    };
    if (image.size() < 0x40 || std::memcmp(image.data(), "\x7f" "ELF\x02\x01", 6) != 0) {
        return out;
    }
    const std::uint64_t phoff = word(e_phoff_at, 8);
    const std::uint64_t phentsize = word(e_phentsize_at, 2);
    const std::uint64_t phnum = word(e_phnum_at, 2);

    std::uint64_t dynamic_at = 0;
    std::uint64_t dynamic_size = 0;
    for (std::uint64_t i = 0; i < phnum; ++i) {
        const auto at = static_cast<std::size_t>(phoff + i * phentsize);
        if (at + 56 > image.size()) {
            return out;
        }
        if (word(at, 4) == pt_dynamic) {
            dynamic_at = word(at + 16, 8);   // p_vaddr
            dynamic_size = word(at + 40, 8); // p_memsz
        }
    }
    const std::byte* const dynamic = at_vaddr(image, dynamic_at, static_cast<std::size_t>(dynamic_size));
    if (dynamic == nullptr) {
        return out;
    }

    std::uint64_t rela = 0;
    std::uint64_t relasz = 0;
    std::uint64_t relaent = 24;
    for (std::uint64_t at = 0; at + 16 <= dynamic_size; at += 16) {
        std::uint64_t tag = 0;
        std::uint64_t value = 0;
        std::memcpy(&tag, dynamic + at, sizeof(tag));
        std::memcpy(&value, dynamic + at + 8, sizeof(value));
        if (tag == dt_null) {
            break;
        }
        if (tag == dt_rela) rela = value;
        if (tag == dt_relasz) relasz = value;
        if (tag == dt_relaent && value != 0) relaent = value;
    }
    if (rela == 0 || relasz == 0) {
        return out;
    }
    const std::byte* const entries = at_vaddr(image, rela, static_cast<std::size_t>(relasz));
    if (entries == nullptr) {
        return out;
    }
    for (std::uint64_t at = 0; at + 24 <= relasz; at += relaent) {
        std::uint64_t place = 0;
        std::uint64_t info = 0;
        std::uint64_t addend = 0;
        std::memcpy(&place, entries + at, sizeof(place));
        std::memcpy(&info, entries + at + 8, sizeof(info));
        std::memcpy(&addend, entries + at + 16, sizeof(addend));
        if (static_cast<std::uint32_t>(info) == r_x86_64_relative) {
            out.emplace_back(place, addend);
        }
    }
    std::sort(out.begin(), out.end());
    return out;
}

// The value a pointer field holds once the loader has been through it: what is
// written at `place` in the file, or the relocation's addend where the file
// holds nothing.
[[nodiscard]] inline std::uint64_t relocated(
    const std::vector<std::pair<std::uint64_t, std::uint64_t>>& relocations, std::uint64_t place,
    std::uint64_t in_file) {
    if (in_file != 0) {
        return in_file;
    }
    const auto found = std::lower_bound(relocations.begin(), relocations.end(),
                                        std::pair<std::uint64_t, std::uint64_t>{place, 0});
    return found != relocations.end() && found->first == place ? found->second : in_file;
}

}  // namespace detail

// The objects a trace needs to be decoded against, by build ID.
//
//     <root>/.build-id/<first two hex digits>/<the rest>[.debug]
//
// which is the layout gdb and llvm-cov already expect from a
// --debug-file-directory, and what tracer::write_dso_directory() produces. The
// whole file is read rather than mapped: a decode reads a handful of scattered
// words out of each object, and a lifetime that ends when this object does is
// worth more here than the pages saved.
class dso_directory {
public:
    explicit dso_directory(std::string root) : root_(std::move(root)) {}

    // Where a decoder looks when its caller says nothing: $TRACE_DSO_DIR, or the
    // working directory.
    dso_directory() {
        const char* const from_env = std::getenv("TRACE_DSO_DIR");
        root_ = from_env != nullptr ? from_env : ".";
    }

    // The object's bytes, or an empty span if the directory does not have it.
    // Misses are remembered too: a trace holds many records from one object, and
    // a missing object should be one failed open rather than thousands.
    [[nodiscard]] std::span<const std::byte> object(const std::string& build_id) {
        const auto found = files_.find(build_id);
        if (found != files_.end()) {
            return found->second;
        }
        std::vector<std::byte> image;
        if (build_id.size() >= 3) {
            const std::string stem =
                root_ + "/.build-id/" + build_id.substr(0, 2) + "/" + build_id.substr(2);
            // With the suffix first, because that is what a debuginfo directory
            // holds; without it for a directory of plain binaries.
            for (const std::string& path : {stem + ".debug", stem}) {
                std::ifstream in(path, std::ios::binary);
                if (in) {
                    const std::vector<char> raw{std::istreambuf_iterator<char>(in),
                                                std::istreambuf_iterator<char>()};
                    image.resize(raw.size());
                    std::memcpy(image.data(), raw.data(), raw.size());
                    break;
                }
            }
        }
        return files_.emplace(build_id, std::move(image)).first->second;
    }

    // Where the object's file is, or an empty string if the directory does not
    // have it. For handing to something that reads objects itself -- addr2line
    // over a stack of frames, say -- rather than for reading here.
    [[nodiscard]] std::string path(const std::string& build_id) const {
        if (build_id.size() < 3) {
            return {};
        }
        const std::string stem =
            root_ + "/.build-id/" + build_id.substr(0, 2) + "/" + build_id.substr(2);
        // Same two candidates, in the same order, as object() above.
        for (const std::string& candidate : {stem + ".debug", stem}) {
            if (std::ifstream(candidate, std::ios::binary)) {
                return candidate;
            }
        }
        return {};
    }

    // The object's relative relocations, read once. A location's file and
    // function pointers are not in the file of a shared object -- see
    // detail::relative_relocations() -- and a trace holds a location per record,
    // so this is built on first use and kept.
    [[nodiscard]] const std::vector<std::pair<std::uint64_t, std::uint64_t>>& relocations(
        const std::string& build_id) {
        const auto found = relocations_.find(build_id);
        if (found != relocations_.end()) {
            return found->second;
        }
        return relocations_.emplace(build_id, detail::relative_relocations(object(build_id)))
            .first->second;
    }

private:
    std::string root_;
    std::map<std::string, std::vector<std::byte>, std::less<>> files_;
    std::map<std::string, std::vector<std::pair<std::uint64_t, std::uint64_t>>, std::less<>>
        relocations_;
};

// The directory a decode uses when its caller names none. One per process, so
// that a program decoding several traces reads each object once; a caller that
// wants a directory of its own passes it to decode() instead.
[[nodiscard]] inline dso_directory& shared_dso_directory() {
    static dso_directory directory;
    return directory;
}

namespace detail {

// How to_string() renders a location, beside the other field renderings above.
inline std::string field_to_string(const source_location& v) { return v.to_string(); }

// A NUL-terminated string at a virtual address, bounded by the end of the file:
// an object naming a location it does not hold is a corrupt object, not a read
// off the end of one.
[[nodiscard]] inline std::string string_at_vaddr(std::span<const std::byte> image,
                                                 std::uint64_t vaddr) {
    const std::byte* const p = at_vaddr(image, vaddr, 1);
    if (p == nullptr) {
        return {};
    }
    const auto* text = reinterpret_cast<const char*>(p);
    const std::size_t room = image.size() - static_cast<std::size_t>(p - image.data());
    const void* const nul = std::memchr(text, '\0', room);
    return nul == nullptr ? std::string{} : std::string(text);
}

}  // namespace detail

)cpp";
}

std::string generate_struct(const tracepoint_kind& kind) {
    std::string code = std::format("// {}:{}\nstruct {} {{\n", kind.first->file, kind.first->line,
                                   kind.name);
    for (const field& f : kind.fields) {
        code += std::format("    {} {};\n", decoded_type(f.type), f.name);
    }

    // "name{a=1, b=two}". The braces are doubled because this is a format
    // string being written into a format string.
    std::string format_string = std::format("{}{{{{", kind.name);
    std::string arguments;
    for (std::size_t i = 0; i < kind.fields.size(); ++i) {
        format_string += std::format("{}{}={{}}", i == 0 ? "" : ", ", kind.fields[i].name);
        arguments += std::format(",\n                           detail::field_to_string({})",
                                 kind.fields[i].name);
    }
    format_string += "}}";

    if (!kind.fields.empty()) code += "\n";
    code += std::format(
        "    [[nodiscard]] std::string to_string() const {{\n"
        "        return std::format(\"{}\"{});\n"
        "    }}\n}};\n\n",
        format_string, arguments);
    return code;
}

// One reader per struct, shared by every entry that decodes into it.
std::string generate_reader(const tracepoint_kind& kind) {
    // The reader of a tracepoint with no parameters reads nothing, and would
    // otherwise be a function whose only parameter is unused.
    const char* const unused = kind.fields.empty() ? "[[maybe_unused]] " : "";
    std::string code = std::format("inline {} read_{}({}const std::byte*& p, {}const std::byte* end) {{\n",
                                   kind.name, kind.name, unused, unused);
    code += std::format("    {} out{{}};\n", kind.name);
    for (const field& f : kind.fields) {
        code += read_field(f);
    }
    code += "    return out;\n}\n\n";
    return code;
}

std::string generate_metadata(std::size_t id, const tracepoint_entry& entry) {
    return std::format(
        "inline constexpr tracepoint_metadata metadata_{}{{\"{}\", \"{}\", {}, \"{}\", 0}};\n", id,
        escape(entry.name), escape(entry.file), entry.line, escape(entry.function));
}

std::string generate_objects(const plan& planned) {
    std::string code = std::format(
        "// Where a record's address comes from. `first_id` is the id of the object's\n"
        "// entry 0, so an address that is `n * entry_stride` past the object's table\n"
        "// belongs to id `first_id + n`.\n"
        "inline constexpr std::size_t entry_stride = {};\n"
        "inline constexpr std::uint32_t trace_magic = {:#x};\n"
        "inline constexpr std::size_t record_header_size = {};\n"
        "inline constexpr std::uint8_t metadata_level = {};\n\n"
        "struct object_descriptor {{\n"
        "    std::string_view build_id;\n"
        "    std::uint32_t first_id;\n"
        "    std::uint32_t count;\n"
        "}};\n\n"
        "inline constexpr object_descriptor objects[] = {{\n",
        sizeof(tracepoint_entry), trace_magic, record_header_size,
        static_cast<unsigned>(event_level::metadata));
    for (const object_plan& object : planned.objects) {
        code += std::format("    {{\"{}\", {}, {}}},\n", escape(object.build_id), object.first_id,
                            object.entries.size());
    }
    code += "};\n\n";
    return code;
}

// Where an address is read against: the objects mapped as of the record being
// read, plus the files their locations are read out of. Emitted after the object
// table because a mapping points into it.
std::string generate_locator(const plan& planned) {
    std::string code = R"cpp(namespace detail {

// One object, for as long as it is mapped. `object` is null for a build ID this
// decoder was not generated from: the mapping is still kept, because it is what
// stops that object's records being attributed to the object below it -- they
// are refused by name instead.
struct mapping {
    std::uint64_t table;  // where the object's tracepoint table was mapped
    std::uint64_t base;   // and where the object itself begins
    std::uint64_t size;   // how far past the base it reaches
    const object_descriptor* object;
    std::string_view build_id;
};

// The mappings a record is read against, and the objects a location is read out
// of. One of these lives for the length of a decode; the mappings change as its
// metadata stream is consumed, which is what makes a record decode against the
// process as it was at the record's own timestamp.
class locator {
public:
    explicit locator(dso_directory& dsos) : dsos_(&dsos) {}

    std::vector<mapping> mappings;  // sorted by table address

    // Fill in a location from the object it points into. Silent about failure
    // by design: a location whose object is not in the directory, or which was
    // recorded before that object was mapped, stays unresolved and keeps the
    // address it came with. That is a decoder missing a file, not a corrupt
    // trace, and it should not stop the other records being read.
    void resolve(source_location& out) const {
        if (out.address == 0) {
            return;  // srcloc::location::none()
        }
        for (const mapping& m : mappings) {
            if (out.address < m.base || out.address - m.base >= m.size) {
                continue;
            }
            out.object = std::string(m.build_id);

            const std::span<const std::byte> image = dsos_->object(out.object);
            if (image.empty()) {
                return;  // the object is named, and not in the directory
            }
            // The four fields of a std::source_location, laid out as
            // srcloc::entry describes: two pointers then two 32-bit words. The
            // pointers hold link-time virtual addresses, and are read the same
            // way the entry itself was -- once the relocation that fills them in
            // has been applied, which in a shared object is where they live.
            const std::uint64_t entry_at = out.address - m.base;
            const std::byte* const entry = at_vaddr(image, entry_at, 24);
            if (entry == nullptr) {
                return;
            }
            std::uint64_t file_at = 0;
            std::uint64_t function_at = 0;
            std::memcpy(&file_at, entry, sizeof(file_at));
            std::memcpy(&function_at, entry + 8, sizeof(function_at));
            // In a shared object those two are zero in the file and the value is
            // in .rela.dyn; in a non-PIE executable they are already right.
            if (file_at == 0 || function_at == 0) {
                const auto& fixups = dsos_->relocations(out.object);
                file_at = relocated(fixups, entry_at, file_at);
                function_at = relocated(fixups, entry_at + 8, function_at);
            }
            std::memcpy(&out.line, entry + 16, sizeof(out.line));
            std::memcpy(&out.column, entry + 20, sizeof(out.column));
            out.file = string_at_vaddr(image, file_at);
            out.function = string_at_vaddr(image, function_at);
            out.resolved = !out.file.empty();
            return;
        }
    }

private:
    dso_directory* dsos_;
};

)cpp";

    // One resolver per struct that has a location in it, so that the switch
    // below is a call rather than a run of field names.
    for (const tracepoint_kind& kind : planned.kinds) {
        std::string body;
        for (const field& f : kind.fields) {
            if (f.type == "srcloc") {
                body += std::format("    where.resolve(out.{});\n", f.name);
            }
        }
        if (body.empty()) {
            continue;
        }
        code += std::format("inline void resolve_{}({}& out, const locator& where) {{\n{}}}\n\n",
                            kind.name, kind.name, body);
    }

    code += "}  // namespace detail\n\n";
    return code;
}

std::string generate_decode(const plan& planned) {
    std::string code = std::format(
        "// The widest \"file:line\" in the trace, for a caller lining up a column of\n"
        "// them.\n"
        "inline constexpr std::size_t fileline_width = {};\n\n",
        planned.fileline_width);

    if (!planned.has_metadata_kinds()) {
        // Not a build failure: what these tables cannot describe is a *trace*,
        // and the structs above are still worth having -- they are what a
        // caller of the generator is usually looking at. So the refusal is
        // where the impossibility is, in the one function that would have to
        // read a metadata stream that nothing in these tables can write.
        return code + std::format(
            R"cpp(// Not decodable. The tables this was generated from carry no "{}"
// tracepoint, so nothing in them can say which object a record's address
// belongs to, and every trace begins with exactly that.
template <typename Callback>
void decode(std::span<const std::byte> trace, Callback&& cb,
            dso_directory& dsos = shared_dso_directory()) {{
    (void)trace;
    (void)cb;
    (void)dsos;
    throw std::runtime_error(
        "this decoder was generated from tracepoint tables without the tracer's own "
        "metadata tracepoints, and a trace cannot be read without them");
}}

}}  // namespace trace
)cpp",
            object_loaded_kind);
    }

    code += R"cpp(// Decode every record in `trace`, in timestamp order, calling cb(event, metadata)
// for each.
//
// `trace` is a whole trace: the magic, then a chunk per level. `cb` is expected
// to have an operator() per tracepoint struct it cares about -- plus, usually, a
// template one for the rest. A struct's string and byte fields point into
// `trace`, so they outlive the call only as long as it does.
//
// The metadata level is not delivered. Its events are how this function knows
// which object an address belongs to at the moment a record was written, so it
// consumes them: they are the frame the rest of the trace is read in rather
// than events of the program's own.
//
// `dsos` is where the objects a source location points into are found, by build
// ID. It is only consulted by a trace that carries a location, and a location it
// cannot place comes out unresolved rather than stopping the decode -- see
// "resolving a source location" above. The default reads $TRACE_DSO_DIR.
//
// Throws std::runtime_error on anything that cannot be decoded. A record is not
// self-delimiting, so a truncated or corrupt stream cannot be resynchronised
// past: the first bad byte ends the decode.
template <typename Callback>
void decode(std::span<const std::byte> trace, Callback&& cb,
            dso_directory& dsos = shared_dso_directory()) {
    const std::byte* p = trace.data();
    const std::byte* const end = p + trace.size();

    if (const auto magic = detail::read_unaligned<std::uint32_t>(p, end); magic != trace_magic) {
        throw std::runtime_error(std::format("not a trace: magic {:#x}", magic));
    }

    // One level's records. The metadata stream is put first because the merge
    // below breaks ties towards the earlier stream: a load event stamped with
    // the same timestamp as the first record from the object it loads has to be
    // read before it, not after.
    struct stream {
        const std::byte* p;
        const std::byte* end;
    };
    std::vector<stream> streams;
    bool have_metadata = false;
    while (p < end) {
        const auto level = detail::read_unaligned<std::uint8_t>(p, end);
        const auto length = detail::read_unaligned<std::uint64_t>(p, end);
        detail::require(p, end, length);
        if (level == metadata_level) {
            if (have_metadata) {
                throw std::runtime_error("a trace has one metadata stream; this one has two");
            }
            have_metadata = true;
            streams.insert(streams.begin(), {p, p + length});
        } else {
            streams.push_back({p, p + length});
        }
        p += length;
    }
    if (!have_metadata) {
        throw std::runtime_error(
            "a trace with no metadata stream: nothing in it says where its objects were mapped");
    }

    // What a record is read against: the objects mapped as of the record being
    // read, kept up to date by the load and unload events below.
    detail::locator where(dsos);
    std::vector<detail::mapping>& mappings = where.mappings;

    const auto load = [&mappings](const trace_object_loaded& event) {
        const object_descriptor* found = nullptr;
        for (const object_descriptor& object : objects) {
            if (object.build_id == event.build_id) {
                found = &object;
            }
        }
        mappings.push_back({event.table_address, event.base_address, event.mapping_size, found,
                            event.build_id});
        // Sorted so that "the object a tracepoint address is in" is a binary
        // search for the greatest table address not above it.
        std::sort(mappings.begin(), mappings.end(),
                  [](const detail::mapping& a, const detail::mapping& b) {
                      return a.table < b.table;
                  });
    };

    // By base address, which is the one thing that is unique per load: two
    // objects may have no tracepoint table between them, and a table is one
    // section inside an object rather than the object itself.
    const auto unload = [&mappings](const trace_object_unloaded& event) {
        for (auto it = mappings.begin(); it != mappings.end(); ++it) {
            if (it->base == event.base_address) {
                mappings.erase(it);
                return;
            }
        }
        throw std::runtime_error(
            std::format("object {} was unloaded without having been loaded", event.build_id));
    };

    // The prologue: a count, and that many load events. Read by that invariant
    // rather than by their addresses, because until they have been read there
    // is no object for an address to be in. See "the metadata stream" in
    // tracer.h.
    {
        stream& meta = streams.front();
        detail::read_unaligned<std::uint64_t>(meta.p, meta.end);  // entry address, not yet placeable
        detail::read_unaligned<std::uint64_t>(meta.p, meta.end);  // timestamp: zero, by construction
        const trace_objects_loaded counted = detail::read_trace_objects_loaded(meta.p, meta.end);
        for (std::uint32_t i = 0; i < counted.count; i++) {
            detail::read_unaligned<std::uint64_t>(meta.p, meta.end);
            detail::read_unaligned<std::uint64_t>(meta.p, meta.end);
            load(detail::read_trace_object_loaded(meta.p, meta.end));
        }
    }

    while (true) {
        // The earliest record still unread, over every stream. A record's
        // address and timestamp are fixed-width and come first, so how long it
        // is may be unknown but when it happened is not.
        stream* next = nullptr;
        std::uint64_t earliest = 0;
        for (stream& candidate : streams) {
            if (candidate.p == candidate.end) {
                continue;
            }
            detail::require(candidate.p, candidate.end, record_header_size);
            std::uint64_t at = 0;
            std::memcpy(&at, candidate.p + sizeof(std::uint64_t), sizeof(at));
            if (next == nullptr || at < earliest) {
                next = &candidate;
                earliest = at;
            }
        }
        if (next == nullptr) {
            break;
        }

        const std::byte*& q = next->p;
        const std::byte* const q_end = next->end;
        const auto address = detail::read_unaligned<std::uint64_t>(q, q_end);
        const auto timestamp = detail::read_unaligned<std::uint64_t>(q, q_end);

        const auto above = std::upper_bound(
            mappings.begin(), mappings.end(), address,
            [](std::uint64_t value, const detail::mapping& m) { return value < m.table; });
        if (above == mappings.begin()) {
            throw std::runtime_error(std::format(
                "tracepoint address {:#x} is below every object loaded at {}", address, timestamp));
        }
        const detail::mapping& from = *(above - 1);
        if (from.object == nullptr) {
            throw std::runtime_error(std::format(
                "tracepoint address {:#x} belongs to object {}, which this decoder was not "
                "generated from",
                address, from.build_id));
        }
        const std::uint64_t offset = address - from.table;
        if (offset % entry_stride != 0 || offset / entry_stride >= from.object->count) {
            throw std::runtime_error(std::format(
                "tracepoint address {:#x} is not an entry of object {}, which is what was at "
                "{:#x} at {}",
                address, from.build_id, from.table, timestamp));
        }
        const std::uint32_t id =
            from.object->first_id + static_cast<std::uint32_t>(offset / entry_stride);

        switch (id) {
)cpp";

    for (const object_plan& object : planned.objects) {
        for (std::size_t i = 0; i < object.entries.size(); ++i) {
            const std::size_t id = object.first_id + i;
            const std::string_view kind = planned.kinds[object.entries[i].kind].name;

            // The tracer's own events are not the program's, so they are
            // applied rather than delivered.
            if (kind == object_loaded_kind || kind == object_unloaded_kind) {
                code += std::format(
                    "            case {}:\n"
                    "                {}(detail::read_{}(q, q_end));\n"
                    "                break;\n",
                    id, kind == object_loaded_kind ? "load" : "unload", kind);
                continue;
            }
            if (kind == objects_loaded_kind) {
                code += std::format(
                    "            case {}:\n"
                    "                throw std::runtime_error(\n"
                    "                    \"a {} event past the start of the metadata stream\");\n",
                    id, kind);
                continue;
            }
            // A struct with a location in it is read and then placed: the
            // reader has the bytes, and only the mappings as of this record
            // know what an address in them means.
            const bool has_location = std::ranges::any_of(
                planned.kinds[object.entries[i].kind].fields,
                [](const field& f) { return f.type == "srcloc"; });
            code += std::format(
                "            case {}: {{\n"
                "                tracepoint_metadata meta = detail::metadata_{};\n"
                "                meta.timestamp = timestamp;\n"
                "                {} event = detail::read_{}(q, q_end);\n"
                "{}"
                "                cb(event, meta);\n"
                "                break;\n"
                "            }}\n",
                id, id, kind, kind,
                has_location
                    ? std::format("                detail::resolve_{}(event, where);\n", kind)
                    : std::string{});
        }
    }

    code += R"cpp(            default:
                throw std::runtime_error(std::format("bad tracepoint id {}", id));
        }
    }
}

}  // namespace trace
)cpp";
    return code;
}

// Reading raw addresses -- stack frames -- back against the objects they are
// in. Fixed text: it only ever touches the tracer's own metadata tracepoints,
// whose shape is the same in every generated decoder.
std::string generate_mappings() {
    return R"cpp(
// Reopened: decode() above closed it, and this is a second, independent way in
// -- nothing here is needed to read a record.
namespace trace {

// One object, as the metadata prologue described it: the build ID that names
// it and the span of addresses it was mapped over.
//
// decode() keeps this to itself, because a *record* is read against the objects
// as they were at its own timestamp. This is for the other kind of address: a
// stack frame, or anything else a tracepoint carries as a bare pointer into
// the process. Those have no reader of their own -- what is at an address is in
// the object, not in the trace -- so a caller is handed the mappings and does
// its own resolving, with llvm-addr2line or anything else that takes a file and
// an offset.
struct object_mapping {
    std::string build_id;
    std::uint64_t base;  // where the object was mapped
    std::uint64_t size;  // how far past the base it reached
};

// The objects the prologue of `trace`'s metadata stream listed, which is every
// object the thread had mapped when its rings were built. A library dlopened
// afterwards is in the metadata stream proper rather than the prologue and does
// not appear here; nothing that traces today does that.
inline std::vector<object_mapping> trace_mappings(std::span<const std::byte> trace) {
    const std::byte* p = trace.data();
    const std::byte* const end = p + trace.size();
    if (detail::read_unaligned<std::uint32_t>(p, end) != trace_magic) {
        throw std::runtime_error("not a trace");
    }
    while (p < end) {
        const auto level = detail::read_unaligned<std::uint8_t>(p, end);
        const auto length = detail::read_unaligned<std::uint64_t>(p, end);
        detail::require(p, end, length);
        if (level != metadata_level) {
            p += length;
            continue;
        }
        const std::byte* q = p;
        const std::byte* const q_end = p + length;
        detail::read_unaligned<std::uint64_t>(q, q_end);  // entry address
        detail::read_unaligned<std::uint64_t>(q, q_end);  // timestamp
        const trace_objects_loaded counted = detail::read_trace_objects_loaded(q, q_end);
        std::vector<object_mapping> out;
        out.reserve(counted.count);
        for (std::uint32_t i = 0; i < counted.count; i++) {
            detail::read_unaligned<std::uint64_t>(q, q_end);
            detail::read_unaligned<std::uint64_t>(q, q_end);
            const trace_object_loaded loaded = detail::read_trace_object_loaded(q, q_end);
            out.push_back({std::string(loaded.build_id), loaded.base_address,
                           loaded.mapping_size});
        }
        return out;
    }
    throw std::runtime_error("a trace with no metadata stream");
}

// The object an address is in, or null. Linear, over a list of a few dozen: the
// mappings are not sorted, and a caller resolving a stack does this once per
// frame and then caches the answer.
[[nodiscard]] inline const object_mapping* mapping_of(
        const std::vector<object_mapping>& mappings, std::uint64_t address) {
    for (const object_mapping& m : mappings) {
        if (address >= m.base && address - m.base < m.size) {
            return &m;
        }
    }
    return nullptr;
}

}  // namespace trace
)cpp";
}

}  // namespace

std::string generate_decoder_source(std::span<const codegen_object> objects) {
    const plan planned = make_plan(objects);

    std::string out = generate_prologue();
    for (const tracepoint_kind& kind : planned.kinds) {
        out += generate_struct(kind);
    }
    out += "namespace detail {\n\n";
    for (const tracepoint_kind& kind : planned.kinds) {
        out += generate_reader(kind);
    }
    for (std::size_t id = 0; id < planned.by_id.size(); ++id) {
        out += generate_metadata(id, *planned.by_id[id]);
    }
    out += "\n}  // namespace detail\n\n";
    out += generate_objects(planned);
    out += generate_locator(planned);
    out += generate_decode(planned);
    out += generate_mappings();
    return out;
}

std::string generate_decoder_source() {
    const std::vector<trace_object> loaded = trace_objects();
    std::vector<codegen_object> objects;
    objects.reserve(loaded.size());
    for (const trace_object& object : loaded) {
        objects.push_back({object.build_id, object.table});
    }
    return generate_decoder_source(objects);
}

}  // namespace tracer
