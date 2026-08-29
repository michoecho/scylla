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
#include <cstring>
#include <format>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
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
void decode(std::span<const std::byte> trace, Callback&& cb) {{
    (void)trace;
    (void)cb;
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
// Throws std::runtime_error on anything that cannot be decoded. A record is not
// self-delimiting, so a truncated or corrupt stream cannot be resynchronised
// past: the first bad byte ends the decode.
template <typename Callback>
void decode(std::span<const std::byte> trace, Callback&& cb) {
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

    // Where an object is mapped, for as long as it is. `object` is null for a
    // build ID this decoder was not generated from: the mapping is still kept,
    // because it is what stops that object's records being attributed to the
    // object below it -- they are refused by name instead.
    struct mapping {
        std::uint64_t address;
        const object_descriptor* object;
        std::string_view build_id;
    };
    std::vector<mapping> mappings;

    const auto load = [&mappings](const trace_object_loaded& event) {
        const object_descriptor* found = nullptr;
        for (const object_descriptor& object : objects) {
            if (object.build_id == event.build_id) {
                found = &object;
            }
        }
        mappings.push_back({event.table_address, found, event.build_id});
        // Sorted so that "the object an address is in" is a binary search for
        // the greatest table address not above it.
        std::sort(mappings.begin(), mappings.end(),
                  [](const mapping& a, const mapping& b) { return a.address < b.address; });
    };

    const auto unload = [&mappings](const trace_object_unloaded& event) {
        for (auto it = mappings.begin(); it != mappings.end(); ++it) {
            if (it->address == event.table_address) {
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
            [](std::uint64_t value, const mapping& m) { return value < m.address; });
        if (above == mappings.begin()) {
            throw std::runtime_error(std::format(
                "tracepoint address {:#x} is below every object loaded at {}", address, timestamp));
        }
        const mapping& from = *(above - 1);
        if (from.object == nullptr) {
            throw std::runtime_error(std::format(
                "tracepoint address {:#x} belongs to object {}, which this decoder was not "
                "generated from",
                address, from.build_id));
        }
        const std::uint64_t offset = address - from.address;
        if (offset % entry_stride != 0 || offset / entry_stride >= from.object->count) {
            throw std::runtime_error(std::format(
                "tracepoint address {:#x} is not an entry of object {}, which is what was at "
                "{:#x} at {}",
                address, from.build_id, from.address, timestamp));
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
            code += std::format(
                "            case {}: {{\n"
                "                tracepoint_metadata meta = detail::metadata_{};\n"
                "                meta.timestamp = timestamp;\n"
                "                cb(detail::read_{}(q, q_end), meta);\n"
                "                break;\n"
                "            }}\n",
                id, id, kind);
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
    out += generate_decode(planned);
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
