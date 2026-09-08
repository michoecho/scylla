// Building the decoder plugin. See decoder_plugin.h for what this is for.
//
// The file is four things, in this order:
//
//   * running a command and reading a file, which is most of what the rest does;
//   * the plan: every object's tracepoint table, folded into a list of distinct
//     tracepoint shapes and a numbering of the entries that a record's address
//     turns into;
//   * the generated source -- one reader per shape, a switch from id to reader,
//     and the record loop from trace_wire.h's primitives;
//   * the cache, the compile, and the dlopen.
//
// Nothing here throws out of `plugin::build`. Everything that can fail ends up
// in `decoder::error`.
//
// --- what the generated code knows about events.h ------------------------------
//
// Almost nothing, and that is the point. The generator has the tables, so it
// knows every field's name and wire type; what it does not know is which of
// them events.h has a member for, or what type that member is -- and it does
// not look, because looking meant parsing C++.
//
// Instead each reader is a *template* on the event type, and every assignment
// is guarded:
//
//     if constexpr (requires { event.task; }) put(event.task, value);
//
// which is a question the compiler that builds the plugin answers, against the
// real events.h, at the point the reader is instantiated with the real struct.
// A field events.h has not got is a branch that is never instantiated; a field
// whose type will not convert is a `put` that expands to nothing. Both are
// silent by construction, so the same conditions are asked a second time in
// `trace_plugin_notes`, which is how they reach the viewer's startup output.
//
// The only names the generator has to be told are the event *structs*, since
// C++ cannot be asked what a namespace contains: they come from
// VIEWER_EVENT_LIST in events.h.

#include "decoder_plugin.h"

#include <dlfcn.h>
#include <sys/wait.h>

#include <algorithm>
#include <array>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <map>
#include <set>
#include <string_view>
#include <system_error>
#include <utility>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include "events.h"
#include "tracepoint_table.h"

// events.h, plugin_abi.h and trace_wire.h, as text, compiled into this binary
// by the `:embedded_sources` genrule.
//
// Not paths to them, because a plugin is compiled at *runtime* and has to be
// compiled against the very events.h this viewer was built from -- a path would
// be a promise that the source tree beside the binary is still the one it came
// from. They are written out into the plugin's cache directory, so the
// directory is a complete, rebuildable record of what the plugin was made of.
#include "embedded_sources.inc"

namespace plugin {
namespace {

// Bumped when anything about the generated source *or how it is compiled*
// changes, so that a cache filled by an older viewer is not read by a newer
// one. Everything else in the key is an input file; this stands for the code
// below, which the key cannot see.
constexpr int generator_version = 2;

// The tracer's own tracepoints, which a decoder has to read before it can read
// anything else: the metadata stream opens with them, and until it has been
// read no address in the trace means anything. See "the metadata stream" in
// tracer.h.
constexpr std::string_view clock_sync_name = "clock_sync";
constexpr std::string_view objects_loaded_name = "trace_objects_loaded";
constexpr std::string_view object_loaded_name = "trace_object_loaded";
constexpr std::string_view object_unloaded_name = "trace_object_unloaded";

// Copied from tracer.h rather than included from it: the plugin generator is
// the trace's *reader*, and taking a dependency on the writer's headers -- and
// so on static_keys and source_location -- to learn two constants would put the
// producer's build back inside the consumer's.
constexpr std::uint32_t trace_magic = 0x32435254;  // "TRC2"
constexpr std::uint8_t metadata_level = 2;         // event_level::metadata
constexpr std::size_t entry_stride = 64;           // sizeof(tracer::tracepoint_entry)

// ============================================================================
//  files and commands
// ============================================================================

bool write_file(const std::filesystem::path& path, std::string_view text) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out.write(text.data(), std::streamsize(text.size()));
    return bool(out);
}

struct command_result {
    int status = -1;
    std::string output;  // stdout and stderr together
};

// Run a shell command and collect everything it said. stderr is folded into
// stdout by the caller's `2>&1`, because what is wanted from a failed compile
// is the diagnostics.
command_result run_command(const std::string& command) {
    command_result out;
    std::FILE* const pipe = ::popen(command.c_str(), "r");
    if (pipe == nullptr) {
        out.output = "could not run a shell";
        return out;
    }
    std::array<char, 4096> buffer{};
    while (const std::size_t n = std::fread(buffer.data(), 1, buffer.size(), pipe)) {
        out.output.append(buffer.data(), n);
    }
    const int closed = ::pclose(pipe);
    out.status = closed == -1 ? -1 : (WIFEXITED(closed) ? WEXITSTATUS(closed) : 128);
    return out;
}

std::string shell_quote(const std::string& text) {
    std::string out = "'";
    for (const char c : text) {
        if (c == '\'') {
            out += "'\\''";
        } else {
            out += c;
        }
    }
    return out + "'";
}

// A cache key, not a checksum: FNV-1a over every input that decides what the
// plugin is. Sixty-four bits is far more than a directory of a handful of
// plugins needs, and a collision costs a wrong plugin rather than a wrong
// answer -- the alternative, spawning sha256sum per build, is not worth it.
class hasher {
public:
    void put(std::string_view text) {
        for (const char c : text) {
            value_ ^= static_cast<unsigned char>(c);
            value_ *= 1099511628211ull;
        }
        value_ ^= 0x5bf03635;  // a separator, so that "ab"+"c" and "a"+"bc" differ
        value_ *= 1099511628211ull;
    }
    [[nodiscard]] std::string hex() const { return fmt::format("{:016x}", value_); }

private:
    std::uint64_t value_ = 14695981039346656037ull;
};

struct toolchain {
    std::string cxx;       // what to run
    std::string identity;  // its --version line, for the cache key
    std::string error;     // non-empty if there is no usable one
};

const toolchain& host_toolchain() {
    static const toolchain found = [] {
        toolchain t;
        std::vector<std::string> candidates;
        if (const char* const named = std::getenv("TRACE_CXX"); named != nullptr) {
            candidates.emplace_back(named);
        } else {
            candidates = {"clang++", "c++", "g++"};
        }
        for (const std::string& candidate : candidates) {
            const command_result version =
                run_command(shell_quote(candidate) + " --version 2>/dev/null");
            if (version.status != 0) {
                continue;
            }
            t.cxx = candidate;
            t.identity = version.output.substr(0, version.output.find('\n'));
            return t;
        }
        t.error = fmt::format(
            "no C++ compiler: none of {} would run. A decoder plugin is compiled at startup, "
            "so the viewer needs one on PATH -- run it inside `nix develop`, or set $TRACE_CXX",
            fmt::join(candidates, ", "));
        return t;
    }();
    return found;
}

std::string escape(std::string_view s) {
    std::string out;
    for (const char c : s) {
        if (c == '\\' || c == '"') out += '\\';
        out += c;
    }
    return out;
}

// ============================================================================
//  the plan
// ============================================================================

// The plan's own types are in decoder_plugin.h, where the tests can reach
// them; everything that folds tables into one is here.
using detail::plan;
using detail::shape;
using detail::slice;

// The events events.h declares, which is the whole of what the generator is
// told about it. See VIEWER_EVENT_LIST there.
const std::set<std::string, std::less<>>& viewer_events() {
    static const std::set<std::string, std::less<>> names = {
#define VIEWER_EVENT_NAME(name) #name,
        VIEWER_EVENT_LIST(VIEWER_EVENT_NAME)
#undef VIEWER_EVENT_NAME
    };
    return names;
}

}  // namespace

namespace detail {

plan make_plan(const std::vector<tracepoints::object>& objects) {
    plan out;
    std::map<std::string, std::size_t, std::less<>> shape_of_key;
    // Which names the tables have that events.h has not, and the other way
    // round: reported once each rather than once per entry.
    std::set<std::string, std::less<>> unbridged;
    std::set<std::string, std::less<>> bridged;

    for (const tracepoints::object& object : objects) {
        slice planned{object.build_id, out.by_id.size(), object.entries.size()};
        for (const tracepoints::entry& entry : object.entries) {
            const std::string key =
                fmt::format("{}|{}|{}", entry.name, entry.signature, entry.timestamps);
            const auto [it, fresh] = shape_of_key.try_emplace(key, out.shapes.size());
            if (fresh) {
                shape s;
                s.name = entry.name;
                s.signature = entry.signature;
                s.timestamps = entry.timestamps;
                s.fields = entry.fields;
                s.bridged = viewer_events().contains(entry.name);
                out.shapes.push_back(std::move(s));
            }
            (out.shapes[it->second].bridged ? bridged : unbridged).insert(entry.name);

            // The one thing a static id has to be is unambiguous: it is on the
            // wire *instead* of the entry's address, so two tracepoints sharing
            // one are two events nothing can tell apart. Two entries of the
            // same tracepoint are not two tracepoints -- one written in a
            // header is compiled into every object that includes it, and a
            // cluster has one per build -- so what is refused is a shared id
            // whose entries disagree about which tracepoint it is.
            if (entry.static_id != 0) {
                const auto [claim, first] =
                    out.static_ids.try_emplace(entry.static_id, out.by_id.size());
                if (!first && out.by_id[claim->second]->name != entry.name) {
                    out.notes.push_back(fmt::format(
                        "static id {} is \"{}\" in one object and \"{}\" in another; records "
                        "carrying it are read as the first",
                        entry.static_id, out.by_id[claim->second]->name, entry.name));
                }
            }

            out.shape_of_id.push_back(it->second);
            out.by_id.push_back(&entry);
        }
        out.slices.push_back(std::move(planned));
    }

    for (const std::string& name : unbridged) {
        if (!bridged.contains(name) && !name.starts_with("trace_") &&
            name != clock_sync_name) {
            out.notes.push_back(
                fmt::format("tracepoint \"{}\" is in these objects and not in events.h; its "
                            "records are read past and dropped",
                            name));
        }
    }
    for (const std::string& name : viewer_events()) {
        if (!bridged.contains(name)) {
            out.notes.push_back(fmt::format(
                "events.h wants \"{}\" and no object here has a tracepoint of that name", name));
        }
    }
    return out;
}

}  // namespace detail

namespace {

// ============================================================================
//  the generated source
// ============================================================================

// The C++ type a wire type token decodes into, or "" for one this viewer has no
// reader for -- which is a tracepoint whose records cannot be read past, and so
// a shape whose reader throws.
std::string_view cpp_type(std::string_view token) {
    if (token == "u64") return "std::uint64_t";
    if (token == "i64") return "std::int64_t";
    if (token == "u32") return "std::uint32_t";
    if (token == "i32") return "std::int32_t";
    if (token == "u16") return "std::uint16_t";
    if (token == "i16") return "std::int16_t";
    if (token == "u8") return "std::uint8_t";
    if (token == "i8") return "std::int8_t";
    if (token == "bool") return "bool";
    if (token == "ptr") return "const void*";
    if (token == "str") return "std::string_view";
    if (token == "bytes") return "std::span<const std::byte>";
    if (token == "srcloc") return "trace::source_location";
    return {};
}

// The expression that reads one field off the wire. A location is the odd one:
// only its address is there, and turning that into a file and a line needs the
// objects as they were at this record's timestamp, so it is done in two steps
// where the mappings are.
std::string read_expression(std::string_view token) {
    if (token == "str") return "trace::detail::read_str(p, end)";
    if (token == "bytes") return "trace::detail::read_bytes(p, end)";
    if (token == "ptr") {
        return "reinterpret_cast<const void*>(\n"
               "            trace::detail::read_unaligned<std::uintptr_t>(p, end))";
    }
    if (token == "srcloc") return "trace::detail::read_unaligned<std::uint64_t>(p, end)";
    return fmt::format("trace::detail::read_unaligned<{}>(p, end)", cpp_type(token));
}

// How a shape's records carry their timestamp, as the name of the reader that
// takes one from the head of its body to the moment it means.
std::string_view timestamp_reader(std::uint8_t encoding) {
    switch (encoding) {
        case 1: return "read_timestamp_sync";
        case 2: return "read_timestamp_none";
        default: return "read_timestamp_delta";
    }
}

// Whether a shape has a parameter this viewer has no reader for. Such a record
// cannot be read *past* either -- the field's length is part of what it is -- so
// the whole stream stops there rather than the field being skipped.
const tracepoints::field* unreadable_field(const shape& s) {
    for (const tracepoints::field& f : s.fields) {
        if (cpp_type(f.type).empty()) {
            return &f;
        }
    }
    return nullptr;
}

// `advance_kN`: read a record's body and throw all of it away.
//
// Every shape gets one, bridged or not. A record of a tracepoint events.h has
// never heard of still has to be walked over exactly, because the record after
// it starts where this one ends and nothing in the stream says where that is.
std::string generate_advance(std::size_t index, const shape& s) {
    std::string body;
    if (const tracepoints::field* bad = unreadable_field(s)) {
        body = fmt::format(
            "    throw std::runtime_error(\n"
            "        \"tracepoint \\\"{}\\\" has a parameter \\\"{}\\\" of type \"\n"
            "        \"\\\"{}\\\", which \"\n"
            "        \"this viewer has no reader for\");\n",
            escape(s.name), escape(bad->name), escape(bad->type));
    } else {
        for (const tracepoints::field& f : s.fields) {
            body += fmt::format("    static_cast<void>({});\n", read_expression(f.type));
        }
    }
    return fmt::format(
        "// {}({})\ninline const std::byte* advance_k{}(\n"
        "    [[maybe_unused]] const std::byte* p, [[maybe_unused]] const std::byte* end) {{\n"
        "{}    return p;\n}}\n\n",
        s.name, s.signature, index, body);
}

// `deliver_kN`: read a record's body into the events.h struct of the same name
// and hand it to the viewer.
//
// A template on the event type so that `requires { event.x; }` is a question
// about a dependent type, and so a field events.h has not got is a branch that
// is never instantiated rather than a compile error. See the file comment.
std::string generate_deliver(std::size_t index, const shape& s) {
    std::string locations;
    std::string body;
    for (const tracepoints::field& f : s.fields) {
        if (f.type == "srcloc") {
            // Declared at the top so that the strings it owns outlive the
            // callback: everything else an event points at points into the
            // trace, and this is the one field that would otherwise point into
            // a block that has already ended.
            locations += fmt::format("    trace::source_location at_{}{{}};\n", f.name);
            body += fmt::format(
                "    at_{0}.address = {1};\n"
                "    where.resolve(at_{0});\n"
                "    if constexpr (requires {{ event.{0}; }}) put_location(event.{0}, at_{0});\n",
                f.name, read_expression(f.type));
            continue;
        }
        body += fmt::format(
            "    {{\n"
            "        const {} value = {};\n"
            "        if constexpr (requires {{ event.{}; }}) put(event.{}, value);\n"
            "    }}\n",
            cpp_type(f.type), read_expression(f.type), f.name, f.name);
    }
    return fmt::format(
        "// {0}({1})\ntemplate <class Event>\n"
        "const std::byte* deliver_k{2}(\n"
        "    void* sink, const trace::meta_info& info, std::uint64_t timestamp,\n"
        "    [[maybe_unused]] const std::byte* p, [[maybe_unused]] const std::byte* end,\n"
        "    [[maybe_unused]] const trace::detail::locator& where) {{\n"
        "    Event event{{}};\n"
        "    viewer::event_meta meta{{}};\n"
        "    fill_meta(meta, info, timestamp);\n"
        "{3}{4}"
        "    on_decode_{0}(sink, event, meta);\n"
        "    return p;\n}}\n\n",
        s.name, s.signature, index, locations, body);
}

// `notes_kN`: the same questions again, out loud.
//
// Every `if constexpr` in the reader above is a thing the viewer will not know
// about these traces, and every one of them is silent where it is. This is the
// only way the answers get out, because they are answers only the compiler that
// built the plugin has.
std::string generate_notes(std::size_t index, const shape& s) {
    std::string body;
    for (const tracepoints::field& f : s.fields) {
        body += fmt::format(
            "    if constexpr (!requires {{ Event{{}}.{0}; }}) {{\n"
            "        emit(ctx, \"{1}.{0} ({2}): events.h has no field of that name\");\n"
            "    }}",
            f.name, escape(s.name), escape(f.type));
        if (f.type == "srcloc") {
            body += "\n";
            continue;
        }
        body += fmt::format(
            " else if constexpr (!convertible<decltype(Event{{}}.{0}), {1}>) {{\n"
            "        emit(ctx, \"{2}.{0}: a {3} does not convert to the type events.h "
            "declares\");\n"
            "    }}\n",
            f.name, cpp_type(f.type), escape(s.name), escape(f.type));
    }
    if (body.empty()) {
        body = "    (void)emit;\n    (void)ctx;\n";
    }
    return fmt::format(
        "template <class Event>\nvoid notes_k{}(trace_plugin_note_fn emit, void* ctx) {{\n{}}}\n\n",
        index, body);
}

// A reader for one of the tracer's own metadata tracepoints: the two or four
// fields the record loop needs, by name, and the rest read past.
//
// By name because that is all these are: `trace_object_loaded` is a tracepoint
// like any other, written by the same macro, and what makes it the frame rather
// than an event is only which name it has.
std::string generate_metadata_reader(const shape& s, std::size_t index,
                                     const std::string& type_name,
                                     const std::vector<tracepoints::field>& wanted,
                                     std::string& error) {
    std::string body;
    std::set<std::string, std::less<>> found;
    for (const tracepoints::field& f : s.fields) {
        const auto want = std::ranges::find_if(wanted, [&](const tracepoints::field& w) {
            return w.name == f.name;
        });
        if (want == wanted.end()) {
            body += fmt::format("    static_cast<void>({});\n", read_expression(f.type));
            continue;
        }
        if (want->type != f.type) {
            error = fmt::format(
                "the tracer's own \"{}\" tracepoint carries \"{}\" as a {}, and this viewer "
                "reads it as a {}",
                s.name, f.name, f.type, want->type);
            return {};
        }
        body += fmt::format("    out.{} = {};\n", f.name, read_expression(f.type));
        found.insert(f.name);
    }
    for (const tracepoints::field& w : wanted) {
        if (!found.contains(w.name)) {
            error = fmt::format(
                "the tracer's own \"{}\" tracepoint has no \"{}\" parameter, and the metadata "
                "stream cannot be read without it",
                s.name, w.name);
            return {};
        }
    }
    return fmt::format("inline {0} read_{0}_k{1}(const std::byte*& p, const std::byte* end) {{\n"
                       "    {0} out{{}};\n{2}    return out;\n}}\n\n",
                       type_name, index, body);
}

// The fixed half of the generated file: everything that does not depend on
// which tracepoints there are. Kept as text rather than in trace_wire.h because
// every line of it is about crossing into events.h, which trace_wire.h has
// never heard of.
constexpr std::string_view preamble =
    R"cpp(// Generated by the trace viewer at startup. Do not edit -- rebuild it by
// deleting this directory and running the viewer again.
//
// The objects a snapshot came from, as a decoder. `trace_wire.h` reads the
// records; the switch at the bottom of this file says what each one *is*, which
// is what those objects' `tracepoints` sections said; and `events.h` is what
// this viewer wants an event to be. Everything between them is generated.

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <exception>
#include <format>
#include <map>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "events.h"
#include "plugin_abi.h"
#include "trace_wire.h"

@@CALLBACKS@@
namespace {

// Whether one of the tables' fields may fill in one of events.h's.
//
// Deliberately narrow. A trace is read to answer questions about microseconds
// and task ids, and a conversion that silently loses the top half of one -- an
// unsigned long into a uint32_t -- gives an answer that looks like an answer and
// is not. So: the same type, or a wider integer of the same signedness. Anything
// else is refused, reported by trace_plugin_notes, and left at its default.
template <class To, class From>
inline constexpr bool convertible =
    std::is_same_v<To, From> ||
    (std::is_integral_v<To> && std::is_integral_v<From> && !std::is_same_v<To, bool> &&
     !std::is_same_v<From, bool> && std::is_signed_v<To> == std::is_signed_v<From> &&
     sizeof(To) >= sizeof(From));

template <class To, class From>
void put(To& to, const From& from) {
    if constexpr (convertible<To, From>) {
        to = from;
    }
}

// A location, and the event's metadata, are the two structs crossed by name
// rather than by a tracepoint's parameter list -- so which members events.h
// gives them is asked here, the same way, and one it has not got is one it does
// not get.
template <class To>
void put_location(To& to, const trace::source_location& from) {
    if constexpr (requires { to.file; }) put(to.file, std::string_view(from.file));
    if constexpr (requires { to.function; }) put(to.function, std::string_view(from.function));
    if constexpr (requires { to.line; }) put(to.line, from.line);
    if constexpr (requires { to.column; }) put(to.column, from.column);
    if constexpr (requires { to.address; }) put(to.address, from.address);
    if constexpr (requires { to.object; }) put(to.object, std::string_view(from.object));
    if constexpr (requires { to.resolved; }) put(to.resolved, from.resolved);
}

template <class Meta>
void fill_meta(Meta& meta, const trace::meta_info& info, std::uint64_t timestamp) {
    if constexpr (requires { meta.name; }) put(meta.name, info.name);
    if constexpr (requires { meta.file; }) put(meta.file, info.file);
    if constexpr (requires { meta.line; }) put(meta.line, info.line);
    if constexpr (requires { meta.function; }) put(meta.function, info.function);
    if constexpr (requires { meta.has_timestamp; }) put(meta.has_timestamp, info.has_timestamp);
    if constexpr (requires { meta.timestamp; }) put(meta.timestamp, timestamp);
}

// What the metadata stream says, in the shape the record loop wants it. The
// readers that fill these in are generated, because the tracer's own
// tracepoints are tracepoints.
struct object_loaded {
    std::string_view build_id;
    std::uint64_t table_address = 0;
    std::uint64_t base_address = 0;
    std::uint64_t mapping_size = 0;
};

struct object_unloaded {
    std::uint64_t base_address = 0;
};

struct objects_loaded {
    std::uint32_t count = 0;
};

// --- what the objects said ----------------------------------------------------

inline constexpr std::uint32_t trace_magic = @@MAGIC@@;
inline constexpr std::uint8_t metadata_level = @@METADATA_LEVEL@@;
inline constexpr std::size_t entry_stride = @@ENTRY_STRIDE@@;

// An address that is `n * entry_stride` past an object's table is its entry
// `n`, and so the id `first_id + n`.
inline constexpr trace::object_descriptor objects[] = {
@@OBJECTS@@};

// One per id: what its entry said about where it was written. Indexed by id,
// which is why it is an array rather than a case in the switch.
inline constexpr trace::meta_info metadata[] = {
@@METADATA@@};

// A record that named its tracepoint by a static id rather than by the address
// of its entry. The id has nothing to do with where anything was mapped, so
// this is the whole of the lookup.
inline constexpr std::uint32_t id_for_static_id(std::uint64_t static_id) {
    switch (static_id) {
@@STATIC_IDS@@        default: return trace::no_decoder_id;
    }
}

// Which of the three timestamp readers a record's body opens with. Only the
// tracepoints that are not timed the usual way get a case; the rest are the
// default, which is also where an id nothing could place ends up -- such an id
// is not an error here, because this runs on the peek that orders the streams,
// where a record may simply be waiting for the load event that explains it.
inline std::uint64_t read_timestamp(std::uint32_t id, const std::byte*& p, const std::byte* end,
                                    std::uint64_t last) {
    switch (id) {
@@TIMESTAMP_CASES@@        default: return trace::detail::read_timestamp_delta(p, end, last);
    }
}

// --- one reader per shape of tracepoint ---------------------------------------

@@READERS@@
// --- the record loop ----------------------------------------------------------

// Decode every record in `trace`, in timestamp order, delivering each to the
// viewer through the `on_decode_*` of its tracepoint.
//
// The metadata level is not delivered. Its events are how this function knows
// which object an address belongs to at the moment a record was written, so it
// consumes them: they are the frame the rest of the trace is read in rather
// than events of the program's own.
//
// Throws std::runtime_error on anything that cannot be decoded. A record is not
// self-delimiting, so a truncated or corrupt stream cannot be resynchronised
// past: the first bad byte ends the decode, and what was read before it has
// already been delivered.
void decode(std::span<const std::byte> trace, void* sink, trace::dso_directory& dsos) {
    const std::byte* p = trace.data();
    const std::byte* const end = p + trace.size();

    if (const auto magic = trace::detail::read_unaligned<std::uint32_t>(p, end);
        magic != trace_magic) {
        throw std::runtime_error(std::format("not a trace: magic {:#x}", magic));
    }

    // One level's records. The metadata stream is put first because the merge
    // below breaks ties towards the earlier stream: a load event stamped with
    // the same timestamp as the first record from the object it loads has to be
    // read before it, not after.
    struct stream {
        const std::byte* p;
        const std::byte* end;
        std::uint64_t last_timestamp = 0;
    };
    std::vector<stream> streams;
    bool have_metadata = false;
    while (p < end) {
        const auto level = trace::detail::read_unaligned<std::uint8_t>(p, end);
        const auto length = trace::detail::read_unaligned<std::uint64_t>(p, end);
        trace::detail::require(p, end, length);
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
    trace::detail::locator where(dsos);
    std::vector<trace::detail::mapping>& mappings = where.mappings;

    const auto load = [&where](const object_loaded& event) {
        const trace::object_descriptor* found = nullptr;
        for (const trace::object_descriptor& object : objects) {
            if (object.build_id == event.build_id) {
                found = &object;
            }
        }
        // Which reads the object out of the directory once, here, rather than
        // once per location resolved against it.
        where.add(event.table_address, event.base_address, event.mapping_size, found,
                  event.build_id);
    };

    // By base address, which is the one thing that is unique per load: two
    // objects may have no tracepoint table between them, and a table is one
    // section inside an object rather than the object itself.
    const auto unload = [&mappings](const object_unloaded& event) {
        for (auto it = mappings.begin(); it != mappings.end(); ++it) {
            if (it->base == event.base_address) {
                mappings.erase(it);
                return;
            }
        }
        throw std::runtime_error("an object was unloaded without having been loaded");
    };

    // Which tracepoint a record names: a static id says it on its own, an
    // address says it only against the objects mapped as of this point in the
    // trace. `no_decoder_id` for one that cannot be placed, without a word
    // about why -- because this is asked twice, once on the peek that orders
    // the streams, where an address whose object has not been loaded *yet* is
    // not an error, and once on the record actually being read, where refuse()
    // says what is wrong.
    const auto placed_id = [&mappings](const trace::detail::record_id& which) -> std::uint32_t {
        if (which.is_static) {
            return id_for_static_id(which.value);
        }
        const std::uint64_t address = which.value;
        const auto above = std::upper_bound(
            mappings.begin(), mappings.end(), address,
            [](std::uint64_t value, const trace::detail::mapping& m) { return value < m.table; });
        if (above == mappings.begin() || (above - 1)->object == nullptr) {
            return trace::no_decoder_id;
        }
        const trace::detail::mapping& from = *(above - 1);
        const std::uint64_t offset = address - from.table;
        if (offset % entry_stride != 0 || offset / entry_stride >= from.object->count) {
            return trace::no_decoder_id;
        }
        return static_cast<std::uint32_t>(from.object->first_id + offset / entry_stride);
    };

    // Why the record at the head of a stream cannot be placed. `after` is the
    // timestamp of the record before it, which is as much as is known about
    // when this one is: how a record says *when* it happened is a fact about
    // which tracepoint it is, and that is the question this one failed.
    const auto refuse = [&mappings](const trace::detail::record_id& which, std::uint64_t after) {
        if (which.is_static) {
            throw std::runtime_error(std::format(
                "static tracepoint id {}, in a record after {}, is not one of these objects'",
                which.value, after));
        }
        const std::uint64_t address = which.value;
        const auto above = std::upper_bound(
            mappings.begin(), mappings.end(), address,
            [](std::uint64_t value, const trace::detail::mapping& m) { return value < m.table; });
        if (above == mappings.begin()) {
            throw std::runtime_error(
                std::format("tracepoint address {:#x} is below every object loaded after {}",
                            address, after));
        }
        const trace::detail::mapping& from = *(above - 1);
        if (from.object == nullptr) {
            throw std::runtime_error(std::format(
                "tracepoint address {:#x} belongs to object {}, whose tracepoint table this "
                "decoder has not got -- is it missing from the dso directory?",
                address, from.build_id));
        }
        throw std::runtime_error(std::format(
            "tracepoint address {:#x} is not an entry of object {}, which is what was at "
            "{:#x} after {}",
            address, from.build_id, from.table, after));
    };

    // The prologue: a clock sync, a count, and that many load events. Read by
    // that invariant rather than by their addresses, because until they have
    // been read there is no object for an address to be in. See "the metadata
    // stream" in tracer.h.
    //
    // How each of them is timed comes from the same invariant. A record's
    // timestamp is read by the code its id selects, and these have no id yet --
    // so the reader each one wants was chosen when this file was generated,
    // from the tracepoint the position is known to hold.
    {
        stream& meta = streams.front();
        trace::detail::read_record_id(meta.p, meta.end);  // not yet placeable
        meta.last_timestamp = trace::detail::@@PROLOGUE_SYNC@@(meta.p, meta.end, 0);
        meta.p = @@SKIP_CLOCK_SYNC@@(meta.p, meta.end);

        trace::detail::read_record_id(meta.p, meta.end);
        meta.last_timestamp =
            trace::detail::@@PROLOGUE_COUNT@@(meta.p, meta.end, meta.last_timestamp);
        const objects_loaded counted = @@READ_OBJECTS_LOADED@@(meta.p, meta.end);
        for (std::uint32_t i = 0; i < counted.count; i++) {
            trace::detail::read_record_id(meta.p, meta.end);
            meta.last_timestamp =
                trace::detail::@@PROLOGUE_LOADED@@(meta.p, meta.end, meta.last_timestamp);
            load(@@READ_OBJECT_LOADED@@(meta.p, meta.end));
        }
    }

    while (true) {
        // The earliest record still unread, over every stream. Reading its
        // timestamp means placing it first -- how the front of a body is timed
        // is a fact about the tracepoint -- and both are peeked without
        // advancing the stream; only the selected one is consumed below.
        stream* next = nullptr;
        std::uint64_t earliest = 0;
        for (stream& candidate : streams) {
            if (candidate.p == candidate.end) {
                continue;
            }
            const std::byte* peek = candidate.p;
            const trace::detail::record_id which =
                trace::detail::read_record_id(peek, candidate.end);
            const std::uint64_t at =
                read_timestamp(placed_id(which), peek, candidate.end, candidate.last_timestamp);
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
        const trace::detail::record_id which = trace::detail::read_record_id(q, q_end);

        // Placed before the timestamp is read rather than after, because how
        // many bytes of the body are the timestamp -- and what they mean, and
        // whether there are any -- is what the id says.
        const std::uint32_t id = placed_id(which);
        if (id == trace::no_decoder_id) {
            refuse(which, next->last_timestamp);
        }
        const std::uint64_t timestamp = read_timestamp(id, q, q_end, next->last_timestamp);
        // Left where it was by a record that carries no timestamp of its own,
        // which is what makes the next record in this buffer a delta from the
        // same place.
        next->last_timestamp = timestamp;

        switch (id) {
@@SWITCH@@            default:
                throw std::runtime_error(std::format("bad tracepoint id {}", id));
        }
    }
}

}  // namespace

extern "C" int trace_plugin_decode(const void* data, std::size_t size, void* sink,
                                   const char* dso_root, char* error, std::size_t error_size) {
    try {
        // One directory per root, kept between calls: a snapshot is a dozen
        // files decoded through this plugin, and each object behind it should be
        // opened and relocated once rather than a dozen times.
        static std::map<std::string, trace::dso_directory> directories;
        const std::string root = dso_root != nullptr ? dso_root : ".";
        const auto found = directories.try_emplace(root, root).first;

        decode(std::span<const std::byte>(static_cast<const std::byte*>(data), size), sink,
               found->second);
        return 0;
    } catch (const std::exception& e) {
        std::snprintf(error, error_size, "%s", e.what());
        return 1;
    } catch (...) {
        std::snprintf(error, error_size, "an exception that is not a std::exception");
        return 1;
    }
}

extern "C" void trace_plugin_notes(trace_plugin_note_fn emit, void* ctx) {
@@NOTES@@}
)cpp";

}  // namespace

namespace detail {

std::string generate(const plan& planned, std::string& error) {
    // The shapes of the tracer's own tracepoints, which the prologue is read
    // by. Looked up by name, and required to be one shape each: a snapshot
    // whose objects disagree about how the metadata stream is written is one
    // whose prologue cannot be read by position, because which position holds
    // what would depend on which object wrote it.
    const auto only_shape = [&](std::string_view name) -> const shape* {
        const shape* found = nullptr;
        for (const shape& s : planned.shapes) {
            if (s.name != name) {
                continue;
            }
            if (found != nullptr) {
                error = fmt::format(
                    "these objects have two different \"{}\" tracepoints (\"{}\" and \"{}\"), "
                    "and the metadata stream is read by position",
                    name, found->signature, s.signature);
                return nullptr;
            }
            found = &s;
        }
        if (found == nullptr) {
            error = fmt::format(
                "no object here has the tracer's own \"{}\" tracepoint, and every trace begins "
                "with one -- these tables are not the ones a trace was written from",
                name);
        }
        return found;
    };
    const shape* const sync = only_shape(clock_sync_name);
    const shape* const counted = only_shape(objects_loaded_name);
    const shape* const loaded = only_shape(object_loaded_name);
    const shape* const unloaded = only_shape(object_unloaded_name);
    if (sync == nullptr || counted == nullptr || loaded == nullptr || unloaded == nullptr) {
        return {};
    }
    const auto index_of = [&](const shape* s) {
        return static_cast<std::size_t>(s - planned.shapes.data());
    };

    std::string readers;
    std::string callbacks;
    std::string notes;
    // One declaration per event, however many builds have a tracepoint of that
    // name; one set of notes per *shape*, because two builds that spell one
    // tracepoint differently disagree with events.h differently.
    std::set<std::string, std::less<>> declared;
    for (std::size_t i = 0; i < planned.shapes.size(); ++i) {
        const shape& s = planned.shapes[i];
        readers += generate_advance(i, s);
        if (!s.bridged || unreadable_field(s) != nullptr) {
            continue;
        }
        if (declared.insert(s.name).second) {
            // So that a viewer that has stopped exporting one is a load-time
            // failure naming the symbol rather than a call into nothing.
            callbacks += fmt::format(
                "extern \"C\" void on_decode_{0}(\n"
                "    void* sink, const viewer::events::{0}& event,\n"
                "    const viewer::event_meta& meta);\n",
                s.name);
        }
        readers += generate_deliver(i, s);
        readers += generate_notes(i, s);
        notes += fmt::format("    notes_k{}<viewer::events::{}>(emit, ctx);\n", i, s.name);
    }
    if (notes.empty()) {
        notes = "    (void)emit;\n    (void)ctx;\n";
    }

    readers += generate_metadata_reader(*counted, index_of(counted), "objects_loaded",
                                        {{"count", "u32"}}, error);
    if (error.empty()) {
        readers += generate_metadata_reader(
            *loaded, index_of(loaded), "object_loaded",
            {{"build_id", "str"}, {"table_address", "u64"}, {"base_address", "u64"},
             {"mapping_size", "u64"}},
            error);
    }
    if (error.empty()) {
        readers += generate_metadata_reader(*unloaded, index_of(unloaded), "object_unloaded",
                                            {{"base_address", "u64"}}, error);
    }
    if (!error.empty()) {
        return {};
    }

    std::string object_table;
    for (const slice& s : planned.slices) {
        object_table += fmt::format("    {{\"{}\", {}, {}}},\n", escape(s.build_id), s.first_id,
                                    s.count);
    }

    std::string metadata_table;
    for (const tracepoints::entry* entry : planned.by_id) {
        metadata_table +=
            fmt::format("    {{\"{}\", \"{}\", {}, \"{}\", {}}},\n", escape(entry->name),
                        escape(entry->file), entry->line, escape(entry->function),
                        entry->timestamps == 2 ? "false" : "true");
    }

    std::string static_ids;
    for (const auto& [static_id, id] : planned.static_ids) {
        static_ids += fmt::format("        case {}: return {};\n", static_id, id);
    }

    std::string timestamps;
    std::string dispatch;
    for (std::size_t id = 0; id < planned.by_id.size(); ++id) {
        const std::size_t index = planned.shape_of_id[id];
        const shape& s = planned.shapes[index];
        if (s.timestamps != 0) {
            timestamps += fmt::format("        case {}: return trace::detail::{}(p, end, last);\n",
                                      id, timestamp_reader(s.timestamps));
        }
        // The tracer's own events are not the program's, so they are applied
        // rather than delivered -- and the count is only ever in the prologue,
        // where it has already been read.
        if (s.name == object_loaded_name) {
            dispatch += fmt::format(
                "            case {}: load(read_object_loaded_k{}(q, q_end)); break;\n", id,
                index);
        } else if (s.name == object_unloaded_name) {
            dispatch += fmt::format(
                "            case {}: unload(read_object_unloaded_k{}(q, q_end)); break;\n", id,
                index);
        } else if (s.name == objects_loaded_name) {
            dispatch += fmt::format(
                "            case {}:\n"
                "                throw std::runtime_error(\n"
                "                    \"a {} event past the start of the metadata stream\");\n",
                id, escape(s.name));
        } else if (s.bridged && unreadable_field(s) == nullptr) {
            dispatch += fmt::format(
                "            case {}:\n"
                "                q = deliver_k{}<viewer::events::{}>(\n"
                "                    sink, metadata[{}], timestamp, q, q_end, where);\n"
                "                break;\n",
                id, index, s.name, id);
        } else {
            dispatch += fmt::format("            case {}: q = advance_k{}(q, q_end); break;\n",
                                    id, index);
        }
    }

    std::string source(preamble);
    const auto fill = [&source](std::string_view marker, std::string_view text) {
        const std::size_t at = source.find(marker);
        source.replace(at, marker.size(), text);
    };
    fill("@@CALLBACKS@@", callbacks);
    fill("@@MAGIC@@", fmt::format("{:#x}", trace_magic));
    fill("@@METADATA_LEVEL@@", fmt::format("{}", metadata_level));
    fill("@@ENTRY_STRIDE@@", fmt::format("{}", entry_stride));
    fill("@@OBJECTS@@", object_table);
    fill("@@METADATA@@", metadata_table);
    fill("@@STATIC_IDS@@", static_ids);
    fill("@@TIMESTAMP_CASES@@", timestamps);
    fill("@@READERS@@", readers);
    fill("@@PROLOGUE_SYNC@@", timestamp_reader(sync->timestamps));
    fill("@@SKIP_CLOCK_SYNC@@", fmt::format("advance_k{}", index_of(sync)));
    fill("@@PROLOGUE_COUNT@@", timestamp_reader(counted->timestamps));
    fill("@@READ_OBJECTS_LOADED@@", fmt::format("read_objects_loaded_k{}", index_of(counted)));
    fill("@@PROLOGUE_LOADED@@", timestamp_reader(loaded->timestamps));
    fill("@@READ_OBJECT_LOADED@@", fmt::format("read_object_loaded_k{}", index_of(loaded)));
    fill("@@SWITCH@@", dispatch);
    fill("@@NOTES@@", notes);
    return source;
}

}  // namespace detail

namespace {

// ============================================================================
//  the cache, the compile, the dlopen
// ============================================================================

std::filesystem::path cache_root() {
    if (const char* const named = std::getenv("TRACE_PLUGIN_CACHE"); named != nullptr) {
        return named;
    }
    if (const char* const xdg = std::getenv("XDG_CACHE_HOME"); xdg != nullptr) {
        return std::filesystem::path(xdg) / "trace-viewer";
    }
    if (const char* const home = std::getenv("HOME"); home != nullptr) {
        return std::filesystem::path(home) / ".cache" / "trace-viewer";
    }
    return std::filesystem::temp_directory_path() / "trace-viewer";
}

// Load a built plugin and find the two symbols it is for.
//
// RTLD_NOW rather than RTLD_LAZY: what the plugin has that is not defined in it
// are the viewer's `on_decode_*`, and an events.h that has gained an event the
// viewer forgot to export should say so here, by name, rather than at the first
// record of that kind three seconds into a decode.
std::string load(decoder& into) {
    ::dlerror();
    void* const handle = ::dlopen(into.object.c_str(), RTLD_NOW | RTLD_LOCAL);
    if (handle == nullptr) {
        const char* const why = ::dlerror();
        return why != nullptr ? why : "dlopen failed";
    }
    void* const decode = ::dlsym(handle, trace_plugin_decode_symbol);
    void* const notes = ::dlsym(handle, trace_plugin_notes_symbol);
    if (decode == nullptr || notes == nullptr) {
        return fmt::format("no {} in the plugin",
                           decode == nullptr ? trace_plugin_decode_symbol
                                             : trace_plugin_notes_symbol);
    }
    into.decode = reinterpret_cast<trace_plugin_decode_fn>(decode);
    reinterpret_cast<trace_plugin_notes_fn>(notes)(
        [](void* ctx, const char* note) {
            static_cast<std::vector<std::string>*>(ctx)->emplace_back(note);
        },
        &into.notes);
    return {};
}

}  // namespace

decoder build(const std::string& dso_root) {
    decoder out;

    const std::vector<tracepoints::object> objects =
        tracepoints::read_tables(dso_root, out.notes);
    if (objects.empty()) {
        out.error = fmt::format(
            "no object under {}/.build-id has a tracepoint table this viewer can read. A trace "
            "means nothing without one -- see $TRACE_DSO_DIR and tools/gather-dsos",
            dso_root);
        return out;
    }
    out.objects = objects.size();

    const plan planned = detail::make_plan(objects);
    out.tracepoints = planned.by_id.size();
    for (std::size_t id = 0; id < planned.by_id.size(); ++id) {
        out.bridged += planned.shapes[planned.shape_of_id[id]].bridged ? 1 : 0;
    }

    std::string why;
    const std::string source = detail::generate(planned, why);
    if (!why.empty()) {
        out.error = why;
        return out;
    }
    // Appended after generate(), which has its own opinions about the tables.
    out.notes.insert(out.notes.end(), planned.notes.begin(), planned.notes.end());

    const toolchain& cxx = host_toolchain();
    if (!cxx.error.empty()) {
        out.error = cxx.error;
        return out;
    }

    // Everything that decides what the plugin is. The generated source stands
    // for the tables it came from, whole rather than by name, so a key that
    // misses means a rebuild and a key that hits means the same plugin.
    hasher key;
    key.put(std::to_string(generator_version));
    key.put(cxx.identity);
    key.put(source);
    key.put(embedded::events_h);
    key.put(embedded::plugin_abi_h);
    key.put(embedded::trace_wire_h);

    const std::filesystem::path directory = cache_root() / key.hex();
    out.object = directory / "plugin.so";
    out.source = directory / "plugin.cc";

    std::error_code ec;
    if (std::filesystem::exists(out.object)) {
        const std::size_t before = out.notes.size();
        out.from_cache = true;
        out.error = load(out);
        if (out.error.empty()) {
            return out;
        }
        // A plugin that will not load is one to build again -- the usual cause
        // is a half-written file from a run that was killed. Whatever it
        // managed to say is dropped; what the tables said is kept.
        out.from_cache = false;
        out.notes.resize(before);
        std::filesystem::remove(out.object, ec);
    }

    std::filesystem::create_directories(directory, ec);
    if (ec) {
        out.error = fmt::format("{}: {}", directory.string(), ec.message());
        return out;
    }

    // The plugin's directory is self-contained: the generated source, and the
    // three headers this viewer was built with that it is compiled against.
    // Everything the compile needs is in it, which is what makes a failed
    // compile something you can reproduce by hand.
    if (!write_file(out.source, source) ||
        !write_file(directory / "events.h", embedded::events_h) ||
        !write_file(directory / "plugin_abi.h", embedded::plugin_abi_h) ||
        !write_file(directory / "trace_wire.h", embedded::trace_wire_h)) {
        out.error = fmt::format("could not write into {}", directory.string());
        return out;
    }

    // Into a temporary and then renamed, so that a compile killed part way
    // through does not leave something the next run will try to dlopen.
    const std::filesystem::path partial = directory / "plugin.so.partial";
    //
    // `-Bsymbolic` is load-bearing, and its absence is silent. The viewer
    // includes trace_wire.h too -- to read the tables -- so its own copies of
    // those inline functions are in its .dynsym, put there by the -rdynamic
    // that the `on_decode_*` symbols need. Without -Bsymbolic the plugin's
    // calls to *its* copies bind to the viewer's instead, and every
    // `at_vaddr`, `read_str` and `locator::resolve` in the record loop becomes
    // a call through the PLT into another object. It costs nothing that shows
    // up as an error: pass_decode is 210 ms rather than 139.
    const command_result compiled = run_command(fmt::format(
        "{} -std=c++23 -O2 -fPIC -shared -Wl,-Bsymbolic -o {} {} 2>&1", shell_quote(cxx.cxx),
        shell_quote(partial.string()), shell_quote(out.source.string())));
    if (compiled.status != 0) {
        out.error =
            fmt::format("compiling {} failed:\n{}", out.source.string(), compiled.output);
        return out;
    }
    std::filesystem::rename(partial, out.object, ec);
    if (ec) {
        out.error = fmt::format("{}: {}", out.object.string(), ec.message());
        return out;
    }

    out.error = load(out);
    return out;
}

}  // namespace plugin
