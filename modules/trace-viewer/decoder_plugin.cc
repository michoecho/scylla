// Building one decoder plugin per build of the traced program. See
// decoder_plugin.h for what this is for.
//
// The file is five things, in this order:
//
//   * running a command and reading a file, which is most of what the rest does;
//   * finding a C++ compiler and asking it where its headers are;
//   * reading a header with libclang, which yields structs and their fields and
//     nothing else -- this is the only place a C++ type is looked at;
//   * matching events.h against a decoder header and writing the bridge between
//     them, which is where a producer/consumer disagreement becomes a note
//     rather than a wrong number;
//   * the cache, the compile, and the dlopen.
//
// Nothing here throws out of `registry::for_build`. Everything that can fail
// ends up in `decoder::error`, because a viewer that refuses to start because
// one node of a cluster shipped a decoder it cannot parse is worse than one
// that reads the other two and says so.

#include "decoder_plugin.h"

#include <clang-c/Index.h>
#include <dlfcn.h>
#include <sys/wait.h>

#include <algorithm>
#include <array>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iterator>
#include <ranges>
#include <set>
#include <sstream>
#include <system_error>
#include <utility>

#include <fmt/format.h>
#include <fmt/ranges.h>

// events.h and plugin_abi.h, as text, compiled into this binary by the
// `:embedded_sources` genrule.
//
// Not a path to them, because a plugin is compiled at *runtime* and has to be
// compiled against the very events.h this viewer was built from -- a path would
// be a promise that the source tree beside the binary is still the one it came
// from, which is exactly the assumption the whole per-build decoder scheme
// exists to stop making. The registry writes them out into the plugin's cache
// directory, so the directory is a complete, rebuildable record of what the
// plugin was made of.
#include "embedded_sources.inc"

namespace plugin {
namespace {

// Bumped when anything about the generated source changes, so that a cache
// filled by an older viewer is not read by a newer one. Everything else in the
// key is an input file; this stands for the code below.
constexpr int generator_version = 3;

// ============================================================================
//  files and commands
// ============================================================================

std::string read_file(const std::filesystem::path& path, std::error_code& ec) {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        ec = std::make_error_code(std::errc::no_such_file_or_directory);
        return {};
    }
    ec.clear();
    return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

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
// plugin's source is. Sixty-four bits is far more than a directory of a handful
// of plugins needs, and a collision costs a wrong plugin rather than a wrong
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

// ============================================================================
//  the compiler
// ============================================================================

struct toolchain {
    std::string cxx;        // what to run
    std::string identity;   // its --version line, for the cache key
    std::string error;      // non-empty if there is no usable one
};

// Where the compiler looks for system headers, as it says itself.
//
// libclang does not run the compiler driver, so it does not get any of this:
// under Nix in particular the whole search path arrives through the driver's
// wrapper and $NIX_CFLAGS_COMPILE rather than being built into the library. So
// the driver is asked once, and its answer is handed to libclang as -isystem.
// The compile itself is done by the driver and needs none of it.
std::vector<std::string> system_includes(const std::string& cxx) {
    const command_result probe =
        run_command(shell_quote(cxx) + " -E -x c++ -std=c++23 -v /dev/null 2>&1");
    std::vector<std::string> out;
    std::istringstream lines(probe.output);
    std::string line;
    bool inside = false;
    while (std::getline(lines, line)) {
        if (line.starts_with("#include <...> search starts here")) {
            inside = true;
            continue;
        }
        if (line.starts_with("End of search list")) {
            break;
        }
        if (!inside) {
            continue;
        }
        const auto first = line.find_first_not_of(" \t");
        if (first == std::string::npos) {
            continue;
        }
        std::string dir = line.substr(first);
        // "(framework directory)" is a macOS thing and never a plain -isystem.
        if (dir.find("(framework directory)") != std::string::npos) {
            continue;
        }
        out.push_back(std::move(dir));
    }
    return out;
}

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

// Asked for separately, and only by a build whose plugin has to be generated:
// the probe is a whole run of the compiler driver, which is 35 ms of a startup
// that otherwise costs a dlopen, and a cached plugin needs none of it.
const std::vector<std::string>& toolchain_includes() {
    static const std::vector<std::string> found = system_includes(host_toolchain().cxx);
    return found;
}

// ============================================================================
//  reading a header
// ============================================================================
//
// What is wanted out of a header is small: the structs it defines, in order,
// with each field's name and its canonical type. Canonical, because the two
// headers are written by different people and one may say `std::uint64_t` where
// the other says `unsigned long`; those are the same type and the generated
// assignment does not care which spelling either used.

std::string to_string(CXString text) {
    const char* const c = clang_getCString(text);
    std::string out = c != nullptr ? c : "";
    clang_disposeString(text);
    return out;
}

struct field_info {
    std::string name;
    std::string canonical;  // the canonical type spelling
    CXTypeKind kind = CXType_Invalid;
    long long size = 0;
    std::string record;  // the unqualified name of the type, if it is a struct in this header
};

struct record_info {
    std::string name;       // unqualified: "run_task"
    std::string qualified;  // "viewer::events::run_task"
    std::string parent;     // the enclosing class or namespace: "events"
    std::vector<field_info> fields;
};

// By unqualified name. Both headers this reads define each struct once -- the
// decoder's are all directly in `trace`, the viewer's all in `viewer::events`
// or beside it -- so the short name is a key, and it is the name the two
// headers are matched on.
using record_map = std::map<std::string, record_info>;

struct header_contents {
    record_map records;
    std::vector<std::string> errors;
};

std::string qualified_name(CXCursor cursor) {
    std::vector<std::string> parts;
    CXCursor at = cursor;
    for (int depth = 0; depth < 16; ++depth) {
        const CXCursorKind kind = clang_getCursorKind(at);
        if (kind == CXCursor_TranslationUnit || clang_isInvalid(kind)) {
            break;
        }
        if (kind == CXCursor_Namespace || kind == CXCursor_StructDecl ||
            kind == CXCursor_ClassDecl) {
            parts.push_back(to_string(clang_getCursorSpelling(at)));
        }
        at = clang_getCursorSemanticParent(at);
    }
    std::ranges::reverse(parts);
    return fmt::format("{}", fmt::join(parts, "::"));
}

struct visit_state {
    std::string main_file;  // as it was handed to libclang, which is how it comes back
    record_map* records = nullptr;
};

// Every struct *this* header defines, and nothing from the headers it includes:
// `std::span` is a struct too, and recursing into the standard library is not
// what this is for. What decides is where the definition is, which is also what
// makes the field-type test below -- "is this type one of ours?" -- a lookup
// rather than a name convention.
CXChildVisitResult visit(CXCursor cursor, CXCursor /*parent*/, CXClientData data) {
    auto& state = *static_cast<visit_state*>(data);
    const CXCursorKind kind = clang_getCursorKind(cursor);
    if (kind != CXCursor_StructDecl && kind != CXCursor_ClassDecl) {
        return CXChildVisit_Recurse;
    }
    if (clang_isCursorDefinition(cursor) == 0) {
        return CXChildVisit_Recurse;
    }
    CXFile file{};
    unsigned line = 0;
    unsigned column = 0;
    unsigned offset = 0;
    clang_getFileLocation(clang_getCursorLocation(cursor), &file, &line, &column, &offset);
    if (to_string(clang_getFileName(file)) != state.main_file) {
        return CXChildVisit_Recurse;
    }

    record_info record;
    record.name = to_string(clang_getCursorSpelling(cursor));
    if (record.name.empty()) {
        return CXChildVisit_Recurse;  // an anonymous struct is not something to match on
    }
    record.qualified = qualified_name(cursor);
    record.parent = to_string(clang_getCursorSpelling(clang_getCursorSemanticParent(cursor)));

    clang_visitChildren(
        cursor,
        [](CXCursor child, CXCursor, CXClientData into) {
            if (clang_getCursorKind(child) != CXCursor_FieldDecl) {
                return CXChildVisit_Continue;
            }
            const CXType type = clang_getCanonicalType(clang_getCursorType(child));
            field_info f;
            f.name = to_string(clang_getCursorSpelling(child));
            f.canonical = to_string(clang_getTypeSpelling(type));
            f.kind = type.kind;
            f.size = clang_Type_getSizeOf(type);
            // The struct a field's type is, if it is one. Meaningless for
            // anything else, and only ever used as a key into the maps below --
            // which hold what *this* header defined and so hold no `span`.
            f.record = to_string(clang_getCursorSpelling(clang_getTypeDeclaration(type)));
            static_cast<std::vector<field_info>*>(into)->push_back(std::move(f));
            return CXChildVisit_Continue;
        },
        &record.fields);

    const std::string name = record.name;
    state.records->emplace(name, std::move(record));
    // Recurse anyway: `events` is itself a struct, and its members are what this
    // is after.
    return CXChildVisit_Recurse;
}

header_contents read_header(const std::filesystem::path& path,
                            const std::vector<std::string>& includes) {
    header_contents out;

    std::vector<std::string> args = {"-x", "c++", "-std=c++23", "-fsyntax-only"};
    for (const std::string& dir : includes) {
        args.emplace_back("-isystem");
        args.push_back(dir);
    }
    std::vector<const char*> argv;
    argv.reserve(args.size());
    for (const std::string& arg : args) {
        argv.push_back(arg.c_str());
    }

    CXIndex index = clang_createIndex(0, 0);
    CXTranslationUnit tu = nullptr;
    const CXErrorCode code = clang_parseTranslationUnit2(
        index, path.c_str(), argv.data(), int(argv.size()), nullptr, 0,
        CXTranslationUnit_SkipFunctionBodies, &tu);
    if (code != CXError_Success || tu == nullptr) {
        out.errors.push_back(fmt::format("libclang could not parse it (error {})", int(code)));
        clang_disposeIndex(index);
        return out;
    }

    // Only errors. A header read on its own warns about all sorts of things
    // that do not stop it describing its structs correctly.
    const unsigned diagnostics = clang_getNumDiagnostics(tu);
    for (unsigned i = 0; i < diagnostics && out.errors.size() < 8; ++i) {
        CXDiagnostic diagnostic = clang_getDiagnostic(tu, i);
        if (clang_getDiagnosticSeverity(diagnostic) >= CXDiagnostic_Error) {
            out.errors.push_back(to_string(clang_formatDiagnostic(
                diagnostic, clang_defaultDiagnosticDisplayOptions())));
        }
        clang_disposeDiagnostic(diagnostic);
    }

    visit_state state{path.string(), &out.records};
    clang_visitChildren(clang_getTranslationUnitCursor(tu), visit, &state);

    clang_disposeTranslationUnit(tu);
    clang_disposeIndex(index);
    return out;
}

// ============================================================================
//  matching the two, and writing the bridge
// ============================================================================

bool is_integer(CXTypeKind kind) {
    switch (kind) {
        case CXType_Bool:
        case CXType_Char_U:
        case CXType_UChar:
        case CXType_UShort:
        case CXType_UInt:
        case CXType_ULong:
        case CXType_ULongLong:
        case CXType_Char_S:
        case CXType_SChar:
        case CXType_Short:
        case CXType_Int:
        case CXType_Long:
        case CXType_LongLong:
            return true;
        default:
            return false;
    }
}

bool is_signed(CXTypeKind kind) {
    switch (kind) {
        case CXType_Char_S:
        case CXType_SChar:
        case CXType_Short:
        case CXType_Int:
        case CXType_Long:
        case CXType_LongLong:
            return true;
        default:
            return false;
    }
}

bool is_string_like(const field_info& f) {
    return f.canonical.starts_with("std::basic_string<char") ||
           f.canonical.starts_with("std::basic_string_view<char");
}

enum class conversion {
    none,       // these will not convert, and the field is left at its default
    assign,     // `to = from`, which covers same-type, widening and string_view-from-string
    recurse,    // both are structs of their own header: convert them field by field
};

// Whether a decoder's field can fill in a viewer's, and how.
//
// Deliberately narrow. A trace is read to answer questions about microseconds
// and task ids, and a conversion that silently loses the top half of one -- an
// `unsigned long` into a `uint32_t` -- gives an answer that looks like an answer
// and is not. So: the same type; a wider integer of the same signedness; a view
// over a string. Anything else is refused and reported, and the viewer carries
// on knowing it does not know.
conversion how_to_convert(const field_info& to, const field_info& from) {
    if (to.canonical == from.canonical) {
        return conversion::assign;
    }
    if (is_integer(to.kind) && is_integer(from.kind)) {
        const bool same_signedness = is_signed(to.kind) == is_signed(from.kind) &&
                                     (to.kind == CXType_Bool) == (from.kind == CXType_Bool);
        return same_signedness && to.size >= from.size ? conversion::assign : conversion::none;
    }
    if (to.canonical.starts_with("std::basic_string_view<char") && is_string_like(from)) {
        return conversion::assign;
    }
    if (!to.record.empty() && to.record == from.record) {
        return conversion::recurse;
    }
    return conversion::none;
}

// The decoder's name for the viewer's `event_meta`. The one pairing that is not
// by name: `tracepoint_metadata` is the frame every record arrives in rather
// than an event, so it is named for what the *producer* calls it, and the
// viewer is entitled to its own word for the thing.
constexpr char decoder_metadata[] = "tracepoint_metadata";
constexpr char viewer_metadata[] = "event_meta";

struct generated {
    std::string source;
    std::vector<std::string> notes;
    std::size_t events = 0;
};

// One `convert()` body: every field of `to` that `from` can fill in.
//
// Appends to `needed` any further pair of structs the fields turned out to
// need, so the caller can emit converters for those too.
std::string convert_body(const record_info& to, const record_info& from,
                         const record_map& viewer, const record_map& decoder,
                         std::set<std::pair<std::string, std::string>>& needed,
                         std::vector<std::string>& notes, std::string_view indent) {
    std::string body;
    for (const field_info& want : to.fields) {
        const auto have = std::ranges::find(from.fields, want.name, &field_info::name);
        if (have == from.fields.end()) {
            notes.push_back(fmt::format(
                "{}.{}: this decoder has no such field; left at its default", to.name,
                want.name));
            continue;
        }
        switch (how_to_convert(want, *have)) {
            case conversion::assign:
                body += fmt::format("{1}put(to.{0}, from.{0});\n", want.name, indent);
                break;
            case conversion::recurse: {
                const auto to_record = viewer.find(want.record);
                const auto from_record = decoder.find(have->record);
                if (to_record == viewer.end() || from_record == decoder.end()) {
                    notes.push_back(fmt::format(
                        "{}.{}: {} is not a struct both headers define; left at its "
                        "default",
                        to.name, want.name, want.record));
                    break;
                }
                needed.emplace(to_record->second.name, from_record->second.name);
                body += fmt::format("{1}convert(to.{0}, from.{0});\n", want.name, indent);
                break;
            }
            case conversion::none:
                notes.push_back(fmt::format(
                    "{}.{}: this decoder has it as `{}`, which will not convert to `{}`; "
                    "left at its default",
                    to.name, want.name, have->canonical, want.canonical));
                break;
        }
    }
    if (body.empty()) {
        body = fmt::format("{0}(void)to;\n{0}(void)from;\n", indent);
    }
    return body;
}

// The plugin's whole source: the two headers, a converter per pair of structs
// that has to be crossed, and a callback object with one `operator()` per event
// both headers agree on.
generated generate(const record_map& viewer, const record_map& decoder,
                   const std::filesystem::path& decoder_header,
                   const std::filesystem::path& events_header,
                   const std::filesystem::path& abi_header) {
    generated out;

    // Both headers' structs are matched by name; an event is a struct nested in
    // `events`, which is what that class is for.
    std::vector<const record_info*> want_events;
    for (const auto& [name, record] : viewer) {
        if (record.parent == "events") {
            want_events.push_back(&record);
        }
    }

    std::set<std::pair<std::string, std::string>> needed;
    std::string bridges;

    // The metadata pair first: every event carries one, so if this cannot be
    // crossed nothing else matters.
    const auto viewer_meta = viewer.find(viewer_metadata);
    const auto decoder_meta = decoder.find(decoder_metadata);
    if (viewer_meta == viewer.end()) {
        out.notes.emplace_back("events.h has no `event_meta`: nothing can be bridged");
        return out;
    }
    if (decoder_meta == decoder.end()) {
        out.notes.emplace_back(
            "this decoder has no `tracepoint_metadata`: nothing can be bridged");
        return out;
    }
    needed.emplace(viewer_meta->second.name, decoder_meta->second.name);

    for (const record_info* want : want_events) {
        const auto have = decoder.find(want->name);
        if (have == decoder.end()) {
            out.notes.push_back(
                fmt::format("{}: this build's decoder has no such tracepoint", want->name));
            continue;
        }
        const std::string body =
            convert_body(*want, have->second, viewer, decoder, needed, out.notes, "        ");
        bridges += fmt::format(
            "    void operator()(const trace::{0}& from,\n"
            "                    const trace::{1}& m) const {{\n"
            "        {2} to{{}};\n"
            "{3}"
            "        {4} meta{{}};\n"
            "        convert(meta, m);\n"
            "        on_decode_{0}(sink, to, meta);\n"
            "    }}\n\n",
            want->name, decoder_metadata, want->qualified, body,
            viewer_meta->second.qualified);
        ++out.events;
    }

    // The converters the bridges asked for, and the ones those asked for in
    // turn. Declared before any of them is defined, so that the order they came
    // out in does not matter.
    std::string declarations;
    std::string definitions;
    std::set<std::pair<std::string, std::string>> done;
    while (true) {
        std::set<std::pair<std::string, std::string>> next;
        std::ranges::set_difference(needed, done, std::inserter(next, next.end()));
        if (next.empty()) {
            break;
        }
        for (const auto& [to_name, from_name] : next) {
            done.emplace(to_name, from_name);
            const record_info& to = viewer.at(to_name);
            const record_info& from = decoder.at(from_name);
            declarations += fmt::format("void convert({}& to, const trace::{}& from);\n",
                                        to.qualified, from.name);
            const std::string body =
                convert_body(to, from, viewer, decoder, needed, out.notes, "    ");
            definitions += fmt::format("void convert({}& to, const trace::{}& from) {{\n{}}}\n\n",
                                       to.qualified, from.name, body);
        }
    }

    // The viewer's side of the boundary. One per event actually bridged, so a
    // viewer that has stopped exporting one is a load-time failure with the
    // symbol in the message rather than a call into nothing.
    std::string callbacks;
    for (const record_info* want : want_events) {
        if (decoder.contains(want->name)) {
            callbacks += fmt::format(
                "extern \"C\" void on_decode_{}(\n"
                "    void* sink, const {}& event, const {}& meta);\n",
                want->name, want->qualified, viewer_meta->second.qualified);
        }
    }

    out.source = fmt::format(
        R"(// Generated by the trace viewer at startup. Do not edit -- rebuild it by
// deleting this directory and running the viewer again.
//
// One build of the traced program, bridged to this viewer's events.h. The
// includes are the two halves of the contract: `{0}` was generated
// from the binary that wrote the traces, `events.h` is what this viewer wants,
// and everything between here and the bridge is the difference between them.

#include <cstddef>
#include <cstdio>
#include <exception>
#include <map>
#include <span>
#include <string>

#include "{0}"
#include "{1}"
#include "{2}"

{3}
namespace {{

// Assign one field. The generator has already decided, from the two headers'
// canonical types, that this conversion is one it is willing to make; the
// `requires` is the belt to that braces, so that a judgement it got wrong is a
// field left at its default rather than a plugin that will not compile.
template <class To, class From>
void put(To& to, const From& from) {{
    if constexpr (requires {{ to = from; }}) {{
        to = from;
    }}
}}

{4}
{5}// What trace::decode() calls. An event both headers know about has an
// `operator()` of its own; everything else -- a tracepoint this build has and
// this viewer does not -- lands on the template and is dropped.
struct bridge {{
    void* sink;

{6}    template <class Event>
    void operator()(const Event&, const trace::{7}&) const {{}}
}};

}}  // namespace

extern "C" int trace_plugin_decode(const void* data, std::size_t size, void* sink,
                                   const char* dso_root, char* error,
                                   std::size_t error_size) {{
    try {{
        // One directory per root, kept between calls: a snapshot is a dozen
        // files decoded through this plugin, and each object behind it should be
        // opened and relocated once rather than a dozen times.
        static std::map<std::string, trace::dso_directory> directories;
        const std::string root = dso_root != nullptr ? dso_root : ".";
        const auto found = directories.try_emplace(root, root).first;

        trace::decode(std::span<const std::byte>(static_cast<const std::byte*>(data), size),
                      bridge{{sink}}, found->second);
        return 0;
    }} catch (const std::exception& e) {{
        std::snprintf(error, error_size, "%s", e.what());
        return 1;
    }} catch (...) {{
        std::snprintf(error, error_size, "an exception that is not a std::exception");
        return 1;
    }}
}}
)",
        decoder_header.filename().string(), events_header.filename().string(),
        abi_header.filename().string(), callbacks, declarations, definitions, bridges,
        decoder_metadata);
    return out;
}

// ============================================================================
//  finding a header, building a plugin
// ============================================================================

// Where a build's decoder is looked for, in the order it is looked for.
//
// The build-id-named form is what a cluster of several versions needs: one
// directory holding `decoder_<build>.h` per build, pointed at by
// $TRACE_DECODER_DIR. The plain `decoder.h` beside the traces is what a
// snapshot has always written, and is still right when a snapshot directory
// holds one node's files -- which is every snapshot this viewer has ever been
// handed.
std::filesystem::path find_header(const std::string& build_id,
                                  const std::filesystem::path& beside) {
    std::vector<std::filesystem::path> roots;
    if (const char* const named = std::getenv("TRACE_DECODER_DIR"); named != nullptr) {
        roots.emplace_back(named);
    }
    roots.push_back(beside);

    if (!build_id.empty()) {
        for (const std::filesystem::path& root : roots) {
            const std::filesystem::path named = root / fmt::format("decoder_{}.h", build_id);
            if (std::filesystem::exists(named)) {
                return named;
            }
        }
    }
    for (const std::filesystem::path& root : roots) {
        const std::filesystem::path plain = root / "decoder.h";
        if (std::filesystem::exists(plain)) {
            return plain;
        }
    }
    return {};
}

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

// Load a built plugin, and find the one symbol it is for.
//
// RTLD_NOW rather than RTLD_LAZY: what the plugin has that is not defined in it
// are the viewer's `on_decode_*`, and an events.h that has gained an event the
// viewer forgot to export should say so here, by name, rather than at the first
// record of that kind three seconds into a decode.
std::string load(decoder& into, std::vector<void*>& handles) {
    ::dlerror();
    void* const handle = ::dlopen(into.object.c_str(), RTLD_NOW | RTLD_LOCAL);
    if (handle == nullptr) {
        const char* const why = ::dlerror();
        return why != nullptr ? why : "dlopen failed";
    }
    handles.push_back(handle);
    void* const symbol = ::dlsym(handle, trace_plugin_decode_symbol);
    if (symbol == nullptr) {
        return fmt::format("no {} in the plugin", trace_plugin_decode_symbol);
    }
    into.decode = reinterpret_cast<trace_plugin_decode_fn>(symbol);
    return {};
}

}  // namespace

registry::registry(std::string dso_root) : dso_root_(std::move(dso_root)) {}

registry::~registry() {
    // Deliberately not dlclose()d. A plugin's static dso_directory holds
    // mappings the tables' strings do not point into, but unmapping the code
    // while anything still holds a function pointer into it is a worse bug than
    // a handle held to exit -- and this object dies with the process.
    handles_.clear();
}

const decoder& registry::for_build(const std::string& build_id,
                                   const std::filesystem::path& beside) {
    const std::string key =
        build_id.empty() ? fmt::format("dir:{}", beside.string()) : build_id;
    if (const auto found = decoders_.find(key); found != decoders_.end()) {
        return found->second;
    }

    decoder& out = decoders_[key];
    out.build_id = build_id;

    out.header = find_header(build_id, beside);
    if (out.header.empty()) {
        out.error = fmt::format(
            "no decoder header for build {}: looked for decoder_{}.h and decoder.h in {}{}",
            build_id.empty() ? "(unnamed)" : build_id,
            build_id.empty() ? "<build>" : build_id, beside.string(),
            std::getenv("TRACE_DECODER_DIR") != nullptr ? " and $TRACE_DECODER_DIR" : "");
        return out;
    }

    std::error_code ec;
    const std::string decoder_text = read_file(out.header, ec);
    if (ec) {
        out.error = fmt::format("{}: {}", out.header.string(), ec.message());
        return out;
    }

    const toolchain& cxx = host_toolchain();
    if (!cxx.error.empty()) {
        out.error = cxx.error;
        return out;
    }

    // Everything that decides what the plugin is. A key that misses means a
    // rebuild; a key that collides would mean the wrong plugin, which is why the
    // decoder header goes in whole rather than by name.
    hasher key_of;
    key_of.put(std::to_string(generator_version));
    key_of.put(cxx.identity);
    key_of.put(embedded::events_h);
    key_of.put(embedded::plugin_abi_h);
    key_of.put(decoder_text);

    const std::filesystem::path directory = cache_root() / key_of.hex();
    out.object = directory / "plugin.so";
    out.source = directory / "plugin.cc";
    // What the generator found, kept beside the object so that a run which
    // reuses the object can still say what this build's decoder and events.h
    // disagree about. First line the count, the rest one note each.
    const std::filesystem::path notes_path = directory / "notes.txt";

    if (std::filesystem::exists(out.object)) {
        std::error_code note_ec;
        std::istringstream lines(read_file(notes_path, note_ec));
        std::string count;
        std::getline(lines, count);
        out.events_bridged = std::strtoul(count.c_str(), nullptr, 10);
        for (std::string line; std::getline(lines, line);) {
            if (!line.empty()) {
                out.notes.push_back(line);
            }
        }
        out.from_cache = true;
        out.error = load(out, handles_);
        if (out.error.empty()) {
            return out;
        }
        // A plugin that will not load is one to build again -- the usual cause
        // is a half-written file from a run that was killed.
        out.from_cache = false;
        out.notes.clear();
        out.events_bridged = 0;
        std::filesystem::remove(out.object, ec);
    }

    std::filesystem::create_directories(directory, ec);
    if (ec) {
        out.error = fmt::format("{}: {}", directory.string(), ec.message());
        return out;
    }

    // The plugin's directory is self-contained: the decoder header it was made
    // from, the events.h and the ABI header this viewer was built with, the
    // generated source, and the object. Everything the compile needs is in it,
    // which is what makes a failed compile something you can reproduce by hand.
    const std::filesystem::path decoder_copy = directory / out.header.filename();
    const std::filesystem::path events_copy = directory / "events.h";
    const std::filesystem::path abi_copy = directory / "plugin_abi.h";
    if (!write_file(decoder_copy, decoder_text) ||
        !write_file(events_copy, embedded::events_h) ||
        !write_file(abi_copy, embedded::plugin_abi_h)) {
        out.error = fmt::format("could not write into {}", directory.string());
        return out;
    }

    const header_contents viewer_side = read_header(events_copy, toolchain_includes());
    if (!viewer_side.errors.empty()) {
        out.error = fmt::format("reading events.h: {}", viewer_side.errors.front());
        return out;
    }
    const header_contents decoder_side = read_header(decoder_copy, toolchain_includes());
    if (!decoder_side.errors.empty()) {
        out.error =
            fmt::format("reading {}: {}", out.header.string(), decoder_side.errors.front());
        return out;
    }

    const generated plugin =
        generate(viewer_side.records, decoder_side.records, decoder_copy, events_copy, abi_copy);
    out.notes = plugin.notes;
    out.events_bridged = plugin.events;
    if (plugin.events == 0) {
        out.error = "no tracepoint in this decoder is one events.h knows about";
        return out;
    }
    if (!write_file(out.source, plugin.source)) {
        out.error = fmt::format("could not write {}", out.source.string());
        return out;
    }
    write_file(notes_path,
               fmt::format("{}\n{}\n", plugin.events, fmt::join(plugin.notes, "\n")));

    // Into a temporary and then renamed, so that a compile killed part way
    // through does not leave something the next run will try to dlopen.
    const std::filesystem::path partial = directory / "plugin.so.partial";
    const command_result compiled = run_command(fmt::format(
        "{} -std=c++23 -O2 -fPIC -shared -o {} {} 2>&1", shell_quote(cxx.cxx),
        shell_quote(partial.string()), shell_quote(out.source.string())));
    if (compiled.status != 0) {
        out.error = fmt::format("compiling {} failed:\n{}", out.source.string(),
                                compiled.output);
        return out;
    }
    std::filesystem::rename(partial, out.object, ec);
    if (ec) {
        out.error = fmt::format("{}: {}", out.object.string(), ec.message());
        return out;
    }

    out.error = load(out, handles_);
    return out;
}

}  // namespace plugin
