// The tracepoint tables of the objects a snapshot came from.
//
// A `TRACEPOINT()` lays a `tracer::tracepoint_entry` down in a section named
// `tracepoints`, and the linker collects every one of an object's into an array
// -- so the description of what a trace means is *in the object that wrote it*,
// and this reads it back out.
//
// That is the whole input to a decoder now. The viewer used to be handed a
// generated `decoder.h` per build and parse the C++; the tables are what that
// header was generated from, they are already in the objects the viewer must
// have anyway to resolve a source location, and reading them directly is one
// step instead of two. See decoder_plugin.h for what is done with them.
//
// The price is that the *layout of an entry* is now something the viewer knows
// rather than something it is told: the offsets below are `tracepoint_entry`'s,
// and a tracer that changes them makes every object built before the change
// unreadable. tracer.h static_asserts the layout so that the producer's side of
// that bargain fails loudly; on this side, an object whose table cannot be read
// as this layout is reported by name and skipped, rather than decoded into
// nonsense.

#pragma once

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

namespace tracepoints {

// One parameter, as the signature spells it: "conn:u32" is {"conn", "u32"}.
struct field {
    std::string name;
    std::string type;  // a wire type token -- "u64", "str", "bytes", "srcloc", ...
};

// One entry of one object's table: one `TRACEPOINT()` call site.
struct entry {
    std::string name;
    std::string file;
    std::string function;
    std::string signature;
    int line = 0;

    // The id this tracepoint's records carry instead of the address of this
    // entry, or 0 for the usual case of one identified by where it landed. See
    // "static ids" in tracer.h.
    std::uint64_t static_id = 0;

    // How its records carry the moment they were taken, as
    // tracer::timestamp_encoding: 0 a delta from the record before it, 1 a
    // delta from its own first parameter, 2 nothing at all.
    std::uint8_t timestamps = 0;

    std::vector<field> fields;  // `signature`, parsed
};

// One object's whole table, under the build ID a trace names it by.
struct object {
    std::string build_id;
    std::filesystem::path path;
    std::vector<entry> entries;
};

// Every object under `<root>/.build-id/` that has a readable tracepoint table,
// by build ID -- the directory tools/gather-dsos writes and $TRACE_DSO_DIR
// points at.
//
// Objects with no `tracepoints` section are not an error and not reported: most
// of what a program links has none. An object that has one this cannot read --
// a table that is not a whole number of entries, an entry whose name is not an
// identifier, a signature that does not parse -- appends a line to `notes`
// saying which object and why, and is left out. Both are the same thing to a
// decode: an address in that object cannot be placed, and the record loop
// refuses it by name.
//
// Sorted by build ID, so that the plugin generated from the result -- and
// therefore its cache key -- does not depend on the order a directory was read
// in.
[[nodiscard]] std::vector<object> read_tables(const std::filesystem::path& root,
                                              std::vector<std::string>& notes);

}  // namespace tracepoints
