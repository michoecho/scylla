#pragma once

// The decoding half of the tracer.
//
// A trace holds tracepoint addresses and packed argument bytes; nothing in it
// says what those bytes mean. The meaning lives in the `tracepoints` sections of
// the objects that produced it -- names, parameter names, wire types -- so the
// decoder has to be built against *those* objects' tables.
//
// Rather than ship an interpreter that walks the tables at runtime, this emits
// the C++ source of a decoder specialised to them: one struct per tracepoint,
// whose members are that tracepoint's parameters under their own names, plus a
// decode() that reads records into those structs and hands each to a callback.
// The generated source is then compiled as a normal build step, so the fields
// of a trace are real, typed members -- `event.conn`, not argument three -- and
// a consumer that misspells one is a compile error rather than a bad line of
// text.
//
// The generator is object-aware because a trace is. Each object is named by its
// build ID and its tracepoints by their offsets within its table, which is the
// pair a record's address decodes into; see "the wire format" in tracer.h. What
// the generated decoder carries is that mapping, so it can read a trace of a
// program whose libraries are mapped somewhere else entirely -- or not loaded
// at all.
//
// Nothing generated here prints on its own account. Each struct has a
// to_string() for when text is what is wanted, and that is the whole of the
// formatting.
//
// --- resolving a location -----------------------------------------------------
//
// One wire type is not decodable from the tables alone. A `srcloc::location`
// parameter -- see modules/source_location -- is the address of a constant the
// compiler laid down in whichever object captured it, so reading it back needs
// that object's *file*, not a description of it. The generated decoder therefore
// carries a small ELF reader and a `dso_directory`: the metadata stream says
// which object was mapped where, subtracting the base turns the address into a
// link-time virtual address, and the object is opened by build ID under
//
//     <root>/.build-id/<first two hex digits>/<the rest>.debug
//
// which is what tracer::write_dso_directory() produces. decode() takes the
// directory as an argument; a location it cannot place comes out unresolved,
// carrying the address it was recorded as, rather than stopping the decode. The
// generated header documents all of this at the point it is emitted.

#include <span>
#include <string>
#include <string_view>

#include "tracer/tracer.h"

namespace tracer {

// One object's tracepoints, as the generator needs them: the build ID a trace
// will name the object by, and the table a record's offset indexes into.
struct codegen_object {
    std::string_view build_id;
    std::span<const tracepoint_entry> table;
};

// A complete translation unit -- a header -- decoding traces produced by the
// given objects, or by the objects loaded into *this* process if none is given.
//
// Two tracepoints of the same name are the normal case once a tracepoint lives
// in a shared header: each object that includes it compiles its own, and both
// mean the same event. They are merged into one struct, and each keeps its own
// metadata. What is refused is two of one name whose parameter lists differ,
// which is one struct name for two shapes of event.
//
// Throws std::runtime_error if the tables cannot be turned into structs: a
// tracepoint or parameter name that is not an identifier, a name used for two
// different parameter lists, or a signature that does not parse. The
// alternative is a generated file that fails to compile, blaming a line nobody
// wrote.
[[nodiscard]] std::string generate_decoder_source(std::span<const codegen_object> objects);
[[nodiscard]] std::string generate_decoder_source();

}  // namespace tracer
