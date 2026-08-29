#pragma once

// The decoding half of the tracer.
//
// A trace holds tracepoint indices and packed argument bytes; nothing in it
// says what those bytes mean. The meaning lives in the producing binary's
// `tracepoints` section -- names, parameter names, wire types -- so the decoder
// has to be built against *that* binary's table.
//
// Rather than ship an interpreter that walks the table at runtime, this emits
// the C++ source of a decoder specialised to it: one struct per tracepoint,
// whose members are that tracepoint's parameters under their own names, plus a
// decode() that reads records into those structs and hands each to a callback.
// The generated source is then compiled as a normal build step, so the fields
// of a trace are real, typed members -- `event.conn`, not argument three -- and
// a consumer that misspells one is a compile error rather than a bad line of
// text.
//
// Nothing generated here prints on its own account. Each struct has a
// to_string() for when text is what is wanted, and that is the whole of the
// formatting.

#include <span>
#include <string>

namespace tracer {

struct tracepoint_entry;

// A complete translation unit -- a header -- decoding traces produced by the
// given tracepoint table, or by *this* binary if none is given.
//
// Throws std::runtime_error if the table cannot be turned into structs: a
// tracepoint or parameter name that is not an identifier, a name used twice, or
// a signature that does not parse. The alternative is a generated file that
// fails to compile, blaming a line nobody wrote.
[[nodiscard]] std::string generate_decoder_source(std::span<const tracepoint_entry> table);
[[nodiscard]] std::string generate_decoder_source();

}  // namespace tracer
