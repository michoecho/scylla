#pragma once

// The decoding half of the tracer.
//
// A trace holds tracepoint indices and packed argument bytes; nothing in it
// says what those bytes mean. The meaning lives in the producing binary's
// `tracepoints` section -- names, format strings, argument signatures -- so the
// decoder has to be built against *that* binary's table.
//
// Rather than ship an interpreter that walks the table at runtime, this emits
// the C++ source of a decoder specialised to it: one reader function per
// tracepoint, each a straight-line sequence of typed reads and a std::format
// call, plus a dispatch table and a main(). The generated program is then
// compiled as a normal build step. The payoff is that argument types become
// real types, so a format string that does not match its arguments is a
// compile error in the generated decoder rather than garbage in the output.

#include <string>

namespace tracer {

// A complete translation unit decoding traces produced by *this* binary.
[[nodiscard]] std::string generate_decoder_source();

}  // namespace tracer
