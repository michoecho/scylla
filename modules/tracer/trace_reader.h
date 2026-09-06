#pragma once

// Reading a trace back, for the tracer's own tests.
//
// The tracer writes records; nothing in this module reads them. What reads them
// is the viewer, in two halves that this puts back together: `trace_wire.h`,
// which knows how a record is laid out and nothing about what any particular one
// contains, and `tracepoint_table.h`, which reads an object's `tracepoints`
// section back out of the ELF file and so knows what each one contains. Given
// both, a record is a name, a moment and a list of typed fields -- which is what
// the tests here assert on.
//
// This is an *interpreter*: it walks the tables per record rather than being
// compiled against them. The viewer does not, and deliberately -- it generates
// a reader per tracepoint and compiles it at startup, because a decode there is
// tens of millions of records and this would be a switch per field. What the
// two share is the format itself, which is the point: the producer's half of it
// is in tracer.h, the reader's half is in trace_wire.h, and a test that writes
// with one and reads with the other is the only thing keeping them the same
// format. There used to be a generated decoder here saying it a third time.
//
// Not part of the tracer library: it lives in the module's test sources, and
// nothing outside them should grow a dependency on it.

#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "tracepoint_table.h"
#include "trace_wire.h"

namespace trace_test {

// One parameter of one record, read and rendered.
struct field {
    std::string name;
    std::string type;  // the wire token the signature gave: "u32", "str", ...
    std::string text;  // as the value prints -- see render() in the source

    // The value, for a test that wants to compare rather than to read. Only one
    // of them is filled in, and which is a fact about `type`.
    std::uint64_t number = 0;             // every integer width, bool and ptr
    std::int64_t signed_number = 0;       // the signed widths, sign extended
    std::string bytes;                    // "str" as text, "bytes" as its bytes
    trace::source_location location;      // "srcloc"
};

// One record.
//
// `has_timestamp` is false for a tracepoint declared with TRACEPOINT_UNTIMED():
// such a record carries no time of its own and is handed the moment of the
// record before it in its buffer. See timestamp_encoding in tracer.h.
struct event {
    std::string name;
    std::string file;
    std::string function;
    int line = 0;
    bool has_timestamp = true;
    std::uint64_t timestamp = 0;
    std::vector<field> fields;

    // The named parameter, or null. A test asking for one that is not there is
    // asking a question about the signature, and gets a null rather than a
    // default-constructed answer that looks like a value.
    [[nodiscard]] const field* find(std::string_view name) const;

    // `name{a=1, b=/index.html}`, which is how the decoded demo trace is
    // snapshotted.
    [[nodiscard]] std::string to_string() const;
};

// A decoder for the objects under `dso_root`: their tracepoint tables, in the
// order the ids run.
//
// Throws if the directory holds no readable table -- a decoder built from
// nothing would refuse every record of every trace, one at a time.
class trace_reader {
public:
    explicit trace_reader(const std::string& dso_root);

    // Every record of `trace`, in timestamp order, with source locations
    // resolved against `dsos`.
    //
    // Throws std::runtime_error on anything that cannot be read. A record is
    // not self-delimiting, so a stream cannot be resynchronised past a bad
    // byte: the first one ends the decode.
    [[nodiscard]] std::vector<event> decode(std::span<const std::byte> trace,
                                            trace::dso_directory& dsos) const;

    // The same, resolving locations against the objects the tables came from --
    // which is the usual case, since a location is an address in one of them.
    [[nodiscard]] std::vector<event> decode(std::span<const std::byte> trace) const;

    // What could not be read of the directory, from tracepoints::read_tables().
    [[nodiscard]] const std::vector<std::string>& notes() const { return notes_; }

private:
    std::string root_;
    std::vector<tracepoints::object> tables_;
    std::vector<std::string> notes_;
};

}  // namespace trace_test
