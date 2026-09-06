// One compiled decoder, generated from the objects a snapshot came from.
//
// Nothing in a trace says what its records mean. The meaning is in the
// `tracepoints` section of the objects that wrote it -- names, parameter names,
// wire types -- and those objects are in the `dsos/` directory the viewer must
// have anyway to resolve a source location. So the decoder is built from there:
//
//   1. read every object's tracepoint table (tracepoint_table.h);
//   2. write a plugin source -- trace_wire.h, this viewer's events.h, and a
//      generated switch that reads each tracepoint's parameters straight into
//      the events.h struct of the same name -- and compile it to a `.so`;
//   3. dlopen it, and decode every file through its `trace_plugin_decode`.
//
// One plugin, not one per build. A build's tracepoints are a slice of the ids,
// keyed by the build ID a trace names its objects with, so a cluster part way
// through an upgrade is several slices in one switch -- and two builds that
// spell one tracepoint differently are two entries with two readers, which the
// old scheme could not have because the two would have wanted one struct name.
//
// Step 2 costs a second or two, so the `.so` is cached under
// `$TRACE_PLUGIN_CACHE` (or `~/.cache/trace-viewer`) against a key covering the
// generated source, the headers it is compiled against and the compiler's
// version: a second run over the same objects does step 1 and step 3 and
// nothing else.
//
// What the tables and events.h disagree about is a note, not a guess and not a
// crash: a tracepoint one side has not got, a field spelled differently, a field
// whose type will not convert without losing something. The field stays at its
// default and the note is printed, every run. The conversion is deliberately
// narrow -- the same type, or a wider integer of the same signedness -- because
// a task id quietly losing its top half would give an answer that looks like an
// answer.
//
// The one thing several builds at once may not disagree about is the *tracer's
// own* tracepoints. A trace's metadata stream opens with a clock sync, a count
// and that many load events, read by that position rather than by their ids --
// which is the only way round the circle, since until they are read no address
// means anything -- so which reader each position wants is decided when the
// plugin is generated. Two builds whose `trace_object_loaded` differs would want
// two answers, and there is nowhere to put the second: the decoder refuses, by
// name, rather than reading one build's prologue as the other's.
//
// Nothing here throws. A directory whose objects cannot be read, or a plugin
// that will not compile, comes back with a null `decode` and an `error` saying
// which, and the caller reports it and decodes nothing -- which is better than a
// viewer that dies on the way up.

#pragma once

#include <cstdint>
#include <filesystem>
#include <map>
#include <string>
#include <vector>

#include "plugin_abi.h"
#include "tracepoint_table.h"

namespace plugin {

// The two steps between a set of tables and a `.so`, as functions on plain
// values: the plan the tables fold into, and the C++ that plan is written as.
//
// They are here rather than hidden in the source because they are the part of
// this with tests. A table two builds disagree about, or one naming a wire type
// this viewer has no reader for, is a sentence to write down and an ELF object
// to produce -- see decoder_plugin_test.cc.
namespace detail {

// One distinct shape of tracepoint: a name, a parameter list, and how its
// records carry their time.
//
// Several entries share one. A tracepoint written in a header is compiled into
// every object that includes it, and a cluster mid-upgrade has the same
// tracepoint in two builds -- one reader each time would be one copy of the
// same code each time. What is *not* required is that two entries of one name
// agree: two builds that spell `run_task` differently are two shapes here, with
// a reader each, both delivering to the same events.h struct. That is the thing
// a generated header could not do, because the two would have wanted one struct
// name.
struct shape {
    std::string name;
    std::string signature;
    std::uint8_t timestamps = 0;
    std::vector<tracepoints::field> fields;
    bool bridged = false;  // events.h has a struct of this name
};

// Where a record's address comes from: one object's run of ids.
struct slice {
    std::string build_id;
    std::size_t first_id = 0;
    std::size_t count = 0;
};

struct plan {
    std::vector<shape> shapes;
    std::vector<std::size_t> shape_of_id;  // by decoder id, in the order the ids run
    std::vector<const tracepoints::entry*> by_id;
    std::vector<slice> slices;
    std::map<std::uint64_t, std::size_t> static_ids;  // static id -> decoder id
    std::vector<std::string> notes;
};

// Fold every object's table into that. Holds pointers into `objects`, which has
// to outlive the plan.
[[nodiscard]] plan make_plan(const std::vector<tracepoints::object>& objects);

// The plugin's whole source, or "" with `error` set for a plan nothing can be
// generated from.
[[nodiscard]] std::string generate(const plan& planned, std::string& error);

}  // namespace detail

// What became of the decoder.
struct decoder {
    std::filesystem::path source;  // the generated plugin source, kept to be read
    std::filesystem::path object;  // the compiled .so

    // Null if no decoder could be built; `error` then says why.
    trace_plugin_decode_fn decode = nullptr;
    std::string error;

    // What the tables and events.h disagreed about, and what could not be read
    // of the directory. Every one of them is a thing the viewer will not know
    // about these traces, so all of them are printed.
    std::vector<std::string> notes;

    std::size_t objects = 0;      // objects with a tracepoint table in them
    std::size_t tracepoints = 0;  // entries across all of them
    std::size_t bridged = 0;      // ... that events.h has a struct for
    bool from_cache = false;      // the .so was already built, and none of it was redone
};

// Build the decoder for the objects under `dso_root`, or find it in the cache.
//
// The handle is deliberately never closed: the process decodes through it until
// it exits, and unmapping code something may still hold a pointer into is a
// worse bug than a handle held to exit.
[[nodiscard]] decoder build(const std::string& dso_root);

}  // namespace plugin
