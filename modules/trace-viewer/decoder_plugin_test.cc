// The decoding half of the viewer, either side of the compiler.
//
// Two things are asserted here, and they meet in the middle. Reading a
// tracepoint table out of an ELF object is checked against real objects -- the
// demo's, from //modules/tracer:dso_dir, which are the only pair in the
// repository: a binary and a shared library, with a tracepoint written in a
// header compiled into both. Everything downstream of that is checked against
// tables written out by hand, because the interesting tables are the ones no
// build produces on purpose -- two builds disagreeing about one tracepoint, a
// static id claimed twice, a parameter of a type this viewer has no reader for.
//
// What is *not* here is the compile and the dlopen. Those want a C++ compiler
// at test time and two seconds per case; what they would catch beyond this is
// that the generated source compiles, and the cheapest honest version of that
// is running the viewer over a snapshot. See DECODING.md, "verifying a change".

#include <cstdlib>
#include <format>
#include <string>
#include <string_view>
#include <vector>

#include <doctest/doctest.h>

#include "decoder_plugin.h"
#include "tracepoint_table.h"

namespace {

using plugin::detail::make_plan;
using plugin::detail::plan;

// A tracepoint entry as a table would hold it, with its signature already
// parsed -- which is what read_tables() hands on, and the one thing a table
// written by hand has to remember to do for itself.
tracepoints::entry make_entry(std::string name, std::string signature, int line = 1,
                              std::uint64_t static_id = 0, std::uint8_t timestamps = 0) {
    tracepoints::entry out;
    out.name = std::move(name);
    out.file = "fake.cc";
    out.function = "void fake()";
    out.signature = std::move(signature);
    out.line = line;
    out.static_id = static_id;
    out.timestamps = timestamps;
    for (std::size_t at = 0; at < out.signature.size();) {
        const std::size_t comma = out.signature.find(',', at);
        const std::string_view one =
            std::string_view(out.signature).substr(at, comma == std::string::npos
                                                          ? std::string::npos
                                                          : comma - at);
        const std::size_t colon = one.find(':');
        REQUIRE(colon != std::string_view::npos);
        out.fields.push_back({std::string(one.substr(0, colon)), std::string(one.substr(colon + 1))});
        if (comma == std::string::npos) {
            break;
        }
        at = comma + 1;
    }
    return out;
}

// The tracer's own tracepoints, which every generated decoder needs whatever
// else is in the tables: the metadata stream is read by position, and these
// four are the positions. See "the metadata stream" in tracer.h.
std::vector<tracepoints::entry> tracer_entries() {
    std::vector<tracepoints::entry> out;
    out.push_back(make_entry("clock_sync", "tsc:u64,realtime_ns:u64,ticks_per_second:u64", 1, 0,
                             /*timestamps=*/1));
    out.push_back(make_entry("trace_objects_loaded", "count:u32", 2));
    out.push_back(make_entry(
        "trace_object_loaded",
        "build_id:str,table_address:u64,base_address:u64,mapping_size:u64", 3));
    out.push_back(make_entry("trace_object_unloaded", "base_address:u64", 4));
    return out;
}

// One object's table: the tracer's own tracepoints and whatever the case is
// about. The build ID is only the name the plan files a slice under; nothing
// here opens an object.
tracepoints::object make_object(std::string build_id,
                                std::vector<tracepoints::entry> entries) {
    tracepoints::object out;
    out.build_id = std::move(build_id);
    out.entries = tracer_entries();
    for (tracepoints::entry& entry : entries) {
        out.entries.push_back(std::move(entry));
    }
    return out;
}

// The source these tables generate, or a FAIL naming why they generate none.
std::string source_of(const std::vector<tracepoints::object>& objects) {
    const plan planned = make_plan(objects);
    std::string error;
    const std::string source = plugin::detail::generate(planned, error);
    REQUIRE_MESSAGE(error.empty(), error);
    return source;
}

// Why these tables generate nothing, or a FAIL if they generate something.
std::string refusal_of(const std::vector<tracepoints::object>& objects) {
    const plan planned = make_plan(objects);
    std::string error;
    const std::string source = plugin::detail::generate(planned, error);
    REQUIRE_MESSAGE(!error.empty(), "these tables were accepted");
    CHECK(source.empty());
    return error;
}

// Whether any note says this. The notes are prose and there are several; a case
// that cares about one of them says which by a phrase out of it.
bool noted(const plan& planned, std::string_view phrase) {
    for (const std::string& note : planned.notes) {
        if (note.find(phrase) != std::string::npos) {
            return true;
        }
    }
    return false;
}

// The demo's objects, from the build. Read once: it is an ELF walk apiece.
const std::vector<tracepoints::object>& demo_tables() {
    static const std::vector<tracepoints::object> tables = [] {
        const char* const root = std::getenv("TRACER_DSOS");
        REQUIRE_MESSAGE(root != nullptr, "TRACER_DSOS is not set");
        std::vector<std::string> notes;
        std::vector<tracepoints::object> read = tracepoints::read_tables(root, notes);
        for (const std::string& note : notes) {
            MESSAGE("read_tables: ", note);
        }
        CHECK_MESSAGE(notes.empty(), "an object under the demo's dsos could not be read");
        return read;
    }();
    return tables;
}

// Every entry of that directory, whichever object it is in.
std::vector<const tracepoints::entry*> demo_entries() {
    std::vector<const tracepoints::entry*> out;
    for (const tracepoints::object& object : demo_tables()) {
        for (const tracepoints::entry& entry : object.entries) {
            out.push_back(&entry);
        }
    }
    return out;
}

const tracepoints::entry* entry_named(std::string_view name) {
    for (const tracepoints::entry* entry : demo_entries()) {
        if (entry->name == name) {
            return entry;
        }
    }
    return nullptr;
}

}  // namespace

// --- reading a table out of an object -----------------------------------------

// The whole input to a decoder: what the linker collected in the `tracepoints`
// section of each object, read back out of the file.
TEST_CASE("a tracepoint table is read back out of the object that holds it") {
    const std::vector<tracepoints::object>& tables = demo_tables();

    // The producer and the library it is linked against, and nothing else: the
    // demo's dsos hold every object that was loaded, and most of what a program
    // links has no tracepoints section at all.
    REQUIRE(tables.size() == 2);
    CHECK(tables[0].build_id != tables[1].build_id);
    CHECK(tables[0].build_id < tables[1].build_id);  // sorted, so ids are stable
    for (const tracepoints::object& object : tables) {
        CHECK(!object.entries.empty());
        CHECK(!object.path.empty());
    }

    // Every entry came out whole. A wrong stride, or a relocation form this
    // reader does not know, shows up as an empty name within an entry or two --
    // which is what read_tables() refuses a table for, and is why the check is
    // worth making on entries it accepted.
    for (const tracepoints::entry* entry : demo_entries()) {
        CHECK(!entry->name.empty());
        CHECK(!entry->file.empty());
        CHECK(entry->line > 0);
    }
}

// What a table says about one tracepoint, against a call site that can be read
// in the source: see open_table() in modules/tracer/trace_producer.cc.
TEST_CASE("an entry says what its tracepoint's parameters are called and how wide they are") {
    const tracepoints::entry* const opened = entry_named("table_opened");
    REQUIRE(opened != nullptr);
    CHECK(opened->file == "modules/tracer/trace_producer.cc");
    CHECK(opened->signature == "name:str,opened_at:srcloc");
    REQUIRE(opened->fields.size() == 2);
    CHECK(opened->fields[0].name == "name");
    CHECK(opened->fields[0].type == "str");
    CHECK(opened->fields[1].name == "opened_at");
    CHECK(opened->fields[1].type == "srcloc");

    // The tracer's own, which the metadata stream is read by position against.
    const tracepoints::entry* const sync = entry_named("clock_sync");
    REQUIRE(sync != nullptr);
    CHECK(sync->timestamps == 1);  // a delta from its own first parameter

    // A tracepoint declared with TRACEPOINT_UNTIMED() says so here, which is
    // the only place a decoder can learn it: nothing in such a record
    // distinguishes the argument that follows the id from a timestamp.
    const tracepoints::entry* const row = entry_named("table_snapshot_row");
    REQUIRE(row != nullptr);
    CHECK(row->timestamps == 2);

    // And one declared with a static id carries it, so that a record naming it
    // in a byte can be placed without any object at all.
    const tracepoints::entry* const shared = entry_named("shared_event");
    REQUIRE(shared != nullptr);
    CHECK(shared->static_id != 0);
}

// The relocation case, which is the one that goes wrong quietly.
//
// `shared_event` is a TRACEPOINT() in a header compiled into both objects, so
// its entry's strings are in a comdat the linker folded -- reached through a
// symbol rather than through a relative relocation, which is the form a reader
// written for the executable's own entries would not have. Getting it wrong
// gives an entry whose name is empty, and an object refused for it.
TEST_CASE("a tracepoint written in a header is read out of both objects that have it") {
    std::vector<std::string> objects;
    for (const tracepoints::object& object : demo_tables()) {
        for (const tracepoints::entry& entry : object.entries) {
            if (entry.name == "shared_event") {
                CHECK(entry.file == "modules/tracer/plugin/common_tracepoints.h");
                CHECK(entry.signature == "sequence:u32");
                objects.push_back(object.build_id);
            }
        }
    }
    CHECK(objects.size() == 2);
}

// --- the plan -----------------------------------------------------------------

// One name is one struct in events.h, and several tracepoints may deliver into
// it: a tracepoint in a header is compiled into every object that includes it,
// and a cluster part way through an upgrade has one per build. Entries that
// agree are one reader; entries that do not are two, which is the thing the
// generated-header scheme could not do because both would have wanted one
// struct name.
TEST_CASE("entries that spell one tracepoint the same way share a reader") {
    const auto entries = [] {
        std::vector<tracepoints::entry> out;
        out.push_back(make_entry("run_task", "task:u32", 7));
        return out;
    };
    const std::vector<tracepoints::object> twice{make_object("00first0", entries()),
                                                 make_object("00second", entries())};
    const plan shared = make_plan(twice);

    // Two ids -- a record still says which copy fired -- and one shape.
    std::size_t run_task_ids = 0;
    for (const tracepoints::entry* entry : shared.by_id) {
        run_task_ids += static_cast<std::size_t>(entry->name == "run_task");
    }
    CHECK(run_task_ids == 2);
    std::size_t run_task_shapes = 0;
    for (const plugin::detail::shape& s : shared.shapes) {
        run_task_shapes += static_cast<std::size_t>(s.name == "run_task");
    }
    CHECK(run_task_shapes == 1);

    // And the ids run in slices, one per object, which is what an address is
    // turned into an id against.
    REQUIRE(shared.slices.size() == 2);
    CHECK(shared.slices[0].first_id == 0);
    CHECK(shared.slices[1].first_id == shared.slices[0].count);
    CHECK(shared.slices[0].count == shared.slices[1].count);

    // Two builds that spell it differently are two shapes and no complaint. The
    // reader each gets fills in whichever fields events.h has.
    std::vector<tracepoints::entry> moved;
    moved.push_back(make_entry("run_task", "task:u32,at:srcloc", 7));
    const std::vector<tracepoints::object> upgrading{make_object("00first0", entries()),
                                                     make_object("00second", std::move(moved))};
    const plan mixed = make_plan(upgrading);
    run_task_shapes = 0;
    for (const plugin::detail::shape& s : mixed.shapes) {
        run_task_shapes += static_cast<std::size_t>(s.name == "run_task");
    }
    CHECK(run_task_shapes == 2);
    CHECK(mixed.notes.size() == make_plan(twice).notes.size());
}

// The price of naming a tracepoint by hand. An address is unique because the
// linker made it so; an id is unique because whoever wrote it down checked, and
// this is what checks for them.
TEST_CASE("a static id names one tracepoint") {
    std::vector<tracepoints::entry> first;
    first.push_back(make_entry("run_task", "task:u32", 1, /*static_id=*/4));
    std::vector<tracepoints::entry> second;
    second.push_back(make_entry("cql_request", "task:u32", 2, /*static_id=*/4));

    const std::vector<tracepoints::object> clashing{
        make_object("00first0", std::move(first)),
        make_object("00second", std::move(second))};
    const plan planned = make_plan(clashing);
    CHECK(noted(planned, "static id 4 is \"run_task\" in one object and \"cql_request\""));

    // Two entries of *one* tracepoint are not a clash: one written in a header
    // is compiled into every object that includes it, and both copies carry the
    // id the header gave it.
    const auto shared_entry = [] {
        std::vector<tracepoints::entry> out;
        out.push_back(make_entry("run_task", "task:u32", 1, /*static_id=*/4));
        return out;
    };
    const std::vector<tracepoints::object> agreeing{make_object("00first0", shared_entry()),
                                                    make_object("00second", shared_entry())};
    const plan fine = make_plan(agreeing);
    CHECK_FALSE(noted(fine, "static id 4"));

    // And the id maps to that tracepoint without consulting any object, which
    // is the other half of what a static id is for. An id nothing was generated
    // from is refused rather than guessed at.
    const std::string source = source_of(agreeing);
    CHECK(source.find("case 4: return") != std::string::npos);
    CHECK(source.find("default: return trace::no_decoder_id;") != std::string::npos);
}

// What the tables and events.h disagree about is a note, not a crash: the
// viewer prints them at startup, and an empty column is more often one of these
// than a bug in a pass.
TEST_CASE("what the tables and events.h disagree about is said once, by name") {
    std::vector<tracepoints::entry> entries;
    entries.push_back(make_entry("run_task", "task:u32", 1));
    entries.push_back(make_entry("cache_hit", "key:str,age:u16", 2));
    const std::vector<tracepoints::object> objects{
        make_object("00fake00", std::move(entries))};
    const plan planned = make_plan(objects);

    // A tracepoint events.h has never heard of: its records are read past and
    // dropped, and that is worth a line.
    CHECK(noted(planned, "tracepoint \"cache_hit\" is in these objects and not in events.h"));

    // One it wants and no object has. Most of events.h is in this state here,
    // since these tables are two tracepoints.
    CHECK(noted(planned, "events.h wants \"cql_request\""));
    CHECK_FALSE(noted(planned, "events.h wants \"run_task\""));

    // The tracer's own are not reported as missing from events.h. They are how
    // a trace is read rather than events of the program's own.
    CHECK_FALSE(noted(planned, "\"trace_object_loaded\" is in these objects"));
}

// --- the generated source ------------------------------------------------------

// Which reader a record's body opens with is a fact about its tracepoint, so it
// is decided here, per id, rather than by the loop that walks the records.
TEST_CASE("the timestamp is read as the front of a body, by the id") {
    std::vector<tracepoints::entry> entries;
    entries.push_back(make_entry("run_task", "task:u32", 1));
    entries.push_back(
        make_entry("cql_request", "task:u32", 2, /*static_id=*/0, /*timestamps=*/2));
    const std::string source =
        source_of({make_object("00fake00", std::move(entries))});

    // The untimed one gets a case of its own; a record timed the usual way is
    // the default, which is also where an id nothing could place ends up.
    CHECK(source.find("read_timestamp_none(p, end, last)") != std::string::npos);
    CHECK(source.find("default: return trace::detail::read_timestamp_delta(p, end, last);") !=
          std::string::npos);

    // And a consumer is told which it was handed, because a record that carries
    // no time of its own is still given one.
    CHECK(source.find("\"cql_request\", \"fake.cc\", 2, \"void fake()\", false}") !=
          std::string::npos);
    CHECK(source.find("\"run_task\", \"fake.cc\", 1, \"void fake()\", true}") !=
          std::string::npos);
}

// A parameter of a type this viewer has no reader for is not a field that can
// be skipped: how long it is is part of what it is. So the record cannot be
// read past either, and the reader for that shape throws rather than the field
// being left out.
TEST_CASE("a parameter this viewer cannot read stops the records of its tracepoint") {
    std::vector<tracepoints::entry> entries;
    entries.push_back(make_entry("run_task", "task:u128", 1));
    const std::string source = source_of({make_object("00fake00", std::move(entries))});
    CHECK(source.find("has a parameter") != std::string::npos);
    CHECK(source.find("u128") != std::string::npos);
}

// The one thing several builds at once may not disagree about.
//
// A trace's metadata stream opens with a clock sync, a count and that many load
// events, read by *position* -- the only way round the circle, since until they
// have been read no address means anything. So which reader each position wants
// is decided when the plugin is generated, and two builds whose tracer
// tracepoints differ would want two answers with nowhere to put the second.
TEST_CASE("the tracer's own tracepoints have to agree across builds") {
    // A second object whose clock_sync is spelled differently. Every other
    // tracepoint may differ; this one may not.
    std::vector<tracepoints::entry> extra;
    tracepoints::object moved = make_object("00second", {});
    for (tracepoints::entry& entry : moved.entries) {
        if (entry.name == "clock_sync") {
            entry = make_entry("clock_sync", "tsc:u64,realtime_ns:u64", 1, 0, /*timestamps=*/1);
        }
    }
    const std::string why = refusal_of({make_object("00first0", {}), std::move(moved)});
    CHECK(why.find("two different \"clock_sync\" tracepoints") != std::string::npos);
    CHECK(why.find("read by position") != std::string::npos);
}

// And tables that are not a tracer's at all -- a directory of libraries, say --
// are refused by name rather than decoded into nonsense.
TEST_CASE("tables with no tracer tracepoints in them are refused") {
    tracepoints::object object;
    object.build_id = "00fake00";
    object.entries.push_back(make_entry("run_task", "task:u32", 1));
    // Named by the tracepoint it wanted: all four are missing here, and the
    // one the message names is whichever was looked for last.
    const std::string why = refusal_of({std::move(object)});
    CHECK(why.find("no object here has the tracer's own") != std::string::npos);
    CHECK(why.find("these tables are not the ones a trace was written from") !=
          std::string::npos);
}

// The tracer's own tracepoints are read into structs this viewer declares, so
// what it reads them as has to be what they were written as -- a parameter of
// the right name and the wrong width is a mapping read as nonsense.
TEST_CASE("a metadata parameter of the wrong shape is refused, by name") {
    tracepoints::object object = make_object("00fake00", {});
    for (tracepoints::entry& entry : object.entries) {
        if (entry.name == "trace_object_loaded") {
            entry = make_entry(
                "trace_object_loaded",
                "build_id:str,table_address:u32,base_address:u64,mapping_size:u64", 3);
        }
    }
    const std::string why = refusal_of({std::move(object)});
    CHECK(why.find("carries \"table_address\" as a u32") != std::string::npos);

    tracepoints::object short_one = make_object("00fake00", {});
    for (tracepoints::entry& entry : short_one.entries) {
        if (entry.name == "trace_object_loaded") {
            entry = make_entry("trace_object_loaded",
                               "build_id:str,table_address:u64,base_address:u64", 3);
        }
    }
    const std::string missing = refusal_of({std::move(short_one)});
    CHECK(missing.find("has no \"mapping_size\" parameter") != std::string::npos);
}

// The demo's tables, all the way through: what the viewer does at startup, less
// the compiler. It is the only case here whose input is a real linker's work.
TEST_CASE("the demo's objects generate a decoder") {
    const plan planned = make_plan(demo_tables());
    CHECK(planned.slices.size() == 2);
    CHECK(planned.by_id.size() > planned.shapes.size());  // shared_event is in both

    std::string error;
    const std::string source = plugin::detail::generate(planned, error);
    CHECK(error.empty());
    REQUIRE(!source.empty());

    // The object table a record's address is placed against: a row per object,
    // holding the build ID a trace names it by and the run of ids its entries
    // were given.
    for (const plugin::detail::slice& s : planned.slices) {
        CHECK(source.find(std::format("{{\"{}\", {}, {}}}", s.build_id, s.first_id, s.count)) !=
              std::string::npos);
    }
}
