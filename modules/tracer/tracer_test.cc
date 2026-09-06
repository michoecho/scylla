#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <format>
#include <fstream>
#include <iterator>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <dlfcn.h>

#include <doctest/doctest.h>

#include "snapshot/check.h"
#include "snapshot/regex_text.h"
#include "static_keys/static_keys.h"
#include "tracer/codegen.h"
#include "tracer/tracer.h"
#include "tracer_generated/decoder.h"

namespace {

using snapshot_testing::check_snapshot;
using snapshot_testing::RegexText;
using snapshot_testing::operator""_snap;

std::string read_file(const char* path) {
    std::ifstream in(path, std::ios::binary);
    REQUIRE_MESSAGE(in.good(), "cannot open ", path);
    return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}

std::string read_env_file(const char* variable) {
    const char* const path = std::getenv(variable);
    REQUIRE_MESSAGE(path != nullptr, variable, " is not set");
    return read_file(path);
}

// The displayed timestamp and source-location columns move as the producer and
// tracer change, but the events and values around them are part of the output
// contract. Keep the real output as the recorded sample while making those
// columns variable.
RegexText serialize_trace_columns(std::string_view text) {
    RegexText out;
    std::size_t from = 0;
    while (from < text.size()) {
        const std::size_t line_end = text.find('\n', from);
        const std::size_t end = line_end == std::string_view::npos ? text.size() : line_end;
        const std::size_t first_digit = text.find_first_not_of(" \t", from);
        const std::size_t after_digits =
            first_digit == std::string_view::npos
                ? std::string_view::npos
                : text.find_first_not_of("0123456789", first_digit);
        const std::size_t pipe = after_digits == std::string_view::npos
                                     ? std::string_view::npos
                                     : text.find('|', after_digits);
        const bool has_timestamp =
            first_digit < end && after_digits < end && pipe < end &&
            text.find_first_not_of(" \t", after_digits) == pipe &&
            text[first_digit] >= '0' && text[first_digit] <= '9';
        const std::size_t body = has_timestamp ? after_digits : from;
        if (has_timestamp) {
            out.variable(text.substr(from, after_digits - from), R"([ \t]*[0-9]+)");
        }

        const std::size_t path = text.find("modules/", body);
        const std::size_t colon = path == std::string_view::npos
                                      ? std::string_view::npos
                                      : text.find(':', path);
        const std::size_t whitespace = colon == std::string_view::npos
                                           ? std::string_view::npos
                                           : text.find_first_of(" \t", colon + 1);
        std::size_t field_end = whitespace;
        while (field_end < end && (text[field_end] == ' ' || text[field_end] == '\t')) {
            ++field_end;
        }
        if (path == std::string_view::npos || path >= end || colon >= end ||
            field_end == whitespace || field_end >= end) {
            out.literal(text.substr(body, end - body));
        } else {
            out.literal(text.substr(body, path - body));
            out.variable(text.substr(path, field_end - path), R"([^\n]*:\d+\s+)");
            out.literal(text.substr(field_end, end - field_end));
        }
        if (line_end == std::string_view::npos) break;
        out.literal("\n");
        from = line_end + 1;
    }
    return out;
}

// The signature TRACEPOINT() would build for this parameter list, without
// recording anything. A macro because the parameter *names* come from the
// argument list as written, which only the preprocessor can hand over.
#define SIGNATURE_OF(...)                                                        \
    std::string_view(::tracer::signature_builder<                                \
                     ::tracer::fixed_string{#__VA_ARGS__},                       \
                     decltype(::tracer::sig_probe(__VA_ARGS__))>::value.data())

// A tracepoint table assembled by hand, which is the only way to hand the code
// generator a table it should reject: a malformed one cannot be written as a
// TRACEPOINT(), and two tracepoints disagreeing under one name is a fact about
// two call sites at once.
//
// The key is left null. Generating a decoder reads names, types and locations
// and never touches a key.
tracer::tracepoint_entry fake(const char* name, const char* signature, int line = 1,
                              tracer::tracepoint_id static_id = tracer::tracepoint_id::none) {
    return {name, "fake.cc", line, "void fake()", signature, nullptr, static_id};
}

// That table as the one object of a program, under a made-up build ID. The
// generator never loads an object; a build ID is only the name it files one
// under.
std::string generate_from(std::span<const tracer::tracepoint_entry> table) {
    const tracer::codegen_object object{"00fake00", table};
    return tracer::generate_decoder_source(std::span{&object, 1});
}

// The same, for two objects -- which is how a tracepoint written in a shared
// header reaches the generator: once from each library that included it.
std::string generate_from(std::span<const tracer::tracepoint_entry> first,
                          std::span<const tracer::tracepoint_entry> second) {
    const std::array<tracer::codegen_object, 2> objects{
        tracer::codegen_object{"00first0", first}, tracer::codegen_object{"00second", second}};
    return tracer::generate_decoder_source(objects);
}

// A trace naming objects that are not this decoder's, plus whatever bytes the
// caller wants read as records -- for the cases where what is being tested is
// the refusal rather than the decode.
//
// The metadata records are written out by hand, which is the point: the entry
// addresses in them are zero, and a decoder reads the prologue anyway, because
// the first N+1 records of a metadata stream are read by the invariant rather
// than by their addresses.
std::vector<std::byte> fake_trace(
    std::span<const std::pair<std::string_view, std::uint64_t>> objects,
    std::span<const std::byte> records = {}) {
    std::vector<std::byte> metadata;
    const auto put = [&metadata](const auto& value) {
        const auto* bytes = reinterpret_cast<const std::byte*>(&value);
        metadata.insert(metadata.end(), bytes, bytes + sizeof(value));
    };
    const auto record_header = [&put] {
        put(std::uint64_t{0});  // the entry address, which the prologue does not need
        put(std::uint8_t{0});   // zero timestamp delta, which comes before everything
    };

    record_header();
    put(static_cast<std::uint32_t>(objects.size()));
    for (const auto& [build_id, address] : objects) {
        record_header();
        put(static_cast<std::uint16_t>(build_id.size()));
        const auto* bytes = reinterpret_cast<const std::byte*>(build_id.data());
        metadata.insert(metadata.end(), bytes, bytes + build_id.size());
        put(address);  // table_address
        put(address);  // base_address: nothing here records a location
        put(std::uint64_t{0});  // mapping_size, for the same reason
    }

    std::vector<std::byte> out;
    const auto* magic = reinterpret_cast<const std::byte*>(&tracer::trace_magic);
    out.insert(out.end(), magic, magic + sizeof(tracer::trace_magic));
    tracer::append_chunk(out, tracer::event_level::metadata, metadata);
    tracer::append_chunk(out, tracer::event_level::info, records);
    return out;
}

// Opens the demo's shared library, whose path the build passes in the
// environment. A guard object, so a failing CHECK cannot leak the handle into
// the next test case -- which would leave its tracepoint table registered and
// make the next count wrong.
class plugin_handle {
public:
    plugin_handle() {
        const char* const path = std::getenv("TRACER_PLUGIN");
        REQUIRE_MESSAGE(path != nullptr, "TRACER_PLUGIN is not set");
        handle_ = ::dlopen(path, RTLD_NOW | RTLD_LOCAL);
        REQUIRE_MESSAGE(handle_ != nullptr, ::dlerror());
    }
    ~plugin_handle() {
        if (handle_ != nullptr) {
            ::dlclose(handle_);
        }
    }
    plugin_handle(const plugin_handle&) = delete;
    plugin_handle& operator=(const plugin_handle&) = delete;

    void close() {
        REQUIRE(::dlclose(handle_) == 0);
        handle_ = nullptr;
    }

    template <typename Fn>
    Fn sym(const char* name) const {
        void* const found = ::dlsym(handle_, name);
        REQUIRE_MESSAGE(found != nullptr, "no symbol ", name);
        return reinterpret_cast<Fn>(found);
    }

private:
    void* handle_ = nullptr;
};

// The loaded objects that hold tracepoints, which is what most of the cases
// below are about. trace_objects() itself describes *every* loaded object --
// a source location may be in any of them, so a trace has to be able to name
// them all -- and that is a dozen and a half libraries nothing here has an
// opinion about.
std::vector<tracer::trace_object> tracing_objects() {
    std::vector<tracer::trace_object> objects = tracer::trace_objects();
    std::erase_if(objects, [](const tracer::trace_object& o) { return o.table.empty(); });
    return objects;
}

// The table address of the one loaded object that is not this test binary --
// which is to say the plugin's, whenever one is open. trace_objects() is
// ordered by build ID, so which end of it the plugin is at depends on a hash;
// asking for "the one that is not us" does not.
std::uintptr_t plugin_table_address(std::string_view self) {
    for (const tracer::trace_object& object : tracing_objects()) {
        if (object.build_id != self) {
            return object.table_address;
        }
    }
    FAIL("no plugin is loaded");
    return 0;
}

// The tracepoints of the process, by name.
std::size_t count_named(std::string_view name) {
    std::size_t found = 0;
    for (const tracer::tracepoint_entry* entry : tracer::tracepoints()) {
        found += static_cast<std::size_t>(entry->name == name);
    }
    return found;
}

// A callback assembled from lambdas, for a test that wants one tracepoint and
// does not care about the rest. decode() calls cb for every record, so the
// catch-all is not optional.
template <typename... Fs>
struct overloaded : Fs... {
    using Fs::operator()...;
};

// A trace consumer: one operator() per tracepoint it has something particular
// to say about, and a template one for the rest. The two named overloads reach
// into the fields by name; the fallback prints whole events through to_string().
//
// At namespace scope rather than inside the test case that uses it, because a
// local class may not have member templates.
struct collector {
    std::string text;

    void operator()(const trace::accepted_connection& event,
                    const trace::tracepoint_metadata& meta) {
        text += std::format("{}: connection {}, keepalive {}\n", meta.name, event.conn,
                            event.keepalive);
    }

    void operator()(const trace::request_header& event, const trace::tracepoint_metadata& meta) {
        text += std::format("{}: {} {}\n", meta.name, event.method, event.path);
    }

    template <typename Event>
    void operator()(const Event& event, const trace::tracepoint_metadata& meta) {
        text += std::format("{}:{} {}\n", meta.file, meta.line, event.to_string());
    }
};

}  // namespace

TEST_CASE("a signature names and types every parameter") {
    CHECK(SIGNATURE_OF() == "");
    CHECK(SIGNATURE_OF("port", std::uint32_t{1}) == "port:u32");
    CHECK(SIGNATURE_OF("skew", std::int64_t{1}, "up", true) == "skew:i64,up:bool");
    CHECK(SIGNATURE_OF("key", std::span<const std::byte>{}) == "key:bytes");

    // Strings are text on the wire, not addresses, whether they arrive as an
    // array, a pointer or a view.
    CHECK(SIGNATURE_OF("path", "/index.html") == "path:str");
    CHECK(SIGNATURE_OF("path", static_cast<const char*>("/")) == "path:str");
    CHECK(SIGNATURE_OF("path", std::string_view{}) == "path:str");

    // Types are selected by width, so the three distinct 64-bit integer types
    // all agree rather than each needing its own case.
    CHECK(SIGNATURE_OF("n", 1L) == "n:i64");
    CHECK(SIGNATURE_OF("n", 1LL) == "n:i64");
    CHECK(SIGNATURE_OF("n", std::uint8_t{1}, "at", static_cast<const void*>(nullptr)) ==
          "n:u8,at:ptr");

    // A parameter name is whatever the literal says, not the expression that
    // follows it: two call sites can name the same value differently.
    const std::uint32_t value = 3;
    CHECK(SIGNATURE_OF("retries", value) == "retries:u32");
    CHECK(SIGNATURE_OF("attempts", value + 1) == "attempts:u32");
}

// Clock sync records are ordinary records in every ring but the metadata one:
// a tracer writes one when it is built and one whenever a ring rotates. The
// cases that count a ring's bytes, or decode this binary's records with a
// decoder generated from another object's table, switch them off for their
// duration and say so by using this.
struct without_clock_sync {
    without_clock_sync() { tracer::set_clock_sync_enabled(false); }
    ~without_clock_sync() { tracer::set_clock_sync_enabled(true); }
};

// The one piece of the wire format that is not a plain field. Both halves are
// asserted here -- the bytes write_int() puts down, and that the generated
// decoder reads them back -- because a producer and a decoder that agree on a
// wrong encoding say nothing.
TEST_CASE("an integer carries its own length in the low bits of its first byte") {
    const auto encode = [](std::uint64_t value) {
        std::array<std::byte, 16> bytes{};
        std::byte* out = bytes.data();
        tracer::write_int(out, value);
        const auto size = static_cast<std::size_t>(out - bytes.data());
        CHECK(size == tracer::vint_size(value));

        const std::byte* p = bytes.data();
        CHECK(trace::detail::read_int(p, bytes.data() + bytes.size()) == value);
        CHECK(static_cast<std::size_t>(p - bytes.data()) == size);

        std::uint64_t word = 0;
        std::memcpy(&word, bytes.data(), sizeof(word));
        return std::pair{size, word};
    };

    // Seven bits: one byte, one tag bit, and that bit is zero.
    const auto [small_size, small] = encode(0x42);
    CHECK(small_size == 1);
    CHECK((small & 0xff) == 0x42 << 1);

    // Fourteen: two bytes, and a tag of one bit set below the value.
    const auto [medium_size, medium] = encode(0x1234);
    CHECK(medium_size == 2);
    CHECK((medium & 0xffff) == ((0x1234 << 2) | 0b01));

    // The largest value that still fits above its tag, and the first one that
    // does not: 56 bits, then 57.
    CHECK(encode((std::uint64_t{1} << 56) - 1).first == 8);
    CHECK(encode(std::uint64_t{1} << 56).first == 9);
    CHECK(encode(UINT64_MAX).first == 9);
    CHECK(encode(0).first == 1);
}

TEST_CASE("tracer records land in the buffer with their header") {
    const without_clock_sync quiet;
    tracer::trace_buffers buffers(4096, 4096, 4096, 512);
    tracer::local_tracer = &buffers;

    REQUIRE(tracer::set_tracepoint_enabled("value_seen", true) == 1);
    TRACEPOINT(tracer::event_level::info, "value_seen", "value", std::uint32_t{7});
    REQUIRE(tracer::set_tracepoint_enabled("value_seen", false) == 1);

    tracer::local_tracer = nullptr;
    const std::vector<std::byte> bytes = buffers.group(tracer::event_level::info).collect();

    // entry address + one-byte timestamp vint + one u32, and nothing else: the parameter's
    // name is not on the wire, and collect() must not return the unwritten tail
    // of the live buffer.
    CHECK(bytes.size() >= tracer::record_header_size + sizeof(std::uint32_t));
}

// What a static id buys, on the wire: a record that names its tracepoint in one
// byte rather than eight. See "static ids" in tracer.h.
TEST_CASE("a static id costs a byte where an entry address costs eight") {
    // The two lengths are constants at the call site, which is what lets the
    // write path store the id and bump its cursor by an immediate.
    static_assert(tracer::static_id_size(tracer::tracepoint_id{1}) == 1);
    static_assert(tracer::static_id_size(tracer::tracepoint_id{63}) == 1);
    static_assert(tracer::static_id_size(tracer::tracepoint_id{64}) == 2);

    const without_clock_sync quiet;
    tracer::trace_buffers buffers(4096, 4096, 4096, 512);
    tracer::local_tracer = &buffers;
    REQUIRE(tracer::set_tracepoint_enabled("cheap_event", true) == 1);
    TRACEPOINT_STATIC_ID(tracer::tracepoint_id{7}, tracer::event_level::info, "cheap_event",
                         "value", std::uint32_t{9});
    REQUIRE(tracer::set_tracepoint_enabled("cheap_event", false) == 1);
    tracer::local_tracer = nullptr;

    const std::vector<std::byte> bytes = buffers.group(tracer::event_level::info).collect();
    REQUIRE(!bytes.empty());
    // Three zero bits at the bottom would have made it an entry address; the
    // doubling the writer does is what keeps them from all being zero.
    CHECK((std::to_integer<unsigned>(bytes[0]) & 0b111) != 0);

    const std::byte* p = bytes.data();
    const std::byte* const end = p + bytes.size();
    const trace::detail::record_id which = trace::detail::read_record_id(p, end);
    CHECK(which.is_static);
    CHECK(which.value == 7);
    CHECK(p - bytes.data() == 1);

    // The rest of the record is the timestamp and the one parameter, and
    // nothing else: a short id shortens the record rather than padding it.
    trace::detail::read_int(p, end);
    CHECK(static_cast<std::size_t>(end - p) == sizeof(std::uint32_t));
}

TEST_CASE("a string parameter is recorded as its bytes, however it arrives") {
    const without_clock_sync quiet;
    const auto recorded_size = [](auto&& record) {
        tracer::trace_buffers buffers(4096, 4096, 4096, 512);
        tracer::local_tracer = &buffers;
        record();
        tracer::local_tracer = nullptr;
        return buffers.group(tracer::event_level::info).collect().size();
    };

    // Six characters and a uint16_t length, and no terminator -- from a
    // literal, from a pointer and from a view alike.
    const std::string owned = "GET / ";
    const std::string_view view = owned;
    const char* const pointer = owned.c_str();

    REQUIRE(tracer::set_tracepoint_enabled("string_seen", true) == 3);
    const std::size_t expected =
        tracer::record_header_size + sizeof(std::uint16_t) + owned.size();

    CHECK(recorded_size([] {
              TRACEPOINT(tracer::event_level::info, "string_seen", "text", "GET / ");
          }) >= expected);
    CHECK(recorded_size([&] {
              TRACEPOINT(tracer::event_level::info, "string_seen", "text", pointer);
          }) >= expected);
    CHECK(recorded_size([&] {
              TRACEPOINT(tracer::event_level::info, "string_seen", "text", view);
          }) >= expected);
    REQUIRE(tracer::set_tracepoint_enabled("string_seen", false) == 3);
}

// The point of the key: a tracepoint switched off costs a nop and writes
// nothing.
//
// local_tracer is deliberately left null. A disabled tracepoint must not reach
// the recording code at all, and there is no null check on that path -- so if
// the branch were taken this would not merely record, it would crash, which is
// a sharper assertion than an empty buffer.
//
// The disable is redundant -- a tracepoint starts disabled, see TRACEPOINT()
// in tracer.h -- and is here to say by name which tracepoint is meant.
TEST_CASE("a tracepoint switched off by name writes nothing") {
    REQUIRE(tracer::set_tracepoint_enabled("never_recorded", false) == 1);
    tracer::local_tracer = nullptr;

    TRACEPOINT(tracer::event_level::info, "never_recorded", "n", std::uint32_t{1});

    // The tracepoint above exists in the table even though it never fired: the
    // entry is emitted by the linker, not by the call.
    std::size_t found = 0;
    for (const tracer::tracepoint_entry* entry : tracer::tracepoints()) {
        if (entry->name == std::string_view("never_recorded")) {
            CHECK_FALSE(tracer::is_enabled(*entry));
            CHECK(entry->signature == std::string_view("n:u32"));
            ++found;
        }
    }
    CHECK(found == 1);
}

TEST_CASE("a tracepoint's key is named after the tracepoint") {
    tracer::trace_buffers buffers(4096, 4096, 4096, 512);
    tracer::local_tracer = &buffers;

    TRACEPOINT(tracer::event_level::info, "keyed_tracepoint");

    tracer::local_tracer = nullptr;

    // The key that static_keys reports under this name and the key the
    // tracepoint table points at are the same object, which is what makes the
    // name a usable handle on the tracepoint from outside the binary.
    const std::vector<const tracer::tracepoint_entry*> table = tracer::tracepoints();
    const auto found = std::find_if(table.begin(), table.end(),
                                    [](const tracer::tracepoint_entry* e) {
                                        return e->name == std::string_view("keyed_tracepoint");
                                    });
    REQUIRE(found != table.end());
    const tracer::tracepoint_entry* const entry = *found;

    const std::vector<static_keys::static_key_info> keys = static_keys::list_static_keys();
    const auto key = std::find_if(keys.begin(), keys.end(),
                                  [](const static_keys::static_key_info& k) {
                                      return k.name == "keyed_tracepoint";
                                  });
    REQUIRE(key != keys.end());
    CHECK(key->file == std::string_view(entry->file));

    // Disabled to begin with, and the name is the handle that turns it on and
    // off again.
    CHECK_FALSE(tracer::is_enabled(*entry));
    REQUIRE(tracer::set_tracepoint_enabled("keyed_tracepoint", true) == 1);
    CHECK(tracer::is_enabled(*entry));
    REQUIRE(tracer::set_tracepoint_enabled("keyed_tracepoint", false) == 1);
    CHECK_FALSE(tracer::is_enabled(*entry));

    // A name nothing was compiled under matches nothing, rather than silently
    // succeeding.
    CHECK(tracer::set_tracepoint_enabled("no_such_tracepoint", true) == 0);
}

TEST_CASE("buffer_group rotates and evicts the oldest records") {
    // Four buffers of 64 bytes: eight records of 32 fill it exactly, and the
    // next eight must displace them.
    tracer::buffer_group group(256, 64);

    auto write_marker = [&](std::byte marker) {
        std::byte* out = group.write(32);
        for (std::size_t i = 0; i < 32; ++i) {
            out[i] = marker;
        }
    };

    for (int i = 0; i < 16; ++i) {
        write_marker(std::byte{0xAA});
    }
    const std::size_t steady = group.collect().size();

    for (int i = 0; i < 16; ++i) {
        write_marker(std::byte{0xBB});
    }
    const std::vector<std::byte> bytes = group.collect();

    // The ring is bounded, and what survives is the recent half.
    CHECK(bytes.size() == steady);
    CHECK(bytes.size() <= 256 + 64);
    CHECK(bytes.back() == std::byte{0xBB});
    CHECK(bytes.front() == std::byte{0xBB});
}

TEST_CASE("buffer_group keeps whole records") {
    tracer::buffer_group group(4096, 64);

    // 40 bytes does not fit twice in a 64-byte buffer, so the second write
    // rotates rather than straddling the boundary.
    std::byte* first = group.write(40);
    std::byte* second = group.write(40);
    CHECK(second != first + 40);
    CHECK(group.collect().size() == 80);
}

// A snapshot has to say when it is from, and the records cannot say it: their
// timestamps are rdtsc ticks. So each buffer notes the wall clock as it goes
// live and as it is retired, and the group reports the span the buffers it still
// holds cover -- which shrinks as the ring evicts, and that is the point.
TEST_CASE("a buffer group says when the records it still holds were written") {
    const std::uint64_t before = tracer::realtime_nanoseconds();
    tracer::buffer_group group(256, 64);

    // Nothing written: the range starts when the group was built and ends now.
    {
        const auto [first, last] = group.time_range();
        CHECK(first >= before);
        CHECK(last >= first);
    }

    const auto write_marker = [&group] {
        std::byte* out = group.write(32);
        for (std::size_t i = 0; i < 32; ++i) {
            out[i] = std::byte{0xAA};
        }
    };

    for (int i = 0; i < 4; ++i) {
        write_marker();
    }
    const auto [early_first, early_last] = group.time_range();
    CHECK(early_first >= before);

    // Fill it several times over. The oldest buffer that survives is now one
    // that went live after the group was built, so the range begins later than
    // it did -- the ring has forgotten the beginning.
    for (int i = 0; i < 64; ++i) {
        write_marker();
    }
    const auto [late_first, late_last] = group.time_range();
    CHECK(late_first > early_first);
    CHECK(late_last >= early_last);
    CHECK(late_last >= late_first);
}

// A snapshot writes one file per record level, so a file has to be readable on
// its own -- which means it carries the metadata chunk whatever level it holds.
// Without it the records inside are addresses against no object.
TEST_CASE("one level collected on its own still carries the metadata chunk") {
    const without_clock_sync quiet;
    tracer::trace_buffers buffers(4096, 4096, 4096, 512);
    tracer::local_tracer = &buffers;

    REQUIRE(tracer::set_tracepoint_enabled("level_split_seen", true) == 1);
    TRACEPOINT(tracer::event_level::info, "level_split_seen", "value", std::uint32_t{7});
    REQUIRE(tracer::set_tracepoint_enabled("level_split_seen", false) == 1);
    tracer::local_tracer = nullptr;

    // The chunk headers of a collected trace: the magic, then a level and a
    // length per chunk.
    const auto chunks = [](const std::vector<std::byte>& trace) {
        std::vector<std::pair<std::uint8_t, std::uint64_t>> out;
        std::uint32_t magic = 0;
        REQUIRE(trace.size() >= sizeof(magic));
        std::memcpy(&magic, trace.data(), sizeof(magic));
        CHECK(magic == tracer::trace_magic);
        std::size_t at = sizeof(magic);
        while (at + 1 + sizeof(std::uint64_t) <= trace.size()) {
            const auto level = std::to_integer<std::uint8_t>(trace[at]);
            std::uint64_t length = 0;
            std::memcpy(&length, trace.data() + at + 1, sizeof(length));
            out.emplace_back(level, length);
            at += 1 + sizeof(length) + length;
        }
        CHECK(at == trace.size());
        return out;
    };

    const auto info = chunks(tracer::collect_trace_level(buffers, tracer::event_level::info));
    REQUIRE(info.size() == 2);
    CHECK(info[0].first == std::uint8_t(tracer::event_level::metadata));
    CHECK(info[0].second > 0);  // the load events the constructor wrote
    CHECK(info[1].first == std::uint8_t(tracer::event_level::info));
    CHECK(info[1].second >= tracer::record_header_size + sizeof(std::uint32_t));

    // The debug part of the same snapshot: the same metadata, and none of the
    // info ring's records. Splitting by level must not duplicate a record.
    const auto debug = chunks(tracer::collect_trace_level(buffers, tracer::event_level::debug));
    REQUIRE(debug.size() == 2);
    CHECK(debug[0] == info[0]);
    CHECK(debug[1].first == std::uint8_t(tracer::event_level::debug));
    CHECK(debug[1].second == 0);
}

// --- clock sync ---------------------------------------------------------------

// The rate is an estimate that nothing is obliged to improve on: a program that
// never calibrates traces with the default, which is the machine this was
// written on and so the right order of magnitude anywhere. Both it and a fresh
// measurement have to land in the range a CPU's tick rate can be -- 10 MHz to
// 10 GHz -- because a rate outside that would turn every converted timestamp
// into nonsense without anything else noticing.
TEST_CASE("the tick rate is a plausible one, measured or not") {
    constexpr std::uint64_t slowest = 10'000'000;
    constexpr std::uint64_t fastest = 10'000'000'000;

    CHECK(tracer::tsc_ticks_per_second() >= slowest);
    CHECK(tracer::tsc_ticks_per_second() <= fastest);

    // The one thing here that sleeps, which is why calibration is optional.
    const std::uint64_t measured = tracer::calibrate_tsc();
    CHECK(measured >= slowest);
    CHECK(measured <= fastest);
    MESSAGE("measured ", measured, " ticks per second");

    // And it is installed, so the sync records written after it carry it.
    CHECK(tracer::tsc_ticks_per_second() == measured);

    // A second measurement of the same clock agrees with the first to within a
    // percent: what would not is a calibration reading two unrelated clocks.
    const std::uint64_t again = tracer::calibrate_tsc();
    CHECK(std::max(measured, again) - std::min(measured, again) < measured / 100);

    tracer::set_tsc_ticks_per_second(tracer::default_tsc_ticks_per_second);
}

// What a sync record is for: a trace is a stream of tick counts, and a tick
// count is only a time beside a wall clock reading taken at the same moment.
//
// One at the head of every ring is not enough on its own, because a ring is
// bounded: the buffer holding the first sync is eventually retired and dropped,
// and a trace collected after that would have none at all. So a rotation writes
// another, into the fresh buffer and ahead of the record that forced it.
//
// Asserted on the demo trace, read back with the generated decoder -- the same
// pair of build steps the snapshot at the bottom of this file covers. It has to
// be that trace and not one taken here: a sync record is written by code
// inlined from tracer.h into whichever object built the tracer, this binary is
// that object for a tracer built here, and a record of this binary's is one the
// generated decoder refuses. The producer sizes its buffers below the size of
// its workload, so a ring rotates part way through it; see emit_trace().
TEST_CASE("every ring opens with a clock sync, and gets another one on rotation") {
    const std::string raw = read_env_file("TRACER_TRACE");
    const std::span<const std::byte> bytes{reinterpret_cast<const std::byte*>(raw.data()),
                                           raw.size()};

    std::vector<trace::clock_sync> syncs;
    std::size_t events = 0;  // everything the trace holds, syncs included
    std::size_t opening_run = 0;  // syncs before the first record of the workload
    trace::decode(bytes, overloaded{
                             [&](const trace::clock_sync& event,
                                 const trace::tracepoint_metadata&) {
                                 ++events;
                                 syncs.push_back(event);
                                 if (syncs.size() == events) {
                                     ++opening_run;
                                 }
                             },
                             [&](const auto&, const trace::tracepoint_metadata&) { ++events; },
                         });

    // Two openers -- the info ring's and the debug ring's, and not the metadata
    // ring's, which never gets one -- ahead of any record the workload wrote.
    CHECK(opening_run == 2);

    // And more than that in total, which is the rotations: a trace whose rings
    // had never rotated would hold exactly the two.
    CHECK(syncs.size() > 2);
    CHECK(syncs.size() < events);

    // Every one carries both halves of the conversion. The wall clock is the
    // demo's fixed one -- its records have to be reproducible for the snapshot
    // the build takes of them -- and the rate is the uncalibrated default,
    // since nothing in the producer calibrates.
    for (const trace::clock_sync& sync : syncs) {
        CHECK(sync.ticks_per_second == tracer::default_tsc_ticks_per_second);
        CHECK(sync.realtime_ns == syncs.front().realtime_ns);
    }
}

// A tracepoint's name becomes a struct's name and its parameters become that
// struct's members, so a table the generator accepts is one C++ will too. What
// it cannot express, it refuses -- naming the call site, because a build step's
// diagnostic is all its author gets.
TEST_CASE("the code generator refuses a table it cannot turn into structs") {
    const auto rejects = [](std::vector<tracer::tracepoint_entry> table) {
        return [table] {
            (void)generate_from(table);
        };
    };

    CHECK_THROWS_AS(rejects({fake("hello world", "")})(), std::runtime_error);
    CHECK_THROWS_AS(rejects({fake("2fast", "")})(), std::runtime_error);
    CHECK_THROWS_AS(rejects({fake("", "")})(), std::runtime_error);

    CHECK_THROWS_AS(rejects({fake("tp", "a:u32,a:u32")})(), std::runtime_error);
    CHECK_THROWS_AS(rejects({fake("tp", "not an identifier:u32")})(), std::runtime_error);
    CHECK_THROWS_AS(rejects({fake("tp", "u32")})(), std::runtime_error);
    CHECK_THROWS_AS(rejects({fake("tp", "a:")})(), std::runtime_error);
    CHECK_THROWS_AS(rejects({fake("tp", "a:u128")})(), std::runtime_error);

    // And a table it does accept produces the struct it promised.
    const std::vector<tracer::tracepoint_entry> table{fake("cache_hit", "key:str,age:u16")};
    const std::string source = generate_from(table);
    CHECK(source.find("struct cache_hit {") != std::string::npos);
    CHECK(source.find("std::string_view key;") != std::string::npos);
    CHECK(source.find("std::uint16_t age;") != std::string::npos);
}

// The price of naming a tracepoint by hand. An address is unique because the
// linker made it so; an id is unique because the person who wrote it down
// checked, and this is what does the checking for them.
TEST_CASE("a static id names one tracepoint") {
    constexpr auto id = tracer::tracepoint_id{4};

    const std::vector<tracer::tracepoint_entry> clashing{
        fake("first_event", "value:u32", 1, id), fake("second_event", "value:u32", 2, id)};
    CHECK_THROWS_AS((void)generate_from(clashing), std::runtime_error);

    // Two entries of *one* tracepoint are not a clash: a tracepoint in a header
    // is compiled into every object that includes it, and both copies carry the
    // id the header gave it.
    const std::vector<tracer::tracepoint_entry> shared{fake("shared_event", "value:u32", 1, id)};
    std::string source;
    CHECK_NOTHROW(source = generate_from(shared, shared));

    // And the decoder maps the id to that tracepoint without consulting any
    // object: an id says which tracepoint it is on its own, which is the other
    // half of what it is for.
    CHECK(source.find("case 4: return 0;") != std::string::npos);

    // An id nothing was generated from is refused rather than guessed at.
    CHECK(source.find("default: return no_decoder_id;") != std::string::npos);
}

// One name is one struct, and several tracepoints may wear it.
//
// This is what a tracepoint written in a shared header looks like from the
// outside: every object that includes it compiles its own, so the process holds
// several entries that mean one event. They are merged rather than rejected --
// but only while they agree, because the struct can only be one shape.
TEST_CASE("the code generator merges tracepoints that share a name") {
    const std::vector<tracer::tracepoint_entry> twice{fake("tp", "a:u32", 7),
                                                      fake("tp", "a:u32", 7)};
    const std::string source = generate_from(twice);

    // One struct and one reader, but an entry apiece: each keeps its own
    // metadata and its own id, so a record still says which copy fired.
    CHECK(source.find("struct tp {") != std::string::npos);
    CHECK(source.rfind("struct tp {") == source.find("struct tp {"));
    CHECK(source.find("metadata_0") != std::string::npos);
    CHECK(source.find("metadata_1") != std::string::npos);

    // The same across two objects, which is the case that actually arises.
    const std::vector<tracer::tracepoint_entry> first{fake("tp", "a:u32", 7)};
    const std::vector<tracer::tracepoint_entry> second{fake("tp", "a:u32", 7),
                                                       fake("other", "b:str", 9)};
    const std::string shared = generate_from(first, second);
    CHECK(shared.find("struct tp {") != std::string::npos);
    CHECK(shared.rfind("struct tp {") == shared.find("struct tp {"));
    CHECK(shared.find("\"00first0\", 0, 1") != std::string::npos);
    CHECK(shared.find("\"00second\", 1, 2") != std::string::npos);

    // Disagreeing about the parameters is still an error, and the complaint
    // names both call sites -- neither of which is wrong on its own.
    const std::vector<tracer::tracepoint_entry> disagreeing{fake("dup", "a:u32", 3),
                                                            fake("dup", "b:u32", 4)};
    try {
        (void)generate_from(disagreeing);
        FAIL("two shapes under one tracepoint name were accepted");
    } catch (const std::runtime_error& e) {
        CHECK(std::string_view(e.what()) ==
              "fake.cc:4 (tracepoint \"dup\"): a tracepoint of this name is defined at "
              "fake.cc:3 with a different parameter list (\"a:u32\" there, \"b:u32\" here)");
    }
}

// A shared library's tracepoints are its own: its own section, its own
// __start/__stop brackets, its own static keys. What makes them the process's
// is the registration in tracer.h, which happens as the library is mapped and
// is undone as it goes away.
TEST_CASE("a dlopen()ed library brings its tracepoints with it and takes them away") {
    const without_clock_sync quiet;
    const std::size_t before = tracer::tracepoints().size();
    const std::size_t objects_before = tracing_objects().size();
    REQUIRE(count_named("plugin_loaded") == 0);

    {
        plugin_handle plugin;
        const auto plugin_count = plugin.sym<std::size_t (*)()>("tracer_plugin_tracepoint_count");
        const auto run = plugin.sym<void (*)(std::uint32_t)>("tracer_plugin_run");

        // Exactly the library's own table arrived, and it is a second object as
        // far as a trace is concerned -- with a build ID of its own.
        CHECK(tracer::tracepoints().size() == before + plugin_count());
        const std::vector<tracer::trace_object> objects = tracing_objects();
        REQUIRE(objects.size() == objects_before + 1);
        CHECK(objects[0].build_id != objects[1].build_id);

        // A name is a handle on a tracepoint wherever it was compiled, so this
        // reaches into the library from outside it.
        CHECK(count_named("plugin_loaded") == 1);
        REQUIRE(tracer::set_tracepoint_enabled("plugin_loaded", true) == 1);
        REQUIRE(tracer::set_tracepoint_enabled("plugin_work", true) == 1);
        REQUIRE(tracer::set_tracepoint_enabled("shared_event", true) == 1);

        // And the library records into this process's tracer, through the
        // thread_local it shares with the executable.
        tracer::trace_buffers buffers(4096, 4096, 4096, 512);
        tracer::local_tracer = &buffers;
        run(2);
        tracer::local_tracer = nullptr;

        // plugin_loaded and shared_event carry a u32 each; the two plugin_work
        // records are on the debug ring. shared_event has a static id, so its
        // record is the shorter kind -- two bytes of header rather than nine.
        CHECK(buffers.group(tracer::event_level::info).collect().size() >=
              tracer::record_header_size + 2 + 2 * sizeof(std::uint32_t));
        CHECK(buffers.group(tracer::event_level::debug).collect().size() >=
              2 * (tracer::record_header_size + sizeof(std::uint32_t) + sizeof(std::uint16_t) +
                   std::string_view("handshake").size()));

        REQUIRE(tracer::set_tracepoint_enabled("plugin_loaded", false) == 1);
        REQUIRE(tracer::set_tracepoint_enabled("plugin_work", false) == 1);
        REQUIRE(tracer::set_tracepoint_enabled("shared_event", false) == 1);

        plugin.close();
    }

    // The table went with the library. Anything else would leave the registry
    // pointing into an unmapped range for the rest of the process's life.
    CHECK(tracer::tracepoints().size() == before);
    CHECK(tracing_objects().size() == objects_before);
    CHECK(count_named("plugin_loaded") == 0);
}

// A tracer describes the objects into its own metadata ring, at the level of
// the records it is describing and through the same macro. What it says is a
// history rather than a snapshot -- a record that outlives its object is only
// decodable against what the ring said at the time it was written -- so the
// ring is only ever added to.
//
// The sharp edge of that bargain is that a load nobody reported is a load the
// ring does not have.
TEST_CASE("a tracer records the objects it was built with, and the changes it is told about") {
    const auto metadata = [](const tracer::trace_buffers& buffers) {
        return buffers.group(tracer::event_level::metadata).collect();
    };

    // One object, so: a count, and one load event. Both carry a build ID, whose
    // length is what makes the size worth asserting relatively rather than
    // exactly.
    tracer::trace_buffers buffers(4096, 4096, 4096, 512);
    const std::vector<std::byte> alone = metadata(buffers);
    CHECK(alone.size() > 2 * tracer::record_header_size);

    // Nothing has changed, so there is nothing to say.
    buffers.note_objects_changed();
    CHECK(metadata(buffers) == alone);

    plugin_handle plugin;

    // Loaded, registered, and genuinely part of the process -- but not yet
    // reported, so the ring still describes the process as it was.
    REQUIRE(tracing_objects().size() == 2);
    CHECK(metadata(buffers) == alone);

    buffers.note_objects_changed();
    const std::vector<std::byte> loaded = metadata(buffers);
    CHECK(loaded.size() > alone.size());
    CHECK(std::equal(alone.begin(), alone.end(), loaded.begin()));

    plugin.close();
    buffers.note_objects_changed();
    const std::vector<std::byte> unloaded = metadata(buffers);
    CHECK(unloaded.size() > loaded.size());
    CHECK(std::equal(loaded.begin(), loaded.end(), unloaded.begin()));

    // A tracer built now describes what is loaded now, which is one object
    // again -- the prologue is the same shape whenever it is written, down to
    // the timestamps that are the only thing separating these two.
    const tracer::trace_buffers fresh(4096, 4096, 4096, 512);
    CHECK(metadata(fresh).size() == alone.size());
}

// Unloading a plugin and loading another over it.
//
// The hazard is that a record names its tracepoint by address: a record left in
// a ring when its object goes away points into a range that the next dlopen()
// may well be given -- and it would decode, plausibly and wrongly, as a
// tracepoint of the new object, if a trace said only where objects are rather
// than which ones were there. Each tracer says what it was built with, so a
// record is read against the mapping its own trace describes.
//
// The traces below are decoded with this build's generated decoder, which knows
// the plugin (it is the same library the pipeline was generated from) and does
// not know this test binary. So only the plugin's tracepoint is switched on:
// what is being tested is that the plugin's records survive its own reload.
TEST_CASE("a plugin can be replaced without its records being misread") {
    // This binary's own records -- which a sync record is -- are not decodable
    // by the generated decoder: it was generated from the producer's table and
    // the plugin's, and this executable is neither.
    const without_clock_sync quiet;

    // Taken before anything is loaded, so that "the object that is not this
    // one" means the plugin for the rest of the test.
    const std::vector<tracer::trace_object> alone = tracing_objects();
    REQUIRE(alone.size() == 1);
    const std::string self = alone.front().build_id;

    // One pass: open the plugin, build a tracer -- which describes what is
    // loaded now, the plugin included -- and record through it.
    const auto record_one = [](std::uintptr_t& address, const std::string& us) {
        const plugin_handle plugin;
        tracer::trace_buffers buffers(4096, 4096, 4096, 512);
        tracer::local_tracer = &buffers;
        address = plugin_table_address(us);

        // Every tracepoint the plugin's run touches, by name -- a tracepoint
        // starts disabled, and the plugin's are freshly mapped each pass, so
        // this has to be redone after every load. "shared_event" is compiled
        // into both objects, hence no count asserted on it.
        REQUIRE(tracer::set_tracepoint_enabled("plugin_loaded", true) == 1);
        REQUIRE(tracer::set_tracepoint_enabled("plugin_work", true) == 1);
        REQUIRE(tracer::set_tracepoint_enabled("shared_event", true) >= 1);
        plugin.sym<void (*)(std::uint32_t)>("tracer_plugin_run")(1);
        REQUIRE(tracer::set_tracepoint_enabled("plugin_loaded", false) == 1);
        REQUIRE(tracer::set_tracepoint_enabled("plugin_work", false) == 1);
        REQUIRE(tracer::set_tracepoint_enabled("shared_event", false) >= 1);

        std::vector<std::byte> trace = tracer::collect_trace(buffers);
        tracer::local_tracer = nullptr;
        return trace;
    };

    std::uintptr_t first_address = 0;
    std::uintptr_t second_address = 0;
    const std::vector<std::byte> first = record_one(first_address, self);
    const std::vector<std::byte> second = record_one(second_address, self);

    // Both decode, and to the same thing -- which is the point, whether or not
    // the second load happened to be given the first one's address. It usually
    // is, and that is exactly the case this exists for.
    MESSAGE("plugin table at ", first_address, " then ", second_address);
    const auto decoded = [](std::span<const std::byte> trace) {
        std::string text;
        trace::decode(trace, [&text](const auto& event, const trace::tracepoint_metadata& meta) {
            text += std::format("{}:{} {}\n", meta.file, meta.line, event.to_string());
        });
        return text;
    };
    CHECK(decoded(first) ==
          "modules/tracer/plugin/trace_plugin.cc:19 plugin_loaded{connections=1}\n"
          "modules/tracer/plugin/trace_plugin.cc:22 plugin_work{step=0, label=handshake}\n"
          "modules/tracer/plugin/common_tracepoints.h:41 shared_event{sequence=1}\n");
    CHECK(decoded(second) == decoded(first));
}

namespace {

// Where the build put the producer's objects. A source location is an address
// inside one of them, so a decoder needs the files themselves -- unlike a
// tracepoint, whose name and file are compiled into the decoder.
std::string dso_dir() {
    const char* const path = std::getenv("TRACER_DSOS");
    REQUIRE_MESSAGE(path != nullptr, "TRACER_DSOS is not set");
    return path;
}

// Every table_opened event of the demo trace, which is the tracepoint carrying a
// location. `dsos` is the caller's, because what these cases differ in is which
// objects the decoder was given.
std::vector<trace::table_opened> opened_tables(trace::dso_directory& dsos) {
    const std::string raw = read_env_file("TRACER_TRACE");
    const std::span<const std::byte> bytes{reinterpret_cast<const std::byte*>(raw.data()),
                                           raw.size()};
    std::vector<trace::table_opened> found;
    trace::decode(bytes, overloaded{
                             [&found](const trace::table_opened& event,
                                      const trace::tracepoint_metadata&) { found.push_back(event); },
                             [](const auto&, const trace::tracepoint_metadata&) {},
                         },
                  dsos);
    return found;
}

}  // namespace

// A location is the address of the compiler's own constant, so decoding one is
// not a table lookup but a read out of the object it points into: the metadata
// stream says where that object was mapped, and the build-ID directory says
// which file it was. This is the whole reason the metadata stream carries an
// object's base address and not just its tracepoint table's.
TEST_CASE("a source location decodes to the place it was captured") {
    trace::dso_directory dsos(dso_dir());
    const std::vector<trace::table_opened> opened = opened_tables(dsos);
    REQUIRE(opened.size() == 3);

    // The two captured ones name their *call sites* -- two different lines of
    // trace_producer.cc -- and not the tracepoint, which is one line inside
    // open_table() and is what meta.file/meta.line would have said.
    CHECK(opened[0].opened_at.resolved);
    CHECK(opened[0].opened_at.file == "modules/tracer/trace_producer.cc");
    CHECK(opened[0].opened_at.function.find("run_demo") != std::string::npos);
    CHECK(opened[0].opened_at.column > 0);

    CHECK(opened[1].opened_at.resolved);
    CHECK(opened[1].opened_at.file == opened[0].opened_at.file);
    CHECK(opened[1].opened_at.line == opened[0].opened_at.line + 1);

    // And the one that was never captured decodes as nothing rather than as an
    // address that happens to be zero.
    CHECK_FALSE(opened[2].opened_at.resolved);
    CHECK(opened[2].opened_at.address == 0);
    CHECK(opened[2].opened_at.to_string() == "<none>");
}

// The bargain the address makes. A tracepoint is decodable from the generated
// header alone; a location is not, and a decoder without the objects has to say
// so rather than invent a file. It says so per location, so the rest of the
// trace still decodes -- a missing object is a decoder that was set up wrong,
// not a trace that is wrong.
TEST_CASE("a location whose object the decoder has not got stays unresolved") {
    trace::dso_directory empty("/nonexistent");
    const std::vector<trace::table_opened> opened = opened_tables(empty);
    REQUIRE(opened.size() == 3);

    CHECK_FALSE(opened[0].opened_at.resolved);
    CHECK(opened[0].opened_at.file.empty());
    // The address it was recorded as survives, and so does the identity of the
    // object it is in -- which is what the metadata stream can say without any
    // file at all.
    CHECK(opened[0].opened_at.address != 0);
    CHECK_FALSE(opened[0].opened_at.object.empty());
    CHECK(opened[0].opened_at.to_string().starts_with("<unresolved 0x"));

    // Two call sites are still two addresses, unresolved or not.
    CHECK(opened[0].opened_at.address != opened[1].opened_at.address);
}

// The end-to-end pipeline, asserted on its output.
//
// Everything upstream of this is a build step: :trace_producer emits both a
// trace and the header of a decoder for its own tracepoint table, :trace_decoder
// is a program built against that header, and :decoded_trace is it run on the
// trace. What lands here is its stdout.
//
// Timestamps are a counter rather than rdtsc (see trace_producer.cc), and the
// snapshot keeps the source files, event values, and the rest of the output
// literal. Timestamps and source line and column numbers are the only fields
// allowed to move.
TEST_CASE("decoded trace") {
    const RegexText decoded = serialize_trace_columns(read_env_file("TRACER_DECODED"));
    check_snapshot(decoded, R"snap(
        |               800 | modules/tracer/include/tracer/tracer.h:1398   | clock_sync{realtime_ns=1700000000000000000, ticks_per_second=3187000000}
        |               900 | modules/tracer/include/tracer/tracer.h:1398   | clock_sync{realtime_ns=1700000000000000000, ticks_per_second=3187000000}
        |              1000 | modules/tracer/trace_producer.cc:58           | listening{port=8080}
        |              1100 | modules/tracer/trace_producer.cc:61           | accepted_connection{conn=0, keepalive=true}
        |              1200 | modules/tracer/trace_producer.cc:66           | request_header{method=GET, path=/}
        |              1300 | modules/tracer/trace_producer.cc:61           | accepted_connection{conn=1, keepalive=false}
        |              1400 | modules/tracer/trace_producer.cc:66           | request_header{method=GET, path=/index.html}
        |              1500 | modules/tracer/trace_producer.cc:61           | accepted_connection{conn=2, keepalive=true}
        |              1600 | modules/tracer/trace_producer.cc:66           | request_header{method=GET, path=/}
        |              1700 | modules/tracer/trace_producer.cc:73           | cache_miss{key=73657373696f6e, slot=0xdeadbeef}
        |              1800 | modules/tracer/trace_producer.cc:76           | clock_skew{nanoseconds=-4200, retries=3}
        |              1900 | modules/tracer/plugin/trace_plugin.cc:19      | plugin_loaded{connections=2}
        |              2000 | modules/tracer/plugin/trace_plugin.cc:22      | plugin_work{step=0, label=handshake}
        |              2100 | modules/tracer/plugin/trace_plugin.cc:22      | plugin_work{step=1, label=handshake}
        |              2200 | modules/tracer/plugin/common_tracepoints.h:41 | shared_event{sequence=2}
        |              2300 | modules/tracer/plugin/common_tracepoints.h:41 | shared_event{sequence=99}
        |              2400 | modules/tracer/trace_producer.cc:51           | table_opened{name=users, opened_at=modules/tracer/trace_producer.cc:86:5}
        |              2500 | modules/tracer/trace_producer.cc:51           | table_opened{name=sessions, opened_at=modules/tracer/trace_producer.cc:87:5}
        |              2600 | modules/tracer/include/tracer/tracer.h:1398   | clock_sync{realtime_ns=1700000000000000000, ticks_per_second=3187000000}
        |              2700 | modules/tracer/trace_producer.cc:89           | table_opened{name=anonymous, opened_at=<none>}
        |              2800 | modules/tracer/trace_producer.cc:91           | shutting_down{}
        )snap"_snap);
}

// The same trace, decoded in this process against the same generated header --
// which is the way a program that wants the events rather than the text would
// use it.
TEST_CASE("a decoded trace is structs, not text") {
    const std::string raw = read_env_file("TRACER_TRACE");
    const std::span<const std::byte> bytes{reinterpret_cast<const std::byte*>(raw.data()),
                                           raw.size()};

    collector out;
    trace::dso_directory dsos(dso_dir());
    trace::decode(bytes, out, dsos);

    check_snapshot(serialize_trace_columns(out.text), R"snap(
        |modules/tracer/include/tracer/tracer.h:1398 clock_sync{realtime_ns=1700000000000000000, ticks_per_second=3187000000}
        |modules/tracer/include/tracer/tracer.h:1398 clock_sync{realtime_ns=1700000000000000000, ticks_per_second=3187000000}
        |modules/tracer/trace_producer.cc:58 listening{port=8080}
        |accepted_connection: connection 0, keepalive true
        |request_header: GET /
        |accepted_connection: connection 1, keepalive false
        |request_header: GET /index.html
        |accepted_connection: connection 2, keepalive true
        |request_header: GET /
        |modules/tracer/trace_producer.cc:73 cache_miss{key=73657373696f6e, slot=0xdeadbeef}
        |modules/tracer/trace_producer.cc:76 clock_skew{nanoseconds=-4200, retries=3}
        |modules/tracer/plugin/trace_plugin.cc:19 plugin_loaded{connections=2}
        |modules/tracer/plugin/trace_plugin.cc:22 plugin_work{step=0, label=handshake}
        |modules/tracer/plugin/trace_plugin.cc:22 plugin_work{step=1, label=handshake}
        |modules/tracer/plugin/common_tracepoints.h:41 shared_event{sequence=2}
        |modules/tracer/plugin/common_tracepoints.h:41 shared_event{sequence=99}
        |modules/tracer/trace_producer.cc:51 table_opened{name=users, opened_at=modules/tracer/trace_producer.cc:86:5}
        |modules/tracer/trace_producer.cc:51 table_opened{name=sessions, opened_at=modules/tracer/trace_producer.cc:87:5}
        |modules/tracer/include/tracer/tracer.h:1398 clock_sync{realtime_ns=1700000000000000000, ticks_per_second=3187000000}
        |modules/tracer/trace_producer.cc:89 table_opened{name=anonymous, opened_at=<none>}
        |modules/tracer/trace_producer.cc:91 shutting_down{}
        )snap"_snap);
}

TEST_CASE("a trace that cannot be decoded stops the decode") {
    const auto ignore = [](const auto&, const trace::tracepoint_metadata&) {};

    // Fewer bytes than the magic, and then bytes that are not a trace at all.
    // Neither is something to read addresses out of.
    const std::array<std::byte, 3> truncated{};
    CHECK_THROWS_AS(trace::decode(truncated, ignore), std::runtime_error);
    const std::array<std::byte, 8> garbage{std::byte{0xFF}};
    CHECK_THROWS_AS(trace::decode(garbage, ignore), std::runtime_error);

    // A record from an object the trace names but this decoder was not
    // generated from. Its address means nothing here, and guessing at the
    // object below it would decode the wrong tracepoint rather than fail.
    const std::array<std::pair<std::string_view, std::uint64_t>, 1> stranger{
        std::pair<std::string_view, std::uint64_t>{"00stranger00", 0x1000}};
    std::array<std::byte, tracer::record_header_size> record{};
    const auto address = std::uint64_t{0x1000};
    std::memcpy(record.data(), &address, sizeof(address));
    CHECK_THROWS_AS(trace::decode(fake_trace(stranger, record), ignore), std::runtime_error);

    // A record below every object loaded at its timestamp, which no offset can
    // be taken from.
    const std::array<std::byte, tracer::record_header_size> below{};
    CHECK_THROWS_AS(trace::decode(fake_trace(stranger, below), ignore), std::runtime_error);

    // A record naming a static id this decoder has never heard of, which is a
    // trace from a build with tracepoints this one has not. Nothing places it:
    // an id says which tracepoint it is on its own or not at all.
    std::array<std::byte, tracer::record_header_size> unknown_id{};
    // Id 63 doubled and made odd is 127, which fits in a byte with an empty
    // length tag -- so one byte of id, and the rest is timestamp.
    unknown_id[0] = std::byte{127 << 1};
    CHECK_THROWS_AS(trace::decode(fake_trace(stranger, unknown_id), ignore), std::runtime_error);

    // Half a record: its address and timestamp vint are there and its arguments are
    // not, which cannot be told from a record that has not been reached yet
    // until it is read.
    const std::array<std::byte, tracer::record_header_size / 2> half{};
    CHECK_THROWS_AS(trace::decode(fake_trace(stranger, half), ignore), std::runtime_error);

    // And a real trace with its last chunk cut short. A chunk says how long it
    // is, so this is caught where the streams are laid out rather than in the
    // middle of a record.
    const std::string raw = read_env_file("TRACER_TRACE");
    const std::span<const std::byte> whole{reinterpret_cast<const std::byte*>(raw.data()),
                                           raw.size()};
    CHECK_NOTHROW(trace::decode(whole, ignore));
    CHECK_THROWS_AS(trace::decode(whole.first(whole.size() - 4), ignore), std::runtime_error);
}
