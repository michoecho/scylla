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
#include "trace_reader.h"
#include "trace_wire.h"
#include "tracer/tracer.h"

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
//
// A line whose timestamp column is not a number is a record of a tracepoint
// that carries no timestamp -- a dash is printed for those -- and it is kept
// literal, column and all: what such a record is worth asserting is exactly
// that it has no time of its own.
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

// A trace naming objects that are not the demo's, plus whatever bytes the
// caller wants read as records -- for the cases where what is being tested is
// the refusal rather than the decode.
//
// The metadata records are written out by hand, which is the point: the entry
// addresses in them are zero, and a decoder reads the prologue anyway, because
// its first N+2 records are read by their position rather than by their
// addresses. Get that shape wrong and the refusal under test would be the
// prologue's rather than the record's, which is why it is written out in full
// here -- a clock sync, a count, and a load event apiece.
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

    // The clock sync every metadata stream opens with. Its timestamp is a
    // delta from its own first parameter, so the vint above is followed by the
    // three parameters of tracer.h's clock_sync.
    record_header();
    put(std::uint64_t{0});  // tsc
    put(std::uint64_t{0});  // realtime_ns
    put(tracer::default_tsc_ticks_per_second);

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

// Where the build put the demo's objects. They are two things at once: the
// tracepoint tables that say what a record means, and the files a source
// location -- an address inside one of them -- is read out of.
std::string dso_dir() {
    const char* const path = std::getenv("TRACER_DSOS");
    REQUIRE_MESSAGE(path != nullptr, "TRACER_DSOS is not set");
    return path;
}

// A decoder for those tables, built once: reading them is an ELF walk per
// object, and every case below wants the same answer from it.
const trace_test::trace_reader& demo_reader() {
    static const trace_test::trace_reader reader(dso_dir());
    return reader;
}

// The demo trace the build took, as bytes. It outlives the events decoded from
// it -- a decoded string field is a copy, but a test comparing bytes wants the
// buffer -- so it is static too.
std::span<const std::byte> demo_trace() {
    static const std::string raw = read_env_file("TRACER_TRACE");
    return {reinterpret_cast<const std::byte*>(raw.data()), raw.size()};
}

// The events of the demo trace, resolved against the objects it came from.
const std::vector<trace_test::event>& demo_events() {
    static const std::vector<trace_test::event> events = demo_reader().decode(demo_trace());
    return events;
}

// Every event of one tracepoint, in the order they were recorded.
std::vector<trace_test::event> events_named(const std::vector<trace_test::event>& events,
                                            std::string_view name) {
    std::vector<trace_test::event> found;
    for (const trace_test::event& e : events) {
        if (e.name == name) {
            found.push_back(e);
        }
    }
    return found;
}

// One field of one event, by name. REQUIREd rather than returned as a pointer:
// a case reaching for a parameter that is not there has already failed, and
// what it wants to say is which parameter.
const trace_test::field& field_of(const trace_test::event& e, std::string_view name) {
    const trace_test::field* const found = e.find(name);
    REQUIRE_MESSAGE(found != nullptr, e.name, " has no parameter ", name);
    return *found;
}

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

// Clock sync records are ordinary records: a tracer writes one into every ring
// when it is built, and one into every ring but the metadata one whenever it
// rotates. The
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

    // The largest value there is, which is where the encoding stops: 56 bits
    // above an eight-bit tag. Anything above it is out of write_int()'s
    // contract, and a record's timestamp is kept inside it by the rebasing
    // described at buffer_group::rebase().
    CHECK(encode(tracer::max_vint_value).first == 8);
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

// What TRACEPOINT_UNTIMED() buys, on the wire: the bytes between the id and the
// arguments, which for a record that follows another one closely is a number
// the record before it already carried. See timestamp_encoding in tracer.h.
TEST_CASE("a tracepoint declared untimed writes no timestamp") {
    const without_clock_sync quiet;
    tracer::trace_buffers buffers(4096, 4096, 4096, 512);
    tracer::local_tracer = &buffers;
    REQUIRE(tracer::set_tracepoint_enabled("untimed_event", true) == 1);
    TRACEPOINT_STATIC_ID_UNTIMED(tracer::tracepoint_id{8}, tracer::event_level::info,
                                 "untimed_event", "value", std::uint32_t{9});
    REQUIRE(tracer::set_tracepoint_enabled("untimed_event", false) == 1);
    tracer::local_tracer = nullptr;

    // One byte of id and four of argument, which is the smallest a record
    // carrying a parameter gets: both of the things a record costs beyond its
    // arguments are gone.
    const std::vector<std::byte> bytes = buffers.group(tracer::event_level::info).collect();
    REQUIRE(bytes.size() == 1 + sizeof(std::uint32_t));

    const std::byte* p = bytes.data();
    const std::byte* const end = p + bytes.size();
    const trace::detail::record_id which = trace::detail::read_record_id(p, end);
    CHECK(which.is_static);
    CHECK(which.value == 8);
    CHECK(static_cast<std::size_t>(end - p) == sizeof(std::uint32_t));

    // And the table says so, which is the only place a decoder can learn it:
    // nothing in the record itself distinguishes the argument that follows the
    // id from a timestamp that would have preceded it.
    const tracer::tracepoint_entry* entry = nullptr;
    for (const tracer::tracepoint_entry* candidate : tracer::tracepoints()) {
        if (std::string_view(candidate->name) == "untimed_event") {
            entry = candidate;
        }
    }
    REQUIRE(entry != nullptr);
    CHECK(entry->timestamps == tracer::timestamp_encoding::none);
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
// Asserted on the demo trace rather than one taken here, and for the same
// reason the whole of the decoding side is: a record is only readable against
// the tracepoint table of the object that wrote it, and the tables the tests
// have are the demo's -- this test binary is not one of them. The producer
// sizes its buffers below the size of its workload, so a ring rotates part way
// through it; see emit_trace().
TEST_CASE("every ring opens with a clock sync, and gets another one on rotation") {
    const std::vector<trace_test::event>& events = demo_events();
    const std::vector<trace_test::event> syncs = events_named(events, "clock_sync");

    // Two openers -- the info ring's and the debug ring's -- ahead of any
    // record the workload wrote. The metadata ring's opening sync is not among
    // them: it is the head of the stream the mappings are read from, consumed
    // rather than delivered.
    std::size_t opening_run = 0;
    for (const trace_test::event& e : events) {
        if (e.name != "clock_sync") {
            break;
        }
        ++opening_run;
    }
    CHECK(opening_run == 2);

    // And more than that in total, which is the rotations: a trace whose rings
    // had never rotated would hold exactly the two.
    CHECK(syncs.size() > 2);
    CHECK(syncs.size() < events.size());

    // Every one carries both halves of the conversion. The wall clock is the
    // demo's fixed one -- its records have to be reproducible for the snapshot
    // the build takes of them -- and the rate is the uncalibrated default,
    // since nothing in the producer calibrates.
    for (const trace_test::event& sync : syncs) {
        CHECK(field_of(sync, "ticks_per_second").number ==
              tracer::default_tsc_ticks_per_second);
        CHECK(field_of(sync, "realtime_ns").number ==
              field_of(syncs.front(), "realtime_ns").number);
    }
}

// The cases that were here -- what a table the generator cannot read does, how
// two entries of one tracepoint are folded together, which timestamp reader an
// id selects -- are now in //modules/trace-viewer:decoder_plugin_test, beside
// the generator they are about. Nothing in this module generates a decoder any
// more.

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
    // again -- the prologue is the same shape whenever it is written.
    //
    // The same *length* is more than can be asked of it. Every record opens
    // with a timestamp delta, and a vint is as long as the gap it measures: the
    // prologue at the top of this test was written into a cold process, this
    // one into a process that has since opened and closed a library, so a gap
    // that took two bytes to describe there may take one here. Three records --
    // the opening clock sync, the count, and the one load event -- and seven
    // bytes of slack apiece. A second object costs the better part of eighty --
    // a build ID and three addresses -- which is what this is really asking
    // about, and is well clear of the slack.
    const tracer::trace_buffers fresh(4096, 4096, 4096, 512);
    const std::size_t slack = 3 * (sizeof(std::uint64_t) - 1);
    const std::size_t refreshed = metadata(fresh).size();
    CHECK(refreshed + slack >= alone.size());
    CHECK(refreshed <= alone.size() + slack);
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
// The traces below are read against the demo's tables, which have the plugin in
// them (it is the same library the demo was built with) and do not have this
// test binary. So only the plugin's tracepoints are switched on: what is being
// tested is that the plugin's records survive its own reload.
TEST_CASE("a plugin can be replaced without its records being misread") {
    // This binary's own records -- which a sync record is -- cannot be read
    // against those tables: an address in this executable is in no object the
    // demo's dsos directory holds.
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
        for (const trace_test::event& e : demo_reader().decode(trace)) {
            text += std::format("{}:{} {}\n", e.file, e.line, e.to_string());
        }
        return text;
    };
    CHECK(decoded(first) ==
          "modules/tracer/plugin/trace_plugin.cc:19 plugin_loaded{connections=1}\n"
          "modules/tracer/plugin/trace_plugin.cc:22 plugin_work{step=0, label=handshake}\n"
          "modules/tracer/plugin/common_tracepoints.h:42 shared_event{sequence=1}\n");
    CHECK(decoded(second) == decoded(first));
}

namespace {

// Every table_opened event of the demo trace, which is the tracepoint carrying
// a location, with its locations resolved against `dsos`. The directory is the
// caller's, because what the two cases below differ in is which objects the
// decoder was given to read a location out of -- and not, as everything else
// here differs, in what it was told a record means.
std::vector<trace::source_location> opened_tables(trace::dso_directory& dsos) {
    std::vector<trace::source_location> found;
    for (const trace_test::event& e : demo_reader().decode(demo_trace(), dsos)) {
        if (e.name == "table_opened") {
            found.push_back(field_of(e, "opened_at").location);
        }
    }
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
    const std::vector<trace::source_location> opened = opened_tables(dsos);
    REQUIRE(opened.size() == 3);

    // The two captured ones name their *call sites* -- two different lines of
    // trace_producer.cc -- and not the tracepoint, which is one line inside
    // open_table() and is what meta.file/meta.line would have said.
    CHECK(opened[0].resolved);
    CHECK(opened[0].file == "modules/tracer/trace_producer.cc");
    CHECK(opened[0].function.find("run_demo") != std::string::npos);
    CHECK(opened[0].column > 0);

    CHECK(opened[1].resolved);
    CHECK(opened[1].file == opened[0].file);
    CHECK(opened[1].line == opened[0].line + 1);

    // And the one that was never captured decodes as nothing rather than as an
    // address that happens to be zero.
    CHECK_FALSE(opened[2].resolved);
    CHECK(opened[2].address == 0);
    CHECK(opened[2].to_string() == "<none>");
}

// The bargain the address makes. What a record *is* comes out of a tracepoint
// table; a location does not, and a decoder without the objects themselves has
// to say so rather than invent a file. It says so per location, so the rest of
// the trace still decodes -- a missing object is a decoder that was set up
// wrong, not a trace that is wrong.
TEST_CASE("a location whose object the decoder has not got stays unresolved") {
    trace::dso_directory empty("/nonexistent");
    const std::vector<trace::source_location> opened = opened_tables(empty);
    REQUIRE(opened.size() == 3);

    CHECK_FALSE(opened[0].resolved);
    CHECK(opened[0].file.empty());
    // The address it was recorded as survives, and so does the identity of the
    // object it is in -- which is what the metadata stream can say without any
    // file at all.
    CHECK(opened[0].address != 0);
    CHECK_FALSE(opened[0].object.empty());
    CHECK(opened[0].to_string().starts_with("<unresolved 0x"));

    // Two call sites are still two addresses, unresolved or not.
    CHECK(opened[0].address != opened[1].address);
}

// The end-to-end pipeline, asserted on its output.
//
// Everything upstream of this is a build step: :trace_producer runs the demo
// workload and dumps the trace, and dumps the objects it was written by. What
// is asserted here is those two put back together -- the records read against
// the tracepoint tables in those objects, which is the whole of what decoding
// is now.
//
// Timestamps are a counter rather than rdtsc (see trace_producer.cc), and the
// snapshot keeps the source files, event values, and the rest of the output
// literal. Timestamps and source line and column numbers are the only fields
// allowed to move.
TEST_CASE("decoded trace") {
    // The two columns a record carries besides its own fields: when it was
    // taken, and where the tracepoint is. A dash for a record of a tracepoint
    // that carries no timestamp -- it is handed the moment of the record before
    // it, which is a real answer to "when" but not one this column should
    // claim. See tracer.h's timestamp_encoding.
    constexpr int fileline_width = 44;
    std::string text;
    for (const trace_test::event& e : demo_events()) {
        text += std::format("{:>18} | {:<{}} | {}\n",
                            e.has_timestamp ? std::format("{}", e.timestamp) : "-",
                            std::format("{}:{}", e.file, e.line), fileline_width, e.to_string());
    }

    check_snapshot(serialize_trace_columns(text), R"snap(
        |               500 | modules/tracer/include/tracer/tracer.h:1682   | clock_sync{tsc=400, realtime_ns=1700000000000000000, ticks_per_second=3187000000}
        |               700 | modules/tracer/include/tracer/tracer.h:1682   | clock_sync{tsc=600, realtime_ns=1700000000000000000, ticks_per_second=3187000000}
        |              1800 | modules/tracer/trace_producer.cc:58           | listening{port=8080}
        |              1900 | modules/tracer/trace_producer.cc:61           | accepted_connection{conn=0, keepalive=true}
        |              2000 | modules/tracer/trace_producer.cc:66           | request_header{method=GET, path=/}
        |              2100 | modules/tracer/trace_producer.cc:61           | accepted_connection{conn=1, keepalive=false}
        |              2200 | modules/tracer/trace_producer.cc:66           | request_header{method=GET, path=/index.html}
        |              2300 | modules/tracer/trace_producer.cc:61           | accepted_connection{conn=2, keepalive=true}
        |              2400 | modules/tracer/trace_producer.cc:66           | request_header{method=GET, path=/}
        |              2500 | modules/tracer/trace_producer.cc:73           | cache_miss{key=73657373696f6e, slot=0xdeadbeef}
        |              2600 | modules/tracer/trace_producer.cc:76           | clock_skew{nanoseconds=-4200, retries=3}
        |              2700 | modules/tracer/plugin/trace_plugin.cc:19      | plugin_loaded{connections=2}
        |              2800 | modules/tracer/plugin/trace_plugin.cc:22      | plugin_work{step=0, label=handshake}
        |              2900 | modules/tracer/plugin/trace_plugin.cc:22      | plugin_work{step=1, label=handshake}
        |              3000 | modules/tracer/plugin/common_tracepoints.h:42 | shared_event{sequence=2}
        |              3100 | modules/tracer/plugin/common_tracepoints.h:42 | shared_event{sequence=99}
        |              3200 | modules/tracer/trace_producer.cc:51           | table_opened{name=users, opened_at=modules/tracer/trace_producer.cc:86:5}
        |              3300 | modules/tracer/trace_producer.cc:51           | table_opened{name=sessions, opened_at=modules/tracer/trace_producer.cc:87:5}
        |              3600 | modules/tracer/include/tracer/tracer.h:1682   | clock_sync{tsc=3500, realtime_ns=1700000000000000000, ticks_per_second=3187000000}
        |              3700 | modules/tracer/trace_producer.cc:89           | table_opened{name=anonymous, opened_at=<none>}
        |              3800 | modules/tracer/trace_producer.cc:102          | table_snapshot_begin{tables=3}
        |                 - | modules/tracer/trace_producer.cc:107          | table_snapshot_row{table=users, rows=10}
        |                 - | modules/tracer/trace_producer.cc:107          | table_snapshot_row{table=sessions, rows=20}
        |                 - | modules/tracer/trace_producer.cc:107          | table_snapshot_row{table=anonymous, rows=30}
        |                 - | modules/tracer/trace_producer.cc:113          | table_snapshot_end{}
        |              3900 | modules/tracer/trace_producer.cc:115          | shutting_down{}
        )snap"_snap);
}

// The same trace, as values rather than as that text. A field is read by the
// name its parameter was declared with and comes back typed -- an integer as a
// number, a string as its bytes -- which is what a consumer that wants the
// events rather than a printout of them gets.
TEST_CASE("a decoded record is fields, not a line of text") {
    const std::vector<trace_test::event>& events = demo_events();

    const std::vector<trace_test::event> accepted =
        events_named(events, "accepted_connection");
    REQUIRE(accepted.size() == 3);
    CHECK(field_of(accepted[0], "conn").number == 0);
    CHECK(field_of(accepted[0], "keepalive").number == 1);
    CHECK(field_of(accepted[1], "conn").number == 1);
    CHECK(field_of(accepted[1], "keepalive").number == 0);

    const std::vector<trace_test::event> headers = events_named(events, "request_header");
    REQUIRE(headers.size() == 3);
    CHECK(field_of(headers[0], "method").bytes == "GET");
    CHECK(field_of(headers[1], "path").bytes == "/index.html");

    // The widths and the signs are the signature's, not the printer's: a
    // negative parameter comes back negative rather than as a very large
    // unsigned one.
    const std::vector<trace_test::event> skew = events_named(events, "clock_skew");
    REQUIRE(skew.size() == 1);
    CHECK(field_of(skew[0], "nanoseconds").signed_number == -4200);
    CHECK(field_of(skew[0], "retries").number == 3);

    // A byte span is bytes, and keeps whatever is in them; the text column
    // above is the hex rendering of these.
    const std::vector<trace_test::event> misses = events_named(events, "cache_miss");
    REQUIRE(misses.size() == 1);
    CHECK(field_of(misses[0], "key").bytes == "session");
    CHECK(field_of(misses[0], "slot").number == 0xdeadbeef);

    // A parameter no tracepoint of this name has is a question with no answer,
    // rather than a zero.
    CHECK(misses[0].find("no_such_parameter") == nullptr);
}

// The other end of TRACEPOINT_UNTIMED(): what a consumer is handed for a record
// that carries no time of its own.
//
// The producer writes a snapshot -- one timed record, three rows, and a record
// closing it -- of which only the first is timed. See run_demo() in
// trace_producer.cc.
TEST_CASE("a record with no timestamp is dated from the record before it") {
    const std::vector<trace_test::event>& events = demo_events();

    const auto at =
        std::ranges::find(events, "table_snapshot_begin", &trace_test::event::name);
    REQUIRE(at != events.end());
    // In the order they were written, which is the order a stream of records
    // with no timestamps between them can be read in and no other.
    REQUIRE(events.end() - at >= 5);
    CHECK(at[0].has_timestamp);
    CHECK(at[1].name == "table_snapshot_row");
    CHECK(at[2].name == "table_snapshot_row");
    CHECK(at[3].name == "table_snapshot_row");
    CHECK(at[4].name == "table_snapshot_end");

    // Each of them carries the moment of the record before it -- which, for a
    // run of them, is the moment the run opened -- and says that the moment is
    // not its own.
    for (const trace_test::event& row : std::span{at + 1, 4}) {
        CHECK_FALSE(row.has_timestamp);
        CHECK(row.timestamp == at[0].timestamp);
    }

    // And the chain of deltas carries on from there rather than from them: the
    // record after the run is measured from the last record that was timed.
    const auto after = at + 5;
    REQUIRE(after != events.end());
    CHECK(after->has_timestamp);
    CHECK(after->timestamp > at[0].timestamp);

    // Everything else in this trace is timed, which is what the flag is for:
    // it is a fact about the tracepoint, and a consumer reads it per record
    // rather than keeping a list of which tracepoints are which.
    for (const trace_test::event& other : events) {
        if (!other.name.starts_with("table_snapshot_")) {
            CHECK(other.has_timestamp);
        }
    }
}

TEST_CASE("a trace that cannot be decoded stops the decode") {
    const trace_test::trace_reader& reader = demo_reader();
    const auto decode = [&reader](std::span<const std::byte> trace) {
        (void)reader.decode(trace);
    };

    // Fewer bytes than the magic, and then bytes that are not a trace at all.
    // Neither is something to read addresses out of.
    const std::array<std::byte, 3> truncated{};
    CHECK_THROWS_AS(decode(truncated), std::runtime_error);
    const std::array<std::byte, 8> garbage{std::byte{0xFF}};
    CHECK_THROWS_AS(decode(garbage), std::runtime_error);

    // A record from an object the trace names and the decoder has no table
    // for. Its address means nothing here, and guessing at the object below it
    // would decode the wrong tracepoint rather than fail.
    const std::array<std::pair<std::string_view, std::uint64_t>, 1> stranger{
        std::pair<std::string_view, std::uint64_t>{"00stranger00", 0x1000}};
    std::array<std::byte, tracer::record_header_size> record{};
    const auto address = std::uint64_t{0x1000};
    std::memcpy(record.data(), &address, sizeof(address));
    CHECK_THROWS_AS(decode(fake_trace(stranger, record)), std::runtime_error);

    // A record below every object loaded at its timestamp, which no offset can
    // be taken from.
    const std::array<std::byte, tracer::record_header_size> below{};
    CHECK_THROWS_AS(decode(fake_trace(stranger, below)), std::runtime_error);

    // A record naming a static id no table here claims, which is a trace from
    // a build with tracepoints these objects have not. Nothing places it:
    // an id says which tracepoint it is on its own or not at all.
    std::array<std::byte, tracer::record_header_size> unknown_id{};
    // Id 63 doubled and made odd is 127, which fits in a byte with an empty
    // length tag -- so one byte of id, and the rest is timestamp.
    unknown_id[0] = std::byte{127 << 1};
    CHECK_THROWS_AS(decode(fake_trace(stranger, unknown_id)), std::runtime_error);

    // Half a record: its address and timestamp vint are there and its arguments are
    // not, which cannot be told from a record that has not been reached yet
    // until it is read.
    const std::array<std::byte, tracer::record_header_size / 2> half{};
    CHECK_THROWS_AS(decode(fake_trace(stranger, half)), std::runtime_error);

    // And a real trace with its last chunk cut short. A chunk says how long it
    // is, so this is caught where the streams are laid out rather than in the
    // middle of a record.
    const std::span<const std::byte> whole = demo_trace();
    CHECK_NOTHROW(decode(whole));
    CHECK_THROWS_AS(decode(whole.first(whole.size() - 4)), std::runtime_error);
}
