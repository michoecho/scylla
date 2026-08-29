#include <algorithm>
#include <array>
#include <cstdlib>
#include <format>
#include <fstream>
#include <iterator>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <doctest/doctest.h>

#include "snapshot/check.h"
#include "static_keys/static_keys.h"
#include "tracer/codegen.h"
#include "tracer/tracer.h"
#include "tracer_generated/decoder.h"

namespace {

using snapshot_testing::check_snapshot;
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

// The signature TRACEPOINT() would build for this parameter list, without
// recording anything. A macro because the parameter *names* come from the
// argument list as written, which only the preprocessor can hand over.
#define SIGNATURE_OF(...)                                                        \
    std::string_view(::tracer::signature_builder<                                \
                     ::tracer::fixed_string{#__VA_ARGS__},                       \
                     decltype(::tracer::sig_probe(__VA_ARGS__))>::value.data())

// A tracepoint table assembled by hand, which is the only way to hand the code
// generator a table it should reject: a malformed one cannot be written as a
// TRACEPOINT(), and a duplicate name is a fact about two call sites at once.
//
// The key is left null. Generating a decoder reads names, types and locations
// and never touches a key.
tracer::tracepoint_entry fake(const char* name, const char* signature) {
    return {name, "fake.cc", 1, "void fake()", signature, nullptr};
}

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
    // all agree rather than falling through to the opaque slot.
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

TEST_CASE("tracer records land in the buffer with their header") {
    tracer::trace_buffers buffers(4096, 4096, 512);
    tracer::local_tracer = &buffers;

    REQUIRE(tracer::set_tracepoint_enabled("value_seen", true) == 1);
    TRACEPOINT(tracer::event_level::info, "value_seen", "value", std::uint32_t{7});
    REQUIRE(tracer::set_tracepoint_enabled("value_seen", false) == 1);

    tracer::local_tracer = nullptr;
    const std::vector<std::byte> bytes = buffers.group(tracer::event_level::info).collect();

    // index + timestamp + one u32, and nothing else: the parameter's name is
    // not on the wire, and collect() must not return the unwritten tail of the
    // live buffer.
    CHECK(bytes.size() == tracer::record_header_size + sizeof(std::uint32_t));
}

TEST_CASE("a string parameter is recorded as its bytes, however it arrives") {
    const auto recorded_size = [](auto&& record) {
        tracer::trace_buffers buffers(4096, 4096, 512);
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
          }) == expected);
    CHECK(recorded_size([&] {
              TRACEPOINT(tracer::event_level::info, "string_seen", "text", pointer);
          }) == expected);
    CHECK(recorded_size([&] {
              TRACEPOINT(tracer::event_level::info, "string_seen", "text", view);
          }) == expected);
    REQUIRE(tracer::set_tracepoint_enabled("string_seen", false) == 3);
}

// The point of the key: an untouched tracepoint costs a nop and writes nothing.
//
// local_tracer is deliberately left null. A disabled tracepoint must not reach
// the recording code at all, and there is no null check on that path -- so if
// the branch were taken this would not merely record, it would crash, which is
// a sharper assertion than an empty buffer.
TEST_CASE("tracepoints are off until their key is enabled") {
    tracer::local_tracer = nullptr;

    TRACEPOINT(tracer::event_level::info, "never_recorded", "n", std::uint32_t{1});

    // The tracepoint above exists in the table even though it never fired: the
    // entry is emitted by the linker, not by the call.
    std::size_t found = 0;
    for (const tracer::tracepoint_entry& entry : tracer::tracepoints()) {
        if (entry.name == std::string_view("never_recorded")) {
            CHECK_FALSE(tracer::is_enabled(entry));
            CHECK(entry.signature == std::string_view("n:u32"));
            ++found;
        }
    }
    CHECK(found == 1);
}

TEST_CASE("a tracepoint's key is named after the tracepoint") {
    tracer::trace_buffers buffers(4096, 4096, 512);
    tracer::local_tracer = &buffers;

    TRACEPOINT(tracer::event_level::info, "keyed_tracepoint");

    tracer::local_tracer = nullptr;

    // The key that static_keys reports under this name and the key the
    // tracepoint table points at are the same object, which is what makes the
    // name a usable handle on the tracepoint from outside the binary.
    const std::span<const tracer::tracepoint_entry> table = tracer::tracepoints();
    const auto entry = std::find_if(table.begin(), table.end(),
                                    [](const tracer::tracepoint_entry& e) {
                                        return e.name == std::string_view("keyed_tracepoint");
                                    });
    REQUIRE(entry != table.end());

    const std::vector<static_keys::static_key_info> keys = static_keys::list_static_keys();
    const auto key = std::find_if(keys.begin(), keys.end(),
                                  [](const static_keys::static_key_info& k) {
                                      return k.name == "keyed_tracepoint";
                                  });
    REQUIRE(key != keys.end());
    CHECK(key->file == std::string_view(entry->file));

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

// A tracepoint's name becomes a struct's name and its parameters become that
// struct's members, so a table the generator accepts is one C++ will too. What
// it cannot express, it refuses -- naming the call site, because a build step's
// diagnostic is all its author gets.
TEST_CASE("the code generator refuses a table it cannot turn into structs") {
    const auto rejects = [](std::vector<tracer::tracepoint_entry> table) {
        return [table] {
            (void)tracer::generate_decoder_source(table);
        };
    };

    CHECK_THROWS_AS(rejects({fake("hello world", "")})(), std::runtime_error);
    CHECK_THROWS_AS(rejects({fake("2fast", "")})(), std::runtime_error);
    CHECK_THROWS_AS(rejects({fake("", "")})(), std::runtime_error);
    CHECK_THROWS_AS(rejects({fake("dup", "a:u32"), fake("dup", "b:u32")})(), std::runtime_error);

    CHECK_THROWS_AS(rejects({fake("tp", "a:u32,a:u32")})(), std::runtime_error);
    CHECK_THROWS_AS(rejects({fake("tp", "not an identifier:u32")})(), std::runtime_error);
    CHECK_THROWS_AS(rejects({fake("tp", "u32")})(), std::runtime_error);
    CHECK_THROWS_AS(rejects({fake("tp", "a:")})(), std::runtime_error);
    CHECK_THROWS_AS(rejects({fake("tp", "a:u128")})(), std::runtime_error);

    // The complaint says which tracepoint, and what is wrong with it.
    try {
        rejects({fake("tp", "a:u32"), fake("tp", "a:u32")})();
        FAIL("a duplicate tracepoint name was accepted");
    } catch (const std::runtime_error& e) {
        CHECK(std::string_view(e.what()) ==
              "fake.cc:1 (tracepoint \"tp\"): a tracepoint of this name is already defined "
              "elsewhere");
    }

    // And a table it does accept produces the struct it promised.
    const std::string source = tracer::generate_decoder_source(
        std::vector<tracer::tracepoint_entry>{fake("cache_hit", "key:str,age:u16")});
    CHECK(source.find("struct cache_hit {") != std::string::npos);
    CHECK(source.find("std::string_view key;") != std::string::npos);
    CHECK(source.find("std::uint16_t age;") != std::string::npos);
}

// The end-to-end pipeline, asserted on its output.
//
// Everything upstream of this is a build step: :trace_producer emits both a
// trace and the header of a decoder for its own tracepoint table, :trace_decoder
// is a program built against that header, and :decoded_trace is it run on the
// trace. What lands here is its stdout.
//
// Timestamps are a counter rather than rdtsc (see trace_producer.cc), so this
// is stable byte for byte. A diff in it is a change in the wire format, the
// generated decoder, or the demo workload.
TEST_CASE("decoded trace") {
    check_snapshot(read_env_file("TRACER_DECODED"), R"snap(
        |               100 | modules/tracer/trace_producer.cc:47 | listening{port=8080}
        |               800 | modules/tracer/trace_producer.cc:59 | cache_miss{key=73657373696f6e, slot=0xdeadbeef}
        |               900 | modules/tracer/trace_producer.cc:62 | clock_skew{nanoseconds=-4200, retries=3}
        |              1000 | modules/tracer/trace_producer.cc:64 | shutting_down{}
        |               200 | modules/tracer/trace_producer.cc:50 | accepted_connection{conn=0, keepalive=true}
        |               300 | modules/tracer/trace_producer.cc:52 | request_header{method=GET, path=/}
        |               400 | modules/tracer/trace_producer.cc:50 | accepted_connection{conn=1, keepalive=false}
        |               500 | modules/tracer/trace_producer.cc:52 | request_header{method=GET, path=/index.html}
        |               600 | modules/tracer/trace_producer.cc:50 | accepted_connection{conn=2, keepalive=true}
        |               700 | modules/tracer/trace_producer.cc:52 | request_header{method=GET, path=/}
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
    trace::decode(bytes, out);

    check_snapshot(out.text, R"snap(
        |modules/tracer/trace_producer.cc:47 listening{port=8080}
        |modules/tracer/trace_producer.cc:59 cache_miss{key=73657373696f6e, slot=0xdeadbeef}
        |modules/tracer/trace_producer.cc:62 clock_skew{nanoseconds=-4200, retries=3}
        |modules/tracer/trace_producer.cc:64 shutting_down{}
        |accepted_connection: connection 0, keepalive true
        |request_header: GET /
        |accepted_connection: connection 1, keepalive false
        |request_header: GET /index.html
        |accepted_connection: connection 2, keepalive true
        |request_header: GET /
        )snap"_snap);
}

TEST_CASE("a trace that cannot be decoded stops the decode") {
    const auto ignore = [](const auto&, const trace::tracepoint_metadata&) {};

    // A record header naming a tracepoint this decoder has never heard of.
    const std::array<std::byte, tracer::record_header_size> bad_id{std::byte{0xFF}};
    CHECK_THROWS_AS(trace::decode(bad_id, ignore), std::runtime_error);

    // Fewer bytes than a header. Records are not self-delimiting, so there is
    // nothing to resynchronise on.
    const std::array<std::byte, 3> truncated{};
    CHECK_THROWS_AS(trace::decode(truncated, ignore), std::runtime_error);
}
