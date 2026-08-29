#include <algorithm>
#include <cstdlib>
#include <fstream>
#include <iterator>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <doctest/doctest.h>

#include "snapshot/check.h"
#include "static_keys/static_keys.h"
#include "tracer/tracer.h"

namespace {

using snapshot_testing::check_snapshot;
using snapshot_testing::operator""_snap;

std::string read_file(const char* path) {
    std::ifstream in(path, std::ios::binary);
    REQUIRE_MESSAGE(in.good(), "cannot open ", path);
    return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}

std::string signature_string(auto... args) {
    return decltype(tracer::signature_probe(args...))::value.data();
}

}  // namespace

TEST_CASE("tracer signatures name the wire type of each argument") {
    CHECK(signature_string() == "");
    CHECK(signature_string(std::uint32_t{1}) == "u32");
    CHECK(signature_string(std::int64_t{1}, true) == "i64,bool");
    CHECK(signature_string(std::span<const std::byte>{}) == "bytes");

    // Selected by width, so the three distinct 64-bit integer types all agree
    // rather than falling through to the opaque slot.
    CHECK(signature_string(1L) == "i64");
    CHECK(signature_string(1LL) == "i64");
    CHECK(signature_string(std::uint8_t{1}, static_cast<const void*>(nullptr)) == "u8,ptr");
}

TEST_CASE("tracer records land in the buffer with their header") {
    tracer::trace_buffers buffers(4096, 4096, 512);
    tracer::local_tracer = &buffers;

    REQUIRE(tracer::set_tracepoint_enabled("value {}", true) == 1);
    TRACEPOINT(tracer::event_level::info, "value {}", tracer::log_level::info, std::uint32_t{7});
    REQUIRE(tracer::set_tracepoint_enabled("value {}", false) == 1);

    tracer::local_tracer = nullptr;
    const std::vector<std::byte> bytes = buffers.group(tracer::event_level::info).collect();

    // index + timestamp + one u32, and nothing else: collect() must not return
    // the unwritten tail of the live buffer.
    CHECK(bytes.size() == tracer::record_header_size + sizeof(std::uint32_t));
}

// The point of the key: an untouched tracepoint costs a nop and writes nothing.
//
// local_tracer is deliberately left null. A disabled tracepoint must not reach
// the recording code at all, and there is no null check on that path -- so if
// the branch were taken this would not merely record, it would crash, which is
// a sharper assertion than an empty buffer.
TEST_CASE("tracepoints are off until their key is enabled") {
    tracer::local_tracer = nullptr;

    TRACEPOINT(tracer::event_level::info, "never recorded {}", tracer::log_level::info,
               std::uint32_t{1});

    // The tracepoint above exists in the table even though it never fired: the
    // entry is emitted by the linker, not by the call.
    std::size_t found = 0;
    for (const tracer::tracepoint_entry& entry : tracer::tracepoints()) {
        if (entry.name == std::string_view("never recorded {}")) {
            CHECK_FALSE(tracer::is_enabled(entry));
            ++found;
        }
    }
    CHECK(found == 1);
}

TEST_CASE("a tracepoint's key is named after the tracepoint") {
    tracer::trace_buffers buffers(4096, 4096, 512);
    tracer::local_tracer = &buffers;

    TRACEPOINT(tracer::event_level::info, "keyed tracepoint", tracer::log_level::info);

    tracer::local_tracer = nullptr;

    // The key that static_keys reports under this name and the key the
    // tracepoint table points at are the same object, which is what makes the
    // name a usable handle on the tracepoint from outside the binary.
    const std::span<const tracer::tracepoint_entry> table = tracer::tracepoints();
    const auto entry = std::find_if(table.begin(), table.end(),
                                    [](const tracer::tracepoint_entry& e) {
                                        return e.name == std::string_view("keyed tracepoint");
                                    });
    REQUIRE(entry != table.end());

    const std::vector<static_keys::static_key_info> keys = static_keys::list_static_keys();
    const auto key = std::find_if(keys.begin(), keys.end(),
                                  [](const static_keys::static_key_info& k) {
                                      return k.name == "keyed tracepoint";
                                  });
    REQUIRE(key != keys.end());
    CHECK(key->file == std::string_view(entry->file));

    CHECK_FALSE(tracer::is_enabled(*entry));
    REQUIRE(tracer::set_tracepoint_enabled("keyed tracepoint", true) == 1);
    CHECK(tracer::is_enabled(*entry));
    REQUIRE(tracer::set_tracepoint_enabled("keyed tracepoint", false) == 1);
    CHECK_FALSE(tracer::is_enabled(*entry));

    // A name nothing was compiled under matches nothing, rather than silently
    // succeeding.
    CHECK(tracer::set_tracepoint_enabled("no such tracepoint", true) == 0);
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

// The end-to-end pipeline, asserted on its output.
//
// Everything upstream of this is a build step: :trace_producer emits both a
// trace and the source of a decoder for its own tracepoint table, :trace_decoder
// is that source compiled, and :decoded_trace is the decoder run on the trace.
// What lands here is the decoder's stdout.
//
// Timestamps are a counter rather than rdtsc (see trace_producer.cc), so this
// is stable byte for byte. A diff in it is a change in the wire format, the
// generated decoder, or the demo workload.
TEST_CASE("decoded trace") {
    const char* const path = std::getenv("TRACER_DECODED");
    REQUIRE_MESSAGE(path != nullptr, "TRACER_DECODED is not set");

    check_snapshot(read_file(path), R"snap(
        |               100 | modules/tracer/trace_producer.cc:47 | info  | listening on port 8080
        |               800 | modules/tracer/trace_producer.cc:61 | warn  | cache miss for key 73657373696f6e at slot 0xdeadbeef
        |               900 | modules/tracer/trace_producer.cc:64 | error | clock skew -4200 ns, retries 3
        |              1000 | modules/tracer/trace_producer.cc:66 | info  | shutting down
        |               200 | modules/tracer/trace_producer.cc:51 | debug | accepted connection 0 (keepalive=true)
        |               300 | modules/tracer/trace_producer.cc:53 | trace | request header 474554202f
        |               400 | modules/tracer/trace_producer.cc:51 | debug | accepted connection 1 (keepalive=false)
        |               500 | modules/tracer/trace_producer.cc:53 | trace | request header 474554202f696e6465782e68746d6c
        |               600 | modules/tracer/trace_producer.cc:51 | debug | accepted connection 2 (keepalive=true)
        |               700 | modules/tracer/trace_producer.cc:53 | trace | request header 474554202f
        )snap"_snap);
}
