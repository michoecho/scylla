#include "perf2perfetto/ftf.h"

#include <cstdint>
#include <cstring>
#include <sstream>
#include <string>
#include <vector>

#include <doctest/doctest.h>

namespace {

using perf2perfetto::ftf::Caches;
using perf2perfetto::ftf::LruTable;
using perf2perfetto::ftf::print_timespan;
using perf2perfetto::ftf::ThreadId;

std::vector<uint64_t> words(const std::string& bytes) {
    REQUIRE(bytes.size() % 8 == 0);
    std::vector<uint64_t> out(bytes.size() / 8);
    std::memcpy(out.data(), bytes.data(), bytes.size());
    return out;
}

std::string timespan(uint64_t start, uint64_t end) {
    char buf[48];
    return std::string(print_timespan(buf, start, end));
}

}  // namespace

TEST_CASE("ftf timestamps are printed with nine fractional digits") {
    CHECK(timespan(0, 0) == "0.000000000,0.000000000");
    CHECK(timespan(1, 2) == "0.000000001,0.000000002");
    // A whole second, and a value with digits on both sides of the point.
    CHECK(timespan(1'000'000'000, 1'234'567'890'123) == "1.000000000,1234.567890123");
    // The widest value the printer has to fit in its 48-byte buffer.
    CHECK(timespan(UINT64_MAX, UINT64_MAX) ==
          "18446744073.709551615,18446744073.709551615");
}

TEST_CASE("ftf header opens with the magic word") {
    std::ostringstream out;
    perf2perfetto::ftf::write_header(out);
    const std::vector<uint64_t> w = words(out.str());

    REQUIRE(!w.empty());
    CHECK(w[0] == 0x0016547846040010ULL);
    // The magic doubles as a one-word metadata record, so a reader walking
    // records from the start stays in step.
    CHECK((w[0] & 0xf) == 0);
    CHECK(((w[0] >> 4) & 0xfff) == 1);

    // The reserved strings are defined up front, "Instructions" first.
    CHECK(out.str().find("Instructions") != std::string::npos);
    CHECK(out.str().find("Timespan") != std::string::npos);
}

TEST_CASE("ftf string cache writes a record once and reuses its index") {
    std::ostringstream out;
    Caches caches;

    const uint64_t first = caches.strings.get_ref(out, "main");
    const size_t after_first = out.str().size();
    const uint64_t again = caches.strings.get_ref(out, "main");

    CHECK(first == again);
    // The second reference costs nothing.
    CHECK(out.str().size() == after_first);
    // Indices past the reserved internal strings.
    CHECK(first >= static_cast<uint64_t>(perf2perfetto::ftf::InternalString::Count));

    // The empty string is index 0 and is never given a record.
    const size_t before_empty = out.str().size();
    CHECK(caches.strings.get_ref(out, "") == 0);
    CHECK(out.str().size() == before_empty);

    // A different string gets a different index, and its own record.
    CHECK(caches.strings.get_ref(out, "other") != first);
    CHECK(out.str().size() > before_empty);
}

TEST_CASE("ftf thread cache writes a record once per thread") {
    std::ostringstream out;
    Caches caches;

    const uint64_t first = caches.threads.get_ref(out, ThreadId{7, 8});
    const size_t after_first = out.str().size();

    CHECK(caches.threads.get_ref(out, ThreadId{7, 8}) == first);
    CHECK(out.str().size() == after_first);
    // Same process, different thread: a track of its own.
    CHECK(caches.threads.get_ref(out, ThreadId{7, 9}) != first);
    // Index 0 is reserved, so a real thread never claims it.
    CHECK(first != 0);
}

TEST_CASE("ftf table recycles the least recently used slot when full") {
    LruTable<std::string, uint16_t> table(2);

    const uint16_t a = table.insert("a");
    const uint16_t b = table.insert("b");
    CHECK(a != b);

    // "a" is used again, so "b" becomes the least recently used entry and is
    // the one whose slot a third string takes over.
    CHECK(table.touch("a") == a);
    const uint16_t c = table.insert("c");
    CHECK(c == b);
    CHECK(table.touch("b") == std::nullopt);
    CHECK(table.touch("a") == a);
}
