#include "source_location/source_location.h"

#include <algorithm>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <doctest/doctest.h>

namespace {

// The shape this module exists for: a function that learns where it was called
// from, without its callers saying anything.
srcloc::location caller_of_traced;
void traced(int /*x*/, srcloc::location loc = {}) { caller_of_traced = loc; }

// The two vague-linkage contexts. An inline function's call site is one entry
// per translation unit; a template's is one per instantiation.
inline srcloc::location from_inline_function() {
    traced(1);
    return caller_of_traced;
}

template <typename T>
srcloc::location from_template() {
    traced(2);
    return caller_of_traced;
}

bool table_contains(srcloc::location loc) {
    const auto all = srcloc::locations();
    return std::ranges::find(all, loc.get()) != all.end();
}

// Never called, so its entry stays zeroed -- the price of filling entries at
// run time rather than at compile time.
srcloc::location never_runs() {
    traced(3);
    return caller_of_traced;
}

}  // namespace

TEST_CASE("srcloc::location is one pointer") {
    static_assert(sizeof(srcloc::location) == sizeof(void*));
    srcloc::location here;
    CHECK(sizeof(here) == sizeof(void*));
}

TEST_CASE("a default argument captures the caller, not the callee") {
    const auto line = static_cast<std::uint32_t>(__LINE__);
    traced(0);

    const srcloc::location loc = caller_of_traced;
    REQUIRE(loc.has_value());
    CHECK(loc.line() == line + 1);
    CHECK(loc.column() > 0);
    CHECK(loc.file() == std::string_view(__FILE__));
    // The *caller's* function, which inside a doctest case is the anonymous one
    // the TEST_CASE macro generated -- not traced().
    CHECK(loc.function().find("traced") == std::string_view::npos);
}

TEST_CASE("a bare location names the line it is written on") {
    const auto line = static_cast<std::uint32_t>(__LINE__);
    const srcloc::location here;
    CHECK(here.line() == line + 1);
}

TEST_CASE("an explicitly empty location holds nothing") {
    const srcloc::location loc = srcloc::location::none();
    CHECK_FALSE(loc.has_value());
    CHECK(loc.line() == 0);
    CHECK(loc.file() == "");
}

TEST_CASE("one call site is one entry, two are two") {
    traced(0);
    const srcloc::location a = caller_of_traced;
    traced(0);
    const srcloc::location b = caller_of_traced;
    CHECK(a != b);

    srcloc::location repeated;
    for (int i = 0; i < 3; ++i) {
        traced(0);
        if (i > 0) {
            CHECK(caller_of_traced == repeated);  // one call site, not one entry per pass
        }
        repeated = caller_of_traced;
    }
}

TEST_CASE("capture works in an inline function and in a template") {
    const srcloc::location inlined = from_inline_function();
    CHECK(inlined.function().find("from_inline_function") != std::string_view::npos);
    CHECK(table_contains(inlined));

    // One call site, two instantiations, two entries -- and each names the
    // instantiation it came from.
    const srcloc::location as_int = from_template<int>();
    const srcloc::location as_double = from_template<double>();
    CHECK(as_int != as_double);
    CHECK(as_int.line() == as_double.line());
    CHECK(as_int.function().find("int") != std::string_view::npos);
    CHECK(as_double.function().find("double") != std::string_view::npos);
}

#if !SRCLOC_COMPILE_TIME_CAPTURE
TEST_CASE("filling an entry is safe from several threads at once") {
    std::vector<std::jthread> threads;
    std::vector<srcloc::location> seen(4);
    for (int i = 0; i < 4; ++i) {
        threads.emplace_back([&seen, i] {
            traced(0);
            seen[static_cast<std::size_t>(i)] = caller_of_traced;
        });
    }
    threads.clear();  // join
    for (const srcloc::location loc : seen) {
        CHECK(loc == seen.front());  // all four raced for the same slot
        CHECK(loc.line() > 0);
        CHECK_FALSE(loc.file().empty());
    }
}

#endif

TEST_CASE("every reached call site is in the section table") {
    traced(0);
    const srcloc::location loc = caller_of_traced;
    CHECK(table_contains(loc));

    // The point of the section: the pointer is an index into a table, and the
    // index is what a decoder outside the process can act on.
    const srcloc::location_index at = srcloc::index_of(loc);
    REQUIRE(at.table != nullptr);
    CHECK(at.table->start + at.index == loc.get());
    CHECK(at.index < static_cast<std::size_t>(at.table->stop - at.table->start));
}

TEST_CASE("the table is a packed array whether or not its entries are filled") {
    const auto all = srcloc::locations();
    REQUIRE(all.size() > 1);

#if SRCLOC_COMPILE_TIME_CAPTURE
    // Assembled at compile time: nothing is blank, not even never_runs().
    CHECK(std::ranges::none_of(all, [](const srcloc::entry* e) { return e->file == nullptr; }));
#else
    // Filled at run time: a call site this run did not reach is still zeroed.
    const auto blank = std::ranges::count_if(
        all, [](const srcloc::entry* e) { return e->file == nullptr; });
    CHECK(blank >= 1);  // never_runs(), at least
#endif

    // A blank entry still occupies its index, so indexes do not shift between
    // runs that reach different code.
    for (const srcloc::location_table* table = srcloc::location_tables(); table != nullptr;
         table = table->next) {
        for (const srcloc::entry* e = table->start; e != table->stop; ++e) {
            CAPTURE(e - table->start);
            CHECK(((e->file == nullptr && e->line == 0) || (e->file != nullptr && e->line > 0)));
        }
    }
}

// The optimised path cannot be exercised from here: this test is compiled the
// way the repository compiles everything, which is -O0, so the header's
// run-time fallback is what the cases above are testing. //:optimized_check is
// the same module built at -O2, asserting the properties only the compile-time
// path has; this reads its output so that the two are reported together.
TEST_CASE("the compile-time capture path is built and checked at -O2") {
    const char* const path = std::getenv("SRCLOC_OPTIMIZED_CHECK");
    REQUIRE(path != nullptr);
    std::ifstream in(path);
    REQUIRE(in);
    std::stringstream text;
    text << in.rdbuf();
    const std::string output = text.str();
    CAPTURE(output);
    CHECK(output.find("all complete") != std::string::npos);
    // -O0 here, -O2 there: the two paths of the same header.
    CHECK(SRCLOC_COMPILE_TIME_CAPTURE == 0);
}
