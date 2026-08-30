#include "source_location/source_location.h"

#include <cstdlib>
#include <fstream>
#include <source_location>
#include <sstream>
#include <string>
#include <string_view>

#include <doctest/doctest.h>

namespace {

// The shape this module exists for: a function that learns where it was called
// from, without its callers saying anything.
srcloc::location caller_of_traced = srcloc::location::none();
void traced(int /*x*/, srcloc::location loc = {}) { caller_of_traced = loc; }

// The two vague-linkage contexts, which an earlier design had to work hard for
// and this one does not: the location is the compiler's own constant, so an
// inline function and a template capture it exactly as anything else does.
inline srcloc::location from_inline_function() {
    traced(1);
    return caller_of_traced;
}

template <typename T>
srcloc::location from_template() {
    traced(2);
    return caller_of_traced;
}

}  // namespace

TEST_CASE("srcloc::location is one pointer") {
    static_assert(sizeof(srcloc::location) == sizeof(void*));
    const srcloc::location here;
    CHECK(sizeof(here) == sizeof(void*));
    CHECK(here.address() == reinterpret_cast<std::uintptr_t>(here.get()));
}

// The assumption the module rests on: that entry is laid out like
// std::source_location::__impl. Nothing in the standard says so, so it is
// checked rather than assumed -- a standard library that disagreed would fail
// here rather than decode to nonsense.
TEST_CASE("an entry is a std::source_location, field for field") {
    CHECK(srcloc::matches_std_source_location());

    const auto line = static_cast<std::uint32_t>(__LINE__);
    const srcloc::location here;
    const std::source_location std_here = std::source_location::current();
    CHECK(here.line() == line + 1);
    CHECK(std_here.line() == line + 2);
    CHECK(here.file() == std_here.file_name());
    CHECK(here.function() == std_here.function_name());
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
    CHECK(loc.address() == 0);
    CHECK(loc.line() == 0);
    CHECK(loc.file() == "");
}

// The identity a trace records. Two call sites are two constants, one call site
// is one -- and the compiler folds identical locations nowhere, because no two
// call sites have the same line and column.
TEST_CASE("one call site is one address, two are two") {
    traced(0);
    const srcloc::location a = caller_of_traced;
    traced(0);
    const srcloc::location b = caller_of_traced;
    CHECK(a != b);
    CHECK(a.address() != b.address());

    srcloc::location repeated = srcloc::location::none();
    for (int i = 0; i < 3; ++i) {
        traced(0);
        if (i > 0) {
            CHECK(caller_of_traced == repeated);  // one call site, not one per pass
        }
        repeated = caller_of_traced;
    }
}

TEST_CASE("capture works in an inline function and in a template") {
    const srcloc::location inlined = from_inline_function();
    CHECK(inlined.function().find("from_inline_function") != std::string_view::npos);

    // One call site, two instantiations. The line is the same and the function
    // is not, so these are two constants: the compiler names the instantiation
    // in __PRETTY_FUNCTION__, and the location carries it.
    const srcloc::location as_int = from_template<int>();
    const srcloc::location as_double = from_template<double>();
    CHECK(as_int != as_double);
    CHECK(as_int.line() == as_double.line());
    CHECK(as_int.function().find("int") != std::string_view::npos);
    CHECK(as_double.function().find("double") != std::string_view::npos);
}

// A location that is already in hand, which is what a decoder ends up with once
// it has placed an address. Not a capture: it names what it is given.
TEST_CASE("an entry in hand can be wrapped without capturing anything") {
    traced(0);
    const srcloc::location captured = caller_of_traced;
    const srcloc::location wrapped = srcloc::location::at(captured.get());
    CHECK(wrapped == captured);
    CHECK(wrapped.line() == captured.line());
    CHECK(srcloc::location::at(nullptr) == srcloc::location::none());
}

// The section is the inline-asm mechanism, so it exists at -O1 and up and not
// here. That is not a gap in what this module offers -- every case above holds
// either way -- and it is asserted rather than skipped so that a build which
// somehow acquired a table would say so.
TEST_CASE("there is no location table in an unoptimised build") {
    CHECK(SRCLOC_LOCATION_TABLE == 0);
    CHECK(srcloc::locations().empty());
}

// :optimized_check is this module at -O1, asserting what only the table has.
// This reads its output so that the two are reported together.
TEST_CASE("the location table is built and checked at -O1") {
    const char* const path = std::getenv("SRCLOC_OPTIMIZED_CHECK");
    REQUIRE(path != nullptr);
    std::ifstream in(path);
    REQUIRE(in);
    std::stringstream text;
    text << in.rdbuf();
    const std::string output = text.str();
    CAPTURE(output);
    CHECK(output.find("all readable") != std::string::npos);
}
