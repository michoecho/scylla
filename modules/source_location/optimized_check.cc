// The location table, exercised at the optimisation level it needs.
//
// A location is the same word at every optimisation level -- see "why not an
// index" in the header -- so everything the module's own tests assert holds at
// the -O0 this repository builds with. The one thing that does not exist there
// is the `source_locations` section: it is assembled by inline asm, which needs
// operands the optimiser has folded.
//
// So this program pins -O1 by its build rule and asserts what only the table
// offers: every location the object mentions, including the call sites this run
// never reached.
//
// It is run by a genrule, so a failure here fails the build rather than waiting
// for someone to read the output.

#include "source_location/source_location.h"

#include <cstdio>
#include <cstdlib>
#include <string_view>

namespace {

int failures = 0;

void check(bool ok, std::string_view what) {
    if (!ok) {
        std::fprintf(stderr, "FAILED: %.*s\n", static_cast<int>(what.size()), what.data());
        ++failures;
    }
}

srcloc::location captured = srcloc::location::none();
void traced(int /*x*/, srcloc::location loc = {}) { captured = loc; }

bool table_mentions(std::string_view function_needle) {
    for (const srcloc::location loc : srcloc::locations()) {
        if (loc.function().find(function_needle) != std::string_view::npos) {
            return true;
        }
    }
    return false;
}

}  // namespace

// Never called. Its location must be in the table all the same: that is the
// whole difference between listing the call sites of an object and collecting
// the ones a run happened to reach. External linkage so that the function is
// emitted despite having no caller -- a discarded function takes its asm, and so
// its row, with it.
void never_runs() { traced(3); }

int main() {
    check(SRCLOC_LOCATION_TABLE == 1, "this program must be built with the table");

    const auto line = static_cast<std::uint32_t>(__LINE__);
    traced(0);
    check(captured.line() == line + 1, "a default argument names the caller's line");
    check(captured.file() == std::string_view(__FILE__), "and its file");
    check(srcloc::matches_std_source_location(), "and it agrees with std::source_location");

    // The point of the whole exercise.
    check(table_mentions("never_runs"), "an unreached call site is still in the table");
    check(table_mentions("main"), "and so is a reached one");

    // Every row is a location that can be read, which is the invariant that
    // makes the table worth having: rows are laid down by the assembler, so
    // there is no such thing as a blank one waiting to be filled in.
    std::size_t total = 0;
    for (const srcloc::location loc : srcloc::locations()) {
        ++total;
        check(loc.has_value() && !loc.file().empty() && loc.line() > 0,
              "every row of the table names a place");
    }
    check(total >= 3, "the table holds the call sites of this program");

    if (failures != 0) {
        std::fprintf(stderr, "%d check(s) failed\n", failures);
        return EXIT_FAILURE;
    }
    std::printf("location table: %zu rows, all readable\n", total);
    return EXIT_SUCCESS;
}
