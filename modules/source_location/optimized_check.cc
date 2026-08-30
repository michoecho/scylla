// The compile-time capture path, exercised at the optimisation level it needs.
//
// The module's own tests are built the way this repository builds everything,
// which is -O0, and there the fallback is what runs. This program is compiled
// with -O2 by its build rule so that the asm path is the one under test, and it
// asserts the properties that only that path has: entries that are finished data
// in the object file, filled whether or not their call site ever runs.
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

srcloc::location captured;
void traced(int /*x*/, srcloc::location loc = {}) { captured = loc; }

inline srcloc::location from_inline_function() {
    traced(1);
    return captured;
}

template <typename T>
srcloc::location from_template() {
    traced(2);
    return captured;
}

const srcloc::entry* find_function(std::string_view needle) {
    for (const srcloc::entry* e : srcloc::locations()) {
        if (e != nullptr && e->function != nullptr &&
            std::string_view(e->function).find(needle) != std::string_view::npos) {
            return e;
        }
    }
    return nullptr;
}

}  // namespace

// Never called. Its entry must exist and be complete all the same: that is the
// whole difference between assembling the table and filling it. External
// linkage so that the function is emitted despite having no caller -- a
// discarded function takes its asm, and so its entry, with it.
void never_runs() { traced(3); }

int main() {
    check(SRCLOC_COMPILE_TIME_CAPTURE == 1, "this program must be built with the asm path");

    const auto line = static_cast<std::uint32_t>(__LINE__);
    traced(0);
    check(captured.line() == line + 1, "a default argument names the caller's line");
    check(captured.column() > 0, "and its column");
    check(captured.file() == std::string_view(__FILE__), "and its file");
    check(captured.function().find("main") != std::string_view::npos,
          "and the caller's function, not traced()");

    const srcloc::location inlined = from_inline_function();
    check(inlined.function().find("from_inline_function") != std::string_view::npos,
          "capture works in an inline function");

    const srcloc::location as_int = from_template<int>();
    const srcloc::location as_double = from_template<double>();
    check(as_int != as_double, "one call site in a template is one entry per instantiation");
    check(as_int.function().find("int") != std::string_view::npos, "named by the instantiation");
    check(as_double.function().find("double") != std::string_view::npos, "and the other one");

    // The point of the whole exercise: never_runs() has never run, and its entry
    // is nonetheless there and complete.
    const srcloc::entry* unreached = find_function("never_runs");
    check(unreached != nullptr, "an unreached call site still has a complete entry");
    if (unreached != nullptr) {
        check(unreached->file != nullptr && unreached->line > 0,
              "filled without ever having been executed");
    }

    // Every entry in the table is finished data, not a blank waiting to be
    // filled -- there is no run-time filling in this build at all.
    std::size_t total = 0;
    for (const srcloc::entry* e : srcloc::locations()) {
        ++total;
        check(e->file != nullptr && e->function != nullptr && e->line > 0,
              "every entry in the section is complete");
    }
    check(total >= 6, "the table holds every call site in this program");

    if (failures != 0) {
        std::fprintf(stderr, "%d check(s) failed\n", failures);
        return EXIT_FAILURE;
    }
    std::printf("compile-time capture: %zu entries, all complete\n", total);
    return EXIT_SUCCESS;
}
