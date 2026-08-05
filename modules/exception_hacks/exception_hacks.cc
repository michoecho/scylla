#include "exception_hacks/exception_hacks.h"

#include "doctest/doctest.h"
#include <boost/stacktrace/this_thread.hpp>
#include <algorithm>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>

// libboost_stacktrace_from_exception interposes __cxa_allocate_exception,
// dumps a trace into the tail of the allocation it makes for the exception
// object, and parks a pointer to that dump in the spare `reserve` field of the
// ABI header sitting just below the object.
//
// from_exception() is not upstream Boost: upstream exposes only the trace of
// the exception currently being handled, and this project applies
// nix/patches/boost-stacktrace-from-exception-ptr.patch to reach the same
// lookup from any exception_ptr.
//
// The path to that library goes through weak symbols, so a build that fails to
// link it (or an --as-needed link that drops it) still compiles and still
// runs -- it just returns empty traces forever. The test below asserts capture
// is actually live, so that regression is loud.
boost::stacktrace::stacktrace stacktrace_of_exception(const std::exception_ptr& eptr) {
    return boost::stacktrace::stacktrace::from_exception(eptr);
}

// Optimization is disabled for this function so the stacktrace captured at
// throw time carries a distinct "throwing_frame" frame; the sanity test below
// looks for it. Under -O the throw is in tail position and this frame is
// elided. clang and gcc spell per-function "no optimization" differently.
#if defined(__clang__)
[[clang::optnone]]
#elif defined(__GNUC__)
[[gnu::optimize("O0")]]
#endif
void throwing_frame() {
    throw std::runtime_error("Error");
}

TEST_SUITE("Exception stacktrace") {
    static bool has_frame(const boost::stacktrace::stacktrace& st, std::string_view name) {
        return std::ranges::any_of(st, [name](const boost::stacktrace::frame& frame) {
            return frame.name().contains(name);
        });
    }

    TEST_CASE("Exception stacktrace sanity") {
        // False means libboost_stacktrace_from_exception never made it into
        // the link, and the traces below would be empty for that reason alone.
        REQUIRE(boost::stacktrace::this_thread::get_capture_stacktraces_at_throw());

        try {
            throwing_frame();
        } catch (...) {
            auto st = stacktrace_of_exception(std::current_exception());
            CHECK(st.size() > 1);
            CHECK(has_frame(st, "throwing_frame"));
            CHECK(has_frame(st, "main"));
        }
    }

    // The point of taking an exception_ptr rather than reading the exception
    // being handled: the trace outlives the catch block that captured it, and
    // is reachable from a thread that never saw the throw.
    TEST_CASE("Exception stacktrace of a stored exception") {
        std::exception_ptr eptr;
        std::thread([&eptr] {
            try {
                throwing_frame();
            } catch (...) {
                eptr = std::current_exception();
            }
        }).join();

        auto st = stacktrace_of_exception(eptr);
        CHECK(st.size() > 1);
        CHECK(has_frame(st, "throwing_frame"));
    }
}
