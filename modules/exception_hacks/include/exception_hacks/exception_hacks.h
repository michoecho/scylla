#pragma once

#include <boost/stacktrace/stacktrace.hpp>
#include <exception>

// Stacktrace of the throw-expression that raised the exception `eptr` refers
// to. The exception need not be the one currently being handled, and may have
// been thrown on another thread; a rethrow with a bare `throw;` keeps the
// original trace. Returns an empty trace if `eptr` is null, if capture was off
// on the throwing thread (see
// boost::stacktrace::this_thread::set_capture_stacktraces_at_throw), or if the
// capturing library was not linked in (see modules/exception_hacks/BUCK).
boost::stacktrace::stacktrace stacktrace_of_exception(const std::exception_ptr& eptr);
