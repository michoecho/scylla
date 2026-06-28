#pragma once

#include <exception>
#include <stacktrace>

const std::stacktrace& stacktrace_of_exception(const std::exception_ptr& eptr);
