#pragma once

#include <chrono>

namespace utils {

// Monotonic elapsed time since the process-start clock was initialized.
std::chrono::nanoseconds time_since_process_start() noexcept;

}  // namespace utils
