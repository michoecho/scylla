#include "utils/clock.h"

namespace utils {
namespace {

using clock = std::chrono::steady_clock;
clock::time_point process_start;

#if defined(__GNUC__) || defined(__clang__)
__attribute__((constructor)) void initialize_process_start() noexcept {
    process_start = clock::now();
}
#else
#error "utils::clock requires a compiler with constructor attributes"
#endif

}  // namespace

std::chrono::nanoseconds time_since_process_start() noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        clock::now() - process_start);
}

}  // namespace utils
