#include "utils/barrier.h"

#include <chrono>
#include <cstddef>
#include <mutex>
#include <optional>

namespace utils {

bool barrier::start_barrier_erased(
    std::size_t n,
    deadline deadline,
    void (*completion)(void*),
    void* state) {
    std::unique_lock lock(mutex_);

    wanted_ = n;
    arrived_ = 0;
    parked_ = 0;
    released_ = false;
    completed_ = false;

    const auto full = [this] { return arrived_ == wanted_; };
    bool filled = true;
    if (n == 0) {
        // Nothing to wait for; the phase is born full.
    } else if (deadline.has_value()) {
        // wait_until re-evaluates the predicate after the deadline passes, so
        // an arrival that raced the timeout still counts and its returned
        // value is the truth about arrived_ -- there is no window where the
        // phase filled but we report a timeout.
        filled = owner_.wait_until(lock, deadline.value(), full);
    } else {
        // No deadline: the untimed wait, which returns only once the
        // predicate holds.
        owner_.wait(lock, full);
    }

    if (filled) {
        completed_ = true;
        // Run the completion without the lock: it is arbitrary user code, and
        // it may well call back into whatever the participants are parked for.
        // Nobody can join in the meantime -- arrived_ == wanted_ turns every
        // try_barrier away -- and nobody can leave, because release happens
        // below.
        lock.unlock();
        try {
            completion(state);
        } catch (...) {
            // A throwing completion has to unwind the phase exactly as a
            // returning one does, drain included. Releasing but not draining
            // would let this call leave -- by exception -- with participants
            // still inside try_barrier; the owner's next start_barrier would
            // then reset parked_ under them, and their decrement would wrap
            // it, so no later phase could ever finish draining.
            lock.lock();
            finish_phase(lock);
            throw;
        }
        lock.lock();
    }

    finish_phase(lock);
    return completed_;
}

void barrier::finish_phase(std::unique_lock<std::mutex>& lock) {
    wanted_ = 0;
    released_ = true;
    participants_.notify_all();

    // Hold the owner until the phase is fully unwound, so that leaving
    // start_barrier -- by return or by exception -- means no thread is still
    // touching this barrier's state on its behalf. A parked participant only
    // needs the mutex to get here, so this cannot outlast a wakeup.
    owner_.wait(lock, [this] { return parked_ == 0; });
}

barrier::arrival barrier::try_barrier() {
    std::unique_lock lock(mutex_);

    // No phase open (wanted_ == 0), or the open one has its full count.
    if (wanted_ == 0 || arrived_ == wanted_) {
        return arrival::not_joined;
    }

    ++arrived_;
    ++parked_;
    if (arrived_ == wanted_) {
        owner_.notify_one();
    }

    participants_.wait(lock, [this] { return released_; });

    // Read the outcome before dropping the lock: the owner cannot open the
    // next phase, which would overwrite completed_, until parked_ hits zero.
    const arrival result = completed_ ? arrival::completed : arrival::timed_out;
    if (--parked_ == 0) {
        owner_.notify_one();
    }
    return result;
}

}  // namespace utils
