#pragma once

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <optional>
#include <type_traits>
#include <utility>

namespace utils {

// An absolute deadline for a barrier phase, or an explicitly untimed phase.
// There is deliberately no default constructor: callers must choose one of
// those two meanings.
class deadline final {
public:
    using time_point = std::chrono::steady_clock::time_point;

    // Intentionally implicit so `steady_clock::now() + duration` can be passed
    // directly to start_barrier().
    constexpr deadline(time_point time) noexcept : time_(time) {}

    static constexpr deadline none() noexcept {
        return deadline(std::nullopt);
    }

    constexpr bool has_value() const noexcept { return time_.has_value(); }
    constexpr time_point value() const noexcept { return *time_; }

private:
    constexpr deadline(std::nullopt_t) noexcept : time_(std::nullopt) {}

    std::optional<time_point> time_;
};

// A rendezvous between one owner thread and `n` participants, with a deadline,
// whose completion function runs on the owner.
//
// std::barrier has neither property: it has no timeout, and its completion
// runs on whichever participant happens to arrive last. Both matter when the
// completion has to touch state only the owner may touch -- rewriting code,
// flipping a static key, publishing a buffer -- and when a participant that
// never arrives must not wedge the ones that did.
//
// The owner opens a phase with start_barrier(n, timeout, completion) and
// blocks. Participants call try_barrier(): a call that lands inside an open
// phase with room left joins it and blocks; any other call returns
// arrival::not_joined immediately, so a participant polling at a safe point
// never blocks waiting for a phase to appear.
//
// When the nth participant arrives, the owner runs `completion` while all n
// stay parked, then releases them; each of their try_barrier() calls returns
// arrival::completed. If the deadline passes first, `completion` does not run
// and whoever had arrived is released with arrival::timed_out.
//
// start_barrier returns only once every participant it parked has left the
// barrier, so the owner may destroy or reuse it as soon as it returns.
//
// Only one thread may be inside start_barrier at a time; try_barrier is free
// for any number of threads, including concurrently with start_barrier.
class barrier {
public:
    // Why a participant's try_barrier() call returned.
    enum class arrival {
        // Joined a phase that reached its count; `completion` has run.
        completed,
        // Joined a phase that hit its deadline; `completion` did not run.
        timed_out,
        // Did not join: no phase was open, or the open one was already full.
        not_joined,
    };

    barrier() noexcept = default;
    barrier(const barrier&) = delete;
    barrier& operator=(const barrier&) = delete;

    // Owner side. Opens a phase for `n` participants and blocks until they all
    // arrive or `deadline` passes. Returns true if the phase completed, in
    // which case `completion` has been called on this thread with every
    // participant still parked. An exception from `completion` propagates
    // only after the phase has been unwound as usual -- participants released
    // and drained -- so the barrier is as reusable after a throw as after a
    // return.
    //
    // The deadline is absolute, and on the steady clock: a phase measures out
    // its own patience, not the wall clock's opinion of what time it is. Say
    // `steady_clock::now() + 50ms` for a relative wait. It is absolute rather
    // than a duration because a relative wait has to be turned back into a
    // deadline by adding it to now(), and for a long enough wait that addition
    // overflows -- the longest expressible wait wraps into a deadline already
    // past, so the phase would time out at once.
    //
    // deadline::none() is a phase with no deadline, which waits on arrivals
    // alone.
    // A time_point far enough out would read the same way, but only as far as
    // the clock arithmetic underneath holds up; the empty case takes the
    // untimed wait instead, so there is no deadline for anything to convert,
    // clamp, or overflow.
    template <class Completion>
    bool start_barrier(
        std::size_t n,
        deadline deadline,
        Completion&& completion) {
        // Type-erased so the wait loop can live in the translation unit rather
        // than in every caller.
        using stored = std::remove_reference_t<Completion>;
        auto invoke = [](void* state) {
            std::forward<Completion>(*static_cast<stored*>(state))();
        };
        return start_barrier_erased(
            n,
            deadline,
            invoke,
            const_cast<void*>(static_cast<const void*>(&completion)));
    }

    // Participant side. See arrival for what the result means.
    arrival try_barrier();

private:
    // Closes the phase to new arrivals, wakes everyone parked in it, and
    // waits for them all to leave. Called with `lock` holding mutex_, which it
    // still holds on return.
    void finish_phase(std::unique_lock<std::mutex>& lock);

    bool start_barrier_erased(
        std::size_t n,
        deadline deadline,
        void (*completion)(void*),
        void* state);

    std::mutex mutex_;
    // The owner waits here for arrivals, and again for the parked count to
    // drain. Only the owner ever waits on it.
    std::condition_variable owner_;
    // Participants wait here to be released.
    std::condition_variable participants_;

    // Participants wanted by the open phase; 0 when no phase is open.
    std::size_t wanted_ = 0;
    // Participants that joined the open phase.
    std::size_t arrived_ = 0;
    // Joined participants that have not yet returned from try_barrier.
    std::size_t parked_ = 0;
    bool released_ = true;
    bool completed_ = false;
};

}  // namespace utils
