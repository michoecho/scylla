#include "utils/barrier.h"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <thread>
#include <vector>

#include <doctest/doctest.h>

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

namespace {

// No deadline at all, so nothing below depends on how long anything takes.
// Every phase that uses it is one the participants are guaranteed to fill, so
// the wait ends on arrivals and never on the clock -- not even for a build
// stopped at a breakpoint, which is what rules out merely-long deadlines.
constexpr deadline kNoDeadline = deadline::none();

// A participant that spins on try_barrier until it lands inside a phase.
//
// Polling, rather than a timed wait, is both how try_barrier is meant to be
// used and what keeps these tests off the clock: the thread keeps trying for
// as long as the test lets it, so "did it arrive in time" is never a question
// the assertions have to guess at.
class participant {
public:
    explicit participant(barrier& target) : target_(&target) {
        thread_ = std::thread([this] {
            while (!stop_.load(std::memory_order_relaxed)) {
                const barrier::arrival arrival = target_->try_barrier();
                if (arrival != barrier::arrival::not_joined) {
                    result_ = arrival;
                    return;
                }
                std::this_thread::yield();
            }
        });
    }

    participant(const participant&) = delete;
    participant& operator=(const participant&) = delete;

    ~participant() { give_up(); }

    // Joins, for a phase this participant is bound to land in. Returns how it
    // was released.
    barrier::arrival wait() {
        thread_.join();
        return result_;
    }

    // Stops polling and joins, for a phase this participant may never have
    // landed in. Returns how it was released, or not_joined if it never got
    // in. Safe to call after wait().
    barrier::arrival give_up() {
        stop_.store(true, std::memory_order_relaxed);
        if (thread_.joinable()) {
            thread_.join();
        }
        return result_;
    }

    std::thread& thread() { return thread_; }

private:
    barrier* target_;
    std::atomic<bool> stop_{false};
    // Written by the thread before it exits, read after joining it.
    barrier::arrival result_ = barrier::arrival::not_joined;
    std::thread thread_;
};

}  // namespace

TEST_CASE("a filled barrier runs the completion on the owner") {
    barrier gate;
    std::vector<std::unique_ptr<participant>> participants;
    for (std::size_t i = 0; i < 3; ++i) {
        participants.push_back(std::make_unique<participant>(gate));
    }

    std::thread::id completion_thread;
    bool full_phase_turns_arrivals_away = false;
    const bool filled = gate.start_barrier(
        participants.size(), kNoDeadline, [&] {
            completion_thread = std::this_thread::get_id();
            // The phase has its count, so a late arrival -- this thread
            // standing in for one -- must bounce rather than park.
            full_phase_turns_arrivals_away =
                gate.try_barrier() == barrier::arrival::not_joined;
        });

    CHECK(filled);
    CHECK(completion_thread == std::this_thread::get_id());
    CHECK(full_phase_turns_arrivals_away);
    for (const auto& p : participants) {
        CHECK(p->wait() == barrier::arrival::completed);
    }
}

TEST_CASE("participants stay parked until the completion returns") {
    barrier gate;
    std::vector<std::unique_ptr<participant>> participants;
    for (std::size_t i = 0; i < 2; ++i) {
        participants.push_back(std::make_unique<participant>(gate));
    }

    // A watcher counts departures, so the completion can ask how many
    // participants have left -- which must be none.
    //
    // This is a safety property: no finite test proves it, it can only fail to
    // catch a violation, and a sleep here would buy probability rather than
    // proof. So there is no sleep. A release-too-early bug shows up when the
    // freed thread happens to be quick, and never shows up as a red build on a
    // correct implementation.
    std::atomic<int> departures{0};
    std::thread watcher([&] {
        for (const auto& p : participants) {
            p->thread().join();
            departures.fetch_add(1, std::memory_order_relaxed);
        }
    });

    int departures_during_completion = -1;
    const bool filled = gate.start_barrier(
        participants.size(), kNoDeadline, [&] {
            departures_during_completion =
                departures.load(std::memory_order_relaxed);
        });
    watcher.join();

    CHECK(filled);
    CHECK(departures_during_completion == 0);
}

TEST_CASE("a phase that misses its count times out without completing") {
    barrier gate;

    // Two wanted, one available: no schedule fills this phase, so every
    // start_barrier below leaves through the deadline.
    //
    // Whether the participant gets in before that deadline is a property of
    // the machine, not of the barrier, so the window is widened until it is
    // observed inside a phase rather than picked and hoped for. No assertion
    // depends on the window's value: a slow machine sends this loop around
    // again, it does not make the test fail.
    for (auto window = std::chrono::microseconds(1);; window *= 4) {
        participant lone(gate);
        bool completion_ran = false;
        const bool filled = gate.start_barrier(
            2, std::chrono::steady_clock::now() + window,
            [&] { completion_ran = true; });

        CHECK_FALSE(filled);
        CHECK_FALSE(completion_ran);

        // start_barrier has returned, so the phase is closed and drained: the
        // participant either joined and has already recorded how it was
        // released, or never joined and never will. No race left to lose.
        const barrier::arrival arrival = lone.give_up();
        if (arrival != barrier::arrival::not_joined) {
            CHECK(arrival == barrier::arrival::timed_out);
            break;
        }
    }
}

TEST_CASE("try_barrier does not block when no phase is open") {
    barrier gate;
    CHECK(gate.try_barrier() == barrier::arrival::not_joined);

    // A deadline already in the past, with nobody around to arrive: the phase
    // ends the moment it opens and the completion does not run. Nothing races
    // here, so this is exact rather than a small hopeful number.
    bool completion_ran = false;
    CHECK_FALSE(gate.start_barrier(1, std::chrono::steady_clock::now(),
                                   [&] { completion_ran = true; }));
    CHECK_FALSE(completion_ran);

    // Still true once a phase has come and gone.
    CHECK(gate.try_barrier() == barrier::arrival::not_joined);
}

TEST_CASE("a throwing completion unwinds the phase before propagating") {
    barrier gate;

    {
        participant lone(gate);
        CHECK_THROWS_AS(
            gate.start_barrier(1, kNoDeadline,
                               [] { throw std::runtime_error("boom"); }),
            std::runtime_error);
        // Released: the completion threw, but the phase still ended.
        CHECK(lone.wait() == barrier::arrival::completed);
    }

    // Drained too, so the next phase starts from a clean count. Had the throw
    // escaped with a participant still inside try_barrier, its decrement would
    // have wrapped the parked count this phase resets, and this phase would
    // hang rather than complete.
    participant lone(gate);
    bool completion_ran = false;
    CHECK(gate.start_barrier(1, kNoDeadline, [&] { completion_ran = true; }));
    CHECK(completion_ran);
    CHECK(lone.wait() == barrier::arrival::completed);
}

TEST_CASE("a barrier is reusable across phases") {
    barrier gate;
    int completions = 0;

    for (int phase = 0; phase < 4; ++phase) {
        participant lone(gate);
        const bool filled =
            gate.start_barrier(1, kNoDeadline, [&] { ++completions; });
        CHECK(filled);
        CHECK(lone.wait() == barrier::arrival::completed);
    }

    CHECK(completions == 4);
}

TEST_CASE("a barrier wanting nobody completes immediately") {
    barrier gate;
    bool completion_ran = false;
    CHECK(gate.start_barrier(0, kNoDeadline, [&] { completion_ran = true; }));
    CHECK(completion_ran);
}

}  // namespace utils
