// Scenario: the scheduler preempts the traced thread mid-stack.
//
// The kernel stops and restarts the PT trace around the context switch, so
// perf reports an ASYNC TRACE_END / TRACE_BEGIN pair. Unlike a syscall the
// gap can be arbitrarily long and the thread can come back somewhere quite
// different, but in the common case it resumes at the very next instruction
// and the stack is still intact.
//
// A competing spinner pinned to the same CPU makes preemption happen while
// we are deep inside preempt_leaf.
//
// Expected trace shape:
//   repro_root > preempt_outer > preempt_middle > preempt_leaf
// with all four frames closing, despite the preemptions inside the leaf.

#define _GNU_SOURCE

#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdint.h>
#include "pt_ctl.h"

static _Atomic int stop;
static volatile uint64_t sink;

static void pin_to_cpu_zero(void) {
  cpu_set_t cpus;
  CPU_ZERO(&cpus);
  CPU_SET(0, &cpus);
  (void)sched_setaffinity(0, sizeof(cpus), &cpus);
}

static void *competing_thread(void *unused) {
  (void)unused;
  pin_to_cpu_zero();
  while (!atomic_load_explicit(&stop, memory_order_relaxed)) {
  }
  return NULL;
}

__attribute__((noinline)) static void preempt_leaf(void) {
  // Long enough to span several scheduler ticks on a contended CPU.
  for (uint64_t i = 0; i < 3000000; ++i) {
    sink += i;
  }
}

__attribute__((noinline)) static void preempt_middle(void) { preempt_leaf(); }

__attribute__((noinline)) static void preempt_outer(void) { preempt_middle(); }

__attribute__((noinline)) static void repro_root(void) { preempt_outer(); }

int main(void) {
  pthread_t competitor;
  pin_to_cpu_zero();
  if (pthread_create(&competitor, NULL, competing_thread, NULL) != 0) {
    return 1;
  }

  pt_enable();
  repro_root();
  pt_disable();

  atomic_store_explicit(&stop, 1, memory_order_relaxed);
  pthread_join(competitor, NULL);
  return 0;
}
