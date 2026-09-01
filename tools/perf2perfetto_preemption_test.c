#define _GNU_SOURCE

#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>

static _Atomic int stop;
static volatile uint64_t main_counter;
static volatile uint64_t competing_counter;

static void pin_to_cpu_zero(void) {
  cpu_set_t cpus;
  CPU_ZERO(&cpus);
  CPU_SET(0, &cpus);
  if (sched_setaffinity(0, sizeof(cpus), &cpus) != 0) {
    perror("sched_setaffinity");
  }
}

static void *competing_thread(void *unused) {
  (void)unused;
  pin_to_cpu_zero();
  while (!atomic_load_explicit(&stop, memory_order_relaxed)) {
    ++competing_counter;
  }
  return NULL;
}

int main(void) {
  pthread_t competitor;
  pin_to_cpu_zero();
  if (pthread_create(&competitor, NULL, competing_thread, NULL) != 0) {
    return 1;
  }

  // Keep both threads runnable on one CPU long enough to force preemption.
  for (uint64_t i = 0; i < 2000000; ++i) {
    ++main_counter;
  }

  atomic_store_explicit(&stop, 1, memory_order_relaxed);
  pthread_join(competitor, NULL);
  fprintf(stderr, "main=%llu competing=%llu\n",
          (unsigned long long)main_counter,
          (unsigned long long)competing_counter);
  return 0;
}
