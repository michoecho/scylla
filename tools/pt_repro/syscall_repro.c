// Scenario: a syscall in the middle of a call stack.
//
// Intel PT stops tracing across the kernel boundary, so perf reports a
// TRACE_END branch (flagged SYSCALL) followed by a TRACE_BEGIN branch that
// resumes at the instruction after `syscall`. Naively that pair looks like a
// decoding gap and costs us the whole stack.
//
// Expected trace shape:
//   repro_root > syscall_outer > syscall_middle > syscall_leaf
// with all four frames closing normally. The syscall must not truncate the
// stack: syscall_leaf resumes and returns to syscall_middle.

#include <sys/syscall.h>
#include "pt_ctl.h"

static volatile long sink;

__attribute__((noinline)) static long do_getpid(void) {
  register long nr asm("rax") = SYS_getpid;
  asm volatile("syscall" : "+a"(nr) : : "rcx", "r11", "memory");
  return nr;
}

__attribute__((noinline)) static long syscall_leaf(void) {
  long pid = do_getpid();
  // Some user-space work after the boundary, so the resumed segment is
  // clearly attributed to this frame rather than to whatever follows.
  for (int i = 0; i < 64; ++i) {
    sink += pid;
  }
  return pid;
}

__attribute__((noinline)) static long syscall_middle(void) { return syscall_leaf(); }

__attribute__((noinline)) static long syscall_outer(void) { return syscall_middle(); }

__attribute__((noinline)) static void repro_root(void) { sink = syscall_outer(); }

int main(void) {
  pt_enable();
  repro_root();
  pt_disable();
  return 0;
}
