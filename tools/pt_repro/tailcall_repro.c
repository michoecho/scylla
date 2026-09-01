// Scenario: a sibling (tail) call.
//
// At -O2 `return tail_callee(x);` compiles to a plain `jmp`, so the callee
// reuses the caller's stack frame and its `ret` goes straight back to the
// caller's caller. perf reports the jump as an unconditional branch, not a
// call, and the later `ret` has no matching call on our shadow stack.
//
// We want to treat the tail call as "the caller returned, the callee
// started": tail_caller closes at the jump and tail_callee opens as its
// sibling, so the single `ret` closes tail_callee and lands back in
// repro_root.
//
// Expected trace shape:
//   repro_root > tail_caller   (closes at the jmp)
//   repro_root > tail_callee   (closes at the ret)
// Both at the same depth, and repro_root closes normally afterwards.

#include "pt_ctl.h"

static volatile int sink;

__attribute__((noinline)) static int tail_callee(int x) {
  for (int i = 0; i < 64; ++i) {
    sink += x;
  }
  return sink;
}

__attribute__((noinline)) static int tail_caller(int x) {
  // The compiler turns this into `jmp tail_callee` at -O2.
  return tail_callee(x + 1);
}

__attribute__((noinline)) static void repro_root(void) {
  sink = tail_caller(sink);
  sink += 1;
}

int main(void) {
  pt_enable();
  repro_root();
  pt_disable();
  return 0;
}
