// Scenario: a call through a PLT trampoline.
//
// A PLT entry is a stub that jumps to the real implementation. The jump
// crosses a symbol boundary and looks exactly like a tail call, but it is
// not one: the callee's `ret` returns to whoever called the stub. Treating
// the stub's jump as a tail call pops the caller's frame, and the eventual
// return then pops one frame too many -- which is why `sigaction@plt` looked
// like it never returned.
//
// Expected trace shape:
//   repro_root > plt_outer > <something for the plt call> > ...
// with every frame closing. The essential property is balance: plt_outer
// must close, and the frame count after the call must match the count
// before it.

#define _GNU_SOURCE

#include <signal.h>
#include <string.h>
#include "pt_ctl.h"

static volatile int sink;
static struct sigaction sa_old;

__attribute__((noinline)) static void plt_call(void) {
  // sigaction() is a genuine PLT-dispatched libc call and the one that
  // originally showed the bug.
  struct sigaction sa;
  memset(&sa, 0, sizeof(sa));
  sa.sa_handler = SIG_IGN;
  sigaction(SIGUSR1, &sa, &sa_old);
}

__attribute__((noinline)) static void plt_outer(void) {
  plt_call();
  sink += 1;
}

__attribute__((noinline)) static void repro_root(void) {
  plt_outer();
  sink += 1;
}

int main(void) {
  // Warm the PLT first so the traced call is a plain stub jump rather than a
  // detour through the dynamic linker's symbol resolver.
  struct sigaction sa;
  memset(&sa, 0, sizeof(sa));
  sa.sa_handler = SIG_IGN;
  sigaction(SIGUSR1, &sa, &sa_old);

  pt_enable();
  repro_root();
  pt_disable();
  return 0;
}
