#include <sys/syscall.h>

__attribute__((noinline)) static long getpid_syscall(void) {
  register long syscall_number asm("rax") = SYS_getpid;
  asm volatile("syscall"
               : "+a"(syscall_number)
               :
               : "rcx", "r11", "memory");
  return syscall_number;
}

void _start(void) {
  volatile long pid = getpid_syscall();

  // Keep execution in user space long enough to inspect instructions after
  // the syscall before exiting.
  for (volatile long i = 0; i < pid * 1000; ++i) {
    asm volatile("" ::: "memory");
  }

  register long syscall_number asm("rax") = SYS_exit;
  register long exit_code asm("rdi") = 0;
  asm volatile("syscall"
               :
               : "a"(syscall_number), "D"(exit_code)
               : "rcx", "r11", "memory");
  __builtin_unreachable();
}
