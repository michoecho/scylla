# perf2perfetto reproducers

Four minimal C programs, one per thing that breaks a naive shadow stack when
Intel PT branch records are folded into a call tree. Each brackets its region
with the `pt-trace` control protocol (`pt_ctl.h`), so the trace covers exactly
the interesting part.

| Program | What it exercises |
|---|---|
| `syscall_repro.c` | a `syscall` in the middle of a call stack |
| `preempt_repro.c` | the scheduler preempting the traced thread mid-stack |
| `plt_repro.c` | a call through a PLT trampoline (`sigaction@plt`) |
| `tailcall_repro.c` | a sibling call compiled to `jmp` at `-O2` |

## Running them

```sh
tools/pt_repro/check.py              # all four
tools/pt_repro/check.py tailcall     # just one
```

`check.py` compiles each program, records it under `tools/pt-trace`, rebuilds
the call tree from the resulting `.ftf` and asserts the shape. `preempt` is
the slow one -- it spins long enough to be preempted a hundred-odd times, and
decoding that many branches takes a few minutes.

To look at a tree by hand:

```sh
tools/pt-trace run --ftf /tmp/x.ftf -- tools/pt_repro/plt_repro
tools/pt_repro/ftfdump.py /tmp/x.ftf
```

`ftfdump.py` reads the Fuchsia trace the dlfilter writes and prints the
nesting per thread, flagging frames that never closed. Nesting is only
meaningful within a thread, which is why it splits them.

## What the traces should look like

The property every scenario shares is **one `TRACE` root per thread**. That
frame is the current contiguous trace segment; a second one means the decoder
hit a gap it could not bridge and the call stack was reset. Dropped AUX data
does that legitimately, so if a scenario starts failing, check
`perf record`'s output for `AUX data lost` before suspecting the filter --
`tools/pt-trace --aux-pages` controls the buffer.

Beyond that:

- **syscall** — `syscall_leaf > do_getpid > [kernel]`. The syscall is reported
  as a call that ends the trace and has no matching return branch, so the
  filter opens a `[kernel]` frame for it and closes it when user space
  resumes. The frames around it are untouched.
- **preempt** — `preempt_leaf` present, and the single `TRACE` root. The
  competing thread is pinned to the same CPU to force the preemptions; the
  run above sees ~150.
- **plt** — `plt_call > {sigaction@plt, __sigaction, __libc_sigaction}` as
  siblings. A PLT stub jumps to its target and the target returns to the
  stub's caller, so the stub is a tail call and its frame ends at the jump.
- **tailcall** — `repro_root > {tail_caller, tail_callee}` as siblings, not
  nested. `tail_callee` inherited the frame, so the single `ret` closes it and
  lands back in `repro_root`.

The `pt_ctl_send` / `read` / `@plt` frames around the region of interest are
the control protocol itself and are expected.

## Recording the kernel too (`-e intel_pt/cyc=1/uk`)

Mostly better, with one real caveat.

Syscalls stop ending the trace, because the kernel side is now traced: the
`syscall` is an ordinary call into `entry_SYSCALL_64` and the `sysret` returns
from it, so you get the real kernel tree instead of the `[kernel]` stand-in.
`is_syscall_entry()` requires TRACE_END for exactly this reason and goes quiet
here. Tail calls and PLT stubs are unaffected.

Two things to know:

- **Decoding needs kernel text**, which is separate from permission to record.
  Without a readable `/proc/kcore` or vmlinux the decoder dies on the first
  kernel instruction ("Failed to get instruction") and the stack resets at
  every syscall -- worse than `/u`. `perf record --kcore` captures one, but
  reading `/proc/kcore` needs root. To decode with a kcore captured elsewhere,
  assemble a directory-format perf.data by hand:

  ```sh
  mkdir uk.data && cp recording uk.data/data && cp -r other/kcore_dir uk.data/
  perf script -i uk.data ...
  ```

  Recording `/uk` at all also needs `perf_event_paranoid < 1`.

- **Preemption resets the stack, and should.** `/uk` traces the kernel's own
  PT save/restore, so a context switch stops in `pt_event_stop`'s `wrmsr` and
  resumes in `pt_event_start`'s. Those are two bytes apart in
  `native_write_msr`, but they sit under completely different call paths, and
  the trace end is attributed to the outgoing thread while the resume goes to
  the incoming one. There is no stack to carry across, so `resumes_in_place()`
  requires an asynchronous resume to be at the *exact* interrupted byte, which
  this is not. Expect several segments per thread on a preemption-heavy `/uk`
  trace; `check_common`'s single-TRACE assertion is a `/u` property.

Interrupts (`int`/`iret`) are still unexercised by all four reproducers.
