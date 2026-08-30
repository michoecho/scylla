# Scylla latency tracing

Trace a Scylla node's task scheduling and disk I/O with binary tracepoints, cut
the result into per-request timelines, and ask where a slow CQL query spent its
time -- on the CPU, waiting for an I/O, or waiting to be scheduled.

Three parts, in two repositories:

| part | where |
|---|---|
| the tracepoint machinery | `modules/tracer` (this repo) |
| the instrumentation and the snapshot API | `third-party/scylladb`, and its `seastar` submodule |
| the viewer | `modules/trace-viewer` (this repo, here) |

The Scylla checkout is **not** a submodule of this repo -- it is an untracked
directory under `third-party/`, with its own history and its own Nix devshell.

## The idea

Every Seastar `task` carries a `task_id`, and `task_id`'s default constructor
reads the task that is currently running. So a member nobody initialises
explicitly records the task that was running when its owner was created -- and
that is the whole propagation mechanism. A continuation, an execution stage's
work item and an I/O descriptor all end up stamped with the id of the request
that caused them, with no plumbing at the sites in between.

A CQL frame mints a fresh id, so one request's records are recovered afterwards
by taking every record whose task is that id.

Two places break the chain and must hand it back explicitly, because the work
does not run in the requesting task at all:

- **The reader concurrency semaphore.** A read is queued as a permit and run by
  the semaphore's own `execution_loop`. `reader_permit::impl` carries a
  `task_id` captured where the permit is created -- in the requester's context
  -- and the loop runs `func()` under it. Without this hop *every read in the
  process* is attributed to whichever request happened to spin the loop up,
  which puts ~100% of the I/O on a single task id.
- **Execution stages.** `work_item` carries a `task_id` and `do_flush()` runs
  the item under it.

If you add instrumentation and find the I/O has collapsed onto a handful of task
ids, look for a third such place.

## The events

Declared in `seastar/include/seastar/core/scylla_tracer.hh`, defined in
`seastar/src/core/scylla_tracer.cc`. The viewer flattens them back into the
numeric event ids its analysis keys off.

| tracepoint | meaning | viewer id |
|---|---|---|
| `run_task{prev, task}` | the reactor picked a task off a run queue | `0` |
| `cql_request{prev, task}` | a CQL frame arrived and opened a new chain | `1` |
| `io_begin{task, io}` | a task submitted an I/O and is now waiting | `0x4` |
| `io_end{task, io}` | that I/O completed | `0x5` |
| `semaphore_execute{prev, task}` | the reader semaphore's loop ran a queued read | `0xa` |
| `execution_stage{prev, task}` | an execution stage ran a queued work item | `0xb` |

The viewer also has a formatter for `0x3`, a reader-semaphore admission
decision, which nothing currently emits.

Task and I/O ids carry the shard in their top bits (`shard << 48`). They are not
shard-local -- a request coordinated on one shard reaches a tablet on another,
and the continuations there inherit its id -- so one request's records appear in
*two* shards' trace files under a single id. The viewer relies on that: it does
not namespace ids by shard, because that would cut every cross-shard request in
half.

## How the two builds meet

Deliberately crudely. Scylla's CMake pulls the tracer in by **absolute path**:

- `seastar/CMakeLists.txt` sets `Scylla_TRACER_REPO` to
  `/home/michal/projects/cpp_template` and compiles `modules/tracer/tracer.cc`
  `modules/tracer/codegen.cc` and `modules/utils/barrier.cc` into
  `libseastar.so`, with `-w` because they are not written to Seastar's
  `-Wall -Werror`. The barrier is what the rendezvous below gathers the shards
  with; its doctest cases live in a separate `barrier_test.cc` precisely so
  that this build never sees them.
- Scylla's top-level `CMakeLists.txt` adds the include directories.

**If you check this repo out elsewhere, edit `Scylla_TRACER_REPO`.** There is no
detection and no fallback.

Everything lives in `libseastar.so` and nowhere else: one tracepoint table, one
static-key jump table, one registry. That is why every `TRACEPOINT()` call site
is in `scylla_tracer.cc` behind an out-of-line hook rather than inlined where it
is wanted -- a tracepoint's static key needs a link-time-constant address, which
it does not have inside an inline or template function in a shared library. The
header comment on `scylla_tracer.hh` has the full reasoning.

## Building Scylla

Scylla has its own devshell; do not build it from this repo's.

```sh
cd third-party/scylladb
nix develop -c ninja -C build Dev/scylla
```

The build directory is already configured (Ninja Multi-Config, `Dev` only). In
`Dev` mode Seastar is a shared library, which is what lets the tracer live in it.

**Builds are slow, and touching a core Seastar header rebuilds nearly
everything.** `scylla_tracer.hh` is included by `task.hh`, which is included by
almost every translation unit in both projects -- so a change there is a ~10
minute full rebuild, while one confined to `scylla_tracer.cc` is a handful of
steps and a relink. It is often worth putting something in the `.cc` with a
dynamic initialiser rather than widening the header.

Do **not** build or run Scylla's tests.

## Generating a trace

Tracepoints start **disabled**, so a node that is never told otherwise records
nothing at all -- each call site is a five-byte nop. Switch them on before the
load, and off again when you are done:

```sh
cd third-party/scylladb
nix develop -c ./run-node.sh 1          # a single node on 127.11.11.1
curl -s -X POST 'http://127.11.11.1:10000/system/tracepoints_enabled?enabled=true'
nix develop -c ./load.py                # 400 inserts, a flush, 10000 selects
curl -s -X POST http://127.11.11.1:10000/system/trace_snapshot
```

Forgetting the enable is the failure to expect: the snapshot succeeds and the
`.trace` files are ~117 bytes each, which is the metadata prologue and no
records.

The endpoint returns the directory it wrote, under `<workdir>/traces/<stamp>/`:

```
decoder.h        generated from this binary's tracepoint table
shard-0.trace    one file per shard
shard-1.trace
```

`load.py` flushes the memtable through
`POST /storage_service/keyspace_flush/tr` and reads back with `BYPASS CACHE`.
Both matter: without them the selects are served from the memtable and the row
cache, and the trace has no request I/O in it at all.

The endpoint lives in `api/system.cc`, alongside the logger endpoints. Each
shard's copy of its rings is synchronous, so what lands on disk is the ring as
it was when the shard was asked; the writing afterwards is blocking I/O in a
seastar thread, which is fine for something done by hand.

### Why enabling is a rendezvous

Flipping a tracepoint's static key **rewrites the branch instruction at its
call site**, and a shard executing that instruction while it changes is
undefined. So the endpoint does not just call the setter:

- `seastar::set_tracepoints_enabled()` (`core/scylla_tracer_control.hh`, split
  out of `scylla_tracer.hh` because that one is included by `task.hh` and may
  not pull in `future.hh`) hands the work to
  `seastar::run_at_rendezvous()` in `core/rendezvous.hh`.
- Every shard runs a **rendezvous poller**, registered last in the reactor's
  poll loop -- the one place a reactor is known not to be inside anybody's
  code. While no request is in flight it is a relaxed atomic load and nothing
  else.
- Shard 0 owns a `utils::barrier` (`modules/utils`, compiled into
  `libseastar.so` alongside the tracer) and opens a phase for the other
  `smp::count - 1` shards. They park in it; the completion -- the patching --
  runs on shard 0 with all of them held.
- A phase has a 10 ms deadline, because a shard busy with a long task does not
  reach its poll loop. Shard 0 retries up to 5 times, 200 ms apart, and then
  gives up; the endpoint returns `false` and **nothing** was switched. It is
  all-or-nothing, never half-patched.
- Requests are queued on a semaphore, so only one is ever in flight.

The flag also keeps the shards awake: an idle reactor sleeps in the kernel and
stops polling, so the poller refuses `try_enter_interrupt_mode()` while a
request is up, and the request pokes every shard once on the way in.

### What it costs

Measured rather than guessed, with a stamp at each stage logged once per
request -- instrumentation since removed, and worth putting back if this ever
looks slow. On the two-shard prototype node, idle or under `load.py`, every
switch took **one attempt** and 40-230 us end to end inside the process:

| stage | us from the call |
|---|---|
| admitted by the semaphore | ~1 |
| shard 0's poller first sees the request | ~10 |
| the phase gathered the other shard | 15-140 |
| the patching returned | +20 |

The gather is the variable part -- it is however far shard 1 was from the end
of its poll loop -- and the retries have never yet been needed. The patching
itself is ~20 us for the whole tracepoint table; note that switching a key to
the state it is already in does nothing, so a repeated `enabled=true` measures
the rendezvous alone.

A `time curl` on this endpoint reads ~5 ms, which is almost entirely curl
starting up: `curl -w %{time_total}` puts the request itself at 0.2-0.5 ms,
and the process-side log line at a few tens of microseconds.

## Viewing it

```sh
buck2 run //modules/trace-viewer:trace_viewer -- \
    third-party/scylladb/ignored/workdir_01/traces/<stamp>/
```

It takes the snapshot **directory**, not a file, and decodes every `*.trace` in
it. This opens a window; there is no headless mode.

## Regenerating `decoder.h`

`decoder.h` here is a **generated file, copied in**. It is emitted by
`tracer::generate_decoder_source()` from the tracepoint table of the running
Scylla binary, which is the only thing that can describe it -- and this repo
cannot build Scylla, so it cannot be a build step.

Whenever you add, remove or change a tracepoint, take the fresh copy from a
snapshot:

```sh
cp third-party/scylladb/ignored/workdir_01/traces/<stamp>/decoder.h \
   modules/trace-viewer/decoder.h
```

A trace is decoded against the object it came from by build ID, so a mismatched
decoder does not silently misdecode -- it refuses. See "the metadata stream" in
`modules/tracer/include/tracer/tracer.h`.

The `decoder.h` and the `.trace` files it reads are **one pair**. The metadata
stream is itself made of tracepoints, so its shape is part of what a decoder is
generated from: a load event now carries the object's base address and extent
beside its tracepoint table's, which is what lets a source location be read back.
An old decoder reads old traces and a new one reads new traces; neither reads the
other's, and the copy checked in here is the one that matches `smoke.trace`.
Regenerate both together after rebuilding Scylla against a newer `modules/tracer`.

### Source locations

A tracepoint parameter may be a `srcloc::location`, which records where its
*caller* was -- see `modules/source_location`. It is one address, so unlike every
other field it cannot be read out of the trace alone: the decoder needs the
object files themselves, found by build ID under

```
$TRACE_DSO_DIR/.build-id/<first two hex digits>/<the rest>.debug
```

which is the layout `gdb --debug-file-directory` and `llvm-cov` already use, and
which `tracer::write_dso_directory()` writes. Without it the rest of the trace
still decodes and each location comes out as `<unresolved 0x...>` rather than as
a guess.

## Debugging a trace without the GUI

When something looks wrong, decode headlessly and count. A ~40 line program
including `decoder.h`, with one `operator()` per tracepoint, will tell you how
many distinct tasks own an `io_begin` -- if that number is small, the chain is
broken somewhere. Also worth counting: how many `cql_request` records there are,
and whether the ids in one shard's file were minted by the other shard's
requests.

## Extra guidance about prototyping

Do not try to integrate Scylla's build with ours in any way, that would be very painful.
Instead, just directly add our source files (via absolute paths, I guess) to Scylla's cmakefiles. If there's some problem with that, you can modify either side to fix that. (E.g. if we have a test in a source file, which Scylla can't deal with without dragging in our whole test framework, you can just extract the test to a separate file).
In general, you can freely modify the relevant parts of this repo if that moves us closer to our goal. This project is a playground, we don't have to keep any compatibility with anything.
We are prototyping prototype. Ignore all Scylla tests. Never run them, and don't even try to build them. It's fine if they break, we don't care at all for now. Don't overthink this, we value iteration speed more than careful design for now.