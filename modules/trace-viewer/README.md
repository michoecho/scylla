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
  and `modules/tracer/codegen.cc` into `libseastar.so`, with `-w` because they
  are not written to Seastar's `-Wall -Werror`.
- Scylla's top-level `CMakeLists.txt` adds the two include directories.

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

```sh
cd third-party/scylladb
nix develop -c ./run-node.sh 1          # a single node on 127.11.11.1
nix develop -c ./load.py                # 400 inserts, a flush, 10000 selects
curl -s -X POST http://127.11.11.1:10000/system/trace_snapshot
```

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