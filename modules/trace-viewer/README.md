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
| symbolising a stack sample's addresses | `modules/address-decoder` (this repo) |

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

The ordinary hooks are declared in `tracing/tracer.hh` and defined in
`tracing/tracer.cc`. `run_task` is the hot-loop
exception: it is emitted directly in `seastar/src/core/reactor.cc` with static
wire id `1`. The viewer flattens them back into the numeric event ids its
analysis keys off.

| tracepoint | meaning | viewer id |
|---|---|---|
| `run_task{task, at}` | the reactor picked a task off a run queue, and where that task was created; static wire id `1` | `0` |
| `task_queue_run_begin{scheduling_group}` / `task_queue_run_end{}` | the reactor gave the cpu to one task queue and took it back; every `run_task` between the two ran under that scheduling group | |
| `cql_request{task}` | a CQL frame arrived and opened a new chain | `1` |
| `io_begin{task, io}` | a task submitted an I/O and is now waiting | `0x4` |
| `io_end{task, io}` | that I/O completed | `0x5` |
| `semaphore_execute{task}` | the reader semaphore's loop ran a queued read | `0xa` |
| `execution_stage{task}` | an execution stage ran a queued work item | `0xb` |
| `stacktrace_sample{shard, time_ns, frames}` | a shard was interrupted for a stack sample | `0xc` |
| `prepared_query_run{id}` | a prepared statement was executed | `0xd` |
| `prepared_statement_added{keyspace, statement, id}` | a prepared statement entered the shard cache | `0xe` |
| `prepared_statement_removed{keyspace, statement, id}` | a prepared statement left the shard cache | `0xf` |
| `prepared_statements_snapshot_{begin,end}` / `prepared_statement_snapshot_entry{keyspace, statement, id}` | the full prepared-statement cache at dump time | `0x10`–`0x12` |

`rpc_connection_open{connection, local, remote, peer_boot_msb, peer_boot_lsb,
peer_shard}` and `rpc_connection_close{connection, peer_boot_msb, peer_boot_lsb,
peer_shard}` carry the far end's identity from the handshake; see "The boot id"
below.

Prepared-query records are reconstructed during load. The viewer walks each
shard's info stream backwards from its snapshot, undoing additions and undoing
removals with the metadata carried by both deltas. It then attaches the
keyspace and statement text to every `prepared_query_run` record. The run and
cache records are also associated with the task that was running on that shard
at their timestamp, so they appear in the corresponding request log.

### Stack samples

Every shard opens a `perf_event_open()` software cpu-clock event on itself at
100 Hz with `PERF_SAMPLE_CALLCHAIN`, and the reactor drains the resulting mmap
ring from its poll loop -- beside the rendezvous poller, which is where a shard
is known to be between tasks. Declared in
`seastar/include/seastar/core/scylla_stacktrace_sampler.hh`, implemented in the
`.cc` beside it.

The kernel walks the user stack by **frame pointer**, which is why both
`CMakeLists.txt` and `seastar/CMakeLists.txt` set `-fno-omit-frame-pointer` at
the top -- before anything is defined, because `add_compile_options()` only
reaches targets created after it, and Seastar is configured as a separate
project in a multi-config build so it does not inherit Scylla's. There is no
`PERF_SAMPLE_STACK_USER` and no DWARF unwinding: a frame-pointer walk is a
handful of loads in the interrupt, and a stack copy is kilobytes a sample.

Sampling follows the tracepoint switch, so a node nobody asked to trace never
opens a perf event. It is *not* switched at the rendezvous -- enabling a perf
event is an `ioctl`, not a patch of live code -- but it is switched only if the
rendezvous succeeded, so a failed switch leaves nothing on.

`perf_event_open` can fail: `/proc/sys/kernel/perf_event_paranoid` above 2, or a
container without the capability. That is one `warn` line per shard and no
samples; everything else in the trace is unaffected, and the failure is not
retried.

Two things are worth knowing about the timestamps:

- A sample carries the **kernel's** time, and the event is opened with
  `clockid = CLOCK_REALTIME` precisely so that it lands in the domain the
  tracer's `clock_sync` records pair rdtsc ticks with. The viewer converts it
  back to ticks (`wall_clock::ticks_from_realtime()`) and the sample sorts among
  the records it interrupted.
- The record's *own* header timestamp is an rdtsc from when the poll loop
  drained it, which is up to a poll period later. Nothing uses it.

`frames` is a run of `uint64_t` return addresses, innermost first, with perf's
`PERF_CONTEXT_*` markers dropped. Like a source location it is an address and
nothing else, so reading it needs the objects -- the same `dsos/` directory, and
the same reasoning about whose job that is.

A `SWITCH` line also carries `sg N`, the scheduling group the reactor was
running under when it picked the task up.  A `run_task` record does not say
that -- it would be a field on the hottest record in the trace -- and the
`task_queue_run_{begin,end}` bracket around it does instead;
`pass_task_queue_runs` in the viewer is what turns the bracket into a column.
The bracket's *end* is load-bearing for a second reason: it is the reactor
saying it gave the cpu back, which is what bounds a green bar and what
`pass_cost` counts to. Before those records existed there was nothing to bound
a stretch but the next switch, and the idle time until a task's own
continuation was billed to it as cpu.  A switch whose bracket
the ring evicted has no `sg` at all rather than a guessed one.

`at` is `seastar::task::location()` -- the `then()` call site, or the `co_await`
a coroutine suspended at, which Seastar was already storing on every task as its
"resume point". It is one address rather than a string (see below), so the viewer
prints it next to each `SWITCH` line only when it has the objects to resolve it
against.

The viewer also has a formatter for `0x3`, a reader-semaphore admission
decision, which nothing currently emits.

Task ids are process-wide 32-bit sequence numbers. They are not shard-local --
a request coordinated on one shard reaches a tablet on another, and the
continuations there inherit its id -- so one request's records appear in *two*
shards' trace files under a single id. I/O ids still carry their shard in the
top bits (`shard << 48`) for pairing. The viewer relies on task ids being shared
across shards; namespacing them by shard would cut every cross-shard request in
half.

## How the two builds meet

Deliberately crudely. Scylla's CMake pulls the tracer in by **absolute path**:

- `seastar/CMakeLists.txt` sets `Scylla_TRACER_REPO` to
  `/home/michal/projects/cpp_template` and compiles `modules/tracer/tracer.cc`
  and `modules/utils/barrier.cc` into
  `libseastar.so` -- alongside `tracing/tracer.cc`,
  `src/core/rendezvous.cc` and `src/core/scylla_stacktrace_sampler.cc` -- with
  `-w` because they are not written to Seastar's
  `-Wall -Werror`. The barrier is what the rendezvous below gathers the shards
  with; its doctest cases live in a separate `barrier_test.cc` precisely so
  that this build never sees them.
- Scylla's top-level `CMakeLists.txt` adds the include directories.

**If you check this repo out elsewhere, edit `Scylla_TRACER_REPO`.** There is no
detection and no fallback.

Everything lives in `libseastar.so` and nowhere else: one tracepoint table, one
static-key jump table, one registry. Nearly every `TRACEPOINT()` call site is in
`tracer.cc` behind an out-of-line hook rather than inlined where it is
wanted. The hot `run_task` event is the direct-call exception; its static id also
keeps its record header short. This is a placement convention, not a language
constraint: a gated tracepoint inside an inline or template function in a shared
library compiles and works, which `modules/tracer/plugin/common_tracepoints.h`
exercises on purpose. The header comment on `tracer.hh` has the full
reasoning, and `key_ref` in `static_keys.h` has the mechanism.

## Building Scylla

Scylla has its own devshell; do not build it from this repo's.

```sh
cd third-party/scylladb
nix develop -c ninja -C build Dev/scylla
```

The build directory is already configured (Ninja Multi-Config, `Dev` only). In
`Dev` mode Seastar is a shared library, which is what lets the tracer live in it.

**Builds are slow, and touching a core Seastar header rebuilds nearly
everything.** `tracer.hh` is included by `task.hh`, which is included by
almost every translation unit in both projects -- so a change there is a ~10
minute full rebuild, while one confined to `tracer.cc` is a handful of
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
<uuid>.trace                    one file per shard *and level*
<uuid>.metadata.json            what that file is: build, process, shard, level, times
...
```

A file is named after a fresh time-based UUID and says nothing about itself in
its name. Everything a reader needs before it opens one is in the
`.metadata.json` beside it:

```json
{
  "trace": "a97c2ba0-a83b-11f1-b83c-2aa316de99ac.trace",
  "build_id": "f6837b4173bba7095abc7bce5a5a04589fe776df",
  "boot_id": "7160e7d6-a83b-11f1-bf69-9bcbd4501077",
  "shard": 0,
  "level": "info",
  "first_record_ns": 1788510868391888616,
  "last_record_ns": 1788510962519339954
}
```

- **`build_id`** is the executable's, which is the name to hand a build-ID server
  -- or to look up under `dsos/` -- for the objects the addresses inside point
  into.
- **`boot_id`** is the *process*: see "The boot id" below.
- **`level`** is `info` or `debug`. There is one file per level now, and each is
  self-contained -- it carries the metadata chunk saying where the objects were
  mapped, so the info file decodes without the debug one beside it. The debug
  ring is an order of magnitude the larger, and a snapshot split this way can
  have its expensive half deleted and stay readable.
- **`first_record_ns`/`last_record_ns`** bracket the *records*, not the snapshot.
  A ring evicts, so a busy shard's debug file may reach back a second while the
  info file beside it reaches back minutes. The times come from the buffers: each
  one notes the wall clock as it goes live and as it is retired, and the range is
  from the activation of the oldest buffer that survived to the moment the
  snapshot stopped the live one. The records themselves cannot answer this --
  their timestamps are rdtsc ticks, and converting those is the two-pass job in
  "reading a sync record back".

The old `shard-N.trace` naming carried the shard and nothing else, and carried
it badly: two nodes' snapshots could not be copied into one directory without
colliding. The viewer still reads a `shard-N.trace` with no metadata beside it,
falling back to the directory for the node and the name for the shard.

## The boot id

Every Seastar process picks a **boot id** during reactor construction: a
version-1 (time-based) UUID, one per process and shared by every shard. It is in
`tracing/tracer.hh`, and the reactor's constructor is
what makes the first call, so it is fixed before any shard can trace or open a
connection.

A build ID is not enough to tell processes apart -- every node of a cluster runs
the same package, and so does the same node after a restart -- and neither is an
address, which a restarted node takes back. What a reader of a distributed trace
actually has to know is which files came out of one address space (so that their
task ids, unique only within one, may be merged) and which process is at the far
end of a connection. That is the boot id's job. Being time-based, it also orders
the runs it names: two snapshot directories from one node sort by when the node
booted.

It goes in two places:

- **Every snapshot's metadata**, as above. The viewer numbers nodes by boot id
  rather than by which directory a file was handed over in, so two snapshots of
  one node merge into a single timeline instead of pretending to be two
  machines.
- **The RPC handshake.** `protocol_features::PEER_IDENTITY` carries the boot id
  and the shard both ways, so each end learns who the other is and puts it in its
  own `rpc_connection_open` / `rpc_connection_close` records. A peer that does
  not know the feature simply does not answer it and the records carry a zero
  identity.

Because the identity is only known once the handshake is done,
`rpc_connection_open` is emitted **after** negotiation rather than when the
socket is set. A connection that never negotiates therefore has no records at
all, which is the right answer -- it never carried a message either.

The `run_task` locations need the objects they point into, and **the node does
not write them** -- see "Source locations" below. Gather them beside the traces
afterwards:

```sh
tools/gather-dsos third-party/scylladb/build/Dev/scylla \
    third-party/scylladb/ignored/workdir_01/traces/<stamp>/dsos --strip-debug
```

from the binary the trace came from, before rebuilding it.

### Three nodes, in one command

The single-node recipe above is the one to reach for when changing a
tracepoint; a distributed trace -- which is what the RPC join and the
cross-node request walk need -- is `capture-trace.sh` beside `run-node.sh`,
and it is the whole of the recipe above for three nodes of two shards:

```sh
cd third-party/scylladb
nix develop -c ./capture-trace.sh ignored/my-run
```

It starts nodes 1-3 in order (node 1 is the seed), waits for each to answer
CQL, switches the tracepoints on, runs `load3.py` -- the CL=ALL variant of
`load.py`, so every request touches all three -- snapshots each node, gathers
the `dsos/`, and stops the nodes.  What comes out is the shape the viewer is
handed: `node1/ node2/ node3/ dsos/`.  `ignored/sched-group-run` was made this
way, as was `ignored/boot-id-run` before it; the two are from different builds,
and handing both to the viewer at once is the shortest demonstration of what
reading several builds' tables into one decoder is for.

The workdirs are kept between runs, because bootstrapping three nodes from
nothing is minutes; `--fresh` wipes them.

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

- `seastar::set_tracepoints_enabled()` (`tracing/tracer_control.hh`, split
  out of `tracer.hh` because that one is included by `task.hh` and may
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

## The viewer

`viewer.cc` is the viewer. It replaced one written around its control flow --
`main.cc`, deleted along with the generated decoder header it was the last
reader of -- with one written around its tables, reading what a record means out
of the objects a snapshot came from rather than out of a header generated beside
it.

```sh
TRACE_DSO_DIR=<run>/dsos buck2 run //modules/trace-viewer:viewer -- \
    <run>/node1 <run>/node2 <run>/node3
```

The old viewer is `:trace_viewer` and still builds. It keeps the stack sample
window, which the new one does not have yet.

**Its design principles are written down in `DESIGN.md` beside it**, which is
the thing to read before changing it -- what follows here is the summary.
`WORKING.md` is the third of the three: how the work is actually done --
building Scylla, capturing a snapshot, profiling the viewer's startup, and the
traps that have already cost somebody a day.

### What it is made of

The whole design is that there is a catalogue of arrays and a sequence of
passes over them, and that the arrays are the part worth thinking about. A
pass says which tables it reads and which it writes, in the comment above it;
that comment is the dependency graph and there is no other. No pass reaches
outside the tables it was handed, there are no globals, and nothing is
encapsulated -- every table is a `std::vector` of a POD row and every reference
between tables is an index into another one.

The event tables are per **(node, shard, event type)**, which is the shape the
questions have: "what did this reactor do" is a scan of one array, and "what
did task T do here" is a lookup in an index built from one. Beside them, one
`timeline` array per reactor holds `(timestamp, table, row)` for every record,
which is what the log window walks and what a click on the plot resolves
against.

```
files nodes cpus                   what the snapshot directories describe
  tables[cpu].switches             the reactor picked up a task
             .tq_runs              the reactor gave the cpu to a task queue
             .io_begins/.io_ends   an I/O was submitted, and completed
             .prep_runs            a prepared statement was executed
             .prep_deltas          the statement cache changed, or was dumped
             .conns                a connection opened, closed, or was dumped
             .rpcs                 a message crossed the wire
             .timeline             all of the above, in time order
             .log_lines            every record of it, rendered as text
             .slices               every rectangle of it, in milliseconds
             .lods                 the same, at coarser and coarser scales
locations statements connections   interned, and joined end to end
queries parts by_latency           one CQL request, and where it ran
```

Strings do not have their own allocations. A row holds a `str` -- an offset and
a length into one arena -- so a table of a million rows is one allocation and
not a million. The offset is not a pointer precisely so that the arena may
grow; the one pass that grows it after the decode does so in a step of its own,
before anything takes a view.

The passes, in the order `run()` calls them:

| pass | reads | writes |
|---|---|---|
| `pass_gather` | argv | files, nodes, cpus |
| `pass_decode` | files | every event table, syncs, locations |
| `pass_order` | the event tables | the same, in timestamp order |
| `pass_retime` | syncs | every timestamp, in node 0's clock |
| `pass_attribute` | switches | `row.task` where the record carried none |
| `pass_index` | the event tables | the per-cpu task indices |
| `pass_io_spans` | io_begins, io_ends | `io_begin.end` |
| `pass_statements` | prep_deltas, prep_runs | statements, `prep_run.statement` |
| `pass_connections` | conns | connections, paired end to end |
| `pass_rpc_pair` | rpcs, connections | `rpc.peer_cpu`, `rpc.peer_row` |
| `pass_queries` | switches, rpcs | queries, parts |
| `pass_query_rows` | parts | `row.query`, on every row |
| `pass_cost` | switches, io spans | `query.t1`, `query.cpu_ticks`, by_latency |
| `pass_prefix_sums` | by_latency, query costs | latency and CPU prefix sums for selection aggregates |
| `pass_render` | every event table | log_lines, slices |
| `pass_lod` | slices | lods: the same rectangles at coarser scales |

All of it runs once, at startup: 27 MB of traces over three nodes and six
shards -- 630 000 events -- is 3.7 seconds to the window, of which the last
pass is half. After that **the UI only reads.** There is no cache to
invalidate and nothing is rebuilt when the selection changes, because
`pass_render` renders the whole trace rather than the selected request:
618 000 log lines into a per-reactor arena (41 MB of text) and 530 000
rectangles, in milliseconds from one origin every row shares.

That is what lets both windows be scrolled *past* the request. A selection
moves them to it and recolours what is in it -- a rectangle takes its colour
from its own `query` field against the selection -- but what exists on screen
does not depend on it. The log is one reactor's whole trace under an
`ImGuiListClipper`, so the handful of visible lines out of a hundred thousand
cost what they look like; the plot culls its rectangles to the visible x range
with a binary search over `slice_reach`, a running maximum of where the
rectangles end, which is what makes "the first slice that can reach into view"
a lookup rather than a scan.

Culling is not enough when the whole trace is in view: everything is visible
then, and most of it is thinner than a pixel. So `pass_lod` builds a pyramid of
levels per reactor -- at scale `s`, every rectangle at least `s` wide as it is,
and one summary per `s`-wide bin standing for the narrower ones inside it,
saying how much of the bin they covered. A frame picks the coarsest level whose
scale is under half a pixel and draws it exactly as it draws the rectangles
themselves, which turns a zoomed-out row from a hundred thousand rectangles
into a couple of thousand. What the summaries lose is whose work they were, so
the picked request's own rectangles are drawn over them; highlighted rectangles
under one pixel are also redrawn when ordinary slices are used, so their
one-pixel minimum is not covered by a neighbour. Hovering a summary says how
many records are in there and offers the zoom that would show them.

### Reading a trace with it

A **query** is a CQL request, and the tool is four windows around it.

- **Queries** is the latency histogram: x is `1/(1 - quantile)` on a log axis,
  so a click picks a *tier* rather than a request -- the median at 2, the 99th
  at 100, the tail at 10000. Holding the left button scrubs through the tiers;
  the button belongs to the picker here rather than to the plot, so panning is
  on the middle button and the wheel still zooms. Two markers: the picked
  request in white, and under it in grey whichever one the pointer is over. That is the whole method: look at a median
  request, look at one from the tail, and find the difference. The magenta
  DragRect selects a range of quantiles, and the numbers under the plot are the
  aggregate over exactly the requests between them.

  Two of them, and only two, because for a distributed request they are the two
  that mean anything: **total latency** and **total cpu time**. Cpu time above
  latency is not a bug -- it is summed over every reactor the request touched,
  and three replicas reading in parallel spend more cpu than the wall clock
  they take.

- **Timeline** is one row per reactor the request ran on, opened on the
  request's own time range and pannable and zoomable along it -- the y axis is
  a list of reactors rather than a quantity, so it is locked.

  The mouse pans and the wheel zooms where you point; **`w` and `s` zoom, `a`
  and `d` pan**, at a doubling of the zoom and a screenful of pan per second
  for as long as the key is held. The keyboard zoom pivots on the pointer when
  the pointer is over the plot, so holding `w` keeps what you are pointing at
  where it is, and on the centre of the screen when it is not, because a hand
  that is nowhere near the plot has said nothing about where to zoom. A request
  picked while a key is down still gets the axis -- the key picks it up again
  the frame after.

  A bar's *shape* says what it is and its *colour* says whose it is. A stretch
  on the cpu is the full height of the row and an I/O is a narrow bar inside
  it, so an I/O in flight over a stretch of cpu leaves that stretch visible --
  and hoverable -- above and below it. Green and blue are the selected
  request's; the same two colours washed out to about a third of their
  saturation are everything else the reactor did, which makes a row the
  reactor's real timeline with this request picked out of it. Each bar's last
  pixel is darkened, because a run of stretches that meet would otherwise be
  one unbroken block with no boundary in it.

  Three colours, not two: green and blue are the *picked* request, amber and
  violet are the one **under the pointer**, and the washed pair is everything
  else. So pointing at a bar picks out that request in every row it touches at
  once, adds the rows it needs that are not already there, moves the
  histogram's grey marker to where it lands in the distribution, and points the
  log at it -- all without disturbing what is picked. Clicking commits it. A
  bar the trace could not attribute to any request leaves the selection alone,
  and neither hovering nor clicking on the timeline moves the x axis: what you
  are pointing at is already on screen.

  Hovering also says what the bars cannot: the task, the source location the
  continuation was created at, how long the stretch is.

  The checkbox beside a row **pins** that reactor: pinned rows come first and
  stay whatever is selected, which is how a shard worth watching -- the one a
  request keeps waiting on -- is kept in view while requests are picked
  through around it.

- **Log** is one shard's *whole* trace, scrolled to the request: what else the
  reactor was doing is most of why a request was slow, and so is what it was
  doing before the request arrived. The selected request's lines are green, the
  record a click landed on is yellow, everything else is grey, and the timestamp
  column is relative to the request wherever in the trace the line is. "Back to
  the request" returns after a scroll. There is no second "full log" window,
  because this is it.

- **Selected query** is both selections at once, the picked one over the one
  under the pointer -- because comparing two requests is the whole method, and
  the second is gone the moment the pointer moves.

- **Nodes** is which process each node number is.

There are two selections, and every window reads both. `clicked` is the one
that survives; `hover` is what the pointer is over right now, cleared at the
top of each frame and refilled by whichever window the pointer is in. Where
there is a hover it wins -- so passing the pointer along the histogram previews
each request in the timeline and the log, and passing it along a row of the
timeline runs the log through the records under it, both without losing what
you picked. Taking the pointer away puts the picked one back.

The hover a window finds while drawing is for the *next* frame: a window
discovers what the pointer is over as it draws, which is too late for itself
and far too late for the windows drawn before it. So a window writes
`view::pending`, that becomes `view::hover` at the top of the next frame, and
every window in a frame then sees the same answer. One frame's lag, which at
60 Hz is not a thing you can see.

The two hovers are not the same gesture, and the plot treats them
differently. A hover **from the histogram** is a *preview*: it asks "what does
this request look like", so the plot shows it the way picking it would -- its
rows, its stretch of time -- with the picked request's rows out of the way. A
hover **from the timeline** is a highlight on the plot that is already there:
it asks "whose is this bar", and moving the plot to answer would take the bar
out from under the pointer, so the rows and the axis stay and only the colours
change.

The plot's rows are therefore the pinned reactors, then the picked request's
(unless a preview has displaced it), then whatever the hovered request needs
that is not there yet. The pins are at the top because they were put there
deliberately, the picked request's rows are stable for as long as it is
picked, and rows the hover adds come and go at the *bottom*, where rows
appearing and disappearing cannot move anything above them out from under the
pointer.

### What a query is, given that nothing carries a query id

A CQL frame mints a task id; continuations inherit it on the shard, and it is
carried to other shards of the same node. On the far side of an RPC a
*different* id is minted for the work the message caused. So a query is a set
of `(cpu, task)` **parts**, grown from the frame's own by two rules -- the same
id on another cpu of the same node, and the task an inbound message opened on
the far end of a connection -- and a part is claimed by the first query to
reach it.

The second rule is three joins, and none of them is a protocol field: a
connection to the one at the other end of the socket, by the pair of addresses
each end names swapped, checked against the boot id and shard the handshake
carried; a sent frame to its arrival, by the sequence number each direction
counts locally; and an arrival to the work it caused, by `rpc_request_handled`.

### Two things it will tell you that look wrong and are not

**A green bar running past the end of the request.** A stretch on the cpu is
bounded by the next task the reactor picked up, and if nothing else ran there
the last bar of a request reaches to whatever came next -- possibly much later.
That is the trace's own information and the plot draws it, but it is not
counted: `pass_cost` clips a request's cpu time at its last record, which is
why the number under the histogram and the bar on screen can disagree about the
final stretch.

**A request whose latency is a second.** There is no "task ended" tracepoint,
so a request reaches as far as the last record any of its parts wrote -- and
the reader concurrency semaphore runs its own housekeeping continuations under
the requesting task's id, sometimes long after the answer went out. The p100
request in `boot-id-run` was 1014 ms of which 0.4 ms is cpu, and the log showed
why: four `reader_concurrency_semaphore.cc:1029` records, a second after the
rest. (`boot-id-run` predates the task-queue tracepoints, so its objects' tables have
not got them and `pass_decoder` says so by name.)

**A snapshot record inside a request.** The statement-cache and connection
dumps are written when the trace is taken, and `pass_attribute` gives a record
that carries no task the one the shard was last running. On an idle shard that
is whichever request was last -- so those lines are highlighted in its log.
They are deliberately not allowed to extend a request's range, which is what
`pass_cost` means by "only records that carry a task extend a query": without
that rule every request that happened to be last on a shard would stretch to
the end of the trace.

### Debugging it without the GUI

```sh
TRACE_HEADLESS=1 ...       the startup counts, then exit
                           (every run prints what each pass cost, and a
                            breakdown of the startup, most expensive first)
TRACE_DUMP_QUERY=0.5 ...   the request at that quantile: its parts, its rows,
                           and its own records on each reactor
```

`TRACE_DUMP_QUERY=1` is the slowest request, `0.5` the median. The counts each
pass prints on the way in are the first thing to read when something looks
wrong -- `0 connections known` means the snapshot was taken after the
tracepoints were switched off, and every location unresolved means a missing or
mismatched `dsos/`.

## Viewing it, with the old viewer

```sh
buck2 run //modules/trace-viewer:trace_viewer -- \
    third-party/scylladb/ignored/workdir_01/traces/<stamp>/
```

Node numbers come from the **boot ids** in the snapshots' metadata, in the order
the directories were given, so node 0 -- the reference clock everything else is
converted into -- is still the first directory on the command line. Files from
one process land under one node number wherever they were handed over. The
**Nodes** window says which process each number is, along with its build ID, its
shards and the stretch of time its files cover; the same lines are printed on the
way in.

To inspect a distributed request, pass one snapshot directory per node, in a
stable order. The viewer aligns their clocks through each node's clock-sync
records and adds time-aligned rows to the existing `Full log plot`: one row per
`(node, shard, task)`. The selected task's own rows come first -- one per cpu it
ran on, because a continuation inherits its id across a cross-shard hop -- and
under them one row for every other task the request reached, in the order it
reached them.

```sh
buck2 run //modules/trace-viewer:trace_viewer -- \
    node0/traces/<stamp>/ node1/traces/<stamp>/ node2/traces/<stamp>/
```

Every row is the plot that was there before, drawn for one task: the same green
on-CPU, blue preempted and white in-I/O rectangles, on the same x axis. Clicking
anywhere in a row selects that row's task -- the log, the full log and the
histogram all follow it -- and leaves the plot itself anchored on the request, so
the rows do not move under the pointer. Anywhere, not only on a rectangle: the
gaps are where the task was preempted or in an I/O, and what the request was
doing then is a fair thing to click on. Holding the button scrubs, as it always has.

A row is named by the **process and cpu it belongs to**, not by the task drawn on
it: `<boot-id-head>/shard<N>`, as a y-axis tick label rather than text inside the
plot. The boot id is cut to its first group -- a time-based UUID's `time_low`,
which differs between two nodes booted a second apart -- because a whole one is
36 characters of axis; the tooltip and the Nodes window have all of it. Text
drawn inside the plot was clipped by the plot rect as soon as anybody panned,
which is how the first row's label came to read `ed task`.

Hovering says what the bars cannot. A row's rectangles are its *own* task's, so
the blue stretch between two green ones says only "this reactor ran something
else"; the tooltip answers *what*, by taking the last switch record at or before
the pointer **on that row's own node and shard** and reading the task off it. It
gives the full boot id, the node and shard, the row's task, the task actually
running there, how long it held the cpu, and -- for a `run_task` -- the source
location it was created at, with the function this time. The stretch that task
held the cpu is washed lightly white, so what the tooltip is describing is
visible rather than inferred from where the pointer is.

Green is only ever bounded by *another task's record on the same shard*. There is
no "task ended" tracepoint, so a stretch where the reactor ran nothing at all --
the request is waiting on an RPC reply, and the shard has no other work -- is
green to the next record. Read a long green bar on an otherwise idle shard as
"nothing else ran here", not as "on the cpu all of it". The worst case in the
three-node trace here is 0.75 ms over eight consecutive switches to one task.

Every row is scoped to its own node **and shard**, and that scoping is
load-bearing: blue means "this reactor was running something else", so counting
another reactor's records as an interruption cuts the green bars short in
proportion to how many of them there are.

The selected task used to be exempt. It was drawn as row 0, scoped to a whole
*node* rather than to one shard, which made it the one row whose green was cut by
work on a cpu that was not preempting it, the one row that could not be named
after a cpu, and -- once the tooltip arrived -- the one row whose highlight did
not step at every boundary its own bars showed, because half of those boundaries
came from the other shard's records. It is now built exactly like the rest, and a
request that hops shards gets a row per shard.

### How a request is followed across nodes

No tracing id goes on the wire -- the handshake carries who the peer *is*, not
which request is in flight. Three joins reconstruct the graph:

  * **A connection to the one at the other end of the socket**, by the
    local/remote endpoint metadata in the connection snapshot. Both ends name
    the same pair of addresses and disagree about which is which. The peer
    identity from the handshake is checked against it where both ends have one:
    `a` must name `b`'s process and shard and `b` must name `a`'s. That rules out
    what the addresses cannot -- a socket whose far end belongs to a node that
    has since restarted and taken the address back.
  * **A sent message to its arrival**, by a sequence number each direction of
    each connection counts locally over the frames it actually writes and reads.
    It is not a protocol field either.
  * **An arrival to the work it caused**, by `rpc_request_handled` -- the record
    the server emits when it opens a task chain for an inbound request, the way
    the CQL server opens one per frame.

The walk then starts at the selected task, takes the messages that task
enqueued, and follows each into the task that handled it, repeating until it
stops finding new ones.

The task on each end has to be recorded explicitly, because neither end of the
wire runs in it: a message is written by the connection's send loop and read by
its receive loop, and both outlive every request on the connection. So
`rpc_message_sent` carries the task that *queued* the buffer, captured by
`outgoing_entry` where it is constructed in the caller. Asking instead what
happened to be running on the shard at the timestamp -- which is all a trace
without those fields can do -- answers "the connection", and a walk seeded on it
pulls in every unrelated request the node handled while this one was in flight.

Replies have no `rpc_request_handled`: a reply resumes the task that was waiting
for it rather than opening a chain, so it already belongs to a row. They do
carry the normal RPC `msg_id`, which is what to look at when inspecting a single
connection.

The second line is the one an endpoint join could never produce: a connection
whose peer boot id is not among the loaded snapshots is a connection to a node
whose trace is simply not here, and saying so is different from failing to pair
it. Loading two nodes of the three above gives `28 paired` and `22 of those to a
node not in these snapshots`.

It takes snapshot **directories**, not files, and decodes every `*.trace` in
them. Set `TRACE_DUMP_RPC=1` to print what the joins had to work with, which
requests reach another node, and the edges of the slowest one that does, then
exit without opening a window:

```
68 connections known, 68 paired with the far end
68 named their peer in the handshake, 0 of those to a node not in these snapshots
110 messages sent (75 from a task), 110 received, 65 opened a task chain
10 of 29 CQL requests reach at least one other node

slowest distributed request 1000000000101 (0.410 ms): 4 RPC messages over 3 node/shards, 5 tasks
  node0:shard1 task 1000000000101 -> node2:shard1 task af5d04065c6d50eb  0.008 ms on the wire, sequence 106
  node0:shard1 task 1000000000101 -> node1:shard1 task 5554fb943c418bb3  0.134 ms on the wire, sequence 106
  node2:shard1 task af5d04065c6d50eb -> node0:shard1 task 1000000000102  0.066 ms on the wire, sequence 53
  node1:shard1 task 5554fb943c418bb3 -> node0:shard1 task 1000000000103  0.070 ms on the wire, sequence 53
```

That is one `CL=ALL` write against a three-node `RF=3` cluster: the coordinator
fans out to both replicas, each opens a task chain of its own, and each answers
into a *new* chain back on the coordinator. None of the four carries a
`reply-msg-id`, which is the tell that Scylla's mutation path answers with a
fresh `MUTATION_DONE` request rather than an RPC-level reply.

**Take the snapshot before switching the tracepoints off.** The connection map
and the prepared-statement cache are written *through* tracepoints, so a
snapshot taken with them already off contains neither, and without the
connection map nothing pairs the two ends of an RPC -- the walk finds nothing
and the rows never appear. `TRACE_DUMP_RPC=1`'s first line is the check:
`0 connections known` means exactly this.

### The sample viewer

The **Stack samples** window is a list of every `stacktrace_sample` in the
trace -- when, which cpu, which task, how deep -- with the decoded backtrace of
the selected one beside it.

Decoding is **lazy, asynchronous and cached**: a minute of a two-shard node is
twelve thousand samples of a couple of dozen frames each, and symbolising them
all at startup would be a quarter of a million lookups for the handful anybody
opens.

It runs in `modules/address-decoder`: one worker thread per object, each owning
a **persistent** `llvm-symbolizer` bound to that object with `--obj`, fed
addresses through a mutexed queue and answering into another one that the render
loop drains once a frame. `$TRACE_SYMBOLIZER` overrides the binary.

Persistent is the whole point. Almost all of the cost is opening the object and
indexing its debug info -- on the `Dev` binary that is a few seconds, and it is
paid by whichever address lands in it first. Spawning a process per click paid it
*again on every click*:

| | first sample | every sample after |
|---|---|---|
| one `addr2line` per click | ~3.4 s | ~3.4 s |
| a symbolizer kept alive | ~3.4 s | 4-170 ms |

Measure it with a list rather than a single index -- the second number is the one
that matters:

```sh
TRACE_DUMP_SAMPLE=1,2,5,40 buck2 run //modules/trace-viewer:trace_viewer -- <dir>
```

Because the answer arrives on another thread, it is *not* available in the frame
that asked for it, and the window has to be able to draw a backtrace it does not
yet have. So it keeps a state per displayed address -- `fresh`, `sent`,
`decoded` -- requesting on the first, watching the reaped results on the second,
and rendering `...` until the third. Selecting another sample puts every address
back to `fresh`; that is this window's bookkeeping only, and costs nothing,
because the decoder still knows every address it has ever answered and a
re-request for one of those resolves on the next frame.

Answers are kept by address, so the second sample through the same call site is
free even across a sample switch.

Frames above the innermost are looked up at `address - 1`: a return address is
the instruction *after* the call, and a call in tail position would otherwise be
attributed to the next function entirely.

The link to the rest of the viewer runs both ways, and in both directions it is
the sample's task that carries it:

- Clicking a sample selects the task that was on the cpu when it was taken, so
  the log, the full log and the plot all move to it -- and both logs scroll to
  the sample's own line.
- A sample taken inside a task appears in that task's log as a `SAMPLE` line,
  and clicking it selects the sample here.

Which task a sample interrupted is worked out once, at load: the samples are
placed on the trace's clock, everything is merged into one timeline, and one
walk carrying the current task **per shard** answers it for all of them. The
task id is process-wide, not the shard running it, so the only honest answer to
"which cpu was this" is which file the record came out of. That is what
`entry::shard` is.

A sample taken while the shard was between tasks keeps task 0 and appears only
in this window.

## Decoders, read out of the objects

Nothing in a trace says what its records mean. A record is an id, a timestamp
and the packed bytes of its arguments; the names, the parameter names and the
wire types are in the `tracepoints` section of the **object that wrote it**,
which the linker filled with one `tracer::tracepoint_entry` per `TRACEPOINT()`.

So that is where the viewer reads them from. `dsos/` already has to be there --
a source location is an address inside one of those objects and cannot be read
back without them -- and every object in it is scanned for that section at
startup:

```
   dsos/.build-id/**            events.h
 (what each build writes)   (what the viewer wants)
           \                      /
            \   the generator    /
             \  matches by name /
              v                v
           plugin.cc --- clang --> plugin.so
                                      |
               trace_plugin_decode()  |  on_decode_<event>()
          <---------------------------->
                    the viewer
```

One plugin, not one per build. A build's tracepoints are a *slice* of the ids,
keyed by the build ID a trace names its objects with, so a cluster part way
through an upgrade is several slices in one switch. Two builds that lay the same
tracepoint out differently are two entries with two readers, both delivering into
the same `events.h` struct -- which the older scheme, where a tracepoint name was
a C++ struct name, could not do at all.

`pass_decoder` does it and prints what it found:

```
decoder: 50 tracepoints in 2 objects, 40 of them bridged into events.h (cached)
    tracepoint "reactor_stall" is in these objects and not in events.h; its
        records are read past and dropped
    events.h wants "task_queue_run_begin" and no object here has a tracepoint of
        that name
    run_task.at (srcloc): events.h has no field of that name
```

Everything after the first line is something the viewer will not know about
these traces: a tracepoint it has not got, a field it spells differently, a field
whose type will not convert. None of it is guessed at -- the field stays at its
default, and the note is printed every run.

`modules/trace-viewer/events.h` is the viewer's half of that contract, and it is
the file to edit when the viewer wants a new field. `decoder_plugin.h` says how
the two halves are matched, and `trace_wire.h` is the part that is not generated
at all: the record format itself.

**`DECODING.md` beside this is the handoff for all of it** -- the contracts it
rests on, what each failure message means, and what was left undone. Read it
before changing anything in that path.

### What the viewer now has to know

Two things that used to be somebody else's problem, and both are the price of
not going through a generated header:

* **the wire format**, which is `trace_wire.h` -- vints, record ids, the three
  timestamp encodings, the metadata stream. A trace written by a tracer whose
  format has moved on is not readable by an older viewer, and says so rather
  than guessing.
* **the layout of a `tracepoint_entry`**, which is asserted at both ends:
  `tracer.h` static_asserts the offsets, and `tracepoint_table.cc` reads them.
  An object whose table is not that layout is reported by name and skipped --
  the check is that every entry's name comes out an identifier, which a wrong
  stride fails within an entry or two.

The upshot: **old snapshots are not readable**, and there is nothing to be done
about it short of keeping an old viewer. That is deliberate; see "prototyping"
at the bottom.

### What it costs, and where it is kept

Generating the plugin and compiling it is about 2.4 s, so the `.so` is cached
under `$TRACE_PLUGIN_CACHE`, or `~/.cache/trace-viewer` if that is unset. The
key covers the generated source -- which stands for the tables it came from,
whole -- plus `events.h`, `trace_wire.h`, the ABI header, the compiler's version
and a version number standing for the generator itself, so a cache hit is the
same plugin and nothing is stale. A hit costs reading the tables again and a
`dlopen`: 21 ms of a 348 ms release startup on `entry-layout-run`.

The plugin is compiled `-Wl,-Bsymbolic`, and that flag is load-bearing in the
default build. The viewer includes `trace_wire.h` too, so at `-O0` its own
out-of-line copies of those inline functions are in its `.dynsym` -- put there by
the `-rdynamic` the `on_decode_*` symbols need -- and without `-Bsymbolic` the
plugin's calls to *its* copies bind to the viewer's unoptimised ones instead.
Nothing fails; `pass_decode` is just 210 ms instead of 137. In release the viewer
inlines them and the flag measures as a no-op. See `DECODING.md`.

Each cache directory is self-contained -- the generated `plugin.cc` and the three
headers it is compiled against -- so a compile that failed can be repeated by
hand from what is in it, and the generated reader can simply be read. It is worth
reading: it is a few hundred lines, and it is the whole of what the viewer thinks
a trace is.

The compiler is `$TRACE_CXX`, or the first of `clang++`, `c++`, `g++` on PATH.
**The viewer therefore needs a compiler at runtime**: run it from inside
`nix develop`. Without one, `pass_decoder` says so and nothing decodes.

(`smoke.trace` predates all of this and no longer decodes against anything;
nothing reads it.)

### Source locations

A tracepoint parameter may be a `srcloc::location`, which records where its
*caller* was -- see `modules/source_location`. It is one address, so unlike every
other field it cannot be read out of the trace alone: the decoder needs the
object files themselves, found by build ID under

```
$TRACE_DSO_DIR/.build-id/<first two hex digits>/<the rest>.debug
```

which is the layout `gdb --debug-file-directory` and `llvm-cov` already use.
Without it the rest of the trace still decodes and each location comes out as
`<unresolved 0x...>` rather than as a guess.

**Filling that directory is not the traced process's job.** A node has no
business copying its own text into its workdir on every snapshot, and in a real
deployment it would not: a trace names its objects by build ID, and a build-ID
server -- debuginfod serves exactly this namespace -- hands them to whoever is
reading the trace. Here it is `tools/gather-dsos`, run by hand from Scylla's
devshell:

```sh
nix develop -c ../../tools/gather-dsos build/Dev/scylla \
    ignored/<run>/dsos
```

For a multi-node trace, use one shared directory when all nodes ran the same
binary, and pass it explicitly to the viewer:

```sh
cd third-party/scylladb
nix develop -c ../../tools/gather-dsos build/Dev/scylla \
    ignored/rpc-task-attribution/dsos
cd ../..
RUN="$PWD/third-party/scylladb/ignored/rpc-task-attribution"
TRACE_DSO_DIR="$RUN/dsos" \
  buck2 run //modules/trace-viewer:trace_viewer -- \
  "$RUN/node1/traces/1788463349684" \
  "$RUN/node2/traces/1788463349690" \
  "$RUN/node3/traces/1788463349695"
```

Do not use `--strip-debug` when stack backtraces with file and line information
are wanted. The viewer prints the resolved/unresolved location count on startup;
for the invocation above it should report `151 resolved, 0 not`, and without
`TRACE_DSO_DIR` the same 151 come out unresolved.

It resolves the binary's libraries with `ldd` -- DT_NEEDED transitively, under
the same RUNPATH rules the loader will use -- rather than looking at a running
process, because a program that never `dlopen`s maps exactly that set, and Scylla
does not. Something arriving only through `dlopen` would be missing, and a
location inside it would read `<unresolved 0x...>` while everything else decoded.

`--strip-debug` is worth taking **if you only want source locations**: a decoder
reads program headers, `.rodata` and the dynamic relocations and never touches
debug info, which on the `Dev` binary is 564 MB down to 162 MB for the same
resolved call sites.

Those objects are **mapped, not read**. A decode touches a few kilobytes of each
-- the program headers, the dynamic relocations, and the strings the locations
point at -- so reading them in copied half a gigabyte to look at almost none of
it, and that copy was essentially the whole of the viewer's startup:

| | time to the window, 565 MB of `dsos/` |
|---|---|
| read each object into the heap | ~18 s |
| `mmap` each object | ~1.6 s |

Same 404 locations, all 404 resolved, either way. The viewer prints that count on
the way in, which is the quick check that a `dsos/` directory is the right one:
every location unresolved means a missing, stripped or mismatched directory.

`mmap` lives in `dso_directory::object()`, in `trace_wire.h`, which the viewer,
every plugin it compiles, and `modules/tracer`'s tests all include. It is the
only copy of it there is; a second one used to live in the decoder header the
tracer generated, and keeping the two the same was nobody's job.

Stack samples are the exception. `llvm-symbolizer` *does* read debug info, so
against stripped objects a backtrace comes out as function names from the
symbol table with no file and no line, and inlined frames vanish. Gather without
`--strip-debug` if the backtraces are what you are here for.

The viewer picks up a `dsos/` directory inside the snapshot on its own;
`$TRACE_DSO_DIR` overrides, for a directory kept somewhere else.

`run_task` carries a location per record, and the same call site turns up
thousands of times, so the viewer interns them by address and each record holds
an index.

Two things a location needs that the obvious reading of "just look it up in the
object" misses, both handled in `trace_wire.h`:

- The `file` and `function` of a `std::source_location` in a **shared** object
  are not in its file. x86-64 uses RELA, so the place holds zero and the value
  is the addend in `.rela.dyn`; the decoder applies those relocations. Without
  that every location in `libseastar.so` reads its strings from offset zero and
  comes out as `ELF:3141:41`.
- Most locations are **not** in an object that traces. Scylla's tracepoints all
  live in `libseastar.so`, while the `then()` call sites are all over the
  executable, so `tracer::trace_objects()` describes every loaded object and not
  only the ones holding a tracepoint table. An object nothing describes is an
  address a decoder cannot even name.

## Debugging a trace without the GUI

For stack samples the viewer itself has a headless path, because a backtrace
that comes out as bare addresses is nearly always a missing or stripped `dsos/`
directory rather than anything wrong with the trace:

```sh
TRACE_DUMP_SAMPLE=5 buck2 run //modules/trace-viewer:trace_viewer -- <snapshot-dir>
```

prints that sample's decoded backtrace and exits without opening a window. It
takes a comma-separated list, and prints how long each one took -- which is also
how the symbolizer pool is checked to be doing its job; see the table above. The
counts the viewer prints on the way in are worth reading too: how many samples
there are, how many were placed on the trace's clock, and how many fell inside a
task.

Expect **far fewer samples than 100 Hz x shards x seconds**. A cpu-clock event
only ticks while the shard is on the cpu, and a node serving `load.py`'s
sequential selects is idle most of the time -- a 30-second run over two shards
gives a few dozen samples, not six thousand. That is the sampler working, not
failing. Note also that the samples are written at the `info` level while the
switches are at `debug`, so a long run's *samples* survive in the ring while its
switches have been evicted: an early sample can be a sample with no task, which
is why the "fell inside a task" count is below the total.

When something else looks wrong, decode headlessly and count. Reading the
generated `plugin.cc` in the cache directory says exactly what the viewer thinks
each record is; a ~40 line program over `trace_wire.h` and the tables --
`modules/tracer/trace_reader.{h,cc}` is one, written for the tracer's tests --
will tell you how many distinct tasks own an `io_begin` -- if that number is small, the chain is
broken somewhere. Also worth counting: how many `cql_request` records there are,
and whether the ids in one shard's file were minted by the other shard's
requests.

## Extra guidance about prototyping

Do not try to integrate Scylla's build with ours in any way, that would be very painful.
Instead, just directly add our source files (via absolute paths, I guess) to Scylla's cmakefiles. If there's some problem with that, you can modify either side to fix that. (E.g. if we have a test in a source file, which Scylla can't deal with without dragging in our whole test framework, you can just extract the test to a separate file).
In general, you can freely modify the relevant parts of this repo if that moves us closer to our goal. This project is a playground, we don't have to keep any compatibility with anything.
We are prototyping prototype. Ignore all Scylla tests. Never run them, and don't even try to build them. It's fine if they break, we don't care at all for now. Don't overthink this, we value iteration speed more than careful design for now.
