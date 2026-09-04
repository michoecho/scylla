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

Declared in `seastar/include/seastar/core/scylla_tracer.hh`, defined in
`seastar/src/core/scylla_tracer.cc`. The viewer flattens them back into the
numeric event ids its analysis keys off.

| tracepoint | meaning | viewer id |
|---|---|---|
| `run_task{prev, task, at}` | the reactor picked a task off a run queue, and where that task was created | `0` |
| `cql_request{prev, task}` | a CQL frame arrived and opened a new chain | `1` |
| `io_begin{task, io}` | a task submitted an I/O and is now waiting | `0x4` |
| `io_end{task, io}` | that I/O completed | `0x5` |
| `semaphore_execute{prev, task}` | the reader semaphore's loop ran a queued read | `0xa` |
| `execution_stage{prev, task}` | an execution stage ran a queued work item | `0xb` |
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

`at` is `seastar::task::location()` -- the `then()` call site, or the `co_await`
a coroutine suspended at, which Seastar was already storing on every task as its
"resume point". It is one address rather than a string (see below), so the viewer
prints it next to each `SWITCH` line only when it has the objects to resolve it
against.

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
  `libseastar.so` -- alongside `src/core/scylla_tracer.cc`,
  `src/core/rendezvous.cc` and `src/core/scylla_stacktrace_sampler.cc` -- with
  `-w` because they are not written to Seastar's
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
decoder.h                       generated from this binary's tracepoint table
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
`seastar/include/seastar/core/scylla_tracer.hh`, and the reactor's constructor is
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
walk carrying the current task **per shard** answers it for all of them. Per
shard is the point -- a task id has the shard that *minted* it in its top bits,
not the one running it, so the only honest answer to "which cpu was this" is
which file the record came out of. That is what `entry::shard` is.

A sample taken while the shard was between tasks keeps task 0 and appears only
in this window.

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
other's, and the copy checked in here is the one that matches the current Scylla
build. Regenerate it after rebuilding Scylla against a newer `modules/tracer`.
(`smoke.trace` predates all of this and no longer decodes against it; nothing
reads it.)

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

`mmap` lives in `dso_directory::object()`, which is in the **generated**
`decoder.h`. The source of truth is `tracer::generate_decoder_source()` in
`modules/tracer/codegen.cc`; the copy checked in here was edited to match, so
regenerating it does not undo this.

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
object" misses, both handled by the generated decoder:

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

When something else looks wrong, decode headlessly and count. A ~40 line program
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
