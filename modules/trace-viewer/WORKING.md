# Working on the tracer, end to end

`README.md` beside this says what the thing *is*. `DESIGN.md` says how
`viewer.cc` is put together and which of its properties are load-bearing. This
one is the third question -- **how the work is actually done**: how to get a
Scylla with your tracepoint in it, how to get a trace out of it, how to find
out where the viewer's startup went, and which of the traps here have already
cost somebody a day.

Everything below was done at least once in the session that wrote it. Where a
number appears it was measured on `ignored/sched-group-run`, not estimated.

---

## The shape of the thing

Two repositories, three histories, and they are *not* nested in the way they
look:

| where | what | history |
|---|---|---|
| `.` (cpp_template) | the tracepoint machinery (`modules/tracer`), the viewer (`modules/trace-viewer`) | this repo |
| `third-party/scylladb` | the instrumentation and the snapshot API | its **own** git repo, untracked from here |
| `third-party/scylladb/seastar` | where nearly all the instrumentation lives | a **submodule** of that one |

So a change to a tracepoint is usually **three commits**: seastar, then the
submodule bump in scylladb, then the viewer here. All three of the last change
are worth reading together:

```
seastar     b4d54fa2  Bracket a task queue's turn on the cpu, and name its scheduling group
scylladb    aefb39f29d Pick up the task queue run tracepoints
cpp_template ee10e2467 Tell every CPU slice which scheduling group it ran under
```

The two builds meet **by absolute path**: `seastar/CMakeLists.txt` sets
`Scylla_TRACER_REPO` to `/home/michal/projects/cpp_template`. Check this repo
out elsewhere and you must edit it; there is no detection and no fallback.

---

## Scylla

### Building

It has its own devshell. Do not build it from this repo's.

```sh
cd third-party/scylladb
nix develop -c ninja -C build Dev/scylla
```

The build directory is already configured (Ninja Multi-Config, `Dev` only). In
`Dev` mode Seastar is a shared library, which is what lets the tracer live in
it. **Do not build or run Scylla's tests.**

Two things about the cost:

- A change confined to `src/core/scylla_tracer.cc` is a handful of steps and a
  relink. A change to `scylla_tracer.hh` is **~15 minutes**, because `task.hh`
  includes it and nearly every translation unit includes `task.hh`. Adding a
  hook means touching both, so budget for the long one; it is often worth
  putting something in the `.cc` with a dynamic initialiser rather than
  widening the header.
- Pipe the build somewhere you can `tail`. `... | tail -30` shows you nothing
  until it finishes, which reads exactly like a hang.

### Adding a tracepoint

Four edits, in this order:

1. **`seastar/include/seastar/core/scylla_tracer.hh`** -- declare the hook.
2. **`seastar/src/core/scylla_tracer.cc`** -- define it, one `TRACEPOINT()`
   inside. *Every* call site lives in this file; the header comment explains
   why (a tracepoint's static key needs a link-time-constant address, which it
   does not have inside an inline or template function in a shared library).
   Add it to the event list in the file comment while you are there.
3. **the call site**, which just calls the hook.
4. the viewer -- see `DESIGN.md`, "How to do the usual things".

Pick the level deliberately. `debug` is the per-task firehose; `info` is
requests, the connection map, the statement cache. They are separate rings with
separate eviction, and that difference bites -- see "the two windows" below.

### Getting a trace out

One node, for checking that a tracepoint fires at all:

```sh
cd third-party/scylladb
nix develop -c ./run-node.sh 1
curl -s -X POST 'http://127.11.11.1:10000/system/tracepoints_enabled?enabled=true'
nix develop -c ./load.py
curl -s -X POST http://127.11.11.1:10000/system/trace_snapshot
```

Three nodes of two shards, which is what the RPC join and the cross-node walk
need, and what the viewer is developed against:

```sh
cd third-party/scylladb
nix develop -c ./capture-trace.sh ignored/my-run
```

That starts nodes 1-3 in order (node 1 is the seed), waits for each to answer
CQL, switches the tracepoints on, runs `load3.py` (the CL=ALL variant, so every
request touches all three), snapshots each node, gathers the `dsos/`, and stops
everything. Out comes `node1/ node2/ node3/ dsos/`, which is the shape the
viewer is handed. Workdirs are kept between runs because bootstrapping three
nodes is minutes; `--fresh` wipes them.

**Tracepoints start disabled**, and forgetting to enable them fails *silently*:
the snapshot succeeds and every `.trace` file is ~117 bytes, which is the
metadata prologue and no records.

### After a capture, before a rebuild

Two things travel with a snapshot and are useless without it:

```sh
cp <run>/node1/decoder.h modules/trace-viewer/decoder.h
```

`decoder.h` is **generated from the binary that wrote the trace**. A mismatched
one refuses rather than misdecoding -- that is the `tracepoint address ...
belongs to object ..., which this decoder was not generated from` message, and
it means you are pointing today's viewer at yesterday's snapshot. Old runs stay
readable only with the `decoder.h` that came with them.

The `dsos/` are gathered by `capture-trace.sh`, but if you take a snapshot by
hand, do it **from the binary the trace came from, before rebuilding it** -- a
rebuild gives it a new build ID and the addresses stop resolving:

```sh
tools/gather-dsos third-party/scylladb/build/Dev/scylla <run>/dsos --strip-debug
```

---

## The viewer

```sh
# the GUI
TRACE_DSO_DIR=<run>/dsos buck2 run //modules/trace-viewer:viewer -- \
    <run>/node1 <run>/node2 <run>/node3

# the counts every pass prints, then exit -- the check to run after any change
TRACE_HEADLESS=1 TRACE_DSO_DIR=<run>/dsos buck2 run //modules/trace-viewer:viewer -- \
    <run>/node1 <run>/node2 <run>/node3

# one request in full: its parts, its plot rows, its records on each reactor
TRACE_HEADLESS=1 TRACE_DUMP_QUERY=0.5 ...      # 0.5 median, 1 slowest
```

`TRACE_DSO_DIR` has to be set: the fallback is `<argv[1]>/dsos`, and `dsos/`
sits *beside* `node1/`, not inside it. Without it the `at` locations come out
unresolved and nothing else changes.

**Run buck2 from the repo root.** From inside `third-party/scylladb` it
resolves `//modules/...` in the `scylladb` cell and tells you the directory
does not exist.

### Debug and release

The default build is `-O0 -g` and it is **six to seven times slower** than
release -- it was nine before the two fixes below, because the worst of what
they removed was worst at `-O0`. This matters more than it sounds: it is easy
to spend an afternoon optimising `-O0`.

```sh
buck2 build //modules/trace-viewer:viewer            # debug, -O0
buck2 build //modules/trace-viewer:viewer -m release # -O3, no -g
```

On `sched-group-run` (~822k events, 6 reactors) startup is ~2.0 s debug and
~300 ms release.

---

## Profiling the startup

### First: read the numbers it already prints

Every pass is timed. `pass_clock` in `viewer.cc` wraps each call, prints its
cost under whatever the pass itself printed, and ends with a breakdown sorted
most-expensive-first. That is usually the whole answer, and it costs nothing to
get:

```
startup: 300.3 ms in 17 passes, most expensive first
     158.9 ms  52.9%  pass_render
      70.5 ms  23.5%  pass_decode
      24.5 ms   8.2%  pass_order
      ...
```

The runs are repeatable to about 1%, so a 5% change is real.

### Then: perf, when you need to know *where inside* a pass

```sh
SNAP=third-party/scylladb/ignored/sched-group-run
REL=$(buck2 build //modules/trace-viewer:viewer -m release --show-full-output | awk '{print $2}')

TRACE_HEADLESS=1 TRACE_DSO_DIR=$SNAP/dsos \
perf record -e task-clock -F 4999 -g --call-graph fp -o rel.data -- \
    bash -c "for i in 1 2 3 4 5; do $REL $SNAP/node1 $SNAP/node2 $SNAP/node3 >/dev/null 2>&1; done"
```

Four choices in there, each of which was wrong the first time:

- **`-e task-clock`, not cycles.** This is a hybrid CPU: `cycles` opens as two
  PMU events (`cpu_atom` and `cpu_core`) and `perf report` shows you one of
  them, so a 12000-sample profile reads as 78 samples. `task-clock` is uniform
  across cores and is what you want for wall-time anyway.
- **`--call-graph fp` works**, because `-fno-omit-frame-pointer` is set
  project-wide in `buck/toolchains/BUCK`. No need for dwarf.
- **Five runs.** One run of a 300 ms program is not enough samples to say
  anything about a pass that takes 20 ms of it.
- **Release has no `-g`**, so everything inlines into `run()` and the flat
  profile is one 36% line. For a real profile, temporarily add `-g` to the
  release select in `buck/toolchains/BUCK`:

  ```
  "root//:build_mode[release]": ["-O3", "-g"],
  ```

  rebuild, profile, and **put it back**.

### Attributing samples to passes

With inline frames expanded, the innermost `pass_*` frame in a stack says which
pass a sample is in. `pass_clock` appears in every one of them, so skip it:

```sh
perf script -i rel.data --inline -F comm,ip,sym > rel.stacks
```

```python
import re, collections
samples, cur = [], None
for line in open("rel.stacks"):
    if line.startswith('\t'):
        if cur: cur['frames'].append(line.strip())
    elif line.strip():
        if cur: samples.append(cur)
        cur = {'comm': line.strip(), 'frames': []}
if cur: samples.append(cur)

tot, by = 0, collections.Counter()
for s in samples:
    if s['comm'] != 'viewer': continue
    tot += 1
    hit = next((m.group(1) for f in s['frames']
                if (m := re.search(r'\b(pass_[a-z_]+)', f)) and m.group(1) != 'pass_clock'),
               'outside run()')
    by[hit] += 1
for k, v in by.most_common():
    print(f"{100*v/tot:5.1f}%  {k}")
```

This agreed with `pass_clock`'s wall times to within a couple of points, which
is the cross-check worth doing before believing either. Narrow further by
bucketing on what else is in the stack (`format_event`, `fmt::`, `sort`,
`istreambuf`) -- that is how "35% of startup is the slice loop, 24% is
formatting log lines" came out.

`perf report -s srcline` is the other useful view: it gives `viewer.cc:2154`
directly, which beats reading a 400-character mangled template name.

---

## What was done, and why

In order, most recent last.

**`ee10e2467` -- scheduling groups.** Seastar now brackets each task queue's
turn on the cpu with `task_queue_run_begin{scheduling_group}` /
`task_queue_run_end`. The viewer gained a `.tq_runs` table and a pass that
walks the *timeline* -- not the two tables in parallel, because a begin and its
first `run_task` routinely share an rdtsc tick and the timeline is the only
place their order survives.

**`5f6803f2c` -- pass timings.** As above. The timer is a wrapper the caller
puts around the call, not a column and not a stopwatch threaded through the
passes: the numbers are about this run of the program, not about the trace.

**`fc1dfbc85` -- two fixes the timings pointed at.** `pass_decode` was reading
each file with a `std::istreambuf_iterator` pair (a byte at a time, vector
growing as it went) on files of tens of megabytes; and `pass_order` checked
`is_sorted` before sorting each event table but then sorted the *timeline*
unconditionally, twice. 94 -> 70 ms and 22 -> 2 ms.

**`a083fbccf` -- the important one.** A stretch of cpu used to be bounded by
the next switch alone, which swallowed every idle microsecond until the task's
own continuation. That was patched by subtracting the task's own I/O from the
stretch. The run *end* is the real bound -- it is the reactor saying it gave
the cpu back -- so the subtraction is gone, and the median request went from
0.412 ms of cpu (against 0.108 ms of latency, which should have been the tell)
to 0.088 ms. **Do not put the subtraction back**; `DESIGN.md` says why at
length. An I/O is now drawn *over* the cpu it overlaps, because a task holding
the cpu while its own read is outstanding is a different thing from one blocked
on it.

**`59518ccd3` -- the hover fix.** Passing the pointer over the histogram and
away again used to put the plot and log back on the picked request, throwing
away wherever you had scrolled. A preview now borrows the view and gives it
back. **Not verified by driving the GUI** -- it compiles and the headless path
is unchanged, but the interaction itself is unconfirmed.

---

## Fixtures

Both under `third-party/scylladb/ignored/`, both untracked, ~190 MB each, both
made with the binary whose `decoder.h` is checked in here.

| | what it is for |
|---|---|
| `sched-group-run` | the reference. 3 nodes x 2 shards, ~822k events, nothing evicted. Median request: 6 parts, 0.108 ms latency, 0.088 ms cpu. 99286 task queue runs, 99280 closed (the 6 open ones are the run each reactor was in when asked) |
| `wrapped-run` | 12x the load, so the debug ring **wrapped** and every shard's trace starts mid-stream. The fixture for anything about missing data |
| `boot-id-run` | older, predates the task queue tracepoints. Readable only with its own `decoder.h` |

---

## Open, and worth knowing before you start

**The two windows.** This is the real hole. `info` and `debug` are separate
rings that evict at different rates -- in `wrapped-run`, 15.1 s of info against
6.1 s of debug on the same shard. `cql_request` is an *info* record and the
switches and RPCs behind it are *debug*, so nine seconds' worth of requests are
seeded with nothing behind them. They are minted as queries anyway and come out
as 1-part local requests: 26868 requests found but only 14947 reaching another
node, against 98% in the unwrapped snapshot, and a p100 of 10 seconds.

Everything else degrades honestly under eviction and says so (11666 switches
"before the first" task queue run; unpaired I/O ends skipped rather than
mispaired; `pass_retime` robust by construction, because
`trace_buffers::write_slow` puts a `clock_sync` at the head of *every* buffer).
Queries are the exception. The data to fix it is already there -- `file_row`
carries `first_record_ns` per level, so each cpu has a "when all my streams are
covered" time -- and a query whose `t0` precedes it on its root cpu should be
counted and kept out of `by_latency` rather than ranked among the complete
ones. Not done.

**`pass_render` is still 53% of startup**, and its slice loop is most of that.
The `equal_range` that used to dominate it went with the I/O subtraction, so
what is left is 822k `format_event` calls and the emit loop. Rendering
everything up front is a deliberate choice (`DESIGN.md` rule 7) -- if it needs
to be faster, make the rendering cheaper, do not add a cache keyed on the
selection.

**There is still no test suite.** The checks are the counts each pass prints
and the headless dump against `sched-group-run`. The table-per-pass shape makes
a real test of one pass easy to write, and nobody has written one.

---

## Traps, in the order they cost time

- **`decoder.h` and the traces are one pair.** Copy the decoder in with the
  snapshot, or nothing decodes.
- **The default build is `-O0`.** 9x. Profile `-m release`.
- **`perf` on a hybrid CPU** splits `cycles` across two PMUs. Use `task-clock`.
- **buck2 from the repo root**, never from `third-party/scylladb`.
- **Do not start a second `ninja` on the same build directory** because the
  first one looks stalled. It is not; the output is buffered.
- **`seastar` has unrelated dirty files** (`util/log.hh`, `src/util/log.cc`)
  that are not yours. Commit by path, never `git commit -a`.
- **Tracepoints start disabled**, and the failure is silent 117-byte files.
- **A snapshot's `.trace` names say nothing about themselves.** Everything a
  reader needs is in the `.metadata.json` beside each one.
