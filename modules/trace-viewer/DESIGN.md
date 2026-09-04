# The design of `viewer.cc`

This is a handoff. `viewer.cc` is a rewrite of `main.cc` beside it, and the
rewrite exists for one reason: the old viewer was a chain of logic, and became
too hard to keep under control. Every step depended on a step before it,
reached into globals a third step had filled in, and could only be understood
by replaying the whole chain in your head. Bugs hid in the gaps between the
steps.

This one is the same job written the other way round. **The tables are the
design. The algorithms between them are expendable.**

If you change one thing here, change an algorithm. If you are about to change
a *principle*, read this document first and be sure you mean it, because the
principles are what stop the file turning back into what it replaced.

---

## The one idea

> There should be no dependencies like "you have to run X before you can run
> Y". There should only be "if you have data structures X and Y, you can use
> this to produce Z".

Everything below follows from that sentence.

A pass is a free function. Its comment says which tables it reads and which
tables it writes. **That comment is the dependency graph, and there is no
other** -- no init flags, no "is_built" booleans, no object that must be
constructed first. If you have the inputs, you may run the pass; if you have
not, the columns it fills stay at their defaults and everything downstream
either copes or is honest about not knowing.

The corollary is that a pass is *disposable*. Delete `pass_rpc_pair` and you
lose exactly two columns -- `rpc_row::peer_cpu` and `peer_row` -- and the query
walk stops crossing nodes. Nothing else notices, nothing else has to be told.
Replace its algorithm with a better one and the rest of the program cannot tell
the difference. That is the property worth protecting; it is what makes this
file safe to work on.

---

## The rules

These are the load-bearing ones. Breaking any of them costs the property
above.

### 1. Tables are plain, and they are not encapsulated

Every table is a `std::vector` of a POD row. No class, no accessor, no
invariant hidden behind a method. A pass reaches into any table it needs and
writes any field it owns.

This is deliberate and it is not laziness. The moment a table gets a wrapper,
the wrapper acquires an invariant, the invariant acquires an initialisation
order, and the initialisation order is the chain of logic we just deleted.

**Do not add a class around a table.** If you find yourself wanting one, what
you actually want is a free function taking the tables as parameters.

### 2. A reference between tables is an index

`int32_t` into another table, with `none` (`-1`) for absent. Not a pointer --
pointers die when a vector grows, and they say nothing when you print them.
Not an iterator. Not a `shared_ptr` to anything, ever.

Indices survive `push_back`, survive being sorted (with a remap -- see
`pass_order`), print as numbers you can grep the log for, and make a row
trivially copyable.

Interned tables reserve **index 0 for "none"** (`locations`); everything else
uses `none`. Both conventions are in the file already; follow whichever the
table you are touching uses, and say which in the comment.

### 3. No allocation per event

A trace is hundreds of thousands of events. A row must not own a `std::string`,
a `std::vector`, or anything else with a destructor that calls `free`.

Strings live in an `arena` -- one growing `std::vector<char>` -- and a row
holds a `str`, which is `{uint32_t off, uint32_t len}`. A table of a million
rows is one allocation, not a million.

`str` is an **offset, not a pointer**, precisely so the arena may grow while
rows referring to it already exist. The flip side is the one real hazard here:

> **A `std::string_view` obtained from the arena is invalidated when the arena
> grows.**

`pass_statements` has to intern statement text, which grows the arena, while
holding views into it as map keys. It does that in a *separate first step*
that finishes all the growth before the sweeps take any views -- and says so
in a comment. If you add a pass that both grows an arena and keys a map on
views into it, do the same, or key the map on `str` handles instead.

### 4. No globals, and no state outside the tables

Everything hangs off one `trace_data`, which a pass is handed. The UI gets a
`view` alongside it. There is no third place for state to hide.

The old viewer had `MULTIPLIER`, `the_clock`, `clock_syncs`, `samples`,
`location_strings`, `node_boot_ids`, `selected_sample`, `frame_states`... and
every one of them was an ordering constraint nobody had written down.

### 5. The event tables are per (node, shard, type)

`trace_data::tables[cpu]` is the tables for one reactor, where a "cpu" is one
`(node, shard)` pair -- `cpus` is the dense index and `cpu_row` says which is
which. Inside it there is one array per event type.

That is the shape the questions have. "What did this reactor do" is a scan of
one array. "What did task T do here" is a lookup in an index built from one.
Neither is a filter over everything.

Beside them, `.timeline` holds `(ts, table, index)` for **every** row on that
reactor, in time order. It is what the log walks and what a click on the plot
resolves against, and it is appended to by the same helper that appends the row
-- so it is complete by construction rather than by a pass somebody has to
remember to run.

*(One documented exception: an `rpc_request_handled` record becomes two rows --
a `switch_row` the timeline points at, and an `rpc_row` carrying the
`(connection, sequence)` the RPC join needs. The rpc row has no timeline entry,
because two log lines for one record would be a lie.)*

### 6. Every row starts with `ROW_COMMON`

```c
#define ROW_COMMON     \
    int64_t ts = 0;    \
    uint64_t task = 0; \
    int32_t query = none
```

`ts` so the generic passes (sorting, retiming) work on any table. `task` so
"whose work is this" is one field wherever you are. `query` so the UI's
colouring and the log's highlight are a field read rather than a lookup per
frame.

`for_each_table()` visits every event table of a reactor, which is what lets
sorting, retiming and counting be three lines instead of seven copies. **A new
event table must be added to `table_id`, to `cpu_tables`, and to
`for_each_table` -- all three, or the generic passes will silently skip it.**

### 7. Do everything once, up front, for the whole trace

Every pass runs at startup. `pass_render` -- the last one -- turns every record
into the line of text and the rectangles that will be drawn for it, for the
entire trace, not for anything selected.

The UI then **only reads**. There is no cache and nothing to invalidate.
Selecting a request moves the windows and recolours what is in them; what
exists on screen does not depend on the selection at all. That is what lets
both windows scroll off the end of the selected request -- which is most of
what you want when asking why a request was slow, because the answer is often
what the reactor was doing before it arrived.

Culling is what pays for drawing everything: the log is an `ImGuiListClipper`
over the reactor's whole timeline, and the plot binary-searches `slice_reach`
(a running maximum of where the rectangles end) for the first one that can
reach into the visible x range.

**If you are about to add a cache keyed on the selection, you are undoing
this.** Render more up front instead.

### 8. Say what you do not know

The trace is a ring buffer of a running system; it is full of things that are
absent, evicted, or unattributable. Every one of those has a sentinel and none
of them is guessed at:

- an I/O whose end was evicted keeps `end == none`;
- a location whose object is not in `dsos/` stays unresolved and keeps its
  address;
- a connection whose peer is not in these snapshots stays unpaired, and is
  *counted separately* from one that failed to pair;
- a record the trace cannot attribute to a request keeps `query == none`, and
  clicking it in the plot leaves the selection alone.

Passes print their counts on the way in for exactly this reason. `0
connections known` means the snapshot was taken after the tracepoints were
switched off; every location unresolved means the wrong `dsos/`. Keep adding
counts when you add passes.

### 9. Assume nothing about the trace's shape that the trace does not say

`pass_order` sorts and remaps rather than trusting that a tracepoint is always
written at one level. In practice it always is, so the pass is a scan of
`is_sorted` checks -- it is there for the trace where it is not.

The same instinct applies elsewhere: shards are found from metadata rather
than from file names (with a documented fallback), nodes are identified by boot
id rather than by which directory they arrived in, and a file that fails to
decode is reported and skipped rather than killing the run.

---

## The tables

### Static shape

| table | one row per |
|---|---|
| `files` | a `*.trace` file, and what its `metadata.json` says |
| `nodes` | a traced process, by boot id |
| `cpus` | a `(node, shard)`: a reactor. "cpu" throughout |
| `tables` | parallel to `cpus`: that reactor's events |
| `syncs` | parallel to `nodes`: its `clock_sync` records |

### Per reactor (`tables[cpu]`)

| table | one row per |
|---|---|
| `.switches` | the reactor picked up a task (five tracepoints; `cause` says which) |
| `.io_begins` / `.io_ends` | an I/O was submitted, and completed |
| `.prep_runs` | a prepared statement was executed |
| `.prep_deltas` | the statement cache changed, or was dumped |
| `.conns` | a connection opened, closed, or was dumped |
| `.rpcs` | a message crossed the wire, or opened a task chain |
| `.timeline` | **all of the above**, in time order |
| `.switch_by_task` etc. | `(task, row)` sorted by task -- built once, read many |
| `.query_of_task` | which request each task on this reactor belongs to |
| `.log_lines` + `.log_text` | every record of it, rendered |
| `.slices` + `.slice_reach` | every rectangle of it, in ms, sorted by start |

The `*_by_task` indices are sorted arrays rather than hash maps on purpose:
built once, read many times, and `equal_range` over one is two cache lines.
Prefer that shape for anything with the same lifecycle.

### Joined and derived (global)

| table | one row per |
|---|---|
| `locations` | an interned source location; index 0 is "none" |
| `statements` | an interned `(id, keyspace, text)` |
| `connections` | one *end* of a connection, joined to the other end |
| `queries` | one CQL request: range, cost, statement, parts range |
| `parts` | a `(cpu, task)` one request was worked on under |
| `by_latency` | query indices sorted by latency -- the histogram's x |

`query_row` holds `parts_begin`/`parts_end` into `parts` rather than a vector
of its own: one allocation for every request's parts, and a request's parts are
contiguous.

---

## The passes

In the order `run()` calls them. The middle column is the whole contract.

| # | pass | reads → writes |
|---|---|---|
| 1 | `pass_gather` | argv → `files`, `nodes`, `cpus` |
| 2 | `pass_decode` | `files` → every event table, `syncs`, `locations` |
| 3 | `pass_order` | event tables → the same, in timestamp order |
| 4 | `pass_retime` | `syncs` → every `ts`, in node 0's clock |
| 5 | `pass_order` | again: retiming is monotone only if the clocks are |
| 6 | `pass_attribute` | `switches` → `row.task` where the record carried none |
| 7 | `pass_index` | event tables → the `*_by_task` indices |
| 8 | `pass_io_spans` | `io_begins` + `io_ends` → `io_begin.end` |
| 9 | `pass_statements` | `prep_deltas` + `prep_runs` → `statements`, `prep_run.statement` |
| 10 | `pass_connections` | `conns` → `connections`, paired end to end |
| 11 | `pass_rpc_pair` | `rpcs` + `connections` → `rpc.peer_cpu`, `.peer_row` |
| 12 | `pass_queries` | `switches` + `rpcs` → `queries`, `parts` |
| 13 | `pass_query_rows` | `parts` → `row.query`, on every row |
| 14 | `pass_cost` | `switches` + io spans → `query.t1`, `.cpu_ticks`, `by_latency` |
| 15 | `pass_query_statement` | `prep_runs` + `queries` → `query.statement` |
| 16 | `pass_render` | every event table → `log_lines`, `slices` |

Two orderings in there are real constraints rather than convention, and both
are commented at the call site: `pass_attribute` must precede `pass_index`,
because the index is keyed on the task it fills in; and `pass_order` runs again
after `pass_retime`.

### The three joins worth understanding

**Prepared statements** (`pass_statements`). A `prepared_query_run` carries an
id and nothing else. What that id meant is the state of the shard's cache at
that moment, and the trace describes the cache as a snapshot written when the
trace was taken plus deltas on either side of it. So it is one sweep in *both*
directions from the snapshot: backwards, an addition is undone by removing and
a removal by adding (both delta records carry the text, which is what makes
this possible); forwards, the obvious thing. A separate walk per run would be
quadratic.

**Connections and messages** (`pass_connections`, `pass_rpc_pair`). Nothing on
the wire carries a tracing id. A connection is joined to the one at the other
end of the socket by the pair of addresses each end names swapped, checked
against the boot id and shard the handshake carried -- which rules out what
addresses cannot, a socket whose far end belongs to a node that has since
restarted and taken the address back. A frame is joined to its arrival by the
sequence number each direction counts locally. Where the arrival opened a task
chain, the send is linked to the `rpc_request_handled` row rather than the
plain receive, because that is the row carrying the task the work continues
under.

**Requests** (`pass_queries`). There is no global request id. A CQL frame mints
a task id; continuations inherit it on the shard and carry it to other shards
of the same node; on the far side of an RPC a *different* id is minted. So a
request is a set of `(cpu, task)` parts, grown from the frame's own by two
rules -- the same id on another cpu of the same node, and the task an inbound
message opened on the far end -- and a part is claimed by the first request to
reach it.

---

## Honesty, and where it is spent

Three places where the obvious implementation would lie, and what is done
instead. Do not "simplify" these away.

**What extends a request.** Only records that *carry* a task extend
`query.t1`. The ones `pass_attribute` gave an ambient task to do not. The
statement-cache and connection snapshots are written when the trace is taken,
and on an idle shard the ambient task is whichever request was last -- so
without this rule every request that happened to be last on a shard would
stretch to the end of the trace.

**What counts as on the cpu.** A stretch is bounded by the next task the
reactor picked up, because there is no "task ended" tracepoint. `pass_cost`
additionally clips it at the request's last record and cuts out the request's
own I/O. The *plot* does not clip at the last record -- it cannot, because
rectangles are rendered before anything is selected -- so a green bar can run
past the end of a request while the number does not count it. That is a
documented disagreement, not a bug.

**Ambient attribution.** `pass_attribute` gives a record that carries no task
the one the shard was last running. It is deliberately *not* applied to the two
RPC send records, which carry the task that queued the buffer: the ambient task
there is the connection's send loop, and a walk seeded on it would pull in
every unrelated request on the node.

---

## The UI layer

The `view` is a selection and two scroll positions. It builds one thing --
`rows`, the list of reactors on the plot -- and that is a `std::vector<uint32_t>`
of cpu indices.

**Two selections.** `clicked` survives; `hover` is what the pointer is over.
Readers take `v.query()`, `v.log_cpu()`, `v.focus()`, which prefer the hover.

**The hover is double-buffered, and it has to be.** A window discovers what the
pointer is over *while it draws* -- too late for itself, and far too late for
windows drawn before it. So a window writes `v.pending`, which becomes
`v.hover` at the top of the next frame. One frame of lag, and in exchange every
window in a frame sees the same answer. Getting this wrong is what made
hovering a bar highlight nothing at all.

**The two hovers are different gestures.** A hover from the histogram is a
*preview*: the plot shows that request the way picking it would, its rows and
its stretch of time, displacing the picked request. A hover from the timeline
is a *highlight* on the plot already there: rows and axis stay, only colours
change, because moving the plot would take the bar out from under the pointer.
`selection::from_timeline` is what tells them apart.

**Row order encodes that.** Pinned reactors, then the picked request's, then
whatever the hovered request needs that is not there yet. Rows the hover adds
go at the **bottom**, where rows appearing and disappearing cannot move
anything above them out from under the pointer.

**A bar's shape says what it is; its colour says whose it is.** Shape from
`s.table` (cpu is the full row height, I/O a narrow bar inside it), colour from
`s.query` against the two selections. Both are pure functions of a row and the
selection, which is exactly why the rectangles never need rebuilding.

---

## Gotchas that have already cost a day

- **`ImPlot::SetupAxisLimits(axis, hi, lo)` does not invert an axis.** The
  limits come back normalised. Use `ImPlotAxisFlags_Invert`.
- **A row's height is not the widget's height divided by the rows.** ImPlot
  spends some of the widget on axis labels and padding. Measure that overhead
  (`height - GetPlotSize().y`) and size the widget from it, or rows change
  height whenever one is added and the boundary under the pointer moves.
- **`SetCursorScreenPos` without a following item aborts.** ImGui tracks a
  window's extent through its items; a hand-moved cursor needs a `Dummy()`
  after it.
- **Compute the axis range before the frame's windows read it.**
  `follow_selection()` runs after the histogram (which can pick) and before
  everything that draws.
- **`decoder.h` and the `.trace` files are one pair.** A mismatched decoder
  refuses rather than misdecoding; that is the `tracepoint address ... belongs
  to object ..., which this decoder was not generated from` message, and the
  fix is to copy the `decoder.h` from beside the traces.
- **Views into an arena die when the arena grows.** See rule 3.

---

## How to do the usual things

**Add a tracepoint.** Give it a row struct with `ROW_COMMON`; add the table to
`table_id`, `cpu_tables` and `for_each_table`; add an `operator()` to
`decode_sink` that `push()`es it (which also puts it in the timeline); add a
case to `format_event`, `task_of` and `query_of`. If it needs joining to
something, that is a new pass, not a branch in an existing one.

**Add a derived fact.** A new column on an existing row, filled by a new pass,
with a comment saying what it reads. Default it to `none`/0 so the program
still runs without the pass.

**Add something to the UI.** Ask first whether it can be rendered in
`pass_render` for the whole trace. If it can, it should be -- then the UI is
one more read.

**Debug without the GUI.** `TRACE_HEADLESS=1` prints the pass counts and exits;
`TRACE_DUMP_QUERY=<quantile>` prints one request's parts, its plot rows and its
records on each reactor. `0.5` is the median, `1` the slowest. Extend these
rather than adding printf to the render loop.

---

## What is deliberately not here

- **Stack samples.** The old viewer's sample window and its symbolizer pool
  (`modules/address-decoder`) have not been ported. The events are decoded and
  dropped by the catch-all in `decode_sink`. Reimplementing means a
  `samples` table, a pass placing them on the clock, and a window.
- **Anything keyed on the selection being cached.** See rule 7.
- **A test suite.** The checks today are the counts each pass prints and the
  headless dump against `ignored/boot-id-run`, where a median request should
  come out as a coordinator and two replicas, three parts, ~0.119 ms latency
  and ~0.159 ms of cpu. That is thin; the table-per-pass shape makes a real
  test of one pass easy to write, and it has not been written.
