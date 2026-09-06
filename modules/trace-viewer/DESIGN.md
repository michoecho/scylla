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
`is_sorted` checks -- it is there for the trace where it is not. The one thing
it really does sort is the *timeline*, and only the first time: a shard's
levels are separate files, read one after the other, so their entries arrive
interleaved in file order and have to be merged. The second run finds it
sorted, because retiming is monotone.

The same instinct applies elsewhere: shards are found from metadata rather
than from file names (with a documented fallback), nodes are identified by boot
id rather than by which directory they arrived in, and a file that fails to
decode is reported and skipped rather than killing the run.

### 10. The events are written down twice, on purpose

`events.h` says what the viewer wants an event to be. The `tracepoints` section
of each object a snapshot came from says what that build actually writes.
**These are two different documents and neither is derived from the other**,
because a cluster part way through an upgrade has several of the second and one
of the first.

So the tables are never a picture of a producer's memory layout. `pass_decoder`
reads every object's table out of the ELF (`tracepoint_table.h`), matches it to
`events.h` by name and by field name, and compiles one shared object holding a
reader per tracepoint that fills in the viewer's struct and calls its exported
`on_decode_<event>`. `decoder_plugin.h` has the mechanics.

The three properties worth keeping:

- **A tracepoint the viewer cannot read does not stop the rest.** An object
  whose table is a layout this viewer does not know is a line naming the object
  and no ids; a tracepoint with a parameter type there is no reader for is a
  line and a reader that throws if a record of it ever turns up.
- **A disagreement is a note, not a guess and not a crash.** A tracepoint one
  side has not got, a field spelled differently, a field whose type will not
  convert without losing something -- the field stays at its default and
  `pass_decoder` prints why, every run. This is rule 8 applied to the wire
  format.
- **The conversion is narrow.** The same type, or a wider integer of the same
  signedness. Nothing else. Widening the rule is how a task id silently loses
  its top half; see `convertible` in the generated source before you touch it.

What the generator does *not* do is read `events.h`. Every assignment it emits
is wrapped in `if constexpr (requires { event.field; })`, so which fields exist
and which types convert are questions the compiler answers against the real
header when it builds the plugin -- and asks a second time in
`trace_plugin_notes`, which is where the notes above come from. The one thing it
has to be told is the list of event *structs*, which is `VIEWER_EVENT_LIST` in
`events.h`.

Nothing else in this file knows any of it happened. `decode_sink` takes
`viewer::events::run_task`, which is the same shape it always took.

`DECODING.md` is the handoff for that whole path, and the place the contracts it
rests on are written down.

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
| `.tq_runs` | the reactor gave the cpu to a task queue, or took it back. The end is what bounds every switch inside it |
| `.io_begins` / `.io_ends` | an I/O was submitted, and completed |
| `.prep_runs` | a prepared statement was executed |
| `.prep_deltas` | the statement cache changed, or was dumped |
| `.conns` | a connection opened, closed, or was dumped |
| `.rpcs` | a message crossed the wire, or opened a task chain |
| `.timeline` | **all of the above**, in time order |
| `.switch_by_task`, `.rpc_by_task` | `(task, row)` sorted by task -- built once, read many |
| `.query_of_task` | which request each task on this reactor belongs to |
| `.log_lines` + `.log_text` | every record of it, rendered |
| `.slices` + `.slice_reach` | every rectangle of it, in ms, sorted by start |
| `.lods` | the same rectangles at coarser and coarser scales, one level per scale, finest first |

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
| 1b | `pass_decoder` | `dsos/` → one compiled decoder (no table) |
| 2 | `pass_decode` | `files` + the decoder → every event table, `syncs`, `locations` |
| 3 | `pass_order` | event tables → the same, in timestamp order |
| 4 | `pass_retime` | `syncs` → every `ts`, in node 0's clock |
| 5 | `pass_order` | again: retiming is monotone only if the clocks are |
| 6 | `pass_attribute` | `switches` → `row.task` where the record carried none |
| 7 | `pass_task_queue_runs` | `tq_runs` + `timeline` → `switch.run`, `.group`, `tq_run.end` |
| 8 | `pass_index` | event tables → the `*_by_task` indices |
| 9 | `pass_io_spans` | `io_begins` + `io_ends` → `io_begin.end` |
| 10 | `pass_statements` | `prep_deltas` + `prep_runs` → `statements`, `prep_run.statement` |
| 11 | `pass_connections` | `conns` → `connections`, paired end to end |
| 12 | `pass_rpc_pair` | `rpcs` + `connections` → `rpc.peer_cpu`, `.peer_row` |
| 13 | `pass_queries` | `switches` + `rpcs` → `queries`, `parts` |
| 14 | `pass_query_rows` | `parts` → `row.query`, on every row |
| 15 | `pass_cost` | `switches` + io spans → `query.t1`, `.cpu_ticks`, `by_latency` |
| 16 | `pass_query_statement` | `prep_runs` + `queries` → `query.statement` |
| 17 | `pass_render` | every event table → `log_lines`, `slices` |
| 18 | `pass_lod` | `slices` → `lods`, the same rectangles at coarser scales |

Three orderings in there are real constraints rather than convention, and each
is commented at the call site: `pass_attribute` must precede `pass_index`,
because the index is keyed on the task it fills in; `pass_order` runs again
after `pass_retime`; and `pass_task_queue_runs` must follow the last `pass_order`,
because it walks the timeline and wants it in the order the rings hold it.

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

Four places where the obvious implementation would lie, and what is done
instead. Do not "simplify" these away.

**What extends a request.** Only records that *carry* a task extend
`query.t1`. The ones `pass_attribute` gave an ambient task to do not. The
statement-cache and connection snapshots are written when the trace is taken,
and on an idle shard the ambient task is whichever request was last -- so
without this rule every request that happened to be last on a shard would
stretch to the end of the trace.

**What a summary of concurrent I/O says.** A bin's density is the time the
rectangles in it covered, over the width of the bin, clamped at one. For the
cpu band nothing is ever clamped -- a reactor runs one task at a time, so the
levels conserve on-cpu time exactly, which is worth keeping true. Two I/Os in
flight together *do* overlap, so an I/O summary says how much of the stretch
had some I/O outstanding rather than how many I/O-seconds were spent in it.
That is the same thing the row draws when it draws them, one over the other,
and it is why the two bands are never added together.

**What counts as on the cpu.** There is still no "task ended" tracepoint, but
there is something better: the reactor says when it *gave the cpu back*. A task
queue run's end bounds every switch inside it, and the next switch on the shard
bounds it too; `switch_ends()` takes whichever the snapshot has. `pass_cost`
additionally clips at the request's last record. The *plot* does not clip at
the last record -- it cannot, because rectangles are rendered before anything
is selected -- so a green bar can run past the end of a request while the
number does not count it. That is a documented disagreement, not a bug.

An earlier version of this had no run end to work with, and bounded a stretch
by the next switch alone. That is wrong whenever the reactor had nothing else
to do: the idle time until the task's own continuation was billed to it as cpu.
It was patched by subtracting the task's own I/O from its stretch, on the
grounds that a shard waiting for a disk is not running -- which made the median
request in `sched-group-run` read 0.412 ms of cpu against 0.108 ms of latency.
With the real bound it reads 0.088 ms. **Do not put the subtraction back.** An
I/O is now drawn *over* the cpu it overlaps rather than out of it, because a
task holding the cpu while its own read is outstanding is a different thing
from one blocked on it, and the overlap is what shows which.

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

**A preview borrows the view and gives it back.** The pan and the zoom a
preview displaces are the *user's*, not the selection's, so `follow_selection`
keeps the axis and the log's scroll as the last frame actually drew them
(`view::axis_lo/axis_hi/log_scroll`) and puts them back when the pointer
leaves. What it must not do is recompute the view from `clicked`, as though the
pointer leaving the histogram were a new selection -- that is what threw away
wherever you had scrolled to, every time the pointer crossed the histogram on
its way somewhere else. The one exception is a preview that ended because it
was *picked*: then the new selection's fit is what was asked for and there is
nothing to give back, which arrives as a preview ending with a different
`clicked.query` than it began with.

**The keyboard writes an instruction, it does not move the axis.** `w`/`s`
zoom and `a`/`d` pan, and `apply_keys` runs between the histogram and the
windows -- the same slot as `follow_selection`, and for the same reason: the
axis has to be decided before anything reads it. It works off `view::axis_lo/
axis_hi`, the axis the plot last *drew*, and leaves `view::key_axis` for the
plot to consume, so the axis is the user's again the moment the key comes up.
Rates rather than steps, scaled by the frame's delta time, so the speed is the
same on a 60 Hz display and a 144 Hz one. The zoom pivots on the pointer, taken
from `view::axis_hovered/axis_mouse` -- the plot's own hover test and mouse
position, kept from the frame that drew them for the same reason the axis is --
and on the centre of the view when the pointer is elsewhere. It is ordered last of the three
things that can seize the axis (`refit`, then `restore_axis`, then the keys),
so a request picked this frame still wins and a held key resumes next frame.

Guard it on `io.WantTextInput` and **not** `io.WantCaptureKeyboard`: with
`NavEnableKeyboard` set, which this program sets, the latter is true whenever
any window has nav focus, and these keys would be permanently dead.

**Row order encodes that.** Pinned reactors, then the picked request's, then
whatever the hovered request needs that is not there yet. Rows the hover adds
go at the **bottom**, where rows appearing and disappearing cannot move
anything above them out from under the pointer.

**A bar's shape says what it is; its colour says whose it is.** Shape from
`s.table` (cpu is the full row height, I/O a narrow bar inside it), colour from
`s.query` against the two selections. Both are pure functions of a row and the
selection, which is exactly why the rectangles never need rebuilding.

**What is too thin to draw is summarised, not dropped.** Zoomed out, a
reactor's hundred thousand rectangles land on two thousand pixels, and drawing
them all was most of a frame for a picture in which nine tenths of them were
smears the next rectangle overwrote. So `pass_lod` builds a pyramid per
reactor: the level with scale `s` holds every rectangle at least `s` wide
verbatim, plus one *summary* per `s`-wide bin standing for the narrower ones
inside it, carrying how much of the bin they covered and how many they were. A
frame picks the coarsest level whose scale is under half a pixel and draws it
with the code that drew `.slices` -- the arrays are the same shape, so it is
still one binary search and a scan, **one array per row per frame**. Not a
query per gap, and no merging of levels: that is what the pyramid is bought
for.

Three properties of it are load-bearing. The scales double and every bin is
aligned to a multiple of its own scale, so bins *nest exactly* and each level
is coarsened from the one below rather than from `.slices` -- which is what
makes a coarse summary an exact sum of finer ones, and the whole pyramid one
pass plus a geometric tail (~0.3x the rectangles, ~1.5% of startup). The cpu
and the I/O bands are accumulated separately, because a stretch of cpu and an
I/O drawn over it are different bands of the picture and adding them up would
say the reactor was busier than it was. And the pyramid *starts* at a small
multiple of the average time between one rectangle and the next: below that,
the zoom that would pick such a level has only a couple of thousand rectangles
in view anyway, and the plot reads `.slices` as it always did. `.lods` empty is
a legal state -- delete `pass_lod` and the plot draws `.slices` at every zoom,
which is exactly what it did before.

**A summary belongs to no request, so the selection is drawn over it.** Its
colour is its band's washed colour with the alpha saying how busy the stretch
was; it has no `query`, and hovering one leaves the selection alone and offers
to zoom rather than naming a record. That would make a request whose every
rectangle is thinner than a pixel vanish from a zoomed-out plot -- which is the
plot you are looking at when you ask where a request went -- so the picked and
hovered requests' own rectangles are drawn again, verbatim, over the summaries.
It costs a binary search and a scan of the request's own stretch of time, not
of the row: a request's rectangles are contiguous in time, because a stretch of
time is what a request is.

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
- **`dsos/` and the `.trace` files are one pair.** The tracepoint tables the
  decoder is built from are in those objects, so a `dsos/` that is not the one
  these traces came from decodes nothing rather than misdecoding: that is the
  `tracepoint address ... belongs to object ..., whose tracepoint table this
  decoder has not got` message. **The plugin is compiled at runtime**, so the
  viewer needs a C++ compiler on PATH -- run it from inside `nix develop`. It is
  compiled `-Wl,-Bsymbolic`, without which its calls to its own copies of
  `trace_wire.h`'s inline functions bind to the viewer's exported ones and
  `pass_decode` silently takes half again as long in the default build. See
  `DECODING.md`.
- **A trace from a tracer older than this viewer does not read at all.** The
  wire format is `trace_wire.h` and the entry layout is asserted in `tracer.h`;
  both have moved, and neither is versioned. Recapture rather than debug.
- **A field `events.h` wants and a build has not got is not an error.** It is a
  line `pass_decoder` prints and a field left at its default. Read those lines
  before believing a column is empty for an interesting reason.
- **Views into an arena die when the arena grows.** See rule 3.

---

## How to do the usual things

**Add a tracepoint.** Five edits, and the first two are the new ones: add the
struct to `viewer::events` in `events.h`, spelled exactly as the tracepoint is,
with a member per parameter spelled exactly as the parameter is, and add its
name to `VIEWER_EVENT_LIST` at the bottom of that file -- which is what both the
generator and the `ON_DECODE` block beside `decode_sink` are written from. Then,
as before: give it a row struct with `ROW_COMMON`; add the table
to `table_id`, `cpu_tables` and `for_each_table`; add an `operator()` to
`decode_sink` that `push()`es it (which also puts it in the timeline); add a
case to `format_event`, `task_of` and `query_of`. If it needs joining to
something, that is a new pass, not a branch in an existing one.

Nothing has to be rebuilt on the producer side to *read* a build that has not
got it yet: `pass_decoder` reports it missing by name and the table stays empty.

**Add a field to an event.** Add it to the struct in `events.h`, under the
parameter's name. A build whose tracepoint has it fills it in; one that has not
is a line of `pass_decoder`'s output. A type that will not convert -- a
narrowing integer, or an integer of the other signedness -- is refused and
reported rather than truncated; see `convertible` in the generated source.

**Add a derived fact.** A new column on an existing row, filled by a new pass,
with a comment saying what it reads. Default it to `none`/0 so the program
still runs without the pass.

**Add something to the UI.** Ask first whether it can be rendered in
`pass_render` for the whole trace. If it can, it should be -- then the UI is
one more read.

**Debug without the GUI.** `TRACE_HEADLESS=1` prints the pass counts and exits;
every pass is also timed, and the breakdown at the end -- most expensive first
-- is where to look when startup is what hurts (today: `pass_render` and
`pass_decode` are five sixths of it);
`TRACE_DUMP_QUERY=<quantile>` prints one request's parts, its plot rows and its
records on each reactor. `0.5` is the median, `1` the slowest. Extend these
rather than adding printf to the render loop.

---

## What is deliberately not here

- **Stack samples.** The old viewer's sample window and its symbolizer pool
  (`modules/address-decoder`) have not been ported. `stacktrace_sample` is not in
  `events.h`, so the plugin reads its records past without them ever crossing into
  the viewer. Reimplementing means a struct in `events.h`, a `VIEWER_EVENT_LIST`
  line, a
  `samples` table, a pass placing them on the clock, and a window.
- **Anything keyed on the selection being cached.** See rule 7.
- **A test suite.** The checks today are the counts each pass prints and the
  headless dump against `ignored/entry-layout-run` -- three nodes of two shards,
  made by `third-party/scylladb/capture-trace.sh`. 2239 requests, all 503 source
  locations resolved, and every switch inside a task queue run (97117 of the
  97123 runs are closed in the snapshot -- the six open ones are the run each of
  the six reactors was in when it was asked). The older fixtures beside it were
  written by a tracer whose wire format has moved on and no longer decode at
  all. That is thin; the table-per-pass shape makes a real test of one pass easy
  to write, and it has not been written.
