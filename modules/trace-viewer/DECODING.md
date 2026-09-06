# How a trace becomes events

`README.md` says what the tracer and the viewer *are*. `DESIGN.md` says how
`viewer.cc` is put together. `WORKING.md` says how the work is done. This one is
narrower and it is a handoff: **everything between the bytes on disk and
`decode_sink`**, as of `513302ae0` -- what it is, what it rests on, what it will
do when it breaks, and what was deliberately left undone.

Read it before changing anything under "the contracts" below. The rest of the
viewer is table-shaped and forgiving; this part is a wire format with three
parties to it, and two of them are compiled into other binaries.

---

## The problem, in one paragraph

A record is an id, a timestamp and the packed bytes of its arguments. Nothing in
a trace says what those bytes *mean* -- not the tracepoint's name, not its
parameters' names, not their widths. That lives in the `tracepoints` section of
the object that wrote the record, one `tracer::tracepoint_entry` per
`TRACEPOINT()` call site, collected there by the linker. So decoding a trace is
always a join between two things: the file, and the objects that produced it.
A cluster part way through an upgrade produces several sets of objects at once,
and they disagree.

---

## The shape of it

```
  dsos/.build-id/**                          events.h
  ------------------                    -------------------
  every object's `tracepoints`          what the viewer wants
  section, read by                      an event to be, plus
  tracepoint_table.cc                   VIEWER_EVENT_LIST
          \                                    /
           \        decoder_plugin.cc         /
            \   matches them by name, and    /
             \    writes plugin.cc          /
              v                            v
            plugin.cc  ---- clang ---->  plugin.so
                 |                           |
          includes trace_wire.h              | dlopen
          (the record format)                v
                                    trace_plugin_decode()
                                             |
                                     on_decode_<event>()
                                             v
                                        decode_sink
```

Five files, and they own exactly one thing each:

| file | owns |
|---|---|
| `trace_wire.h` | the record format, and reading an ELF object. Included by the viewer *and* by every plugin. Nothing in it knows a single tracepoint's name |
| `tracepoint_table.{h,cc}` | turning an object file into a list of tracepoints. The only place the entry layout is written down on this side |
| `decoder_plugin.{h,cc}` | the plan (ids, shapes, slices), the generated source, the cache, the compile, the dlopen |
| `plugin_abi.h` | the three symbols that cross the boundary |
| `events.h` | what the viewer wants, and `VIEWER_EVENT_LIST` |

One plugin covers **all** the objects, not one per build. Each object's
tracepoints are a contiguous slice of the ids, keyed by build ID, so several
builds are several slices in one switch. Two builds that spell one tracepoint
differently are two *shapes* with a reader each, both delivering into the same
`events.h` struct -- which the older, header-per-build scheme could not do at
all, because there a tracepoint name was a C++ struct name and two shapes wanted
one name.

---

## What the generator knows about `events.h`

Almost nothing, and that is the design. It never parses the header. Every
assignment it writes is guarded:

```cpp
if constexpr (requires { event.task; }) put(event.task, value);
```

so *does this field exist* and *does this type convert* are questions the
compiler answers, against the real header, when it builds the plugin. A field
`events.h` has not got is a branch never instantiated. A field whose type will
not convert is a `put` that expands to nothing.

Both are silent where they are, so the generated source asks the same questions a
second time in `trace_plugin_notes`, and that is where the lines under
`pass_decoder` come from. **Read them.** An empty column in the UI is more often
one of those lines than a bug in a pass.

The one thing that cannot be asked of the compiler is what a namespace contains,
so the list of event structs is written down once, in `VIEWER_EVENT_LIST`, and
both the generator and the `ON_DECODE` block in `viewer.cc` are built from it.

The conversion rule is deliberately narrow -- the same type, or a wider integer
of the same signedness. Widening it is how a task id silently loses its top half.
See `convertible` in the generated source.

---

## The contracts

Five, and every one of them is load-bearing. They are the reason this document
exists.

**1. The layout of a `tracepoint_entry`.** 64 bytes, with the offsets asserted at
both ends: `static_assert` in `tracer.h`, constants in `tracepoint_table.cc`.
Changing it is a compile error on the producer's side and a paired edit here, and
it makes every object built before the change unreadable. An object whose table
is not that layout is reported by name and skipped -- the check is that every
entry's name comes out an identifier, which a wrong stride fails within an entry
or two.

**2. The wire format**, which is `trace_wire.h`: vints, record ids, the three
timestamp encodings, the metadata stream. **It is not versioned.** A trace
written by a tracer whose format has moved on does not read, and the failure is
loud rather than subtle.

**3. `-rdynamic` on the viewer.** The plugin calls back by name; the
`on_decode_*` symbols have to be in `.dynsym` for `dlopen` to bind them.

**4. `-Wl,-Bsymbolic` on the plugin.** The viewer includes `trace_wire.h` too, so
its own copies of those inline functions can end up in its `.dynsym` -- put there
by the `-rdynamic` above. Without `-Bsymbolic` the plugin's calls to *its* copies
bind to the viewer's instead, and every `at_vaddr`, `read_str` and
`locator::resolve` in the record loop becomes a call through the PLT into another
object.

Which copies those are is what makes this worth knowing. In the **default
build** the viewer is `-O0`, so every one of those inline functions is emitted
out of line as a weak symbol and exported -- and the plugin, itself `-O2`, ends
up calling the viewer's unoptimised ones: `pass_decode` is 210 ms instead of
137. In release the viewer inlines them and only `string_at_vaddr` survives in
`.dynsym`, so the flag measures as a no-op (89 ms either way). It stays because
the default build is what everybody runs day to day, and because nothing about
the failure looks like a failure. If you change how the plugin is linked,
measure `pass_decode` in the **default** build, where it shows.

**5. No exception crosses the boundary.** The viewer links its C++ runtime
statically, so a `std::runtime_error` thrown in a plugin would be compared
against a different `std::type_info` and go uncaught. `trace_plugin_decode`
catches everything and returns a message in a buffer.

---

## What it costs

Measured on `ignored/entry-layout-run` -- three nodes of two shards, 814k
events delivered (more records than that are read and dropped), 163 MB of
objects -- at `513302ae0`. Both build modes, because the two differ by more than
the usual factor here and the plugin is `-O2` in either:

| | release | default (`-O0`) |
|---|---|---|
| `pass_decoder`, first run for a set of objects | 2.4 s | 2.4 s |
| `pass_decoder`, cached | 21 ms | 27 ms |
| `pass_decode` | 87 ms | 137 ms |
| whole startup | 348 ms | 1.7 s |

The cold figure is generating and compiling; the cached one is reading the tables
again, generating the source to hash it, and a `dlopen`. Neither depends much on
the viewer's own build mode, since the work is clang's and the plugin's.

The cache is under `$TRACE_PLUGIN_CACHE`, or `~/.cache/trace-viewer`. The key
covers the generated source -- which stands for the tables it came from, whole --
plus `events.h`, `trace_wire.h`, `plugin_abi.h`, the compiler's `--version` line,
and `generator_version`, which stands for the generator's own code and *for how
the plugin is compiled*. **Bump `generator_version` when you change the compile
command**, because the key cannot see the flags.

Each cache directory is self-contained: the generated `plugin.cc` and the three
headers it was compiled against. A failed compile can be repeated by hand from
what is in it. It is worth reading anyway -- a few hundred lines, and it is the
whole of what the viewer thinks a trace is.

---

## When it breaks

Every one of these is a real message; the cause is the part worth knowing.

| what it says | what happened |
|---|---|
| `no object under .../.build-id has a tracepoint table this viewer can read` | wrong `dsos/`, or every object was refused -- the per-object notes under it say which and why |
| `its tracepoints section is N bytes, which is not a whole number of 64-byte entries` | that object's tracer lays an entry out differently. Almost always: a snapshot older than contract 1 |
| `entry N ... has "" where a tracepoint name should be` | the strings did not survive, or a relocation form this reader does not know. See "relocations" below |
| `tracepoint address 0x... belongs to object X, whose tracepoint table this decoder has not got` | that object is missing from `dsos/`, or was stripped of its section headers |
| `no object here has the tracer's own "trace_object_loaded" tracepoint` | these tables are not the ones a trace was written from -- a directory of libraries, say |
| `these objects have two different "clock_sync" tracepoints` | see the limit below |
| `not a trace: magic 0x...` | not a trace, or a truncated one |
| `no C++ compiler` | run it from inside `nix develop` |

A decode that fails part way through keeps what it read. A record is not
self-delimiting, so the stream cannot be resynchronised past the first bad byte;
the file is reported and the others still decode.

### Relocations

A pointer inside a shared object is not in the object's file. Two forms turn up
in a tracepoint table and both are handled in `pointer_relocations`:

* `R_X86_64_RELATIVE` -- a pointer to something in the same translation unit.
  The value is the addend.
* `R_X86_64_64` -- a pointer to a string in an *inline* function, whose comdat
  the linker folded. The value is the symbol's address plus the addend, and
  reading it needs `.dynsym`.

Only collecting the first is a table whose header-defined tracepoints come back
with empty names -- which is what happens if you port this code from the older
`decoder.h`, which only ever needed the first.

---

## How to do the usual things

**Add a tracepoint the viewer should see.** Add the struct to `viewer::events`,
named exactly as the tracepoint is, with a member per parameter named exactly as
the parameter is; add its name to `VIEWER_EVENT_LIST`. Then the `viewer.cc` side:
a row struct, `table_id`/`cpu_tables`/`for_each_table`, an `operator()` on
`decode_sink`, and the `format_event`/`task_of`/`query_of` cases. See
`DESIGN.md`.

**Add a field.** Add the member under the parameter's name. A build that has it
fills it in; one that has not is a line of `pass_decoder`'s output.

**Find out what the viewer thinks a record is.** Read `plugin.cc` in the cache
directory. The switch at the bottom is the whole answer.

**Force a rebuild of the plugin.** Delete its cache directory, or bump
`generator_version`.

**Check a `dsos/` is the right one.** `pass_decode` prints how many source
locations resolved. All of them, or you have the wrong directory.

---

## Deliberately unfinished

In the order somebody should probably pick them up.

**The wire format is written down twice.** `modules/tracer/codegen.cc` still
generates a standalone `decoder.h` -- for the tracer module's own demo and its
snapshot test -- and about 900 of its lines are the same readers, ELF code and
record loop that now live in `trace_wire.h` and in `decoder_plugin.cc`'s
`preamble`. Nothing in the viewer uses it. It was kept because it is the only
*tested* description of the format: `tracer_test.cc` round-trips real records
through it and a snapshot test pins the output. The right end state is one
description -- port those tests onto `trace_wire.h` and delete `codegen.{h,cc}`,
the `:decoder`/`:trace_decoder`/`:decoded_trace` chain, and (with `main.cc`) the
1853-line `decoder.h` checked in here. That is roughly 3500 lines out, and the
test port is the only real work in it.

**Nothing tests `trace_wire.h` directly.** Its checks today are that
`entry-layout-run` decodes and that the counts match. The module has no test
target at all.

**Old snapshots do not decode.** `sched-group-run`, `boot-id-run`, `wrapped-run`
and `latte-run` were all written by tracers older than contracts 1 and 2. The
numbers taken on them are kept in `WORKING.md` as a record of what was measured
when, and they cannot be reproduced without recapturing.

**The tracer's own tracepoints must agree across builds.** The metadata stream is
read by *position* -- a clock sync, a count, that many load events -- because
until it has been read no address means anything, so which reader each position
wants is fixed when the plugin is generated. Two builds whose `trace_object_loaded`
differs would want two answers and there is nowhere to put the second, so the
decoder refuses by name. In practice this bites only while the tracer's own
tracepoints are being changed, and the fix if it ever matters is to select the
prologue's readers per file, from the build ID in its `.metadata.json`.

**One entry layout is supported, not a set.** Adding a second would mean choosing
between them per object -- the identifier check is already the evidence that
would decide it -- but nothing needs it yet, and the loud refusal is better than
a wrong guess.

**`main.cc` still reads the checked-in `decoder.h`.** It is the old viewer, it is
not ported, and it has to have that file replaced by hand from a snapshot that
carries one.

---

## Verifying a change here

Two checks, both cheap, and between them they caught everything that went wrong
while this was written.

**Against the demo, which needs nothing but this repo.** `modules/tracer`'s
producer emits a trace and a `dsos/` in the current format, *and* a decoder
generated in-process from the same tables. Point the viewer at them and compare
the generated `plugin.cc`'s object table, id numbering, static-id switch and
timestamp dispatch with the generated `decoder.h`'s. They should be identical,
line for line. If the ELF reader is wrong, this is where it shows.

```sh
buck2 build //modules/tracer:trace_bin //modules/tracer:dso_dir //modules/tracer:decoder_src
```

**Against Scylla, decoded both ways.** Capture with
`third-party/scylladb/capture-trace.sh`, which writes a `decoder.h` beside each
node's traces. Run the current viewer headless, then `git stash` your change,
run it again, and diff the output: every count should match to the digit. That
is how the rewrite was verified -- 2239 requests, 10135 parts, 503 of 503 source
locations, 97117 of 97123 task queue runs closed, 575093 rectangles, identical
both ways.
