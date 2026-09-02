# Handover: parallelising the Intel PT decoder

Branch `pt-parallel-decode` in `third-party/linux`, two commits:

- `d2831b77fd19` "perf intel-pt: decode one trace with several threads" --
  the decode split itself.
- `3fa8259a13fa` "perf intel-pt: deliver a parallel decode in order" -- ordered
  delivery, which is what this document is now mostly about.

The nix `patchPhase` edits to `scripts/`, `pmu-events/` etc. are still
uncommitted and unrelated.

Build: `nix develop -i nixpkgs-stable#perf --inputs-from ~/projects/cpp_template --build`
run from `third-party/linux/tools/perf`. ~4s incremental.

## Where the time goes

On `tools/pt_repro/preempt.perf.data` (12MB of PT) with `--itrace=bei0ns`,
measured with a null dlfilter (source at the bottom):

| stage | serial time |
|---|---|
| decode only (`--itrace=e`) | 0.9 s |
| + sample synthesis (delivery stubbed out) | 8.5 s |
| + delivery and the filter call | 14.6 s |

So synthesis is ~7.6s and delivery ~6.1s. **Where the parallel/serial cut is
made decides the ceiling**, and delivery is far too expensive to leave serial:
cutting at `perf_session__deliver_synth_event` caps out at about 2.3x. The cut
is therefore made at the filter call itself.

A profile of the serial run says the 6.1s is spread over
`dlfilter__do_filter_event` 5%, `machines__deliver_event` 4%, `evsel__get`/`put`
8%, `process_sample_event` 2.3%, `addr_location__init`/`exit` 3%,
`evlist__id2evsel`/`id2sid` 2.3%, and `dump_sample` 1.8% -- all of it work a
worker can do.

## Result

`--itrace=...jN` (bare `j` = one job per online CPU).

| jobs | wall | vs serial | peak RSS |
|---|---|---|---|
| serial | 14.6 s | 1.0x | 30 MB |
| 2 | 8.4 s | 1.7x | 68 MB |
| 4 | 4.2 s | 3.5x | 108 MB |
| 8 | 2.2 s | 6.5x | 290 MB |
| 16 | **1.8 s** | **8.3x** | 557 MB |
| 32 | 2.3 s | 6.5x | 557 MB |

With the real perf2perfetto dlfilter: 15.9s serial, 2.6s at j=16.

j=1 is 17.5s, i.e. *slower* than serial: that is the per-chunk warmup, which is
pure overhead at one job and hidden at four or more.

`perf script --itrace=bei0ns` output is **byte for byte identical** to the
serial decode, all 146.5M samples, and so is the `.ftf` perf2perfetto writes.
That is the correctness check to use -- the sample *count* matching (which is
what the first commit was tested against) misses tid, period, insn_cnt and
timestamp bugs, all of which were present and are now fixed.

## How it works

All in `util/intel-pt.c` unless noted.

**Splitting.** `intel_pt_stream_map()` maps every buffer of the queue once;
`intel_pt_stream_index_psbs()` scans for the 16-byte PSB pattern;
`intel_pt_plan_chunks()` cuts the stream into ~32KB chunks on PSB boundaries.
Chunks are the unit of work *and* of buffering, so there are many more of them
than there are jobs -- 380 for this trace.

Each chunk decodes from `INTEL_PT_WARMUP_PSBS` (4) PSBs before its own bytes so
the return-compression stack is rebuilt, then from `INTEL_PT_PRIME_SZ` (1KB)
before them it also *synthesizes* samples and throws them away. Priming exists
because period/insn_cnt/cyc_cnt are deltas since the last sample and warmup
synthesizes nothing, so without it the first sample of every chunk had a
garbage period. Then it emits, and stops at the next chunk's first PSB.

**Scheduling.** Workers take chunks from a shared cursor (`intel_pt_claim_chunk`),
so a slow chunk does not idle a worker. A worker may run at most
`INTEL_PT_INFLIGHT_PER_JOB` (4) x jobs chunks ahead of the main thread, and
stops allocating once buffered output passes a byte budget (32MB per job, capped
at 512MB). **The chunk the main thread is draining is exempt from the budget**
-- that is what makes the scheme deadlock-free, since that chunk always
progresses and its blocks are always being freed.

Blocks are 64KB and are *recycled* through `sched->free_blocks` rather than
freed. With 32 threads allocating and one freeing, returning them to glibc cost
~1.7x in RSS.

**Buffering.** A `struct intel_pt_rec` is 64 bytes: the fields that differ from
one sample to the next. Everything else -- id, cpu, event name, the ~20 fields
that are always zero -- is reconstructed on replay. At ~12 samples per trace
byte, the whole decode would be ~9.4GB of these, which is why the run-ahead
window exists. Sample kinds that need more (ptwrite, power, PSB, iflag) and
error events are stored whole as `INTEL_PT_REC_FAT`/`_EVENT`; they are rare.

**Replay.** `intel_pt_replay()` on the main thread drains chunks in order,
following the producer at block granularity so it can start before a chunk is
finished. Per record it writes ~10 fields into a `perf_sample` and a
`perf_dlfilter_sample` that were built once per sample kind, and calls the
filter through the new `dlfilter__filter_prepared()`. If the filter *keeps* the
sample the record goes the long way round through
`perf_session__deliver_synth_event()`, with `dlfilter_early_done` set so the
early filter is not called twice. With no dlfilter at all everything takes the
long way and the speedup is much smaller -- correct, just not fast.

`dlfilter__do_filter_event()` is split into `dlfilter__prep_sample()` (build the
filter's view) and `dlfilter__filter_prepared()` (call it), and
`dlfilter__current()` hands intel-pt the filter perf script is running.

**Threads.** `struct thread` cannot be created from several threads at once --
`comm_strs__findnew()` interns into a shared table, and ASAN caught it doing so
concurrently. Every thread a worker might need is therefore made before any
worker runs (`intel_pt_sched_resolve_tids()`,
`intel_pt_shard_clone_threads()`), one clone per tid per worker over the real
thread's maps, and `thread_stack__free()` empties the call stacks between
chunks.

**tid tracking.** With `have_sched_switch` the tid on a cpu changes as the trace
is decoded, and `intel_pt_set_pid_tid_cpu()` reads it off the machine. A
parallel decode has processed all the sideband before it starts, so
`intel_pt_switch_tid()` logs every change with its timestamp and
`intel_pt_shard_follow_tid()` looks the answer up as the worker's timestamp
advances.

Refused (falls back to serial, `pr_debug` says why): snapshot/sampling mode,
timeless decoding, sync_switch, VM time correlation, `--time` ranges, callchain
or last-branch synthesis, injected events, guest sideband, a gap in the stream,
more than one queue with data, a queue with no fixed tid.

### The de-contention work from the first commit, still load-bearing

Threads alone gave zero speedup. Borrowed evsel (`util/sample.c`), the per-queue
map cache (`intel_pt_ptq_find_map()`), thread-local dlfilter context
(`util/dlfilter.[ch]`) and the thread-local DSO chunk cache (`util/dso.c`) are
all still needed; see `d2831b77fd19`'s message. The per-thread synthesized-event
counters it added are *not* -- ordered replay counts them on one thread -- so
`perf_session__set_synth_stats()` and `events_stats__add()` were removed again.

### Tunables

`INTEL_PT_CHUNK_SZ`, `INTEL_PT_WARMUP_PSBS` and the budget can be overridden
with `PERF_INTEL_PT_CHUNK_SZ`, `PERF_INTEL_PT_WARMUP_PSBS`,
`PERF_INTEL_PT_PRIME_SZ` and `PERF_INTEL_PT_BUDGET_MB`. These were how the
defaults were chosen and are useful for a trace with a different sample yield.
**They should probably become `perf_config` entries** (`intel-pt.cache-divisor`
is the precedent) before this goes anywhere near upstream.

What the tuning found:

- Chunk size trades warmup against buffer footprint. 16KB: more warmup, 1.4GB at
  j=32. 128KB: little warmup but a chunk's output no longer fits the budget and
  workers convoy. 32KB is the knee.
- The budget has to hold roughly one chunk's output per worker. Below that,
  workers stall mid-chunk holding memory that no one can drain, only the head
  chunk progresses, and j=32 collapses to 8s.
- Warmup 1 PSB was enough for exact output on this trace; 8 cost 2x at j=1. 4 is
  a margin, not a measured requirement.

## THE OPEN PROBLEM: tid at a context switch

`tools/pt_repro/check.py`'s **preempt** scenario fails: the tree has 9 `TRACE`
roots instead of 1. syscall, tailcall and plt pass.

The cause is visible in the text diff on a freshly recorded preempt capture --
around a context switch, a run of samples is attributed to the tid that was
running *before* the switch where the serial decoder has already moved to the
new one. So the timeline in `intel_pt_switch_tid()` /
`intel_pt_shard_follow_tid()` is not yet an exact model of when the serial
decoder applies a switch.

The serial rule, as far as it was worked out: `intel_pt_process_event()` calls
`intel_pt_process_queues(pt, tsc)` *before* handling the switch, and
`intel_pt_set_pid_tid_cpu()` runs once per resumption, so a sample is emitted
with the tid established by every switch event strictly earlier in the stream --
i.e. the last log entry with `tsc <= ptq->timestamp`, which is what the code
implements. Something about that is wrong; suspects are switches where
`intel_pt_sync_switch()` returns <= 0 and no entry is logged at all,
`intel_pt_context_switch_in()`'s early return when the tid already matches, and
whether `perf_time_to_tsc(sample->time)` is the same clock the comparison
assumes.

Note the stored `tools/pt_repro/*.perf.data` are **rewritten by check.py**, so a
benchmark baseline taken before running it does not compare against one taken
after. The 12MB preempt capture used above has 146,535,164 samples at
`bei0ns`; the one check.py recorded later has 143,621,435.

## Other loose ends

- **Chunk-boundary timestamps.** At some boundaries a handful of samples get a
  timestamp one unit different from serial, because the decoder's byte position
  runs ahead of the samples it is still draining from a TNT, so samples whose
  packets precede a PSB are attributed to the chunk after it. It showed up at
  16KB chunks and not at 32KB or 64KB, i.e. it depends on where the cut lands.
  The exact fix is for the decoder to report the position of the *packet* a
  state came from rather than `decoder->pos`.
- **j=32 is slower than j=16.** The replay thread saturates around 1.8s; beyond
  that more workers only add contention.
- `perf_sample_borrows_evsel` is set globally once parallel decoding is used and
  never cleared.
- Multi-queue traces (system-wide, several threads) fall back to serial. Each
  queue's chunks are already ordered internally, so a k-way merge by timestamp
  across queues would do it, on top of the machinery that now exists.
- `tools/pt-trace` grew `--jobs/-j` (default: one per online CPU) and appends
  the `j` suffix to the `--itrace` spec it builds. It still defaults `--perf` to
  whatever is on `$PATH`, so check.py needs the tree's perf ahead of it:
  `ln -s .../tools/perf/perf /tmp/bin/perf; PATH=/tmp/bin:$PATH tools/pt_repro/check.py`.
  Giving pt-trace a `PT_TRACE_PERF` env default would be kinder.

## Verifying

The check that matters, and the one that found every bug after the first:

    D=tools/pt_repro/preempt.perf.data
    cmp <(perf script -i $D --itrace=bei0ns) <(perf script -i $D --itrace=bei0nsj16)

~3 minutes, must be identical. The `.ftf` comparison is a weaker but much faster
proxy:

    for j in "" j16; do
      perf script -i $D --itrace=bei0ns$j --dlfilter libperf2perfetto.so \
          --dlarg out$j.ftf --dlarg c >/dev/null
    done
    cmp out.ftf outj16.ftf

Sample counts alone (`--dlfilter null-dlfilter.so`) catch slicing and gating
bugs only.

ASAN found the two real memory bugs and is worth keeping to hand:

    mkdir -p /tmp/asan && nix develop -i nixpkgs-stable#perf --inputs-from ~/projects/cpp_template --command \
      make -j16 O=/tmp/asan WERROR=0 ARCH=x86_64 NO_GTK2=1 \
        CC="gcc -fsanitize=address -fno-omit-frame-pointer" EXTRA_CFLAGS="-g -O1"

`EXTRA_LDFLAGS=-fsanitize=address` does *not* work -- it reaches the `ld -r`
partial links -- hence overriding `CC`.

### null-dlfilter.c

Per-thread counters folded in on thread exit; an atomic counter makes the
harness itself the contention being measured.

```c
#include <perf/perf_dlfilter.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>

static _Atomic unsigned long long total;
static pthread_key_t key;
static pthread_once_t once = PTHREAD_ONCE_INIT;

static void fold(void *p) { total += *(unsigned long long *)p; free(p); }
static void make_key(void) { pthread_key_create(&key, fold); }

int filter_event_early(void *data, const struct perf_dlfilter_sample *s, void *ctx)
{
	unsigned long long *n;
	(void)data; (void)s; (void)ctx;
	pthread_once(&once, make_key);
	n = pthread_getspecific(key);
	if (!n) { n = calloc(1, sizeof(*n)); pthread_setspecific(key, n); }
	++*n;
	return 1;
}

int stop(void *data, void *ctx)
{
	unsigned long long *n;
	(void)data; (void)ctx;
	pthread_once(&once, make_key);
	n = pthread_getspecific(key);
	if (n) { total += *n; *n = 0; }
	fprintf(stderr, "null-dlfilter: %llu samples\n", total);
	return 0;
}
```

    gcc -c -I third-party/linux/tools/perf/include -fpic -O2 null-dlfilter.c -o null-dlfilter.o
    gcc -shared -o null-dlfilter.so null-dlfilter.o -lpthread
