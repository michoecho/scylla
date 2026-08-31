# Scylla trace converter

`trace_converter_bin` converts a Scylla trace snapshot into Perfetto's native
binary `Trace` protobuf. The output can be opened directly in the Perfetto UI
or passed to `trace_processor_shell`.

```sh
buck2 build //modules/trace-converter:trace_converter_bin
buck2 run //modules/trace-converter:trace_converter_bin -- \
    /path/to/traces/<stamp> /tmp/scylla.perfetto-trace
```

The decoder header in `modules/trace-viewer/decoder.h` is generated from the
Scylla binary's tracepoint table. Replace that header when Scylla's tracepoints
change, just as for the trace viewer.

The output contains exactly one track per shard. Each track has the scheduler
slices plus all decoded events for that shard. Scheduler-start events such as
`run_task` span the complete task slice they start. Task IDs, I/O IDs, source
locations, and stack frames remain available as event details without creating
additional tracks or flows.
