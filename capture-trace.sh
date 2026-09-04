#!/usr/bin/env bash
# Capture one distributed trace snapshot: three nodes of two shards each, a
# CL=ALL read/write load across them, and every node's rings written out
# together with the objects their addresses point into.
#
#   nix develop -c ./capture-trace.sh [OUTDIR]
#
# OUTDIR defaults to ignored/run-<stamp> and comes out in the shape the viewer
# is handed -- the same as ignored/sched-group-run:
#
#   OUTDIR/node1/decoder.h, <uuid>.trace, <uuid>.metadata.json ...
#   OUTDIR/node2/...
#   OUTDIR/node3/...
#   OUTDIR/dsos/.build-id/...
#
#   TRACE_DSO_DIR=OUTDIR/dsos buck2 run //modules/trace-viewer:viewer -- \
#       OUTDIR/node1 OUTDIR/node2 OUTDIR/node3
#
# The nodes keep their workdirs between runs, because bootstrapping three of
# them from nothing is minutes and the load is idempotent.  --fresh wipes them
# for a run that must not see another one's sstables.
#
# Tracepoints start disabled and are switched on only around the load, so what
# lands in the rings is the load and not the bootstrap.  Forgetting that is the
# failure to expect, and it is silent: the snapshot succeeds and every .trace
# file is ~117 bytes, which is the metadata prologue and no records.
set -euo pipefail
cd "$(dirname "$0")"

NODES=(1 2 3)
OUT="ignored/run-$(date +%Y%m%d-%H%M%S)"
FRESH=0
for arg in "$@"; do
    case "$arg" in
        --fresh) FRESH=1 ;;
        *) OUT="$arg" ;;
    esac
done

pids=()
cleanup() {
    for pid in "${pids[@]:-}"; do
        kill "$pid" 2>/dev/null || true
    done
    for pid in "${pids[@]:-}"; do
        wait "$pid" 2>/dev/null || true
    done
}
trap cleanup EXIT

api() {  # api <node> <method> <path...>
    local n="$1" method="$2"; shift 2
    curl -sf -X "$method" "http://127.11.11.$n:10000$*"
}

for n in "${NODES[@]}"; do
    if [[ $FRESH == 1 ]]; then
        rm -rf "ignored/workdir_0$n"
    fi
done

# One at a time, and node 1 first: it is the seed, and a node that starts
# before its seed answers CQL only once it has found one.
for n in "${NODES[@]}"; do
    echo "== starting node $n"
    ./run-node.sh "$n" >"ignored/node$n.log" 2>&1 &
    pids+=($!)
    for _ in $(seq 300); do
        if [[ "$(api "$n" GET /storage_service/native_transport 2>/dev/null)" == "true" ]]; then
            break
        fi
        sleep 1
    done
    if [[ "$(api "$n" GET /storage_service/native_transport 2>/dev/null)" != "true" ]]; then
        echo "node $n did not come up; see ignored/node$n.log" >&2
        exit 1
    fi
done

for n in "${NODES[@]}"; do
    echo "== tracepoints on, node $n: $(api "$n" POST '/system/tracepoints_enabled?enabled=true')"
done

./load3.py

for n in "${NODES[@]}"; do
    # The snapshot is a copy of the rings as they are when the shard is asked,
    # so it may be taken with the tracepoints still on; switching them off
    # first would only lose the records the switch itself makes.
    dir="$(api "$n" POST /system/trace_snapshot | tr -d '"')"
    echo "== node $n snapshot: $dir"
    mkdir -p "$OUT/node$n"
    cp "$dir"/* "$OUT/node$n/"
    api "$n" POST '/system/tracepoints_enabled?enabled=false' >/dev/null
done

# From the binary the trace came from, and before it is rebuilt: a rebuild gives
# it a new build ID, and the trace names the one it ran with.
../../tools/gather-dsos build/Dev/scylla "$OUT/dsos" --strip-debug

echo
echo "wrote $OUT"
du -sh "$OUT"
