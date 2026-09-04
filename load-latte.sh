#!/usr/bin/env bash
# The load3.py workload at load-generator speed: millions of CL=ALL reads,
# driven by latte (https://github.com/pkolaczk/latte) against the three nodes
# capture-trace.sh started.
#
#   nix develop -c ./capture-trace.sh ignored/latte-run --load ./load-latte.sh
#
# Environment:
#   LATTE     the latte binary (default: whatever is on PATH)
#   CYCLES    read cycles in the measured phase (default 2000000)
#   ROWS      rows inserted before it (default 100000)
#   THREADS / CONCURRENCY   latte's client-side parallelism
#
# The three phases are not equal as far as the trace is concerned:
#
#   schema + load   inserts ROWS rows, then flushes them to sstables so the
#                   reads afterwards have somewhere to go.  Nothing here is
#                   worth tracing, and at 32 MiB of debug ring per shard it
#                   would be evicted by the read phase regardless -- so the
#                   tracepoints are switched OFF for it and back on after.
#                   That leaves a snapshot whose oldest records are reads.
#   run             the point of the exercise.  CYCLES reads, as fast as the
#                   cluster will take them, which is what fills the rings.
#
# capture-trace.sh turns the tracepoints on before calling this and takes the
# snapshot after, so this script must hand them back on.
set -euo pipefail
cd "$(dirname "$0")"

LATTE="${LATTE:-latte}"
CYCLES="${CYCLES:-2000000}"
ROWS="${ROWS:-100000}"
THREADS="${THREADS:-4}"
CONCURRENCY="${CONCURRENCY:-128}"

NODES=(127.11.11.1 127.11.11.2 127.11.11.3)
WORKLOAD=latte-tr.rn

tracepoints() {  # tracepoints true|false
    for host in "${NODES[@]}"; do
        curl -sf -X POST "http://$host:10000/system/tracepoints_enabled?enabled=$1" >/dev/null
    done
    echo "== tracepoints $1"
}

# latte writes a JSON report per run into the working directory; keep them out
# of the source tree.
mkdir -p ignored/latte
cd ignored/latte

tracepoints false

echo "== schema"
"$LATTE" schema "../../$WORKLOAD" "${NODES[@]}" -P "rows=$ROWS"

echo "== load $ROWS rows"
"$LATTE" load "../../$WORKLOAD" "${NODES[@]}" -P "rows=$ROWS" --quiet

# Same reason load3.py flushes: BYPASS CACHE only reaches disk if the rows are
# on disk, and until this they are in the memtable.
for host in "${NODES[@]}"; do
    echo "== flush $host: $(curl -sf -X POST "http://$host:10000/storage_service/keyspace_flush/latte_tr" -o /dev/null -w '%{http_code}')"
done

tracepoints true

echo "== run $CYCLES reads"
"$LATTE" run "../../$WORKLOAD" "${NODES[@]}" -P "rows=$ROWS" \
    --consistency ALL -d "$CYCLES" -t "$THREADS" -p "$CONCURRENCY" \
    --warmup 10000
