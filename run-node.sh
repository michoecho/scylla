#!/usr/bin/env bash
# Single-node Scylla for the tracing prototype.  N defaults to 1, and picks
# both the workdir and the 127.11.11.N address the node listens on.
set -euo pipefail
N="${1:-1}"
cd "$(dirname "$0")"
mkdir -p "ignored/workdir_0$N"
exec build/Dev/scylla \
    --workdir="ignored/workdir_0$N" --memory=2G --smp=2 --developer-mode=1 \
    --listen-address="127.11.11.$N" --rpc-address="127.11.11.$N" --api-address="127.11.11.$N" \
    --seed-provider-parameters seeds="127.11.11.1" \
    --kernel-page-cache=1 --unsafe-bypass-fsync=1 --overprovisioned --num-tokens=16
