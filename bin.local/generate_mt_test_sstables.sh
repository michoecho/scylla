#!/usr/bin/env bash
# Convert `ms` test sstables to `mt` format for failing sstable tests.
set -euo pipefail

SCYLLA=build/dev/scylla
COMMON_ARGS=(
    sstable upgrade
    --preserve-generation=on
    --sstable-version=mt
    --preserve-output-dir=on
)

# Each entry is "<sstable-path>[|<schema-file>]". The schema file is needed
# for sstables that scylla-sstable can't autodetect a schema for (e.g.
# COMPACT STORAGE tables).
FILES=(
    test/resource/sstables/large_partition/try1/data-1c6ace40fad111e7b9cf000000000002/ms-3-big-Data.db
    test/resource/sstables/multi_schema_test/test/test_multi_schema-1c6ace40fad111e7b9cf000000000002/ms-1-big-Data.db
    test/resource/sstables/sliced_mutation_reads/ks/sliced_mutation_reads_test-1c6ace40fad111e7b9cf000000000002/ms-1-big-Data.db
    test/resource/sstables/wrong_range_tombstone_order/ks/wrong_range_tombstone_order-1c6ace40fad111e7b9cf000000000002/ms-1-big-Data.db|test/resource/sstables/wrong_range_tombstone_order/ks/wrong_range_tombstone_order-1c6ace40fad111e7b9cf000000000002/schema.cql
    test/resource/sstables/counter_test/ks/counter_test-1c6ace40fad111e7b9cf000000000002/ms-5-big-Data.db
    test/resource/sstables/promoted_index_read/ks/promoted_index_read-1c6ace40fad111e7b9cf000000000002/ms-1-big-Data.db
    test/resource/sstables/partition_skipping/ks/test_skipping_partitions-1c6ace40fad111e7b9cf000000000002/ms-1-big-Data.db
    test/resource/sstables/wrong_counter_shard_order/scylla_bench/test_counters-1c6ace40fad111e7b9cf000000000002/ms-2-big-Data.db
)

for entry in "${FILES[@]}"; do
    f="${entry%%|*}"
    schema=""
    if [[ "$entry" == *"|"* ]]; then
        schema="${entry#*|}"
    fi
    echo "=== Converting $f ==="
    extra_args=()
    if [[ -n "$schema" ]]; then
        extra_args+=(--schema-file "$schema")
    fi
    ./cr "$SCYLLA" "${COMMON_ARGS[@]}" "${extra_args[@]}" "$f"
done

