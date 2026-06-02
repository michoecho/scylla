#!/usr/bin/env python3
"""Convert `ms` test sstables to a newer format for failing sstable tests."""

import argparse
import subprocess
import sys
from pathlib import Path

SCYLLA = "build/dev/scylla"

# Each entry is (sstable_path, schema_file_or_None). The schema file is needed
# for sstables that scylla-sstable can't autodetect a schema for (e.g.
# COMPACT STORAGE tables).
FILES = [
    ("test/resource/sstables/large_partition/try1/data-1c6ace40fad111e7b9cf000000000002/ms-3-big-Data.db", None),
    ("test/resource/sstables/multi_schema_test/test/test_multi_schema-1c6ace40fad111e7b9cf000000000002/ms-1-big-Data.db", None),
    ("test/resource/sstables/sliced_mutation_reads/ks/sliced_mutation_reads_test-1c6ace40fad111e7b9cf000000000002/ms-1-big-Data.db", None),
    ("test/resource/sstables/wrong_range_tombstone_order/ks/wrong_range_tombstone_order-1c6ace40fad111e7b9cf000000000002/ms-1-big-Data.db",
     "test/resource/sstables/wrong_range_tombstone_order/ks/wrong_range_tombstone_order-1c6ace40fad111e7b9cf000000000002/schema.cql"),
    ("test/resource/sstables/counter_test/ks/counter_test-1c6ace40fad111e7b9cf000000000002/ms-5-big-Data.db", None),
    ("test/resource/sstables/promoted_index_read/ks/promoted_index_read-1c6ace40fad111e7b9cf000000000002/ms-1-big-Data.db", None),
    ("test/resource/sstables/partition_skipping/ks/test_skipping_partitions-1c6ace40fad111e7b9cf000000000002/ms-1-big-Data.db", None),
    ("test/resource/sstables/wrong_counter_shard_order/scylla_bench/test_counters-1c6ace40fad111e7b9cf000000000002/ms-2-big-Data.db", None),
]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "format_version",
        help="Target sstable format version (e.g. mt, mu).",
    )
    args = parser.parse_args()

    common_args = [
        "sstable", "upgrade",
        "--preserve-generation=on",
        f"--sstable-version={args.format_version}",
        "--preserve-output-dir=on",
    ]

    for sstable, schema in FILES:
        print(f"=== Converting {sstable} ===", flush=True)
        extra_args = ["--schema-file", schema] if schema else []
        cmd = [SCYLLA, *common_args, *extra_args, sstable]
        subprocess.run(cmd, check=True)

    return 0


if __name__ == "__main__":
    sys.exit(main())
