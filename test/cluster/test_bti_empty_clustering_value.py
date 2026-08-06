#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""Reproducer for the BTI encoder's mishandling of "empty" clustering key values.

An "empty" value -- a zero-sized value of a CQL type which normally has a fixed
size (int, bigint, varint, ...) -- is a legal value of a clustering key column,
and CQL will happily store one. The BTI byte-comparable encoder has no encoding
for it: comparable_bytes_from_compound() passes the zero-sized buffer straight
to the per-type encoders, which don't expect it. What that does depends on the
type; `varint` is used here because it dereferences an empty fragment and takes
the node down with SIGSEGV, which is unambiguous to detect.

The node dies while *writing* the sstable, in bti_row_index_writer::add() --
building the Rows.db index is the first thing that needs the clustering
positions in byte-comparable form -- so the test fails with the driver losing
its connection rather than with a failed assertion.
"""
import logging

from test.pylib.manager_client import ManagerClient

logger = logging.getLogger(__name__)


async def test_bti_empty_clustering_key_value(manager: ManagerClient) -> None:
    # The test cluster writes mt-format (BTI) sstables by default.
    #
    # `column_index_size_in_kb` is lowered so that a modest partition is enough
    # to get its clustering positions into the Rows.db index -- that index is
    # what needs the byte-comparable encoding. A partition small enough to fit
    # in a single index block never encodes a clustering position at all, and
    # doesn't reproduce the bug -- two rows over the limit are enough.
    server = await manager.server_add(config={'column_index_size_in_kb': 1})
    cql = manager.get_cql()

    await cql.run_async(
        "CREATE KEYSPACE ks WITH replication = "
        "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
    )
    await cql.run_async("CREATE TABLE ks.t (pk int, ck varint, v blob, PRIMARY KEY (pk, ck))")

    # Two rows, each over `column_index_size_in_kb`, so that the partition spans
    # more than one index block. The first holds the "empty" value --
    # `blobAsVarint(0x)`, a varint of zero length -- in its clustering key.
    insert_empty = cql.prepare(
        "INSERT INTO ks.t (pk, ck, v) VALUES (0, blobAsVarint(0x), ?)")
    insert = cql.prepare("INSERT INTO ks.t (pk, ck, v) VALUES (0, ?, ?)")
    await cql.run_async(insert_empty, [bytes(2048)])
    await cql.run_async(insert, [1, bytes(2048)])

    # Writing the partition to an sstable builds the BTI index over its
    # clustering positions, which is where the empty value has to be encoded.
    await manager.api.keyspace_flush(server.ip_addr, "ks", "t")

    # BYPASS CACHE so that the read goes through the sstable index rather than
    # being served from the memtable or the row cache.
    rows = await cql.run_async("SELECT pk, ck FROM ks.t WHERE pk = 0 BYPASS CACHE")
    assert len(rows) == 2, f"expected 2 rows back, got {len(rows)}"
