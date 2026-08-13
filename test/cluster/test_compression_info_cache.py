#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import asyncio
import logging
import random
import re

from cassandra.query import SimpleStatement

from test.pylib.manager_client import ManagerClient

logger = logging.getLogger(__name__)

# The size of the tables written by this test. Big enough to be spread over many
# compression chunks (the chunk length is 4 kiB), small enough to be covered by a
# single bucket of chunk offsets: with 4 kiB chunks, one bucket covers ~2 MiB of
# uncompressed data. The reads below check that they only ever touch bucket 0.
N_PARTITIONS = 64
VALUE_SIZE = 4096

# An arbitrary partition. All the reads below read this one, so that they all
# read the same sstable (with tablets, different partitions can live in
# different tablets, and thus in different sstables) and the same bucket.
CHOSEN_PK = 0


async def drop_data_caches(manager: ManagerClient, servers) -> None:
    """Drops the caches which could otherwise serve a read without touching the
    Data.db: the row cache and the cached index files.

    Notably, this does *not* drop the compression info cache, which is exactly
    what makes it useful here: after it, a read is guaranteed to go to the
    Data.db, and thus to ask for the chunk offsets, but whether it has to read
    them from CompressionInfo.db depends only on the compression info cache.
    """
    await asyncio.gather(*[manager.api.drop_sstable_caches(s.ip_addr) for s in servers])


async def populate(cql, manager: ManagerClient, servers, ks: str, cf: str) -> None:
    """Creates a compressed table, fills it, and flushes it to a fresh sstable."""
    await cql.run_async(
        f"CREATE TABLE {ks}.{cf} (pk int PRIMARY KEY, v blob) "
        f"WITH compression = {{'sstable_compression': 'LZ4Compressor', 'chunk_length_in_kb': 4}};"
    )
    insert = cql.prepare(f"INSERT INTO {ks}.{cf} (pk, v) VALUES (?, ?);")
    for pk in range(N_PARTITIONS):
        await cql.run_async(insert, (pk, random.randbytes(VALUE_SIZE)))
    await asyncio.gather(*[manager.api.keyspace_flush(s.ip_addr, ks, cf) for s in servers])
    # A memtable flush also populates the row cache with the flushed data, so
    # without this the reads wouldn't touch the sstable at all.
    await drop_data_caches(manager, servers)


def read_partition(cql, ks: str, cf: str, bypass_cache: bool) -> tuple[int, int]:
    """Reads the chosen partition with tracing on, and returns the number of
    compression info cache misses and hits the read caused."""
    bypass = " BYPASS CACHE" if bypass_cache else ""
    stmt = SimpleStatement(f"SELECT pk, v FROM {ks}.{cf} WHERE pk={CHOSEN_PK}{bypass};")
    result = cql.execute(stmt, trace=True)
    assert result.one().pk == CHOSEN_PK

    misses = 0
    hits = 0
    for event in result.get_query_trace().events:
        if "compression info cache" not in event.description:
            continue
        logger.debug(f"Trace event: {event.description}")
        # If a read touched a bucket other than 0, the table is bigger than this
        # test assumes, and the counts below don't mean what the test thinks.
        bucket = re.search(r"bucket=(\d+)", event.description)
        assert bucket and bucket.group(1) == "0", f"unexpected bucket in {event.description!r}"
        if "compression info cache miss" in event.description:
            misses += 1
        elif "compression info cache hit" in event.description:
            hits += 1
    return misses, hits


async def test_compression_info_cache_read_path(manager: ManagerClient) -> None:
    """Checks, via tracing, when the chunk offsets of a compressed Data.db are
    read from CompressionInfo.db and when they are served from the cache:
    a fresh sstable starts out with its compression info uncached, a regular
    read caches it, and a BYPASS CACHE read neither uses nor populates the cache.
    """
    cassandra_logger = logging.getLogger('cassandra')
    cassandra_logger.setLevel(logging.INFO)

    ks = "ks"

    # `--smp=1` because this test uses CQL tracing. Trace events are written to
    # trace tables asynchronously w.r.t. the traced statements, and the Python
    # driver's polling mechanism for traces is only reliable if the entire
    # statement runs on a single shard (it only waits for the coordinator, not
    # the replicas, to write their events). We aren't testing any multi-shard
    # mechanisms here anyway.
    servers = await manager.servers_add(1, cmdline=['--smp=1'], config={
        # The offsets have to be evictable for them to be read on demand at all,
        # and the cache needs an allowance, or else a bucket could be evicted as
        # soon as it stops being used.
        'compressioninfo_is_evictable': True,
        'compressioninfo_cache_fraction': 0.1,
    })
    cql = manager.get_cql()

    await cql.run_async(
        f"CREATE KEYSPACE {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}};"
    )

    # A fresh sstable starts out with no compression info cached, so the first
    # read of it has to read the offsets from CompressionInfo.db...
    await populate(cql, manager, servers, ks, "t1")
    misses, hits = read_partition(cql, ks, "t1", bypass_cache=False)
    assert misses > 0, "compression info of a fresh sstable was already cached"
    assert hits == 0

    # ...and a regular read caches them, so the next read of the same data
    # doesn't have to read them again, even though it does read the Data.db.
    await drop_data_caches(manager, servers)
    misses, hits = read_partition(cql, ks, "t1", bypass_cache=False)
    assert misses == 0, "compression info was re-read despite being cached by a regular read"
    assert hits > 0, "the read didn't use the compression info cache at all"

    # A second table, to check the BYPASS CACHE behaviour against a cold cache.
    await populate(cql, manager, servers, ks, "t2")

    # A BYPASS CACHE read reads the offsets into a bucket private to the read,
    # so it neither hits nor populates the shared cache -- every such read reads
    # them from CompressionInfo.db again.
    for _ in range(2):
        misses, hits = read_partition(cql, ks, "t2", bypass_cache=True)
        assert misses > 0, "BYPASS CACHE read used the shared compression info cache"
        assert hits == 0, "BYPASS CACHE read used the shared compression info cache"
        await drop_data_caches(manager, servers)

    # And it left the shared cache cold, so the next regular read still has to
    # read the offsets, while the one after it doesn't.
    misses, hits = read_partition(cql, ks, "t2", bypass_cache=False)
    assert misses > 0, "BYPASS CACHE read populated the shared compression info cache"
    assert hits == 0

    await drop_data_caches(manager, servers)
    misses, hits = read_partition(cql, ks, "t2", bypass_cache=False)
    assert misses == 0, "compression info was re-read despite being cached by a regular read"
    assert hits > 0, "the read didn't use the compression info cache at all"

    manager.driver_close()
