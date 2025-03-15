# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

import asyncio
import random
import logging
import pytest
import itertools
from test.pylib.manager_client import ManagerClient, ServerInfo
from test.pylib.rest_client import read_barrier
from cassandra.cluster import ConsistencyLevel
from test.pylib.rest_client import ScyllaMetrics

logger = logging.getLogger(__name__)

async def get_metrics(manager: ManagerClient, servers: list[ServerInfo]) -> list[ScyllaMetrics]:
    return await asyncio.gather(*[manager.metrics.query(s.ip_addr) for s in servers])
def dict_memory(metrics: list[ScyllaMetrics]) -> int:
    return sum([m.get("scylla_sstable_compression_dicts_total_live_memory_bytes") for m in metrics])

async def test_retrain_dict(manager: ManagerClient):
    """
    Tests basic functionality of SSTable compression with shared dictionaries.
    - Creates a table.
    - Populates it with artificial data which compresses extremly well with dicts but extremely badly without dicts.
    - Calls retrain_dict to retrain the recommended dictionary for that table.
    - For both supported algorithms (lz4 and zstd):
        - Rewrites the existing sstables using the new dictionary.
        - Checks that sstable sizes decreased greatly after the rewrite.
        - Checks that the rewritten files are readable.
    - Checks that the recommended dictionary isn't forgotten after a reboot.
    - Checks that dictionaries are cleared after the corresponding table is dropped.
    """
    # Bootstrap cluster and configure server
    logger.info("Bootstrapping cluster")
    servers = (await manager.servers_add(2, cmdline=[
        '--logger-log-level=storage_service=debug',
        '--logger-log-level=api=trace',
        '--logger-log-level=database=debug',
        '--abort-on-seastar-bad-alloc',
        '--dump-memory-diagnostics-on-alloc-failure-kind=all',
    ]))

    # Create keyspace and table
    logger.info("Creating table")
    cql = manager.get_cql()
    await cql.run_async(
        "CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}"
    )
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c blob);")
    blob = random.randbytes(32*1024);

    # Disable autocompaction
    logger.info("Disabling autocompaction for the table")
    await asyncio.gather(*[manager.api.disable_autocompaction(s.ip_addr, "test", "test") for s in servers])

    # Populate data
    logger.info("Populating table")
    insert = cql.prepare("INSERT INTO test.test (pk, c) VALUES (?, ?);")
    insert.consistency_level = ConsistencyLevel.ALL;
    n_blobs = 1000
    for pks in itertools.batched(range(n_blobs), n=100):
        await asyncio.gather(*[
            cql.run_async(insert, [k, blob])
            for k in pks
        ])

    # Flush to get initial sstables
    await asyncio.gather(*[manager.api.keyspace_flush(s.ip_addr, "test", "test") for s in servers])

    async def get_data_size_for_server(server: ServerInfo) -> int:
        sstable_info = await manager.api.get_sstable_info(server.ip_addr, "test", "test")
        sizes = [x['data_size'] for s in sstable_info for x in s['sstables']]
        return sum(sizes)

    async def get_total_data_size() -> int:
        return sum(await asyncio.gather(*[get_data_size_for_server(s) for s in servers]))

    total_expected_size = len(blob) * n_blobs * 2
    # Get initial sstable info
    logger.info("Checking initial SSTables")
    assert (await get_total_data_size()) > 0.9 * total_expected_size

    # Alter compression to zstd
    logger.info("Altering table to use zstd compression")
    await cql.run_async(
        "ALTER TABLE test.test WITH COMPRESSION = {'sstable_compression': 'ZstdWithDictsCompressor'};"
    )

    # Flush again to trigger compaction with new compression
    await asyncio.gather(*[manager.api.keyspace_upgrade_sstables(s.ip_addr, "test") for s in servers])

    # Get initial sstable info
    logger.info("Checking SSTables after upgrade to zstd")
    assert (await get_total_data_size()) > 0.9 * total_expected_size

    logger.info("Rewriting dict")
    await manager.api.retrain_dict(servers[0].ip_addr, "test", "test")
    await asyncio.gather(*[read_barrier(manager.api, s.ip_addr) for s in servers])
    logger.info("Rewriting SSTables")
    await asyncio.gather(*[manager.api.keyspace_upgrade_sstables(s.ip_addr, "test") for s in servers])

    logger.info("Checking SSTable sizes")
    assert (await get_total_data_size()) < 0.1 * total_expected_size

    logger.info("Checking again after reboot")
    await asyncio.gather(*[manager.server_stop_gracefully(s.server_id) for s in servers])
    await asyncio.gather(*[manager.server_start(s.server_id) for s in servers])
    await asyncio.gather(*[manager.api.keyspace_upgrade_sstables(s.ip_addr, "test") for s in servers])
    assert (await get_total_data_size()) < 0.1 * total_expected_size

    logger.info("Validating query results")
    await manager.driver_connect(server=servers[0])
    cql = manager.get_cql()
    select = cql.prepare("SELECT c FROM test.test WHERE pk = ?;")
    select.consistency_level = ConsistencyLevel.ALL;
    results = await cql.run_async(select, [42])
    assert results[0][0] == blob

    # Also check with lz4.
    logger.info("Altering table to use lz4 compression")
    await cql.run_async(
        "ALTER TABLE test.test WITH COMPRESSION = {'sstable_compression': 'LZ4WithDictsCompressor'};"
    )
    logger.info("Rewriting SSTables")
    await asyncio.gather(*[manager.api.keyspace_upgrade_sstables(s.ip_addr, "test") for s in servers])
    logger.info("Checking SSTable sizes")
    assert (await get_total_data_size()) < 0.1 * total_expected_size
    logger.info("Validating query results")
    results = await cql.run_async(select, [42])
    assert results[0][0] == blob

    # Test the estimator
    other_blob = random.randbytes(32*1024);
    for pks in itertools.batched(range(n_blobs, 2*n_blobs), n=100):
        await asyncio.gather(*[
            cql.run_async(insert, [k, other_blob])
            for k in pks
        ])
    await asyncio.gather(*[manager.api.keyspace_flush(s.ip_addr, "test", "test") for s in servers])
    logger.info(await manager.api.estimate_compression_ratios(servers[0].ip_addr, "test", "test"))

    # Check that dropping the table also drops the dict.
    assert (await cql.run_async("SELECT COUNT(name) FROM system.dicts"))[0][0] == 1
    logger.info("Dropping the table")
    await cql.run_async("DROP TABLE test.test")
    await asyncio.gather(*[read_barrier(manager.api, s.ip_addr) for s in servers])
    assert (await cql.run_async("SELECT COUNT(name) FROM system.dicts"))[0][0] == 0

    logger.info("Test completed successfully")

#@pytest.mark.skip("slow")
async def test_dict_memory_limit(manager: ManagerClient):
    # Bootstrap cluster and configure server
    logger.info("Bootstrapping cluster")
    servers = (await manager.servers_add(1, cmdline=[
        '--logger-log-level=storage_service=debug',
        '--logger-log-level=api=trace',
        '--logger-log-level=database=debug',
        '--abort-on-seastar-bad-alloc',
        #'--memory=1G', # test.py forces --memory=1G, and it can't be overwritten
        '--smp=2',
        '--sstable-compression-with-dictionaries-memory-budget-fraction=0.005',
        '--dump-memory-diagnostics-on-alloc-failure-kind=all',
    ]))

    intended_dict_memory_bugdet = 0.005 * (1*1024*1024*1024 / 2)

    # Create keyspace and table
    cql = manager.get_cql()
    await cql.run_async(
        "CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
    )

    for algo in ['ZstdWithDictsCompressor', 'LZ4WithDictsCompressor']:
        logger.info(f"Creating table with algo {algo}")

        await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c blob);")

        # Disable autocompaction
        logger.info("Disabling autocompaction for the table")
        await asyncio.gather(*[manager.api.disable_autocompaction(s.ip_addr, "test", "test") for s in servers])
        blob = random.randbytes(2048)

        async def get_data_size_for_server(server: ServerInfo) -> int:
            sstable_info = await manager.api.get_sstable_info(server.ip_addr, "test", "test")
            sizes = [x['data_size'] for s in sstable_info for x in s['sstables']]
            return sum(sizes)

        async def get_total_data_size() -> int:
            return sum(await asyncio.gather(*[get_data_size_for_server(s) for s in servers]))

        # Alter compression to zstd
        logger.info("Altering table to use zstd compression")
        await cql.run_async(
            f"ALTER TABLE test.test WITH COMPRESSION = {{'sstable_compression': '{algo}'}};"
        )

        leeway = 500e3

        for i in range(100):
            lastblob = random.randbytes(2048) + blob
            insert = cql.prepare("INSERT INTO test.test (pk, c) VALUES (?, ?);")
            await cql.run_async(insert, [0, lastblob])
            await asyncio.gather(*[manager.api.keyspace_flush(s.ip_addr, "test", "test") for s in servers])
            await manager.api.retrain_dict(servers[0].ip_addr, "test", "test")
            dict_mem = dict_memory(await get_metrics(manager, servers))
            assert dict_mem < intended_dict_memory_bugdet + leeway
            logger.info(f"Round 0, step {i}: size={(await get_total_data_size())}, dictmem={dict_mem}")

        assert dict_mem > intended_dict_memory_bugdet - leeway
        await asyncio.gather(*[manager.api.keyspace_upgrade_sstables(s.ip_addr, "test") for s in servers])
        assert dict_memory(await get_metrics(manager, servers)) < leeway

        for i in range(100):
            lastblob = random.randbytes(2048) + blob
            insert = cql.prepare("INSERT INTO test.test (pk, c) VALUES (?, ?);")
            await cql.run_async(insert, [0, lastblob])
            await asyncio.gather(*[manager.api.keyspace_flush(s.ip_addr, "test", "test") for s in servers])
            await manager.api.retrain_dict(servers[0].ip_addr, "test", "test")
            dict_mem = dict_memory(await get_metrics(manager, servers))
            assert dict_mem < intended_dict_memory_bugdet + leeway
            logger.info(f"Round 1, step {i}: size={(await get_total_data_size())}, dictmem={dict_mem}")

        assert dict_mem > intended_dict_memory_bugdet - leeway

        logger.info("Validating query results")
        await manager.driver_connect(server=servers[0])
        cql = manager.get_cql()
        select = cql.prepare("SELECT c FROM test.test WHERE pk = ?;")
        select.consistency_level = ConsistencyLevel.ALL;
        results = await cql.run_async(select, [0])
        assert results[0][0] == lastblob

        logger.info("dicts before DROP")
        results = await cql.run_async("SELECT * from system.dicts", [])
        for r in results:
            logger.info(r[0])

        await cql.run_async("DROP TABLE test.test")

        results = await cql.run_async("SELECT * from system.dicts", [])
        logger.info("dicts after DROP")
        for r in results:
            logger.info(r[0])
        await asyncio.gather(*[read_barrier(manager.api, s.ip_addr) for s in servers])
        assert dict_memory(await get_metrics(manager, servers)) == 0
