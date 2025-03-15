# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

import asyncio
import random
import logging
import pytest
import itertools
import os
import pathlib
import contextlib
import time
from test.pylib.manager_client import ManagerClient, ServerInfo
from test.pylib.rest_client import read_barrier
from cassandra.cluster import ConsistencyLevel
from cassandra.policies import FallthroughRetryPolicy, ConstantReconnectionPolicy
from cassandra.query import SimpleStatement
import cassandra
import test

logger = logging.getLogger(__name__)

async def test_upgrade_and_rollback(manager: ManagerClient):
    old_exe = await test.pylib.scylla_cluster.get_scylla_2025_1_executable()
    env_exe = os.getenv("SCYLLA")
    assert env_exe
    new_exe = pathlib.Path(env_exe)

    logger.info("Bootstrapping cluster")
    servers = (await manager.servers_add(2, cmdline=[
        '--logger-log-level=storage_service=debug',
        '--logger-log-level=api=trace',
        '--logger-log-level=database=debug',
        '--abort-on-seastar-bad-alloc',
        '--dump-memory-diagnostics-on-alloc-failure-kind=all',
    ], executable=old_exe))

    # Create keyspace and table
    logger.info("Creating tables")
    cql = manager.get_cql()

    algorithms = ['Zstd', 'LZ4', 'Snappy', 'Deflate', 'LZ4WithDicts', 'ZstdWithDicts']
    initial_algorithms = ['Zstd', 'LZ4', 'Snappy', 'Deflate', 'LZ4', 'Zstd']

    await cql.run_async("""
        CREATE KEYSPACE test
        WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}
    """)
    await asyncio.gather(*[
        cql.run_async(f'''
            CREATE TABLE test."{algo}" (pk int PRIMARY KEY, c blob)
            WITH COMPRESSION = {{'sstable_compression': '{initial_algo}Compressor'}};
        ''')
        for algo, initial_algo in zip(algorithms, initial_algorithms)
    ])
    await asyncio.gather(*[read_barrier(manager.api, s.ip_addr) for s in servers])

    # Disable autocompaction
    logger.info("Disabling autocompaction for the tables")
    for algo in algorithms:
        await asyncio.gather(*[manager.api.disable_autocompaction(s.ip_addr, "test", algo) for s in servers])

    # Populate data
    blob = random.randbytes(16*1024);
    logger.info("Populating table")
    n_blobs = 100
    for algo in algorithms:
        insert = cql.prepare(f'''INSERT INTO test."{algo}" (pk, c) VALUES (?, ?);''')
        insert.consistency_level = ConsistencyLevel.ALL;
        for pks in itertools.batched(range(n_blobs), n=100):
            await asyncio.gather(*[
                cql.run_async(insert, [k, blob])
                for k in pks
            ])

    # Flush to get initial sstables
    await asyncio.gather(*[manager.api.keyspace_flush(s.ip_addr, "test") for s in servers])

    async def validate_select():
        for algo in algorithms:
            select = cql.prepare(f'''SELECT c FROM test."{algo}" WHERE pk = ? BYPASS CACHE;''')
            results = await cql.run_async(select, [42])
            assert results[0][0] == blob

    async def get_data_size_for_server(server: ServerInfo, cf: str) -> int:
        sstable_info = await manager.api.get_sstable_info(server.ip_addr, "test", cf)
        sizes = [x['data_size'] for s in sstable_info for x in s['sstables']]
        return sum(sizes)

    async def get_total_data_size(cf: str) -> int:
        return sum(await asyncio.gather(*[get_data_size_for_server(s, cf) for s in servers]))

    total_expected_size = len(blob) * n_blobs
    # Get initial sstable info
    logger.info("Checking initial SSTables")
    for algo in algorithms:
        assert (await get_total_data_size(algo)) > 0.9 * total_expected_size

    try:
        await manager.api.retrain_dict(servers[0].ip_addr, "test", algorithms[0])
    except test.pylib.rest_client.HTTPError as e:
        assert e.code == 404
    else:
        raise Exception(f'Expected HTTPError, got no exception')

    async def change_version(s: ServerInfo, exe: pathlib.Path):
        await manager.server_stop_gracefully(s.server_id)
        await manager.server_switch_executable(s.server_id, pathlib.Path(new_exe))
        await manager.server_start(s.server_id)

    logger.info("Upgrading server 0")
    await change_version(servers[0], new_exe)

    try:
        await manager.api.retrain_dict(servers[0].ip_addr, "test", algorithms[0])
    except test.pylib.rest_client.HTTPError as e:
        assert e.code == 500
    else:
        raise Exception(f'Expected HTTPError, got no exception')

    await validate_select()

    hosts = await test.pylib.util.wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    for new_algo in ['LZ4WithDicts', 'ZstdWithDicts']:
        @contextlib.asynccontextmanager
        async def with_expect_server_error(msg):
            try:
                yield
            except cassandra.protocol.ServerError as e:
                if e.message != msg:
                    raise
            else:
                raise Exception('Expected a ServerError, got no exceptions')

        expected_error = f"sstable_compression {new_algo}Compressor can't be used before all nodes are upgraded to a versions which supports it"

        async with with_expect_server_error(expected_error):
            await cql.run_async(SimpleStatement(f'''
                CREATE TABLE test.bad (pk int PRIMARY KEY, c blob)
                WITH COMPRESSION = {{'sstable_compression': '{new_algo}Compressor'}};
            ''', retry_policy=FallthroughRetryPolicy()), host=hosts[0])

        async with with_expect_server_error(expected_error):
            await cql.run_async(SimpleStatement(f'''
                ALTER TABLE test."Zstd"
                WITH COMPRESSION = {{'sstable_compression': '{new_algo}Compressor'}};
            ''', retry_policy=FallthroughRetryPolicy()), host=hosts[0])


    logger.info("Rewriting SSTables after upgrade")
    await manager.api.keyspace_upgrade_sstables(servers[0].ip_addr, "test")

    await validate_select()

    logger.info("Downgrading server 0")
    await change_version(servers[0], old_exe)

    await validate_select()

    logger.info("Upgrading both servers")
    await asyncio.gather(
        change_version(servers[0], new_exe),
        change_version(servers[1], new_exe)
    )

    logger.info("Waiting for SSTABLE_COMPRESSION_DICTS cluster feature")
    hosts = await test.pylib.util.wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    await asyncio.gather(*(test.pylib.util.wait_for_feature("SSTABLE_COMPRESSION_DICTS", cql, h, time.time() + 60) for h in hosts))

    logger.info("Enabling dict-aware algorithms")
    await asyncio.gather(*[
        cql.run_async(f'''
            ALTER TABLE test."{algo}" WITH COMPRESSION = {{'sstable_compression': '{algo}Compressor'}};
        ''')
        for algo in algorithms
    ])
    await asyncio.gather(*[read_barrier(manager.api, s.ip_addr) for s in servers])

    logger.info("Rewriting dict")
    await asyncio.gather(*[
        manager.api.retrain_dict(servers[0].ip_addr, "test", algo)
        for algo in algorithms
    ])
    await asyncio.gather(*[read_barrier(manager.api, s.ip_addr) for s in servers])

    logger.info("Rewriting SSTables")
    await asyncio.gather(*[manager.api.keyspace_upgrade_sstables(s.ip_addr, "test") for s in servers])

    await validate_select()

    logger.info("Checking SSTable sizes")
    assert (await get_total_data_size("ZstdWithDicts")) < 0.1 * total_expected_size
    assert (await get_total_data_size("LZ4WithDicts")) < 0.1 * total_expected_size
    assert (await get_total_data_size("Zstd")) > 0.9 * total_expected_size
    assert (await get_total_data_size("LZ4")) > 0.9 * total_expected_size
    assert (await get_total_data_size("Snappy")) > 0.9 * total_expected_size
    assert (await get_total_data_size("Deflate")) > 0.9 * total_expected_size
