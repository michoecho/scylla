#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from cassandra.query import SimpleStatement, ConsistencyLevel

from test.pylib.internal_types import ServerInfo
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error_one_shot, HTTPError, read_barrier
from test.pylib.util import wait_for_cql_and_get_hosts
from test.pylib.tablets import get_tablet_replica, get_all_tablet_replicas
from test.topology.conftest import skip_mode
from test.topology.util import reconnect_driver

import pytest
import asyncio
import logging
import time
import random
import os
import glob


logger = logging.getLogger(__name__)


async def inject_error_one_shot_on(manager, error_name, servers):
    errs = [inject_error_one_shot(manager.api, s.ip_addr, error_name) for s in servers]
    await asyncio.gather(*errs)


async def inject_error_on(manager, error_name, servers):
    errs = [manager.api.enable_injection(s.ip_addr, error_name, False) for s in servers]
    await asyncio.gather(*errs)

async def repair_on_node(manager: ManagerClient, server: ServerInfo, servers: list[ServerInfo], ranges: str = ''):
    node = server.ip_addr
    await manager.servers_see_each_other(servers)
    live_nodes_wanted = [s.ip_addr for s in servers]
    live_nodes = await manager.api.get_alive_endpoints(node)
    live_nodes_wanted.sort()
    live_nodes.sort()
    assert live_nodes == live_nodes_wanted
    logger.info(f"Repair table on node {node} live_nodes={live_nodes} live_nodes_wanted={live_nodes_wanted}")
    await manager.api.repair(node, "test", "test", ranges)

async def load_repair_history(cql, hosts):
    all_rows = []
    for host in hosts:
        logging.info(f'Query hosts={host}');
        rows = await cql.run_async("SELECT * from system.repair_history", host=host)
        all_rows += rows
    for row in all_rows:
        logging.info(f"Got repair_history_entry={row}")
    return all_rows

async def safe_server_stop_gracefully(manager, server_id, timeout: float = 60, reconnect: bool = False):
    # Explicitly close the driver to avoid reconnections if scylla fails to update gossiper state on shutdown.
    # It's a problem until https://github.com/scylladb/scylladb/issues/15356 is fixed.
    manager.driver_close()
    await manager.server_stop_gracefully(server_id, timeout)
    cql = None
    if reconnect:
        cql = await reconnect_driver(manager)
    return cql

async def safe_rolling_restart(manager, servers, with_down):
    # https://github.com/scylladb/python-driver/issues/230 is not fixed yet, so for sake of CI stability,
    # driver must be reconnected after rolling restart of servers.
    await manager.rolling_restart(servers, with_down)
    cql = await reconnect_driver(manager)
    return cql

@pytest.mark.asyncio
async def test_tablet_cleanup(manager: ManagerClient):
    cmdline = ['--smp=2', '--commitlog-sync=batch', '--logger-log-level=sstable=trace', '--logger-log-level=trie=trace']

    logger.info("Start first node")
    servers = [await manager.server_add(cmdline=cmdline)]
    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    logger.info("Populate table")
    cql = manager.get_cql()
    n_tablets = 32
    n_partitions = 1000
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    await manager.servers_see_each_other(servers)
    await cql.run_async("CREATE KEYSPACE test WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND tablets = {{'initial': {}}};".format(n_tablets))
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY);")
    await asyncio.gather(*[cql.run_async(f"INSERT INTO test.test (pk) VALUES ({k});") for k in range(1000)])

    logger.info("Start second node")
    servers.append(await manager.server_add(cmdline=cmdline))

    s0_host_id = await manager.get_host_id(servers[0].server_id)
    s1_host_id = await manager.get_host_id(servers[1].server_id)

    logger.info("Read system.tablets")
    tablet_replicas = await get_all_tablet_replicas(manager, servers[0], 'test', 'test')
    assert len(tablet_replicas) == n_tablets

    # Randomly select half of all tablets.
    sample = random.sample(tablet_replicas, n_tablets // 2)
    moved_tokens = [x.last_token for x in sample]
    moved_src = [x.replicas[0] for x in sample]
    moved_dst = [(s1_host_id, random.choice([0, 1])) for _ in sample]

    # Migrate the selected tablets to second node.
    logger.info("Migrate half of all tablets to second node")
    for t, s, d in zip(moved_tokens, moved_src, moved_dst):
        await manager.api.move_tablet(servers[0].ip_addr, "test", "test", *s, *d, t)

    # Sanity check. All data we inserted should be still there.
    assert n_partitions == (await cql.run_async("SELECT COUNT(*) FROM test.test"))[0].count

    # Wipe data on second node.
    logger.info("Wipe data on second node")
    await manager.server_stop_gracefully(servers[1].server_id, timeout=120)
    await manager.server_wipe_sstables(servers[1].server_id, "test", "test")
    await manager.server_start(servers[1].server_id)
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    await manager.servers_see_each_other(servers)
    partitions_after_loss = (await cql.run_async("SELECT COUNT(*) FROM test.test"))[0].count
    assert partitions_after_loss < n_partitions

    # Migrate all tablets back to their original position.
    # Check that this doesn't resurrect cleaned data.
    logger.info("Migrate the migrated tablets back")
    for t, s, d in zip(moved_tokens, moved_dst, moved_src):
        await manager.api.move_tablet(servers[0].ip_addr, "test", "test", *s, *d, t)
    assert partitions_after_loss == (await cql.run_async("SELECT COUNT(*) FROM test.test"))[0].count

    # Kill and restart first node.
    # Check that this doesn't resurrect cleaned data.
    logger.info("Brutally restart first node")
    await manager.server_stop(servers[0].server_id)
    await manager.server_start(servers[0].server_id)
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    await manager.servers_see_each_other(servers)
    assert partitions_after_loss == (await cql.run_async("SELECT COUNT(*) FROM test.test"))[0].count

    # Bonus: check that commitlog_cleanups doesn't have any garbage after restart.
    assert 0 == (await cql.run_async("SELECT COUNT(*) FROM system.commitlog_cleanups", host=hosts[0]))[0].count
