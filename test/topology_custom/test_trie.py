#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import asyncio
import pytest
import logging
import time
from scripts.intel_pt_trace import with_perf_enabled, with_perf_record
from test.pylib.manager_client import ManagerClient
from test.pylib.random_tables import RandomTables
from test.pylib.util import wait_for_cql_and_get_hosts
from test.topology.util import reconnect_driver
from test.topology.conftest import skip_mode
from test.pylib.random_tables import Column, TextType

logger = logging.getLogger(__name__)

@pytest.mark.skip
@pytest.mark.asyncio
async def test_trie(manager: ManagerClient):
    cmdline = [
        "--logger-log-level=sstable=trace",
        "--logger-log-level=trie=trace",
        "--logger-log-level=compaction=warn",
        "--smp=1"]
    servers = [await manager.server_add(cmdline=cmdline)]
    cql = manager.cql
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    logger.info("Node started")

    cql.execute("CREATE KEYSPACE test_ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 1 }")
    cql.execute("CREATE TABLE test_ks.test_cf(pk text primary key, v bigint)")
    logger.info("Test table created")

    insert = cql.prepare("insert into test_ks.test_cf(pk, v) values (?, ?)");
    select = cql.prepare("select v from test_ks.test_cf where pk = ? bypass cache");
    select_all = cql.prepare("select v from test_ks.test_cf bypass cache");
    cql.execute(insert, ["a", 0]);
    cql.execute(insert, ["b", 1]);
    cql.execute(insert, ["c", 2]);
    cql.execute(insert, ["d", 3]);
    cql.execute(insert, ["e", 4]);
    cql.execute(insert, ["f", 5]);
    cql.execute(insert, ["g", 6]);
    await manager.api.keyspace_flush(node_ip=servers[0].ip_addr, keyspace="test_ks", table="test_cf")
    res = cql.execute(select, "d")
    assert res[0][0] == 3
    res = cql.execute(select_all)
    assert [x[0] for x in res] == [0, 1, 2, 3, 4, 5, 6]

@pytest.mark.asyncio
@pytest.mark.skip
async def test_trie_clustering(manager: ManagerClient):
    cmdline = [
        "--logger-log-level=sstable=trace",
        "--logger-log-level=trie=trace",
        "--logger-log-level=compaction=warn",
        "--smp=1"]
    servers = [await manager.server_add(cmdline=cmdline)]
    pids = await asyncio.gather(*[manager.server_get_pid(s.server_id) for s in servers])
    pidstring = ",".join(str(p) for p in pids)
    cql = manager.cql
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    logger.info("Node started")

    cql.execute("CREATE KEYSPACE test_ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 1 }")
    cql.execute("CREATE TABLE test_ks.test_cf(pk text, ck text, v bigint, pad text, primary key (pk, ck))")
    logger.info("Test table created")

    insert = cql.prepare("insert into test_ks.test_cf(pk, ck, v, pad) values (?, ?, ?, ?)")
    select_asc_one = cql.prepare("select v from test_ks.test_cf where pk = ? and ck = ? bypass cache")
    select_asc_range = cql.prepare("select v from test_ks.test_cf where pk = ? and ck >= ? and ck <= ? bypass cache")
    select_desc_one = cql.prepare("select v from test_ks.test_cf where pk = ? and ck = ? order by ck desc bypass cache")
    pad = "a" * 66000
    cql.execute(insert, ["a", "a", 0, pad]);
    cql.execute(insert, ["a", "b", 1, pad]);
    cql.execute(insert, ["a", "c", 2, pad]);
    cql.execute(insert, ["a", "d", 3, pad]);
    cql.execute(insert, ["a", "e", 4, pad]);
    await manager.api.keyspace_flush(node_ip=servers[0].ip_addr, keyspace="test_ks", table="test_cf")
    async with with_perf_record(".", [f"--pid={pidstring}", f"--event=intel_pt/cyc=1/"]) as control:
        async with with_perf_enabled(control):
            res = cql.execute(select_asc_one, ["a", "c"])
            assert [x[0] for x in res] == [2]
            res = cql.execute(select_asc_range, ["a", "b", "d"])
            assert [x[0] for x in res] == [1, 2, 3]
            res = cql.execute(select_desc_one, ["a", "c"])
            assert [x[0] for x in res] == [2]

@pytest.mark.skip
@pytest.mark.asyncio
async def test_trie_clustering_2(manager: ManagerClient):
    cmdline = [
        "--logger-log-level=sstable=trace",
        "--logger-log-level=trie=trace",
        "--logger-log-level=compaction=warn",
        "--smp=1"]
    servers = [await manager.server_add(cmdline=cmdline)]
    pids = await asyncio.gather(*[manager.server_get_pid(s.server_id) for s in servers])
    pidstring = ",".join(str(p) for p in pids)
    cql = manager.cql
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    logger.info("Node started")

    cql.execute("CREATE KEYSPACE test_ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 1 }")
    cql.execute("CREATE TABLE test_ks.test_cf(pk text, ck int, v text, primary key (pk, ck))")
    logger.info("Test table created")

    insert = cql.prepare("insert into test_ks.test_cf(pk, ck, v) values (?, ?, ?)")
    select = cql.prepare("select v from test_ks.test_cf where pk = ? and ck = ? bypass cache")
    select_all = cql.prepare("select v from test_ks.test_cf bypass cache")
    cql.execute(insert, ["a", "1", 'a1']);
    await manager.api.keyspace_flush(node_ip=servers[0].ip_addr, keyspace="test_ks", table="test_cf")
    res = cql.execute(select, ["a", 0])
    assert [x[0] for x in res] == []
    res = cql.execute(select_all, [])
    assert [x[0] for x in res] == ['a1']

@pytest.mark.asyncio
async def test_trie_clustering_real(manager: ManagerClient):
    cmdline = [
        "--logger-log-level=sstable=trace",
        "--logger-log-level=trie=trace",
        "--logger-log-level=compaction=warn",
        "--smp=1"]
    servers = [await manager.server_add(cmdline=cmdline)]
    pids = await asyncio.gather(*[manager.server_get_pid(s.server_id) for s in servers])
    pidstring = ",".join(str(p) for p in pids)
    cql = manager.cql
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    logger.info("Node started")

    cql.execute("CREATE KEYSPACE test_ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 1 }")
    cql.execute("CREATE TABLE test_ks.test_cf(pk text, ck text, v text, pad text, primary key (pk, ck))")
    logger.info("Test table created")

    pad = "a" * 66000

    insert = cql.prepare("insert into test_ks.test_cf(pk, ck, v, pad) values (?, ?, ?, ?)")
    select = cql.prepare("select v from test_ks.test_cf where pk = ? and ck = ? bypass cache")
    cql.execute(insert, ["a", "a", "a", pad]);
    cql.execute(insert, ["a", "ab", "ab", pad]);
    cql.execute(insert, ["a", "ac", "ac", pad]);
    cql.execute(insert, ["a", "b", "b", pad]);
    await manager.api.keyspace_flush(node_ip=servers[0].ip_addr, keyspace="test_ks", table="test_cf")
    res = cql.execute(select, ["a", "ac"])
    assert [x[0] for x in res] == ["ac"]
