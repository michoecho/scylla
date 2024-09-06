#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import asyncio
import pytest
import logging
import time
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

@pytest.mark.asyncio
async def test_trie_clustering(manager: ManagerClient):
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
    cql.execute("CREATE TABLE test_ks.test_cf(pk text, ck text, v bigint, pad text, primary key (pk, ck))")
    logger.info("Test table created")

    insert = cql.prepare("insert into test_ks.test_cf(pk, ck, v, pad) values (?, ?, ?, ?)")
    select = cql.prepare("select v from test_ks.test_cf where pk = ? and ck = ? bypass cache")
    pad = "a" * 66000
    cql.execute(insert, ["a", "a", 0, pad]);
    cql.execute(insert, ["a", "b", 1, pad]);
    cql.execute(insert, ["a", "c", 2, pad]);
    cql.execute(insert, ["a", "d", 3, pad]);
    cql.execute(insert, ["a", "e", 4, pad]);
    await manager.api.keyspace_flush(node_ip=servers[0].ip_addr, keyspace="test_ks", table="test_cf")
    res = cql.execute(select, ["a", "c"])
    assert res[0][0] == 2
