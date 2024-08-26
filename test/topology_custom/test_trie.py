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

@pytest.mark.asyncio
async def test_trie(manager: ManagerClient):
    cmdline = [
        "--logger-log-level=sstable=trace",
        "--logger-log-level=trie=trace",
        "--logger-log-level=compaction=warn",
        "--smp=1"]
    servers = [await manager.server_add(cmdline=cmdline)]
    cql, hosts = await manager.get_ready_cql(servers)
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
    assert sorted([x[0] for x in res]) == [0, 1, 2, 3, 4, 5, 6]

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
    cql, hosts = await manager.get_ready_cql(servers)
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

@pytest.mark.asyncio
async def test_trie_clustering_real(manager: ManagerClient):
    cmdline = [
        "--logger-log-level=sstable=trace",
        "--logger-log-level=trie=trace",
        "--logger-log-level=compaction=warn",
        "--smp=1"]
    servers = [await manager.server_add(cmdline=cmdline)]
    cql, hosts = await manager.get_ready_cql(servers)
    logger.info("Node started")

    cql.execute("CREATE KEYSPACE test_ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 1 }")
    cql.execute("CREATE TABLE test_ks.test_cf(pk text, ck text, v text, pad text, primary key (pk, ck))")
    logger.info("Test table created")

    pad = "a" * 66000

    insert = cql.prepare("insert into test_ks.test_cf(pk, ck, v, pad) values (?, ?, ?, ?)")
    select = cql.prepare("select v from test_ks.test_cf where pk = ? and ck = ? bypass cache")
    for pk in ["a", "b", "c"]:
        cql.execute(insert, [pk, "a", "a", pad])
        cql.execute(insert, [pk, "ab", "ab", pad])
        cql.execute(insert, [pk, "ac", "ac", pad])
        cql.execute(insert, [pk, "b", "b", pad])
    await manager.api.keyspace_flush(node_ip=servers[0].ip_addr, keyspace="test_ks", table="test_cf")
    res = cql.execute(select, ["b", "ac"])
    assert [x[0] for x in res] == ["ac"]

@pytest.mark.asyncio
async def test_trie_clustering_real2(manager: ManagerClient):
    cmdline = [
        "--logger-log-level=sstable=trace",
        "--logger-log-level=trie=trace",
        "--logger-log-level=compaction=warn",
        "--smp=1"]
    servers = [await manager.server_add(cmdline=cmdline)]
    cql, hosts = await manager.get_ready_cql(servers)
    logger.info("Node started")

    cql.execute("CREATE KEYSPACE test_ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 1 }")
    cql.execute("CREATE TABLE test_ks.test_cf(pk text, ck text, v text, pad text, primary key (pk, ck))")
    logger.info("Test table created")

    pad = "a" * 66000

    insert = cql.prepare("insert into test_ks.test_cf(pk, ck, v, pad) values (?, ?, ?, ?)")
    select_one = cql.prepare("select v from test_ks.test_cf where pk = ? and ck = ? bypass cache")
    select_le = cql.prepare("select v from test_ks.test_cf where pk = ? and ck > ? and ck < ? bypass cache")
    select_leq = cql.prepare("select v from test_ks.test_cf where pk = ? and ck >= ? and ck <= ? bypass cache")
    for pk in ["a", "b", "c"]:
        cql.execute(insert, [pk, "a", "a", pad])
        cql.execute(insert, [pk, "ab", "ab", pad])
        cql.execute(insert, [pk, "ac", "ac", pad])
        cql.execute(insert, [pk, "b", "b", pad])
        cql.execute(insert, [pk, "ba", "b", pad])
    await manager.api.keyspace_flush(node_ip=servers[0].ip_addr, keyspace="test_ks", table="test_cf")
    for pk in ["c"]:
        res = cql.execute(select_one, [pk, "ac"])
        assert [x[0] for x in res] == ["ac"]
        res = cql.execute(select_le, [pk, "ab", "b"])
        assert [x[0] for x in res] == ["ac"]
        res = cql.execute(select_leq, [pk, "ab", "b"])
        assert [x[0] for x in res] == ["ab", "ac", "b"]

@pytest.mark.asyncio
async def test_trie_clustering_exh(manager: ManagerClient):
    cmdline = [
        "--logger-log-level=sstable=trace",
        "--logger-log-level=trie=trace",
        "--logger-log-level=compaction=warn",
        "--smp=1"]
    servers = [await manager.server_add(cmdline=cmdline)]
    cql, hosts = await manager.get_ready_cql(servers)
    logger.info("Node started")

    cql.execute("CREATE KEYSPACE test_ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 1 }")
    cql.execute("CREATE TABLE test_ks.test_cf(pk text, ck text, v text, pad text, primary key (pk, ck))")
    logger.info("Test table created")

    pad = "a" * 33000

    insert = cql.prepare("insert into test_ks.test_cf(pk, ck, v, pad) values (?, ?, ?, ?)")
    select_one = cql.prepare("select v from test_ks.test_cf where pk = ? and ck = ? bypass cache")
    select_le_ge = cql.prepare("select v from test_ks.test_cf where pk = ? and ck > ? and ck < ? bypass cache")
    select_leq_geq = cql.prepare("select v from test_ks.test_cf where pk = ? and ck >= ? and ck <= ? bypass cache")
    select_le = cql.prepare("select v from test_ks.test_cf where pk = ? and ck < ? bypass cache")
    select_ge = cql.prepare("select v from test_ks.test_cf where pk = ? and ck > ? bypass cache")
    pks = ["a", "b", "c"]
    cks = ["a", "ab", "ac", "b", "ba", "c", "ca", "cab", "cb"]
    for pk in pks:
        for ck in cks:
            cql.execute(insert, [pk, ck, ck, pad])
    await manager.api.keyspace_flush(node_ip=servers[0].ip_addr, keyspace="test_ks", table="test_cf")
    for pk in pks:
        for lo in range(len(cks)):
            res = cql.execute(select_one, [pk, cks[lo]])
            assert [x[0] for x in res] == [cks[lo]]
            res = cql.execute(select_le, [pk, cks[lo]])
            assert [x[0] for x in res] == cks[0:lo]
            res = cql.execute(select_ge, [pk, cks[lo]])
            assert [x[0] for x in res] == cks[lo+1:]
            for hi in range(lo, len(cks)):
                res = cql.execute(select_le_ge, [pk, cks[lo], cks[hi]])
                assert [x[0] for x in res] == cks[lo+1:hi]
                res = cql.execute(select_leq_geq, [pk, cks[lo], cks[hi]])
                assert [x[0] for x in res] == cks[lo:hi+1]

@pytest.mark.asyncio
async def test_trie_clustering_exh_reverse(manager: ManagerClient):
    cmdline = [
        "--logger-log-level=sstable=trace",
        "--logger-log-level=trie=trace",
        "--logger-log-level=compaction=warn",
        "--smp=1"]
    servers = [await manager.server_add(cmdline=cmdline)]
    cql, hosts = await manager.get_ready_cql(servers)
    logger.info("Node started")

    cql.execute("CREATE KEYSPACE test_ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 1 }")
    cql.execute("CREATE TABLE test_ks.test_cf(pk text, ck text, v text, pad text, primary key (pk, ck))")
    logger.info("Test table created")

    pad = "a" * 33000

    insert = cql.prepare("insert into test_ks.test_cf(pk, ck, v, pad) values (?, ?, ?, ?)")
    select_one = cql.prepare("select v from test_ks.test_cf where pk = ? and ck = ? order by ck desc bypass cache")
    select_le_ge = cql.prepare("select v from test_ks.test_cf where pk = ? and ck > ? and ck < ? order by ck desc bypass cache")
    select_leq_geq = cql.prepare("select v from test_ks.test_cf where pk = ? and ck >= ? and ck <= ? order by ck desc bypass cache")
    select_le = cql.prepare("select v from test_ks.test_cf where pk = ? and ck < ? order by ck desc bypass cache")
    select_ge = cql.prepare("select v from test_ks.test_cf where pk = ? and ck > ? order by ck desc bypass cache")
    pks = ["a", "b", "c"]
    cks = ["a", "ab", "ac", "b", "ba", "c", "ca", "cab", "cb"]
    for pk in pks:
        for ck in cks:
            cql.execute(insert, [pk, ck, ck, pad])
    await manager.api.keyspace_flush(node_ip=servers[0].ip_addr, keyspace="test_ks", table="test_cf")
    for pk in pks:
        for lo in range(len(cks)):
            res = cql.execute(select_one, [pk, cks[lo]])
            assert [x[0] for x in res] == list(reversed([cks[lo]]))
            res = cql.execute(select_le, [pk, cks[lo]])
            assert [x[0] for x in res] == list(reversed(cks[0:lo]))
            res = cql.execute(select_ge, [pk, cks[lo]])
            assert [x[0] for x in res] == list(reversed(cks[lo+1:]))
            for hi in range(lo, len(cks)):
                res = cql.execute(select_le_ge, [pk, cks[lo], cks[hi]])
                assert [x[0] for x in res] == list(reversed(cks[lo+1:hi]))
                res = cql.execute(select_leq_geq, [pk, cks[lo], cks[hi]])
                assert [x[0] for x in res] == list(reversed(cks[lo:hi+1]))

@pytest.mark.asyncio
async def test_trie_token_range(manager: ManagerClient):
    cmdline = [
        "--logger-log-level=sstable=trace",
        "--logger-log-level=trie=trace",
        "--logger-log-level=compaction=warn",
        "--num-tokens=1",
        "--initial-token=-9223372036854775807",
        "--smp=1"]
    servers = [await manager.server_add(cmdline=cmdline)]
    cql, hosts = await manager.get_ready_cql(servers)
    logger.info("Node started")

    cql.execute("CREATE KEYSPACE test_ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 1 }")
    cql.execute("CREATE TABLE test_ks.test_cf(pk text, primary key (pk))")
    logger.info("Test table created")

    insert = cql.prepare("insert into test_ks.test_cf(pk) values (?)")
    pks = ["a", "b", "c", "d", "e"]
    for pk in pks:
        cql.execute(insert, [pk])
    await manager.api.keyspace_flush(node_ip=servers[0].ip_addr, keyspace="test_ks", table="test_cf")
    res = cql.execute("SELECT token(pk) from test_ks.test_cf BYPASS CACHE")
    tokens = [x[0] for x in res]
    assert len(pks) == len(tokens)
    assert tokens == sorted(tokens)

    sele = cql.prepare("select token(pk) from test_ks.test_cf where token(pk) > ? and token(pk) < ? bypass cache")
    res = cql.execute(sele, [tokens[1], tokens[3]])
    assert [x[0] for x in res] == tokens[2:3]

    sele = cql.prepare("select token(pk) from test_ks.test_cf where token(pk) >= ? and token(pk) <= ? bypass cache")
    res = cql.execute(sele, [tokens[1], tokens[3]])
    assert [x[0] for x in res] == tokens[1:4]
