#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import struct
import time

import pytest
from cassandra import ConsistencyLevel  # type: ignore
from cassandra.metadata import Murmur3Token  # type: ignore
from cassandra.query import SimpleStatement  # type: ignore

from test.cluster.util import new_test_keyspace
from test.pylib.async_cql import _wrap_future
from test.pylib.internal_types import ServerInfo
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for


async def read_retries(manager: ScyllaClusterManager, server: ServerInfo) -> int:
    metrics = await manager.metrics.query(server.ip_addr)
    return int(metrics.get("scylla_storage_proxy_coordinator_read_retries") or 0)


def key_token(pk: int) -> int:
    return Murmur3Token.from_key(struct.pack('>i', pk)).value


async def keys_within_one_vnode(manager: ScyllaClusterManager, server: ServerInfo, count: int) -> list[int]:
    """Return `count` int partition keys, in token order, which one vnode range holds."""
    cql = await manager.get_cql_exclusive(server)
    ring = [int(token)
            for query in ["SELECT tokens FROM system.local", "SELECT tokens FROM system.peers"]
            for row in await cql.run_async(query)
            for token in row.tokens]
    keys = sorted(range(10000), key=key_token)
    for i in range(len(keys) - count + 1):
        window = keys[i:i + count]
        first, last = key_token(window[0]), key_token(window[-1])
        # A vnode range (a, b] ends at a ring token b. So a ring token at or
        # after the first key's token, and before the last key's, separates
        # them.
        if not any(first <= token < last for token in ring):
            return window
    raise RuntimeError(f"no vnode range holds {count} of the candidate keys")


async def fetch_pages(cql, statement: SimpleStatement, max_pages: int) -> list:
    """Return the rows of all pages of `statement`. Fail if it has more than `max_pages` pages."""
    response_future = cql.execute_async(statement)
    rows = []
    for _ in range(max_pages):
        rows.extend(await _wrap_future(response_future))
        if not response_future.has_more_pages:
            return rows
        response_future.start_fetching_next_page()
    pytest.fail(f"more than {max_pages} pages for {statement.query_string!r}, rows so far: {rows}")


@pytest.mark.asyncio
async def test_digest_match_preserves_empty_short_page_cursor(manager: ScyllaClusterManager) -> None:
    """
    Preserve the paging cursor of an empty short data page when digests match.

    This is the normal data-and-digest path, not mutation reconciliation. The
    data replica stops after reaching its tombstone limit. It returns no live
    rows, but it does return a cursor saying how far it scanned. The other
    replica finishes the range and therefore has no cursor. The empty results
    have equal digests.

    An empty short page is valid only while its explicit cursor is preserved:
    it has no returned row from which the pager could reconstruct one.
    """
    cfg = {
        'query_tombstone_page_limit': 10,
        'hinted_handoff_enabled': False,
        # The local replica, node 0, supplies data.
        'cache_hit_rate_read_balancing': False,
    }
    servers = await manager.servers_add(2, config=cfg, auto_rack_dc="dc1")
    cql, _ = await manager.get_ready_cql(servers)
    cql0 = await manager.get_cql_exclusive(servers[0])

    async with new_test_keyspace(manager, "WITH replication = "
                                 "{'class': 'NetworkTopologyStrategy', 'replication_factor': 2} "
                                 "AND tablets = {'enabled': false}") as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} (pk int, ck int, v int, PRIMARY KEY (pk, ck)) "
                            "WITH tombstone_gc = {'mode': 'disabled'}")

        # Both replicas have the same range tombstone and therefore return the
        # same empty result.
        await cql0.run_async(SimpleStatement(
            f"DELETE FROM {table} WHERE pk = 0 AND ck >= 0 AND ck <= 99",
            consistency_level=ConsistencyLevel.ALL))

        # Only the data replica has enough additional, redundant row tombstones
        # to stop early. Hinted handoff is disabled, so they remain local.
        await manager.server_stop_gracefully(servers[1].server_id)
        delete_row = cql0.prepare(f"DELETE FROM {table} WHERE pk = 0 AND ck = ?")
        delete_row.consistency_level = ConsistencyLevel.ONE
        for ck in range(100):
            await cql0.run_async(delete_row, [ck])
        await manager.server_start(servers[1].server_id, wait_others=1)

        select = SimpleStatement(f"SELECT pk, ck FROM {table}",
                                 consistency_level=ConsistencyLevel.QUORUM,
                                 fetch_size=10)
        rows = await cql0.run_async(select, all_pages=True)
        assert rows == []


@pytest.mark.asyncio
async def test_digest_match_uses_earliest_reported_cursor(manager: ScyllaClusterManager) -> None:
    """
    Continue from the earliest cursor reported by a replica whose digest matched.

    Ignoring an absent cursor is only half of the fix above. It would preserve
    the data replica's cursor, but that replica may have scanned farther than a
    digest replica. Continuing from the data replica would skip data which the
    less-advanced replica has not examined.

    All replicas initially return ck=0 and matching digests. Their progress is
    deliberately different:

    * node 1 stops before ck=30, which only it stores;
    * node 0, the data replica, stops after ck=30;
    * node 2 finishes the range and reports no cursor.

    The next page must start from node 1's cursor so that ck=30 is discovered.
    """
    tombstone_limit = 20
    divergent_row = 30
    cfg = {
        'query_tombstone_page_limit': tombstone_limit,
        'hinted_handoff_enabled': False,
        # The local replica, node 0, supplies data.
        'cache_hit_rate_read_balancing': False,
    }
    servers = await manager.servers_add(3, config=cfg, cmdline=['--smp', '2'], auto_rack_dc="dc1")
    cql, _ = await manager.get_ready_cql(servers)

    async def write_to_only(server_idx: int, statements: list[str]) -> None:
        others = [s for i, s in enumerate(servers) if i != server_idx]
        for server in others:
            await manager.server_stop_gracefully(server.server_id)
        cql_one = await manager.get_cql_exclusive(servers[server_idx])
        for statement in statements:
            await cql_one.run_async(SimpleStatement(statement, consistency_level=ConsistencyLevel.ONE))
        for server in others:
            await manager.server_start(server.server_id)
        await manager.get_ready_cql(servers)

    async with new_test_keyspace(manager, "WITH replication = "
                                 "{'class': 'NetworkTopologyStrategy', 'replication_factor': 3} "
                                 "AND tablets = {'enabled': false}") as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} (pk int, ck int, v int, PRIMARY KEY (pk, ck)) "
                            "WITH tombstone_gc = {'mode': 'disabled'}")
        await cql.run_async(SimpleStatement(
            f"INSERT INTO {table} (pk, ck, v) VALUES (0, 0, 0)",
            consistency_level=ConsistencyLevel.ALL))

        await write_to_only(
            1,
            [f"DELETE FROM {table} WHERE pk = 0 AND ck = {ck}"
             for ck in range(1, divergent_row)]
            + [f"INSERT INTO {table} (pk, ck, v) VALUES "
               f"(0, {divergent_row}, {divergent_row})"])

        await write_to_only(
            0,
            [f"DELETE FROM {table} WHERE pk = 0 AND ck = {ck}"
             for ck in range(divergent_row + 1, divergent_row + 1 + 2 * tombstone_limit)])

        cql0 = await manager.get_cql_exclusive(servers[0])
        select = SimpleStatement(f"SELECT pk, ck FROM {table}",
                                 consistency_level=ConsistencyLevel.ALL,
                                 fetch_size=10)
        rows = await cql0.run_async(select, all_pages=True)
        assert [r.ck for r in rows] == [0, divergent_row]


@pytest.mark.asyncio
async def test_digest_match_lowered_cursor_keeps_paging(manager: ScyllaClusterManager) -> None:
    """
    Mark a result short when a digest replica lowers an exhausted data replica's cursor.

    The data replica supplies an empty, exhausted result. A matching digest
    stops on tombstones before reaching its divergent live row. The coordinator
    must both copy that cursor and keep paging from it, or the row is missed.
    """
    cfg = {
        'query_tombstone_page_limit': 10,
        'hinted_handoff_enabled': False,
        'cache_hit_rate_read_balancing': False,
    }
    servers = await manager.servers_add(3, config=cfg, auto_rack_dc="dc1")
    cql, _ = await manager.get_ready_cql(servers)

    async with new_test_keyspace(manager, "WITH replication = "
                                 "{'class': 'NetworkTopologyStrategy', 'replication_factor': 3} "
                                 "AND tablets = {'enabled': false}") as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} (pk int, ck int, v int, PRIMARY KEY (pk, ck)) "
                            "WITH tombstone_gc = {'mode': 'disabled'}")

        cql0 = await manager.get_cql_exclusive(servers[0])
        select = SimpleStatement(f"SELECT pk, ck FROM {table} WHERE pk = 0",
                                 consistency_level=ConsistencyLevel.ALL,
                                 fetch_size=10)

        # Give every replica the same range tombstone. The constrained replica's
        # extra row tombstones below are redundant, so the first-page digests
        # still match even though only that replica reaches its tombstone limit.
        await cql0.run_async(SimpleStatement(
            f"DELETE FROM {table} WHERE pk = 0 AND ck >= 0 AND ck <= 49",
            consistency_level=ConsistencyLevel.ALL))

        # With read balancing disabled, the local replica (node 0) supplies
        # data. Keep it and the second digest replica empty; only node 1 has a
        # constrained digest and the later live row.
        digest_replica = 1
        empty_replicas = [0, 2]
        for replica in empty_replicas:
            await manager.server_stop_gracefully(servers[replica].server_id)
        cql_digest = await manager.get_cql_exclusive(servers[digest_replica])
        delete_row = cql_digest.prepare(f"DELETE FROM {table} WHERE pk = 0 AND ck = ?")
        delete_row.consistency_level = ConsistencyLevel.ONE
        for ck in range(50):
            await cql_digest.run_async(delete_row, [ck])
        divergent_row = 100
        await cql_digest.run_async(SimpleStatement(
            f"INSERT INTO {table} (pk, ck, v) VALUES (0, {divergent_row}, {divergent_row})",
            consistency_level=ConsistencyLevel.ONE))
        for replica in empty_replicas:
            await manager.server_start(servers[replica].server_id)
        await manager.get_ready_cql(servers)

        cql0 = await manager.get_cql_exclusive(servers[0])
        rows = await cql0.run_async(select, all_pages=True)
        assert [r.ck for r in rows] == [divergent_row]


@pytest.mark.asyncio
@pytest.mark.parametrize("data_tombstone", [
    pytest.param(9, id="equal-cursors"),
    pytest.param(5, id="earlier-data-cursor"),
])
async def test_digest_match_digest_replica_stop_keeps_paging(manager: ScyllaClusterManager, data_tombstone: int) -> None:
    """
    Keep paging when a matching digest replica stopped early, whatever its cursor.

    The data replica, node 0, holds one row tombstone and finishes the
    partition. A single-partition read reports the last fragment it consumed,
    so the data cursor is that tombstone. The digest replica, node 1, holds row
    tombstones at ck=0..9 and a live row at ck=100. It reaches its tombstone
    limit at ck=9 and stops there. Both pages are empty, so the digests match.

    The digest replica's cursor equals or follows the data cursor, so it does
    not lower the data cursor. Paging must continue from it nevertheless, or
    the live row is missed.
    """
    tombstone_limit = 10
    cfg = {
        'query_tombstone_page_limit': tombstone_limit,
        'hinted_handoff_enabled': False,
        # The local replica, node 0, supplies data.
        'cache_hit_rate_read_balancing': False,
    }
    servers = await manager.servers_add(2, config=cfg, auto_rack_dc="dc1")
    cql, _ = await manager.get_ready_cql(servers)
    cql0 = await manager.get_cql_exclusive(servers[0])

    async with new_test_keyspace(manager, "WITH replication = "
                                 "{'class': 'NetworkTopologyStrategy', 'replication_factor': 2} "
                                 "AND tablets = {'enabled': false}") as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} (pk int, ck int, v int, PRIMARY KEY (pk, ck)) "
                            "WITH tombstone_gc = {'mode': 'disabled'}")

        await cql0.run_async(SimpleStatement(
            f"DELETE FROM {table} WHERE pk = 0 AND ck = {data_tombstone}",
            consistency_level=ConsistencyLevel.ALL))

        # Only the digest replica has the other tombstones and the live row.
        # Hinted handoff is disabled, so they remain there.
        await manager.server_stop_gracefully(servers[0].server_id)
        cql1 = await manager.get_cql_exclusive(servers[1])
        delete_row = cql1.prepare(f"DELETE FROM {table} WHERE pk = 0 AND ck = ?")
        delete_row.consistency_level = ConsistencyLevel.ONE
        for ck in range(tombstone_limit):
            if ck != data_tombstone:
                await cql1.run_async(delete_row, [ck])
        divergent_row = 100
        await cql1.run_async(SimpleStatement(
            f"INSERT INTO {table} (pk, ck, v) VALUES (0, {divergent_row}, {divergent_row})",
            consistency_level=ConsistencyLevel.ONE))
        await manager.server_start(servers[0].server_id, wait_others=1)
        # Node 0 has restarted. Let the shared session reconnect before the
        # keyspace is dropped with it.
        await manager.get_ready_cql(servers)
        cql0 = await manager.get_cql_exclusive(servers[0])

        select = SimpleStatement(f"SELECT pk, ck FROM {table} WHERE pk = 0",
                                 consistency_level=ConsistencyLevel.ALL,
                                 fetch_size=10)
        rows = await cql0.run_async(select, all_pages=True)
        assert [r.ck for r in rows] == [divergent_row]


@pytest.mark.asyncio
async def test_digest_match_does_not_truncate_unpaged_multi_partition_read(manager: ScyllaClusterManager) -> None:
    """
    Do not mark an unpaged result short when matching digests report different cursors.

    A single-partition replica read reports the last fragment it consumed, even
    when it finished the partition. The data replica, node 0, holds a trailing
    row tombstone which node 1 lacks. The tombstone is not live data, so the
    digests match, but node 1 reports an earlier position.

    An unpaged read cannot resume from a lowered cursor. The coordinator merges
    the per-partition results of an IN query only up to the first short one, so
    marking the results short silently drops partitions.
    """
    cfg = {
        'hinted_handoff_enabled': False,
        # The local replica, node 0, supplies data.
        'cache_hit_rate_read_balancing': False,
    }
    servers = await manager.servers_add(2, config=cfg, auto_rack_dc="dc1")
    cql, _ = await manager.get_ready_cql(servers)
    cql0 = await manager.get_cql_exclusive(servers[0])

    async with new_test_keyspace(manager, "WITH replication = "
                                 "{'class': 'NetworkTopologyStrategy', 'replication_factor': 2} "
                                 "AND tablets = {'enabled': false}") as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} (pk int, ck int, v int, PRIMARY KEY (pk, ck)) "
                            "WITH tombstone_gc = {'mode': 'disabled'}")

        partitions = [1, 2]
        live_rows = list(range(5))
        insert = cql0.prepare(f"INSERT INTO {table} (pk, ck, v) VALUES (?, ?, ?)")
        insert.consistency_level = ConsistencyLevel.ALL
        for pk in partitions:
            for ck in live_rows:
                await cql0.run_async(insert, [pk, ck, ck])

        # Give every partition the trailing tombstone. Every per-partition
        # result is then affected, whatever order the coordinator merges them in.
        await manager.server_stop_gracefully(servers[1].server_id)
        delete_row = cql0.prepare(f"DELETE FROM {table} WHERE pk = ? AND ck = 10")
        delete_row.consistency_level = ConsistencyLevel.ONE
        for pk in partitions:
            await cql0.run_async(delete_row, [pk])
        await manager.server_start(servers[1].server_id, wait_others=1)

        select = SimpleStatement(f"SELECT pk, ck FROM {table} WHERE pk IN (1, 2)",
                                 consistency_level=ConsistencyLevel.QUORUM,
                                 fetch_size=None)
        rows = await cql0.run_async(select)
        assert sorted((r.pk, r.ck) for r in rows) == [(pk, ck) for pk in partitions for ck in live_rows]


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
async def test_digest_match_ignores_cursor_of_late_replica(manager: ScyllaClusterManager) -> None:
    """
    Take the cursor from the same responses as the digest decision.

    The coordinator decides whether digests match once enough replicas have
    responded for the consistency level. It may have contacted more replicas.
    Their responses can arrive before the coordinator handles the decision.
    Such a response must not change the result's cursor, because the decision
    does not account for it.

    With speculative_retry = 'ALWAYS' and CL=ONE, the coordinator, node 0,
    reads data from both replicas. Node 0 holds a live row at ck=100 and
    reaches consistency alone. Node 1 holds the same row after row tombstones
    at ck=0..29, so its page stops on the tombstone limit before the row. Its
    response arrives before the coordinator handles the decision. If the
    coordinator took node 1's cursor, the next page would return ck=100 again.
    """
    tombstone_limit = 10
    live_row = 100
    cfg = {
        'query_tombstone_page_limit': tombstone_limit,
        'hinted_handoff_enabled': False,
        # The local replica, node 0, supplies data.
        'cache_hit_rate_read_balancing': False,
    }
    servers = await manager.servers_add(2, config=cfg, auto_rack_dc="dc1")
    cql, _ = await manager.get_ready_cql(servers)

    async with new_test_keyspace(manager, "WITH replication = "
                                 "{'class': 'NetworkTopologyStrategy', 'replication_factor': 2} "
                                 "AND tablets = {'enabled': false}") as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} (pk int, ck int, v int, PRIMARY KEY (pk, ck)) "
                            "WITH tombstone_gc = {'mode': 'disabled'} AND speculative_retry = 'ALWAYS'")
        await cql.run_async(SimpleStatement(
            f"INSERT INTO {table} (pk, ck, v) VALUES (0, {live_row}, {live_row})",
            consistency_level=ConsistencyLevel.ALL))

        # Only node 1 has the tombstones. Hinted handoff is disabled, so they
        # remain there.
        await manager.server_stop_gracefully(servers[0].server_id)
        cql1 = await manager.get_cql_exclusive(servers[1])
        delete_row = cql1.prepare(f"DELETE FROM {table} WHERE pk = 0 AND ck = ?")
        delete_row.consistency_level = ConsistencyLevel.ONE
        for ck in range(3 * tombstone_limit):
            await cql1.run_async(delete_row, [ck])
        await manager.server_start(servers[0].server_id, wait_others=1)
        await manager.get_ready_cql(servers)
        cql0 = await manager.get_cql_exclusive(servers[0])

        # Hold node 1's response until node 0 has reached consistency alone.
        # The coordinator then handles the result only after node 1's response
        # has arrived too.
        replica_injection = "storage_proxy::handle_read"
        coordinator_injection = "storage_proxy::digest_read_wait_for_all_responses"
        await manager.api.enable_injection(servers[1].ip_addr, replica_injection, one_shot=True,
                                           parameters={'cf_name': 't'})
        await manager.api.enable_injection(servers[0].ip_addr, coordinator_injection, one_shot=True)

        select = SimpleStatement(f"SELECT pk, ck FROM {table} WHERE pk = 0",
                                 consistency_level=ConsistencyLevel.ONE,
                                 fetch_size=10)
        read = asyncio.ensure_future(cql0.run_async(select, all_pages=True))

        async def coordinator_reached_consistency() -> bool | None:
            entered = await manager.api.get_injection_enter_count(servers[0].ip_addr, coordinator_injection)
            return True if entered else None
        await wait_for(coordinator_reached_consistency, time.time() + 60)
        await manager.api.message_injection(servers[1].ip_addr, replica_injection)

        rows = await read
        assert [r.ck for r in rows] == [live_row]


@pytest.mark.asyncio
@pytest.mark.parametrize("reverse_order", [False, True], ids=["forward", "reverse"])
async def test_tombstone_limited_page_defers_static_only_row(manager: ScyllaClusterManager, reverse_order: bool) -> None:
    """
    Decide on a static-only row only after the whole partition has been read.

    This is the plain data path, without reconciliation. A replica stops a page
    after query_tombstone_page_limit tombstones, even inside a partition and
    before any live row. At that point it cannot know whether the partition has
    live rows. Partition 0 has live rows after its tombstones, so a static-only
    row on the first page is spurious and pushes a real row out of the limit.
    Partition 1 has no live rows, so it must return exactly one static-only row,
    although its pages stop inside the partition and continuation pages
    restrict clustering keys. This holds even if the query selects no static
    column.
    """
    tombstone_limit = 10
    servers = await manager.servers_add(1, config={'query_tombstone_page_limit': tombstone_limit},
                                        auto_rack_dc="dc1")
    cql, _ = await manager.get_ready_cql(servers)

    async with new_test_keyspace(manager, "WITH replication = "
                                 "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} (pk int, ck int, s int static, v int, PRIMARY KEY (pk, ck)) "
                            "WITH tombstone_gc = {'mode': 'disabled'}")

        tombstone_base = 1000 if reverse_order else 0
        live_row_base = 0 if reverse_order else 1000

        delete_row = cql.prepare(f"DELETE FROM {table} WHERE pk = ? AND ck = ?")
        for pk in [0, 1]:
            await cql.run_async(f"INSERT INTO {table} (pk, s) VALUES ({pk}, 1)")
            # Every page, including the last one in partition 1, stops on the
            # tombstone limit.
            for ck in range(tombstone_base, tombstone_base + 3 * tombstone_limit):
                await cql.run_async(delete_row, [pk, ck])
        live_rows = list(range(live_row_base, live_row_base + 3))
        for ck in live_rows:
            await cql.run_async(f"INSERT INTO {table} (pk, ck, v) VALUES (0, {ck}, {ck})")

        limit = 2
        order_by = " ORDER BY ck DESC" if reverse_order else ""
        select = SimpleStatement(f"SELECT pk, ck, s, v FROM {table} WHERE pk = 0{order_by} LIMIT {limit}",
                                 fetch_size=1)
        rows = await cql.run_async(select, all_pages=True)
        expected_rows = (list(reversed(live_rows)) if reverse_order else live_rows)[:limit]
        assert [(r.ck, r.s, r.v) for r in rows] == [(ck, 1, ck) for ck in expected_rows]

        select = SimpleStatement(f"SELECT pk, ck, s, v FROM {table} WHERE pk = 1{order_by}",
                                 fetch_size=1)
        rows = await cql.run_async(select, all_pages=True)
        assert [(r.ck, r.s, r.v) for r in rows] == [(None, 1, None)]

        # A live static row makes the partition return a row even if the query
        # selects no static column (see test_static.py::test_static_not_selected).
        # The pager must record that the row is pending for such a query too.
        select = SimpleStatement(f"SELECT pk, ck, v FROM {table} WHERE pk = 1{order_by}",
                                 fetch_size=1)
        rows = await cql.run_async(select, all_pages=True)
        assert [(r.ck, r.v) for r in rows] == [(None, None)]


@pytest.mark.asyncio
async def test_tombstone_limited_page_returns_distinct_row_of_live_static_row(manager: ScyllaClusterManager) -> None:
    """
    Return a partition's DISTINCT row on a page which stops inside the partition.

    A DISTINCT query returns one row per partition. A live static row
    establishes it, whatever clustering rows follow. So a page which stops on
    the tombstone limit inside the partition, before any live row, still
    returns it. An older coordinator does not continue a partition of a
    DISTINCT query, so no later page would return it instead.

    Partition 0 has a live row after its tombstones. Partition 1 has none.
    """
    tombstone_limit = 10
    servers = await manager.servers_add(1, config={'query_tombstone_page_limit': tombstone_limit},
                                        auto_rack_dc="dc1")
    cql, _ = await manager.get_ready_cql(servers)

    async with new_test_keyspace(manager, "WITH replication = "
                                 "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} (pk int, ck int, s int static, v int, PRIMARY KEY (pk, ck)) "
                            "WITH tombstone_gc = {'mode': 'disabled'}")

        partitions = [0, 1]
        delete_row = cql.prepare(f"DELETE FROM {table} WHERE pk = ? AND ck = ?")
        for pk in partitions:
            await cql.run_async(f"INSERT INTO {table} (pk, s) VALUES ({pk}, {pk})")
            for ck in range(3 * tombstone_limit):
                await cql.run_async(delete_row, [pk, ck])
        await cql.run_async(f"INSERT INTO {table} (pk, ck, v) VALUES (0, 1000, 1000)")

        # Scan the whole table: a single-partition DISTINCT query is not paged.
        select = SimpleStatement(f"SELECT DISTINCT pk, s FROM {table}", fetch_size=1)
        rows = await cql.run_async(select, all_pages=True)
        assert sorted((r.pk, r.s) for r in rows) == [(pk, pk) for pk in partitions]

        # The live static value establishes the row even if the query does not
        # select it.
        select = SimpleStatement(f"SELECT DISTINCT pk FROM {table}", fetch_size=1)
        rows = await cql.run_async(select, all_pages=True)
        assert sorted(r.pk for r in rows) == partitions


@pytest.mark.asyncio
async def test_tombstone_limited_page_continues_undecided_distinct_partition(manager: ScyllaClusterManager) -> None:
    """
    Continue a DISTINCT query inside a partition which no page returned yet.

    A DISTINCT query returns one row per partition. Without live static
    content, only a live clustering row establishes that row. A page which
    stops on the tombstone limit inside a partition, before any live row,
    returns nothing from it. The pager then skipped the rest of the
    partition, as it does for a partition which a page returned, and lost
    the partition.

    Partition 0 has a live row after its tombstones, so it must be returned.
    Partition 1 has only tombstones, so it must not be. Partition 2 has a
    live row and no tombstones. Check a table without static columns, and a
    table whose static column is not set.
    """
    tombstone_limit = 10
    servers = await manager.servers_add(1, config={'query_tombstone_page_limit': tombstone_limit},
                                        auto_rack_dc="dc1")
    cql, _ = await manager.get_ready_cql(servers)

    async with new_test_keyspace(manager, "WITH replication = "
                                 "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        for table, static_column in [(f"{ks}.t", ""), (f"{ks}.t_static", "s int static, ")]:
            await cql.run_async(f"CREATE TABLE {table} (pk int, ck int, {static_column}v int, PRIMARY KEY (pk, ck)) "
                                "WITH tombstone_gc = {'mode': 'disabled'}")

            delete_row = cql.prepare(f"DELETE FROM {table} WHERE pk = ? AND ck = ?")
            for pk in [0, 1]:
                for ck in range(3 * tombstone_limit):
                    await cql.run_async(delete_row, [pk, ck])
            await cql.run_async(f"INSERT INTO {table} (pk, ck, v) VALUES (0, 1000, 1000)")
            await cql.run_async(f"INSERT INTO {table} (pk, ck, v) VALUES (2, 0, 0)")

            # Scan the whole table: a single-partition DISTINCT query is not paged.
            select = SimpleStatement(f"SELECT DISTINCT pk FROM {table}", fetch_size=1)
            rows = await cql.run_async(select, all_pages=True)
            assert sorted(r.pk for r in rows) == [0, 2], table


@pytest.mark.asyncio
async def test_tombstone_limited_page_continues_distinct_partition_past_range_tombstone(manager: ScyllaClusterManager) -> None:
    """
    Continue an undecided partition of a DISTINCT query past a range tombstone.

    The pager continues a partition of a DISTINCT query which a page left
    undecided. With a tombstone limit of one, a page stopped on the change
    which opens a range tombstone, before the partition's first live row. The
    next page started at the same position, and stopped on the same change
    again, so paging never ended.

    Partition 0 has a range tombstone over ck=0..9, followed by a live row at
    ck=100.
    """
    servers = await manager.servers_add(1, config={'query_tombstone_page_limit': 1},
                                        auto_rack_dc="dc1")
    cql, _ = await manager.get_ready_cql(servers)

    async with new_test_keyspace(manager, "WITH replication = "
                                 "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} (pk int, ck int, v int, PRIMARY KEY (pk, ck)) "
                            "WITH tombstone_gc = {'mode': 'disabled'}")
        await cql.run_async(f"DELETE FROM {table} WHERE pk = 0 AND ck >= 0 AND ck <= 9")
        await cql.run_async(f"INSERT INTO {table} (pk, ck, v) VALUES (0, 100, 100)")

        # Scan the whole table: a single-partition DISTINCT query is not paged.
        select = SimpleStatement(f"SELECT DISTINCT pk FROM {table}", fetch_size=10)
        rows = await fetch_pages(cql, select, max_pages=10)
        assert [r.pk for r in rows] == [0]


@pytest.mark.asyncio
async def test_undecided_distinct_partition_reuses_cached_querier(manager: ScyllaClusterManager) -> None:
    """
    Reuse the cached querier when a DISTINCT query continues an undecided partition.

    The pager continues a partition of a DISTINCT query if a page stopped
    inside it before deciding its row. The next page then starts its
    partition range at that partition, inclusively. The replica must accept
    that bound for its cached querier. Otherwise it drops the querier, and
    every such page recreates its reader.

    Partition 0 has row tombstones at ck=0..29 and a live row at ck=1000. The
    first three pages stop on the tombstone limit inside the partition. The
    fourth returns the partition's row and reaches the end of the table.
    """
    tombstone_limit = 10
    # With a single shard, each page looks up exactly one cached querier.
    servers = await manager.servers_add(1, config={'query_tombstone_page_limit': tombstone_limit},
                                        cmdline=['--smp', '1'], auto_rack_dc="dc1")
    cql, _ = await manager.get_ready_cql(servers)

    async def querier_cache_stats() -> tuple[int, int]:
        metrics = await manager.metrics.query(servers[0].ip_addr)
        return (int(metrics.get("scylla_database_querier_cache_lookups") or 0),
                int(metrics.get("scylla_database_querier_cache_drops") or 0))

    async with new_test_keyspace(manager, "WITH replication = "
                                 "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} (pk int, ck int, v int, PRIMARY KEY (pk, ck)) "
                            "WITH tombstone_gc = {'mode': 'disabled'}")
        delete_row = cql.prepare(f"DELETE FROM {table} WHERE pk = 0 AND ck = ?")
        for ck in range(3 * tombstone_limit):
            await cql.run_async(delete_row, [ck])
        await cql.run_async(f"INSERT INTO {table} (pk, ck, v) VALUES (0, 1000, 1000)")

        lookups_before, drops_before = await querier_cache_stats()
        # The page size exceeds the number of partitions, so no page stops
        # after returning a partition's row. The pager moves on from such a
        # partition, and the querier is then rightly dropped.
        select = SimpleStatement(f"SELECT DISTINCT pk FROM {table}", fetch_size=10)
        rows = await cql.run_async(select, all_pages=True)
        assert [r.pk for r in rows] == [0]
        lookups_after, drops_after = await querier_cache_stats()
        assert lookups_after - lookups_before >= 3
        assert drops_after == drops_before


@pytest.mark.asyncio
async def test_tombstone_limited_page_does_not_stop_on_dead_static_row(manager: ScyllaClusterManager) -> None:
    """
    Do not stop a page on a dead static row.

    A dead static row counts against query_tombstone_page_limit. A page which
    stops on it has no clustering position to continue from, so the pager
    skips the rest of the partition. Partition 0 has a retained static-cell
    tombstone and a live row. With a tombstone limit of one, the first page
    once stopped on the static row, and paging never returned the live row.

    Dead static rows must still stop a page at the tombstone limit, or a scan
    of partitions with only a dead static row would not be bounded. Such a
    page stops at the end of the partition instead.
    """
    servers = await manager.servers_add(1, config={'query_tombstone_page_limit': 1},
                                        auto_rack_dc="dc1")
    cql, _ = await manager.get_ready_cql(servers)

    async with new_test_keyspace(manager, "WITH replication = "
                                 "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        table = f"{ks}.t"
        rowless_table = f"{ks}.rowless"
        for t in [table, rowless_table]:
            await cql.run_async(f"CREATE TABLE {t} (pk int, ck int, s int static, v int, PRIMARY KEY (pk, ck)) "
                                "WITH tombstone_gc = {'mode': 'disabled'}")

        await cql.run_async(f"DELETE s FROM {table} WHERE pk = 0")
        await cql.run_async(f"INSERT INTO {table} (pk, ck, v) VALUES (0, 100, 100)")

        for where in ["", " WHERE pk = 0", " WHERE pk = 0 ORDER BY ck DESC"]:
            select = SimpleStatement(f"SELECT pk, ck, v FROM {table}{where}", fetch_size=10)
            rows = await cql.run_async(select, all_pages=True)
            assert [(r.pk, r.ck, r.v) for r in rows] == [(0, 100, 100)], where

        partitions = range(3)
        for pk in partitions:
            await cql.run_async(f"DELETE s FROM {rowless_table} WHERE pk = {pk}")
        select = SimpleStatement(f"SELECT pk, ck, v FROM {rowless_table}", fetch_size=10)
        response_future = cql.execute_async(select)
        first_page = await _wrap_future(response_future)
        assert first_page == []
        assert response_future.has_more_pages


@pytest.mark.asyncio
@pytest.mark.parametrize("q_has_live_row", [False, True], ids=["static-only-row", "live-row"])
async def test_per_partition_limit_counts_rows_of_cursor_partition(
        manager: ScyllaClusterManager, q_has_live_row: bool) -> None:
    """
    Count only rows of the cursor's partition against its per-partition limit.

    With a per-partition limit, the pager records how many rows it has returned
    from the partition at the cursor. The next page's filter subtracts them from
    that partition's allowance.

    Partition P precedes partition Q in token order. P has one live row. Q has
    exactly one tombstone page's worth of row tombstones. After them, Q has
    either a live row, or no live row but a live static value, so that it
    returns a static-only row. With PER PARTITION LIMIT 1 and a page size of two
    rows, the first page returns P's row and stops on the tombstone limit
    inside Q, before returning anything from Q. The pager once counted P's row
    as a row of Q, so the next page's filter dropped Q's row.
    """
    tombstone_limit = 10
    servers = await manager.servers_add(1, config={'query_tombstone_page_limit': tombstone_limit},
                                        auto_rack_dc="dc1")
    cql, _ = await manager.get_ready_cql(servers)

    # Pick keys whose tokens follow each other in this order, with no vnode
    # boundary between them, so that one read returns P's row and stops in Q.
    p, q = await keys_within_one_vnode(manager, servers[0], 2)

    async with new_test_keyspace(manager, "WITH replication = "
                                 "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1} "
                                 "AND tablets = {'enabled': false}") as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} (pk int, ck int, s int static, v int, PRIMARY KEY (pk, ck)) "
                            "WITH tombstone_gc = {'mode': 'disabled'}")

        await cql.run_async(f"INSERT INTO {table} (pk, ck, v) VALUES ({p}, 0, 0)")
        # The first page stops on Q's last tombstone.
        delete_row = cql.prepare(f"DELETE FROM {table} WHERE pk = {q} AND ck = ?")
        for ck in range(tombstone_limit):
            await cql.run_async(delete_row, [ck])
        if q_has_live_row:
            await cql.run_async(f"INSERT INTO {table} (pk, ck, v) VALUES ({q}, 1000, 1000)")
            expected_q_row = (q, 1000, None, 1000)
        else:
            await cql.run_async(f"INSERT INTO {table} (pk, s) VALUES ({q}, 1)")
            expected_q_row = (q, None, 1, None)

        select = SimpleStatement(f"SELECT pk, ck, s, v FROM {table} PER PARTITION LIMIT 1", fetch_size=2)
        rows = await cql.run_async(select, all_pages=True)
        assert [(r.pk, r.ck, r.s, r.v) for r in rows] == [(p, 0, None, 0), expected_q_row]


@pytest.mark.asyncio
async def test_per_partition_limit_count_survives_empty_page(manager: ScyllaClusterManager) -> None:
    """
    Keep the per-partition count across an empty page in the same partition.

    The partition has a live row, three tombstone pages' worth of row
    tombstones, and another live row. With PER PARTITION LIMIT 1 and a page
    size of two rows, the first page returns the first live row and stops on
    the tombstone limit. The next pages return nothing, and stop on the
    tombstone limit in the same partition. The pager once reset its count of
    the partition's returned rows on such a page, so a later page returned the
    second live row as well.
    """
    tombstone_limit = 10
    servers = await manager.servers_add(1, config={'query_tombstone_page_limit': tombstone_limit},
                                        auto_rack_dc="dc1")
    cql, _ = await manager.get_ready_cql(servers)

    async with new_test_keyspace(manager, "WITH replication = "
                                 "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} (pk int, ck int, v int, PRIMARY KEY (pk, ck)) "
                            "WITH tombstone_gc = {'mode': 'disabled'}")

        await cql.run_async(f"INSERT INTO {table} (pk, ck, v) VALUES (0, 0, 0)")
        delete_row = cql.prepare(f"DELETE FROM {table} WHERE pk = 0 AND ck = ?")
        for ck in range(1, 1 + 3 * tombstone_limit):
            await cql.run_async(delete_row, [ck])
        await cql.run_async(f"INSERT INTO {table} (pk, ck, v) VALUES (0, 1000, 1000)")

        select = SimpleStatement(f"SELECT pk, ck, v FROM {table} PER PARTITION LIMIT 1", fetch_size=2)
        rows = await cql.run_async(select, all_pages=True)
        assert [(r.pk, r.ck, r.v) for r in rows] == [(0, 0, 0)]


@pytest.mark.asyncio
async def test_tombstone_limited_page_does_not_stop_on_range_tombstone_start(manager: ScyllaClusterManager) -> None:
    """
    Do not stop a page on the range tombstone change which starts it.

    A range tombstone change counts against query_tombstone_page_limit. A page
    which stops on the change which opens a range tombstone ends before the
    tombstone's first key. The next page starts at that position, inclusively,
    so its reader first emits the same change again. With a tombstone limit of
    one, that page stopped on it too, and paging never ended.

    Partition 0 has live rows at ck=0 and ck=100, and a range tombstone over
    ck=40..49 between them. Check forward and reversed order.
    """
    servers = await manager.servers_add(1, config={'query_tombstone_page_limit': 1},
                                        auto_rack_dc="dc1")
    cql, _ = await manager.get_ready_cql(servers)

    async with new_test_keyspace(manager, "WITH replication = "
                                 "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} (pk int, ck int, v int, PRIMARY KEY (pk, ck)) "
                            "WITH tombstone_gc = {'mode': 'disabled'}")
        for ck in [0, 100]:
            await cql.run_async(f"INSERT INTO {table} (pk, ck, v) VALUES (0, {ck}, {ck})")
        await cql.run_async(f"DELETE FROM {table} WHERE pk = 0 AND ck >= 40 AND ck <= 49")

        for where, expected in [("", [0, 100]),
                                (" WHERE pk = 0", [0, 100]),
                                (" WHERE pk = 0 ORDER BY ck DESC", [100, 0])]:
            select = SimpleStatement(f"SELECT pk, ck, v FROM {table}{where}", fetch_size=10)
            rows = await fetch_pages(cql, select, max_pages=10)
            assert [r.ck for r in rows] == expected, where


@pytest.mark.asyncio
async def test_reconciliation_treats_static_only_replica_as_complete_partition(manager: ScyllaClusterManager) -> None:
    """
    Do not trim a reconciled page behind a replica which returned only a static row.

    Unlike the tests above, this test deliberately creates a digest mismatch and
    enters mutation reconciliation. Both replicas have the static row, but only
    node 0 has clustering rows.

    Node 1's frozen mutation has no clustering-row key. That does not mean it
    stopped before ck=0: mutation reads act on a static-row size limit only at
    partition end, so node 1 examined the complete partition. Reconciliation can
    safely combine its complete static/tombstone state with node 0's rows.

    The old code represented the missing clustering-row key as the position
    before every clustering row. It then trimmed away the only partition and
    produced an empty short page without a paging cursor.
    """
    servers = await manager.servers_add(
        2,
        config={'hinted_handoff_enabled': False},
        auto_rack_dc="dc1")
    cql, _ = await manager.get_ready_cql(servers)
    cql0 = await manager.get_cql_exclusive(servers[0])

    async with new_test_keyspace(manager, "WITH replication = "
                                 "{'class': 'NetworkTopologyStrategy', 'replication_factor': 2}") as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} (pk int, ck int, s int static, v int, "
                            "PRIMARY KEY (pk, ck))")
        await cql0.run_async(SimpleStatement(
            f"INSERT INTO {table} (pk, s) VALUES (0, 1)",
            consistency_level=ConsistencyLevel.ALL))

        await manager.server_stop_gracefully(servers[1].server_id)
        for ck in range(3):
            await cql0.run_async(SimpleStatement(
                f"INSERT INTO {table} (pk, ck, v) VALUES (0, {ck}, {ck})",
                consistency_level=ConsistencyLevel.ONE))
        await manager.server_start(servers[1].server_id, wait_others=1)

        select = SimpleStatement(f"SELECT pk, ck, s, v FROM {table} WHERE pk = 0",
                                 consistency_level=ConsistencyLevel.QUORUM,
                                 fetch_size=1)
        retries_before = await read_retries(manager, servers[0])
        rows = await cql0.run_async(select, all_pages=True)
        assert [(r.ck, r.s, r.v) for r in rows] == [(0, 1, 0), (1, 1, 1), (2, 1, 2)]

        # Node 1 completed this partition. Treating it as an early stop would
        # cause either destructive trimming or an unnecessary larger retry.
        assert await read_retries(manager, servers[0]) == retries_before


@pytest.mark.asyncio
async def test_reconciliation_counts_one_row_per_distinct_partition(manager: ScyllaClusterManager) -> None:
    """
    Count one row per partition of a DISTINCT query when reconciling a page.

    A DISTINCT query returns one row per partition. Reconciliation must count
    a partition's rows in the same way when it finds the last row which the
    page returns.

    Partitions P, Q and R follow each other in token order. Node 0 holds P's
    row 1, row tombstones in Q and a tombstone for R's row 0. Node 1 holds
    P's row 2, Q's row 0 and R's row 0, which is older than node 0's
    tombstone. With a page size of three rows, node 0's mutation page stops
    on its size limit within Q's tombstones, so it does not return R's
    tombstone. Node 1's page returns its three rows.

    The merged P has two live rows, but the page returns one row for P.
    Reconciliation once counted two. It then took Q's row as the page's last
    row, so it did not trim R, which follows node 0's stop. The page returned
    R's row, although node 0 had deleted it.
    """
    cfg = {
        'query_page_size_in_bytes': 1024,
        'hinted_handoff_enabled': False,
    }
    servers = await manager.servers_add(2, config=cfg, auto_rack_dc="dc1")
    cql, _ = await manager.get_ready_cql(servers)

    # Pick keys whose tokens follow each other in this order, with no vnode
    # boundary between them, so that the scan reconciles them in one read.
    p, q, r = await keys_within_one_vnode(manager, servers[0], 3)

    async with new_test_keyspace(manager, "WITH replication = "
                                 "{'class': 'NetworkTopologyStrategy', 'replication_factor': 2} "
                                 "AND tablets = {'enabled': false}") as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} (pk int, ck int, v int, PRIMARY KEY (pk, ck)) "
                            "WITH tombstone_gc = {'mode': 'disabled'}")

        # Q's tombstones follow node 1's row of Q, so node 0's stop follows
        # that row. Node 1's row of R has an old timestamp, so node 0's
        # tombstone deletes it.
        writes = [
            [f"INSERT INTO {table} (pk, ck, v) VALUES ({p}, 1, 1)"]
            + [f"DELETE FROM {table} WHERE pk = {q} AND ck = {ck}" for ck in range(1000, 1200)]
            + [f"DELETE FROM {table} WHERE pk = {r} AND ck = 0"],
            [f"INSERT INTO {table} (pk, ck, v) VALUES ({p}, 2, 2)",
             f"INSERT INTO {table} (pk, ck, v) VALUES ({q}, 0, 0)",
             f"INSERT INTO {table} (pk, ck, v) VALUES ({r}, 0, 0) USING TIMESTAMP 1"],
        ]
        for server_idx, statements in enumerate(writes):
            other = servers[1 - server_idx]
            await manager.server_stop_gracefully(other.server_id)
            cql_one = await manager.get_cql_exclusive(servers[server_idx])
            for statement in statements:
                await cql_one.run_async(SimpleStatement(statement, consistency_level=ConsistencyLevel.ONE))
            await manager.server_start(other.server_id, wait_others=1)
        # Both nodes have restarted. Let the shared session reconnect before
        # the keyspace is dropped with it.
        await manager.get_ready_cql(servers)
        cql0 = await manager.get_cql_exclusive(servers[0])

        select = SimpleStatement(f"SELECT DISTINCT pk FROM {table}",
                                 consistency_level=ConsistencyLevel.ALL,
                                 fetch_size=3)
        rows = await cql0.run_async(select, all_pages=True)
        assert [row.pk for row in rows] == [p, q]


@pytest.mark.asyncio
async def test_reconciliation_decides_full_distinct_page_by_returned_rows(manager: ScyllaClusterManager) -> None:
    """
    Decide whether a reconciled DISTINCT page is full by counting one row per partition.

    Reconciliation counts the merged result's live rows. If the count reaches
    the page's row limit, the page is full. Otherwise, if a replica returned a
    full page, reconciliation marks the result short, so that paging goes on.
    A DISTINCT query returns one row per partition, so the count must not
    exceed one per partition.

    Partitions P, Q and R follow each other in token order. With a page size
    of two rows, each replica returns P and Q, and stops at its row limit.
    Node 0 holds P's row 1, and node 1 holds P's row 2. Both hold Q's row 0,
    which each of them considers live, but which is dead after merging. Node
    0 holds an old value of a and a newer tombstone for b. Node 1 holds an
    old value of b and a newer tombstone for a. Both hold R's row. The
    replicas' static values in P differ, so their digests differ.

    The merged P has two live rows, and Q none. Reconciliation once counted
    two rows, and took the page as full. The page returned only P, and was
    not short, so the pager ended the query and lost R.
    """
    cfg = {
        'hinted_handoff_enabled': False,
    }
    servers = await manager.servers_add(2, config=cfg, auto_rack_dc="dc1")
    cql, _ = await manager.get_ready_cql(servers)

    # Pick keys whose tokens follow each other in this order, with no vnode
    # boundary between them, so that the scan reconciles them in one read.
    p, q, r = await keys_within_one_vnode(manager, servers[0], 3)

    async with new_test_keyspace(manager, "WITH replication = "
                                 "{'class': 'NetworkTopologyStrategy', 'replication_factor': 2} "
                                 "AND tablets = {'enabled': false}") as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} (pk int, ck int, s int static, a int, b int, PRIMARY KEY (pk, ck)) "
                            "WITH tombstone_gc = {'mode': 'disabled'}")
        await cql.run_async(SimpleStatement(
            f"INSERT INTO {table} (pk, ck, a) VALUES ({r}, 0, 0)",
            consistency_level=ConsistencyLevel.ALL))

        # Q's row has no row marker, so only its cells make it live. Node 1
        # writes its static value later, so the merged value is 2.
        writes = [
            [f"INSERT INTO {table} (pk, ck, a) VALUES ({p}, 1, 1)",
             f"UPDATE {table} SET s = 1 WHERE pk = {p}",
             f"UPDATE {table} USING TIMESTAMP 1 SET a = 1 WHERE pk = {q} AND ck = 0",
             f"DELETE b FROM {table} USING TIMESTAMP 4 WHERE pk = {q} AND ck = 0"],
            [f"INSERT INTO {table} (pk, ck, a) VALUES ({p}, 2, 2)",
             f"UPDATE {table} SET s = 2 WHERE pk = {p}",
             f"UPDATE {table} USING TIMESTAMP 2 SET b = 1 WHERE pk = {q} AND ck = 0",
             f"DELETE a FROM {table} USING TIMESTAMP 3 WHERE pk = {q} AND ck = 0"],
        ]
        for server_idx, statements in enumerate(writes):
            other = servers[1 - server_idx]
            await manager.server_stop_gracefully(other.server_id)
            cql_one = await manager.get_cql_exclusive(servers[server_idx])
            for statement in statements:
                await cql_one.run_async(SimpleStatement(statement, consistency_level=ConsistencyLevel.ONE))
            await manager.server_start(other.server_id, wait_others=1)
        # Both nodes have restarted. Let the shared session reconnect before
        # the keyspace is dropped with it.
        await manager.get_ready_cql(servers)
        cql0 = await manager.get_cql_exclusive(servers[0])

        select = SimpleStatement(f"SELECT DISTINCT pk, s FROM {table}",
                                 consistency_level=ConsistencyLevel.ALL,
                                 fetch_size=2)
        rows = await cql0.run_async(select, all_pages=True)
        assert [(row.pk, row.s) for row in rows] == [(p, 2), (r, None)]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("reverse_order", "legacy_reverse_format"),
    [
        pytest.param(False, False, id="forward"),
        pytest.param(True, False, id="reverse-native"),
        pytest.param(
            True,
            True,
            id="reverse-legacy",
            marks=pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode"),
        ),
    ],
)
async def test_reconciliation_uses_range_tombstone_as_rowless_replica_position(
        manager: ScyllaClusterManager, reverse_order: bool, legacy_reverse_format: bool) -> None:
    """
    Use the last range tombstone as progress for a rowless mutation page.

    Mutation reads account range-tombstone memory continuously, but cannot stop
    and expose a cursor at an individual range-tombstone boundary. They act on
    the page-size stop at partition end. A tombstone-only partition can therefore
    exceed the page-size target and be marked short, but the partition it returns
    is complete. The mutation format does not record that fact, however, and a
    future replica may be able to stop at a range-tombstone boundary.

    Conservatively use the last tombstone boundary as this replica's progress.
    The first page then ends there instead of including node 0's later live rows.
    It is empty, but carries a cursor which lets paging reach those rows without
    a larger reconciliation retry.
    """
    cfg = {
        'query_page_size_in_bytes': 1024,
        'hinted_handoff_enabled': False,
    }
    if legacy_reverse_format:
        cfg['error_injections_at_startup'] = [
            {'name': 'suppress_features', 'value': 'NATIVE_REVERSE_QUERIES'},
        ]
    servers = await manager.servers_add(2, config=cfg, auto_rack_dc="dc1")
    cql, _ = await manager.get_ready_cql(servers)
    cql0 = await manager.get_cql_exclusive(servers[0])

    async with new_test_keyspace(manager, "WITH replication = "
                                 "{'class': 'NetworkTopologyStrategy', 'replication_factor': 2}") as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} (pk int, ck int, v int, PRIMARY KEY (pk, ck)) "
                            "WITH tombstone_gc = {'mode': 'disabled'}")

        tombstone_base = 100000 if reverse_order else 0
        live_row_base = 0 if reverse_order else 100000

        delete_range = cql0.prepare(f"DELETE FROM {table} WHERE pk = 0 AND ck >= ? AND ck <= ?")
        delete_range.consistency_level = ConsistencyLevel.ALL
        for i in range(200):
            await cql0.run_async(delete_range, [tombstone_base + 10 * i, tombstone_base + 10 * i + 5])

        await manager.server_stop_gracefully(servers[1].server_id)
        live_rows = list(range(live_row_base, live_row_base + 3))
        for ck in live_rows:
            await cql0.run_async(SimpleStatement(
                f"INSERT INTO {table} (pk, ck, v) VALUES (0, {ck}, {ck})",
                consistency_level=ConsistencyLevel.ONE))
        await manager.server_start(servers[1].server_id, wait_others=1)

        order_by = " ORDER BY ck DESC" if reverse_order else ""
        select = SimpleStatement(f"SELECT pk, ck, v FROM {table} WHERE pk = 0{order_by}",
                                 consistency_level=ConsistencyLevel.QUORUM,
                                 fetch_size=1)
        retries_before = await read_retries(manager, servers[0])

        # Node 1 actually scanned the whole partition, but its last mutation is
        # a range tombstone well before node 0's live rows. Use that conservative
        # boundary for the first page rather than assuming partition completion.
        response_future = cql0.execute_async(select)
        first_page = await _wrap_future(response_future)
        assert first_page == []
        assert response_future.has_more_pages

        response_future.start_fetching_next_page()
        rows = await _wrap_future(response_future, all_pages=True)
        assert [r.ck for r in rows] == (list(reversed(live_rows)) if reverse_order else live_rows)

        assert await read_retries(manager, servers[0]) == retries_before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("reverse_order", "legacy_reverse_format"),
    [
        pytest.param(False, False, id="forward"),
        pytest.param(True, False, id="reverse-native"),
        pytest.param(
            True,
            True,
            id="reverse-legacy",
            marks=pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode"),
        ),
    ],
)
async def test_reconciliation_trimming_does_not_return_static_only_row(
        manager: ScyllaClusterManager, reverse_order: bool, legacy_reverse_format: bool) -> None:
    """
    Do not turn a partition trimmed at a replica stop into a static-only row.

    This extends the range-tombstone test above with a live static column
    which both replicas share. Node 1 returns the static row and range
    tombstones. It reaches the end of the partition, but its page exceeds the
    page size, so it is marked short. As in the test above, the coordinator
    conservatively takes the end of node 1's last range tombstone as its stop.
    Node 0 also returns a later live row. Reconciliation trims that row at the
    inferred stop, which leaves only the static row.

    The query has no clustering restriction, so conversion to a data result
    emits a static-only row for a partition without live clustering rows. This
    partition has live rows after the stop, which the continuation returns, so
    a static-only row is spurious. It also consumes the query limit and pushes
    out a real row.
    """
    cfg = {
        'query_page_size_in_bytes': 1024,
        'hinted_handoff_enabled': False,
    }
    if legacy_reverse_format:
        cfg['error_injections_at_startup'] = [
            {'name': 'suppress_features', 'value': 'NATIVE_REVERSE_QUERIES'},
        ]
    servers = await manager.servers_add(2, config=cfg, auto_rack_dc="dc1")
    cql, _ = await manager.get_ready_cql(servers)
    cql0 = await manager.get_cql_exclusive(servers[0])

    async with new_test_keyspace(manager, "WITH replication = "
                                 "{'class': 'NetworkTopologyStrategy', 'replication_factor': 2}") as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} (pk int, ck int, s int static, v int, PRIMARY KEY (pk, ck)) "
                            "WITH tombstone_gc = {'mode': 'disabled'}")

        tombstone_base = 100000 if reverse_order else 0
        live_row_base = 0 if reverse_order else 100000

        await cql0.run_async(SimpleStatement(
            f"INSERT INTO {table} (pk, s) VALUES (0, 1)",
            consistency_level=ConsistencyLevel.ALL))
        delete_range = cql0.prepare(f"DELETE FROM {table} WHERE pk = 0 AND ck >= ? AND ck <= ?")
        delete_range.consistency_level = ConsistencyLevel.ALL
        for i in range(200):
            await cql0.run_async(delete_range, [tombstone_base + 10 * i, tombstone_base + 10 * i + 5])

        await manager.server_stop_gracefully(servers[1].server_id)
        live_rows = list(range(live_row_base, live_row_base + 3))
        for ck in live_rows:
            await cql0.run_async(SimpleStatement(
                f"INSERT INTO {table} (pk, ck, v) VALUES (0, {ck}, {ck})",
                consistency_level=ConsistencyLevel.ONE))
        await manager.server_start(servers[1].server_id, wait_others=1)

        limit = 2
        order_by = " ORDER BY ck DESC" if reverse_order else ""
        select = SimpleStatement(f"SELECT pk, ck, s, v FROM {table} WHERE pk = 0{order_by} LIMIT {limit}",
                                 consistency_level=ConsistencyLevel.QUORUM,
                                 fetch_size=1)
        rows = await cql0.run_async(select, all_pages=True)
        expected_rows = (list(reversed(live_rows)) if reverse_order else live_rows)[:limit]
        assert [(r.ck, r.s, r.v) for r in rows] == [(ck, 1, ck) for ck in expected_rows]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("reverse_order", "legacy_reverse_format"),
    [
        pytest.param(False, False, id="forward"),
        pytest.param(True, False, id="reverse-native"),
        pytest.param(
            True,
            True,
            id="reverse-legacy",
            marks=pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode"),
        ),
    ],
)
async def test_reconciliation_trimming_keeps_static_only_row_of_rowless_partition(
        manager: ScyllaClusterManager, reverse_order: bool, legacy_reverse_format: bool) -> None:
    """
    Return the static-only row of a partition whose trimmed rows turn out to be dead.

    Both replicas hold the live static value. Node 0 also holds a live row.
    Node 1 holds row tombstones before that row in query order, and a newer
    tombstone for the row itself. Node 1's mutation page stops on its page size
    within the early tombstones, so it does not return the later one.
    Reconciliation trims node 0's row at node 1's stop, which leaves only the
    static row.

    The trimmed row is dead: node 1's later tombstone deletes it. The partition
    therefore has no live clustering rows, and the query, which does not
    restrict clustering keys, must return one static-only row for it. The
    continuation pages restrict clustering keys, so the first page must not
    discard the partition's eligibility for that row.
    """
    cfg = {
        'query_page_size_in_bytes': 1024,
        'hinted_handoff_enabled': False,
    }
    if legacy_reverse_format:
        cfg['error_injections_at_startup'] = [
            {'name': 'suppress_features', 'value': 'NATIVE_REVERSE_QUERIES'},
        ]
    servers = await manager.servers_add(2, config=cfg, auto_rack_dc="dc1")
    cql, _ = await manager.get_ready_cql(servers)
    cql0 = await manager.get_cql_exclusive(servers[0])

    async with new_test_keyspace(manager, "WITH replication = "
                                 "{'class': 'NetworkTopologyStrategy', 'replication_factor': 2}") as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} (pk int, ck int, s int static, v int, PRIMARY KEY (pk, ck)) "
                            "WITH tombstone_gc = {'mode': 'disabled'}")

        # Put the early tombstones before the row in query order. In a reversed
        # query, larger clustering keys come first.
        tombstone_base = 100000 if reverse_order else 0
        row = 0 if reverse_order else 100000

        await cql0.run_async(SimpleStatement(
            f"INSERT INTO {table} (pk, s) VALUES (0, 1)",
            consistency_level=ConsistencyLevel.ALL))

        await manager.server_stop_gracefully(servers[1].server_id)
        await cql0.run_async(SimpleStatement(
            f"INSERT INTO {table} (pk, ck, v) VALUES (0, {row}, {row})",
            consistency_level=ConsistencyLevel.ONE))
        await manager.server_start(servers[1].server_id, wait_others=1)

        # The tombstones are written after the row, so the one for the row is newer.
        await manager.server_stop_gracefully(servers[0].server_id)
        cql1 = await manager.get_cql_exclusive(servers[1])
        delete_row = cql1.prepare(f"DELETE FROM {table} WHERE pk = 0 AND ck = ?")
        delete_row.consistency_level = ConsistencyLevel.ONE
        for ck in [*range(tombstone_base, tombstone_base + 200), row]:
            await cql1.run_async(delete_row, [ck])
        await manager.server_start(servers[0].server_id, wait_others=1)
        # Both nodes have restarted. Let the shared session reconnect before
        # the keyspace is dropped with it.
        await manager.get_ready_cql(servers)
        cql0 = await manager.get_cql_exclusive(servers[0])

        order_by = " ORDER BY ck DESC" if reverse_order else ""
        select = SimpleStatement(f"SELECT pk, ck, s, v FROM {table} WHERE pk = 0{order_by}",
                                 consistency_level=ConsistencyLevel.QUORUM,
                                 fetch_size=1)
        rows = await cql0.run_async(select, all_pages=True)
        assert [(r.ck, r.s, r.v) for r in rows] == [(None, 1, None)]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("reverse_order", "legacy_reverse_format"),
    [
        pytest.param(False, False, id="forward"),
        pytest.param(True, False, id="reverse-native"),
        pytest.param(
            True,
            True,
            id="reverse-legacy",
            marks=pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode"),
        ),
    ],
)
async def test_reconciliation_without_live_rows_does_not_return_static_only_row(
        manager: ScyllaClusterManager, reverse_order: bool, legacy_reverse_format: bool) -> None:
    """
    Do not emit a static-only row for a partition in which every replica stopped early.

    Both replicas hold row tombstones followed by live rows. Their static
    values differ, so the digests differ and the coordinator reconciles. Both
    mutation pages stop on their page size within the tombstones. The merged
    page holds the static row and tombstones, but no live clustering row, and
    nothing is trimmed.

    The partition does have live rows after the stop, which the continuation
    returns. A static-only row on the first page is therefore spurious. It
    also consumes the query limit and pushes out a real row.
    """
    cfg = {
        'query_page_size_in_bytes': 1024,
        'hinted_handoff_enabled': False,
    }
    if legacy_reverse_format:
        cfg['error_injections_at_startup'] = [
            {'name': 'suppress_features', 'value': 'NATIVE_REVERSE_QUERIES'},
        ]
    servers = await manager.servers_add(2, config=cfg, auto_rack_dc="dc1")
    cql, _ = await manager.get_ready_cql(servers)
    cql0 = await manager.get_cql_exclusive(servers[0])

    async with new_test_keyspace(manager, "WITH replication = "
                                 "{'class': 'NetworkTopologyStrategy', 'replication_factor': 2}") as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} (pk int, ck int, s int static, v int, PRIMARY KEY (pk, ck)) "
                            "WITH tombstone_gc = {'mode': 'disabled'}")

        tombstone_base = 100000 if reverse_order else 0
        live_row_base = 0 if reverse_order else 100000

        await cql0.run_async(SimpleStatement(
            f"INSERT INTO {table} (pk, s) VALUES (0, 1)",
            consistency_level=ConsistencyLevel.ALL))
        delete_row = cql0.prepare(f"DELETE FROM {table} WHERE pk = 0 AND ck = ?")
        delete_row.consistency_level = ConsistencyLevel.ALL
        for ck in range(tombstone_base, tombstone_base + 200):
            await cql0.run_async(delete_row, [ck])
        live_rows = list(range(live_row_base, live_row_base + 3))
        insert = cql0.prepare(f"INSERT INTO {table} (pk, ck, v) VALUES (0, ?, ?)")
        insert.consistency_level = ConsistencyLevel.ALL
        for ck in live_rows:
            await cql0.run_async(insert, [ck, ck])

        # Only node 0 holds the newer static value.
        await manager.server_stop_gracefully(servers[1].server_id)
        await cql0.run_async(SimpleStatement(
            f"INSERT INTO {table} (pk, s) VALUES (0, 2)",
            consistency_level=ConsistencyLevel.ONE))
        await manager.server_start(servers[1].server_id, wait_others=1)

        limit = 2
        order_by = " ORDER BY ck DESC" if reverse_order else ""
        select = SimpleStatement(f"SELECT pk, ck, s, v FROM {table} WHERE pk = 0{order_by} LIMIT {limit}",
                                 consistency_level=ConsistencyLevel.QUORUM,
                                 fetch_size=1)
        rows = await cql0.run_async(select, all_pages=True)
        expected_rows = (list(reversed(live_rows)) if reverse_order else live_rows)[:limit]
        assert [(r.ck, r.s, r.v) for r in rows] == [(ck, 2, ck) for ck in expected_rows]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("reverse_order", "legacy_reverse_format"),
    [
        pytest.param(False, False, id="forward"),
        pytest.param(True, False, id="reverse-native"),
        pytest.param(
            True,
            True,
            id="reverse-legacy",
            marks=pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode"),
        ),
    ],
)
async def test_reconciliation_does_not_page_past_earliest_range_tombstone_stop(
        manager: ScyllaClusterManager, reverse_order: bool, legacy_reverse_format: bool) -> None:
    """
    Keep paging at the earliest replica stop when later range tombstones remain.

    A mutation page from one replica contains many range tombstones and no live
    rows, while another replica contains live rows on both sides of a range
    tombstone which covers neither row. Reconciliation trims the result to the
    first replica's tombstone stop. The tombstone from the other replica must
    not become the outgoing cursor, or the live row between the two positions
    is skipped on the continuation page.

    The continuation uses the paging state from the first response, so a new
    query cannot hide a cursor which was advanced too far.
    """
    cfg = {
        'query_page_size_in_bytes': 1024,
        'hinted_handoff_enabled': False,
        'cache_hit_rate_read_balancing': False,
    }
    if legacy_reverse_format:
        cfg['error_injections_at_startup'] = [
            {'name': 'suppress_features', 'value': 'NATIVE_REVERSE_QUERIES'},
        ]
    servers = await manager.servers_add(2, config=cfg, auto_rack_dc="dc1")
    cql, _ = await manager.get_ready_cql(servers)
    cql0 = await manager.get_cql_exclusive(servers[0])

    async with new_test_keyspace(manager, "WITH replication = "
                                 "{'class': 'NetworkTopologyStrategy', 'replication_factor': 2}") as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} (pk int, ck int, v int, PRIMARY KEY (pk, ck)) "
                            "WITH tombstone_gc = {'mode': 'disabled'}")

        # Put the tombstone-only page's progress before the live rows in query
        # order. In a reversed query, larger clustering keys come first.
        tombstone_base = 100000 if reverse_order else 0
        await manager.server_stop_gracefully(servers[1].server_id)
        delete_range = cql0.prepare(f"DELETE FROM {table} WHERE pk = 0 AND ck >= ? AND ck <= ?")
        delete_range.consistency_level = ConsistencyLevel.ONE
        for ck in range(tombstone_base, tombstone_base + 100, 2):
            await cql0.run_async(delete_range, [ck, ck + 1])
        await manager.server_start(servers[1].server_id, wait_others=1)

        await manager.server_stop_gracefully(servers[0].server_id)
        cql1 = await manager.get_cql_exclusive(servers[1])
        delete_range_on_cql1 = cql1.prepare(f"DELETE FROM {table} WHERE pk = 0 AND ck >= ? AND ck <= ?")
        delete_range_on_cql1.consistency_level = ConsistencyLevel.ONE
        await cql1.run_async(delete_range_on_cql1, [160, 200])
        for ck in [150, 250]:
            await cql1.run_async(SimpleStatement(
                f"INSERT INTO {table} (pk, ck, v) VALUES (0, {ck}, {ck})",
                consistency_level=ConsistencyLevel.ONE))
        await manager.server_start(servers[0].server_id, wait_others=1)
        # Both nodes have restarted. Let the shared session reconnect before
        # the keyspace is dropped with it.
        await manager.get_ready_cql(servers)
        cql0 = await manager.get_cql_exclusive(servers[0])

        order_by = " ORDER BY ck DESC" if reverse_order else ""
        select = SimpleStatement(f"SELECT pk, ck, v FROM {table} WHERE pk = 0{order_by}",
                                 consistency_level=ConsistencyLevel.ALL,
                                 fetch_size=2)
        response_future = cql0.execute_async(select)
        first_page = await _wrap_future(response_future)
        assert first_page == []
        assert response_future.has_more_pages
        response_future.start_fetching_next_page()
        rows = await _wrap_future(response_future, all_pages=True)

        assert [r.ck for r in rows] == ([250, 150] if reverse_order else [150, 250])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("reverse_order", "legacy_reverse_format"),
    [
        pytest.param(False, False, id="forward"),
        pytest.param(True, False, id="reverse-native"),
        pytest.param(
            True,
            True,
            id="reverse-legacy",
            marks=pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode"),
        ),
    ],
)
async def test_reconciliation_does_not_page_past_untrimmed_replica_stop(
        manager: ScyllaClusterManager, reverse_order: bool, legacy_reverse_format: bool) -> None:
    """
    Do not let a trailing range tombstone move the cursor past a short replica's stop.

    Node 0 holds large live rows and stops on its page size after a few of
    them. Node 1 holds no rows, only a range tombstone after all of node 0's
    rows in query order, and finishes its range. Node 0's stop is its last
    returned row, which is also the last reconciled row, so reconciliation does
    not trim the result. Converting the result nevertheless consumes node 1's
    tombstone. Its end must not become the cursor, or node 0's remaining rows
    are skipped.
    """
    cfg = {
        'query_page_size_in_bytes': 1024,
        'hinted_handoff_enabled': False,
    }
    if legacy_reverse_format:
        cfg['error_injections_at_startup'] = [
            {'name': 'suppress_features', 'value': 'NATIVE_REVERSE_QUERIES'},
        ]
    servers = await manager.servers_add(2, config=cfg, auto_rack_dc="dc1")
    cql, _ = await manager.get_ready_cql(servers)
    cql0 = await manager.get_cql_exclusive(servers[0])

    async with new_test_keyspace(manager, "WITH replication = "
                                 "{'class': 'NetworkTopologyStrategy', 'replication_factor': 2}") as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} (pk int, ck int, v text, PRIMARY KEY (pk, ck)) "
                            "WITH tombstone_gc = {'mode': 'disabled'}")

        # Put the tombstone after the live rows in query order. In a reversed
        # query, larger clustering keys come first.
        live_rows = list(range(1000, 1010)) if reverse_order else list(range(10))
        tombstone_start, tombstone_end = (100, 200) if reverse_order else (1000, 1100)

        # Each row is large enough that node 0 stops after a few of them.
        await manager.server_stop_gracefully(servers[1].server_id)
        insert = cql0.prepare(f"INSERT INTO {table} (pk, ck, v) VALUES (0, ?, ?)")
        insert.consistency_level = ConsistencyLevel.ONE
        for ck in live_rows:
            await cql0.run_async(insert, [ck, 'x' * 400])
        await manager.server_start(servers[1].server_id, wait_others=1)

        await manager.server_stop_gracefully(servers[0].server_id)
        cql1 = await manager.get_cql_exclusive(servers[1])
        await cql1.run_async(SimpleStatement(
            f"DELETE FROM {table} WHERE pk = 0 AND ck >= {tombstone_start} AND ck <= {tombstone_end}",
            consistency_level=ConsistencyLevel.ONE))
        await manager.server_start(servers[0].server_id, wait_others=1)
        # Both nodes have restarted. Let the shared session reconnect before
        # the keyspace is dropped with it.
        await manager.get_ready_cql(servers)
        cql0 = await manager.get_cql_exclusive(servers[0])

        # The page is large enough that no replica reaches its row limit.
        order_by = " ORDER BY ck DESC" if reverse_order else ""
        select = SimpleStatement(f"SELECT pk, ck, v FROM {table} WHERE pk = 0{order_by}",
                                 consistency_level=ConsistencyLevel.ALL,
                                 fetch_size=100)
        rows = await cql0.run_async(select, all_pages=True)
        assert [r.ck for r in rows] == (list(reversed(live_rows)) if reverse_order else live_rows)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("reverse_order", "legacy_reverse_format"),
    [
        pytest.param(False, False, id="forward"),
        pytest.param(True, False, id="reverse-native"),
        pytest.param(
            True,
            True,
            id="reverse-legacy",
            marks=pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode"),
        ),
    ],
)
async def test_reconciliation_of_identical_pages_does_not_return_static_only_row(
        manager: ScyllaClusterManager, reverse_order: bool, legacy_reverse_format: bool) -> None:
    """
    Do not emit a static-only row for a partition in which identical mutation pages stop early.

    Both replicas hold the same static value and the same row tombstones. Only
    node 0 also holds live rows after the tombstones. Its data page reaches
    them, so the digests differ and the coordinator reconciles. Both mutation
    pages stop on their page size within the tombstones. They are identical, so
    reconciliation finds nothing to repair.

    The merged page holds the static row and tombstones, but no live clustering
    row. The partition does have live rows after the stop, which the
    continuation returns. A static-only row on the first page is therefore
    spurious. It also consumes the query limit and pushes out a real row.
    """
    cfg = {
        'query_page_size_in_bytes': 1024,
        'hinted_handoff_enabled': False,
    }
    if legacy_reverse_format:
        cfg['error_injections_at_startup'] = [
            {'name': 'suppress_features', 'value': 'NATIVE_REVERSE_QUERIES'},
        ]
    servers = await manager.servers_add(2, config=cfg, auto_rack_dc="dc1")
    cql, _ = await manager.get_ready_cql(servers)
    cql0 = await manager.get_cql_exclusive(servers[0])

    async with new_test_keyspace(manager, "WITH replication = "
                                 "{'class': 'NetworkTopologyStrategy', 'replication_factor': 2}") as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} (pk int, ck int, s int static, v int, PRIMARY KEY (pk, ck)) "
                            "WITH tombstone_gc = {'mode': 'disabled'}")

        tombstone_base = 100000 if reverse_order else 0
        live_row_base = 0 if reverse_order else 100000

        await cql0.run_async(SimpleStatement(
            f"INSERT INTO {table} (pk, s) VALUES (0, 1)",
            consistency_level=ConsistencyLevel.ALL))
        delete_row = cql0.prepare(f"DELETE FROM {table} WHERE pk = 0 AND ck = ?")
        delete_row.consistency_level = ConsistencyLevel.ALL
        for ck in range(tombstone_base, tombstone_base + 200):
            await cql0.run_async(delete_row, [ck])

        # Only node 0 holds the live rows.
        await manager.server_stop_gracefully(servers[1].server_id)
        live_rows = list(range(live_row_base, live_row_base + 3))
        for ck in live_rows:
            await cql0.run_async(SimpleStatement(
                f"INSERT INTO {table} (pk, ck, v) VALUES (0, {ck}, {ck})",
                consistency_level=ConsistencyLevel.ONE))
        await manager.server_start(servers[1].server_id, wait_others=1)

        limit = 2
        order_by = " ORDER BY ck DESC" if reverse_order else ""
        select = SimpleStatement(f"SELECT pk, ck, s, v FROM {table} WHERE pk = 0{order_by} LIMIT {limit}",
                                 consistency_level=ConsistencyLevel.QUORUM,
                                 fetch_size=1)
        rows = await cql0.run_async(select, all_pages=True)
        expected_rows = (list(reversed(live_rows)) if reverse_order else live_rows)[:limit]
        assert [(r.ck, r.s, r.v) for r in rows] == [(ck, 1, ck) for ck in expected_rows]


@pytest.mark.asyncio
async def test_reconciliation_keeps_cursor_of_page_filled_before_replica_stop(manager: ScyllaClusterManager) -> None:
    """
    Continue after the last returned row when a reconciled page fills before a replica's stop.

    Partition P precedes partition Q in token order, and one vnode range
    holds both, so that one read reconciles them. Node 0 holds P's rows 1
    and 3, node 1 holds its rows 2 and 4. Both hold Q's live static value and
    row tombstones, but no live row of Q. With a page size of three rows, both
    mutation pages return their rows of P and stop on their page size within
    Q's tombstones. Reconciliation drops Q's static row, because Q's
    static-only row is not decided yet. Conversion returns P's rows 1, 2 and 3
    and stops at the row limit.

    The next page must continue after row 3. Continuing from the replicas' stop
    in Q skips row 4.
    """
    cfg = {
        'query_page_size_in_bytes': 1024,
        'hinted_handoff_enabled': False,
    }
    servers = await manager.servers_add(2, config=cfg, auto_rack_dc="dc1")
    cql, _ = await manager.get_ready_cql(servers)
    cql0 = await manager.get_cql_exclusive(servers[0])

    # Pick keys whose tokens follow each other in this order, with no vnode
    # boundary between them, so that the scan reconciles them in one read.
    p, q = await keys_within_one_vnode(manager, servers[0], 2)

    async with new_test_keyspace(manager, "WITH replication = "
                                 "{'class': 'NetworkTopologyStrategy', 'replication_factor': 2} "
                                 "AND tablets = {'enabled': false}") as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} (pk int, ck int, s int static, v int, PRIMARY KEY (pk, ck)) "
                            "WITH tombstone_gc = {'mode': 'disabled'}")

        await cql0.run_async(SimpleStatement(
            f"INSERT INTO {table} (pk, s) VALUES ({q}, 1)",
            consistency_level=ConsistencyLevel.ALL))
        delete_row = cql0.prepare(f"DELETE FROM {table} WHERE pk = {q} AND ck = ?")
        delete_row.consistency_level = ConsistencyLevel.ALL
        for ck in range(200):
            await cql0.run_async(delete_row, [ck])

        # Each node holds half of P's rows.
        for server_idx, cks in [(0, [1, 3]), (1, [2, 4])]:
            other = servers[1 - server_idx]
            await manager.server_stop_gracefully(other.server_id)
            cql_one = await manager.get_cql_exclusive(servers[server_idx])
            for ck in cks:
                await cql_one.run_async(SimpleStatement(
                    f"INSERT INTO {table} (pk, ck, v) VALUES ({p}, {ck}, {ck})",
                    consistency_level=ConsistencyLevel.ONE))
            await manager.server_start(other.server_id, wait_others=1)
        # Both nodes have restarted. Let the shared session reconnect before
        # the keyspace is dropped with it.
        await manager.get_ready_cql(servers)
        cql0 = await manager.get_cql_exclusive(servers[0])

        select = SimpleStatement(f"SELECT pk, ck, s, v FROM {table}",
                                 consistency_level=ConsistencyLevel.ALL,
                                 fetch_size=3)
        response_future = cql0.execute_async(select)
        rows = list(await _wrap_future(response_future))
        # Conversion fills the first page with P's rows before it reaches the
        # replicas' stop in Q.
        assert [(r.pk, r.ck) for r in rows] == [(p, 1), (p, 2), (p, 3)]
        while response_future.has_more_pages:
            response_future.start_fetching_next_page()
            rows.extend(await _wrap_future(response_future))
        assert [(r.pk, r.ck, r.s, r.v) for r in rows] == \
            [(p, ck, None, ck) for ck in [1, 2, 3, 4]] + [(q, None, 1, None)]
