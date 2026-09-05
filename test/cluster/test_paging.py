#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import pytest
from cassandra import ConsistencyLevel  # type: ignore
from cassandra.query import SimpleStatement  # type: ignore

from test.cluster.util import new_test_keyspace
from test.pylib.async_cql import _wrap_future
from test.pylib.internal_types import ServerInfo
from test.pylib.scylla_cluster_manager import ScyllaClusterManager


async def read_retries(manager: ScyllaClusterManager, server: ServerInfo) -> int:
    metrics = await manager.metrics.query(server.ip_addr)
    return int(metrics.get("scylla_storage_proxy_coordinator_read_retries") or 0)


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
