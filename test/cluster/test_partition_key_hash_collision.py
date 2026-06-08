# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

import asyncio
import logging
import uuid
from test.pylib.manager_client import ManagerClient
from cassandra.cluster import ConsistencyLevel

logger = logging.getLogger(__name__)

async def test_partition_key_hash_collision(manager: ManagerClient):
    """
    Reproduces a hash collision between two distinct composite partition keys.
    Both keys hash to token -5713792590567535071, which previously caused an
    "out-of-order partition key" failure during sstable writing.
    """
    logger.info("Bootstrapping cluster")
    servers = await manager.servers_add(1, cmdline=["--sstable-format=ms"])

    ks_name = "test"
    cf_name = "counters"

    cql = manager.get_cql()

    logger.info("Creating keyspace and table")
    await cql.run_async(
        f"CREATE KEYSPACE {ks_name} WITH replication = "
        f"{{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}"
    )
    await cql.run_async(f"""
        CREATE TABLE {ks_name}.{cf_name} (
            subscription_uuid uuid,
            counter_spec text,
            bucket bigint,
            event_uuid uuid,
            delta bigint,
            PRIMARY KEY ((subscription_uuid, counter_spec, bucket), event_uuid)
        )
    """)

    # Two distinct partition keys that hash to the same token
    # (-5713792590567535071), which was the source of the "out-of-order
    # partition key" failure.
    key1 = (
        uuid.UUID("a248eeea-6d0f-11e5-81a5-4000c9205a74"),
        "e9089e51-f9dd-4822-ba90-259699e9ff91"
        "#f06c7a86-8044-11ef-9091-c000e78e8f37"
        "#15m#1779300000",
        1779363900000,
    )
    key2 = (
        uuid.UUID("57371272-b6d3-11e8-8df2-c000ab94f1e6"),
        "05709e26-6048-4823-b5a0-a16367e8be56"
        "#6c53aeca-49d4-11ed-8115-00004e970775"
        "#15m#1707300000",
        1707992100000,
    )

    insert = cql.prepare(
        f"INSERT INTO {ks_name}.{cf_name} "
        f"(subscription_uuid, counter_spec, bucket, event_uuid, delta) "
        f"VALUES (?, ?, ?, ?, ?)"
    )
    insert.consistency_level = ConsistencyLevel.ALL

    logger.info("Inserting colliding partition keys")
    event_uuid = uuid.uuid4()
    await cql.run_async(insert, [*key1, event_uuid, 1])
    await cql.run_async(insert, [*key2, event_uuid, 2])

    logger.info("Flushing table")
    await asyncio.gather(*[
        manager.api.keyspace_flush(s.ip_addr, ks_name, cf_name) for s in servers
    ])

    logger.info("Validating both rows are readable")
    select = cql.prepare(
        f"SELECT delta FROM {ks_name}.{cf_name} "
        f"WHERE subscription_uuid = ? AND counter_spec = ? "
        f"AND bucket = ? AND event_uuid = ?"
    )
    select.consistency_level = ConsistencyLevel.ALL
    r1 = await cql.run_async(select, [*key1, event_uuid])
    r2 = await cql.run_async(select, [*key2, event_uuid])
    assert r1[0][0] == 1
    assert r2[0][0] == 2
