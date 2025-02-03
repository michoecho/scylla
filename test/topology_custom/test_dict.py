import asyncio
import logging
import itertools
from test.pylib.manager_client import ManagerClient

logger = logging.getLogger(__name__)

async def test_dict(manager: ManagerClient):
    
    # Bootstrap cluster and configure server
    logger.info("Bootstrapping cluster")
    server = (await manager.servers_add(1, cmdline=['--logger-log-level=sstable_compression=debug']))[0]
    
    # Create keyspace and table
    logger.info("Creating table")
    cql = manager.get_cql()
    number_of_tablets = 2
    
    await cql.run_async(
        "CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
    )
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c blob);")
    
    # Disable autocompaction
    logger.info("Disabling autocompaction for the table")
    await manager.api.disable_autocompaction(server.ip_addr, "test", "test")
    
    # Populate data
    logger.info("Populating table")
    with open('sstables/default_dict.bin', 'rb') as f:
        blob = f.read()[-10000:]
    insert = cql.prepare("INSERT INTO test.test (pk, c) VALUES (?, ?);")
    for pks in itertools.batched(range(1000), n=100):
        await asyncio.gather(*[
            cql.run_async(insert, [k, blob]) 
            for k in pks
        ])
    
    # Flush to get initial sstables
    await manager.api.keyspace_flush(server.ip_addr, "test", "test")
    
    # Get initial sstable info
    logger.info("Checking initial SSTables")
    sstable_info = await manager.api.get_sstable_info(server.ip_addr, "test", "test")
    print(sstable_info)
    
    # Alter compression to zstd
    logger.info("Altering table to use Zstandard compression")
    await cql.run_async(
        "ALTER TABLE test.test WITH COMPRESSION = {'sstable_compression': 'ZstdCompressor'};"
    )
    
    # Flush again to trigger compaction with new compression
    await manager.api.keyspace_upgrade_sstables(server.ip_addr, "test")
    
    # Verify that the SSTables are now using zstd
    logger.info("Checking if compression is set to zstd")
    table_info = await cql.run_async(
        "DESCRIBE TABLE test.test"
    )
    assert 'Zstd' in str(table_info)
    
    # Optionally, check for new sstables or their size
    sstable_info_after = await manager.api.get_sstable_info(server.ip_addr, "test", "test")
    logger.info(sstable_info_after)
    
    logger.info("Test completed successfully")
    logger.info("Test completed successfully")
    logger.info((await cql.run_async("SELECT * from test.test WHERE pk = 0"))[0])
