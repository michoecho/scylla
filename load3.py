#!/usr/bin/env python3
"""RF=3 load for the distributed tracing prototype: CL=ALL writes and reads."""
import urllib.request
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel

NODES = ["127.11.11.1", "127.11.11.2", "127.11.11.3"]
ROWS = 200
SELECTS = 2000

cluster = Cluster(NODES)
session = cluster.connect()
session.execute("CREATE KEYSPACE IF NOT EXISTS tr WITH replication="
                "{'class':'NetworkTopologyStrategy','replication_factor':3}")
session.execute("CREATE TABLE IF NOT EXISTS tr.t (k int PRIMARY KEY, v text)")

insert = session.prepare("INSERT INTO tr.t (k,v) VALUES (?,?)")
insert.consistency_level = ConsistencyLevel.ALL
for i in range(ROWS):
    session.execute(insert, (i, "value-%d" % i))
print("inserted %d rows" % ROWS)

for node in NODES:
    with urllib.request.urlopen(urllib.request.Request(
            "http://%s:10000/storage_service/keyspace_flush/tr" % node, method="POST")) as r:
        print("flush", node, r.status)

select = session.prepare("SELECT * FROM tr.t WHERE k=? BYPASS CACHE")
select.consistency_level = ConsistencyLevel.ALL
for i in range(SELECTS):
    session.execute(select, (i % ROWS,))
print("selected %d times" % SELECTS)
