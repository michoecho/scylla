#!/usr/bin/env python3
"""Load generator for the tracing prototype.

Writes a table, flushes it to sstables, then reads it back with BYPASS CACHE so
that the selects actually go to disk -- otherwise everything is served from the
memtable and the row cache, and the trace has no io_begin/io_end to account for.

Run against a node started by ./run-node.sh, from inside `nix develop`.
"""
import sys
import urllib.request

from cassandra.cluster import Cluster

NODE = sys.argv[1] if len(sys.argv) > 1 else "127.11.11.1"
ROWS = 400
SELECTS = 10000

cluster = Cluster([NODE])
session = cluster.connect()

session.execute("CREATE KEYSPACE IF NOT EXISTS tr WITH replication="
                "{'class':'NetworkTopologyStrategy','replication_factor':1}")
session.execute("CREATE TABLE IF NOT EXISTS tr.t (k int PRIMARY KEY, v text)")

insert = session.prepare("INSERT INTO tr.t (k,v) VALUES (?,?)")
for i in range(ROWS):
    session.execute(insert, (i, "value-%d" % i))
print("inserted %d rows" % ROWS)

# Push the memtable out to sstables, so there is something on disk to read.
with urllib.request.urlopen(urllib.request.Request(
        "http://%s:10000/storage_service/keyspace_flush/tr" % NODE,
        method="POST")) as response:
    print("flush:", response.status, response.read().decode().strip())

# BYPASS CACHE so each read goes to the sstables rather than to the row cache.
select = session.prepare("SELECT * FROM tr.t WHERE k=? BYPASS CACHE")
for i in range(SELECTS):
    session.execute(select, (i % ROWS,))
print("selected %d times" % SELECTS)

cluster.shutdown()
