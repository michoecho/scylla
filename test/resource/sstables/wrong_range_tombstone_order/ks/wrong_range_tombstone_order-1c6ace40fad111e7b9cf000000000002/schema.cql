CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE ks.wrong_range_tombstone_order (
    p int,
    a int,
    b int,
    c int,
    r int,
    PRIMARY KEY (p, a, b, c)
) WITH COMPACT STORAGE;
