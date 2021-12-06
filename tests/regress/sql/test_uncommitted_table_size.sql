-- temp table
begin;
CREATE TEMP TABLE t1(i int);
INSERT INTO t1 SELECT generate_series(1, 100000);
SELECT pg_sleep(5);
SELECT tableid::regclass, size, segid FROM diskquota.table_size WHERE tableid = 't1'::regclass ORDER BY segid DESC;
SELECT pg_table_size('t1');
commit;

drop table t1;

-- heap table
begin;
CREATE TABLE t2(i int);
INSERT INTO t2 SELECT generate_series(1, 100000);
SELECT pg_sleep(5);
SELECT tableid::regclass, size, segid FROM diskquota.table_size WHERE tableid = 't2'::regclass ORDER BY segid DESC;
SELECT pg_table_size('t2');
commit;

drop table t2;

-- toast table
begin;
CREATE TABLE t3(t text);
INSERT INTO t3 SELECT repeat('a', 10000) FROM generate_series(1, 1000);
SELECT pg_sleep(5);
SELECT tableid::regclass, size, segid FROM diskquota.table_size WHERE tableid = 't3'::regclass ORDER BY segid DESC;
SELECT pg_table_size('t3');
commit;

drop table t3;

-- AO table
begin;
CREATE TABLE ao (i int) WITH (appendonly=true);
INSERT INTO ao SELECT generate_series(1, 100000);
SELECT pg_sleep(5);
SELECT tableid::regclass, size, segid FROM diskquota.table_size WHERE tableid = 'ao'::regclass ORDER BY segid DESC;
SELECT pg_table_size('ao');
commit;

DROP TABLE ao;

-- AOSC table
begin;
CREATE TABLE aocs (i int, t text) WITH (appendonly=true, orientation=column);
INSERT INTO aocs SELECT i, repeat('a', 1000) FROM generate_series(1, 10000) AS i;
SELECT pg_sleep(5);
SELECT tableid::regclass, size, segid FROM diskquota.table_size WHERE tableid = 'aocs'::regclass ORDER BY segid DESC;
SELECT pg_table_size('aocs');
commit;

DROP TABLE aocs;
