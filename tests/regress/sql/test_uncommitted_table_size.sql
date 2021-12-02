-- temp table
begin;
CREATE TEMP TABLE t1(i int);
INSERT INTO t1 SELECT generate_series(1, 100000);
SELECT pg_sleep(5);
SELECT tableid::regclass, size, segid from diskquota.table_size where tableid = 't1'::regclass order by segid desc;SELECT pg_table_size('t1');
commit;

drop table t1;

-- heap table
begin;
CREATE TABLE t2(i int);
INSERT INTO t2 SELECT generate_series(1, 100000);
SELECT pg_sleep(5);
SELECT tableid::regclass, size, segid from diskquota.table_size where tableid = 't2'::regclass order by segid desc;SELECT pg_table_size('t2');
commit;

drop table t2;

-- AO table
begin;
CREATE TABLE ao (i int) WITH (appendonly=true);
INSERT INTO ao SELECT generate_series(1, 100000);
SELECT pg_sleep(5);
SELECT tableid::regclass, size, segid from diskquota.table_size where tableid = 'ao'::regclass order by segid desc;SELECT pg_table_size('ao');
commit;

DROP TABLE ao;

-- AOSC table
begin;
CREATE TABLE aocs (i int, t text) WITH (appendonly=true, orientation=column);
INSERT INTO aocs SELECT i, repeat('a', 1000) FROM generate_series(1, 10000) AS i;
SELECT pg_sleep(5);
SELECT tableid::regclass, size, segid from diskquota.table_size where tableid = 'aocs'::regclass order by segid desc;SELECT pg_table_size('aocs');
commit;

DROP TABLE aocs;
