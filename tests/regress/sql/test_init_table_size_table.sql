-- heap table
CREATE TABLE t(i int);
INSERT INTO t SELECT generate_series(1, 100000);

-- heap table index
CREATE INDEX idx on t(i);

-- toast table
CREATE TABLE toast(t text);
INSERT INTO toast SELECT repeat('a', 10000) FROM generate_series(1, 1000);

-- toast table index
CREATE INDEX toast_idx on toast(t);

-- AO table
CREATE TABLE ao (i int) WITH (appendonly=true);
INSERT INTO ao SELECT generate_series(1, 100000);

-- AO table index
CREATE INDEX ao_idx on ao(i);

-- AOCS table
CREATE TABLE aocs (i int, t text) WITH (appendonly=true, orientation=column);
INSERT INTO aocs SELECT i, repeat('a', 1000) FROM generate_series(1, 10000) AS i;

-- AOCS table index
CREATE INDEX aocs_idx on aocs(i);

SELECT pg_sleep(5);

select tableid::regclass, size from diskquota.table_size where segid = -1 and tableid::regclass::name not like '%.%' order by tableid;

-- init diskquota.table_size
SELECT diskquota.init_table_size_table();
select tableid::regclass, size from diskquota.table_size where segid = -1 and tableid::regclass::name not like '%.%' order by tableid;

DROP TABLE t;
DROP TABLE toast;
DROP TABLE ao;
DROP TABLE aocs;
