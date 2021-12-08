CREATE SCHEMA s;
SELECT diskquota.set_schema_quota('s', '10MB');
SET search_path to s;

CREATE TABLE t1 AS SELECT generate_series(1, 100000000);

CREATE TEMP TABLE t2 AS SELECT generate_series(1, 100000000);

CREATE TABLE toast_table AS SELECT repeat('a', 10000) FROM generate_series(1, 10000);

CREATE TABLE ao_table WITH(appendonly=true) AS SELECT generate_series(1, 100000000);

CREATE TABLE aocs_table WITH(appendonly=true, orientation=column) AS SELECT i, repeat('a', 10000) FROM generate_series(1, 10000) AS i;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS toast_table;
DROP TABLE IF EXISTS ao_table;
DROP TABLE IF EXISTS aocs_table;
RESET search_path;
DROP SCHEMA s;