create temp table t1(i int);
insert into t1 select generate_series(1, 10000);
select diskquota.relation_size_all_segs('t1', true);
select pg_table_size('t1');

create table t2(i int);
insert into t2 select generate_series(1, 10000);
select diskquota.relation_size_all_segs('t2', false);
select pg_table_size('t2');

-- start_ignore
\! mkdir /tmp/test_spc
-- end_ignore
DROP TABLESPACE  IF EXISTS test_spc;
CREATE TABLESPACE test_spc LOCATION '/tmp/test_spc';

alter table t1 set tablespace test_spc;
insert into t1 select generate_series(1, 10000);
select diskquota.relation_size_all_segs('t1', true);
select pg_table_size('t1');

alter table t2 set tablespace test_spc;
insert into t2 select generate_series(1, 10000);
select diskquota.relation_size_all_segs('t2', false);
select pg_table_size('t2');


drop table t1, t2;
drop tablespace test_spc;
\! rm -rf /tmp/test_spc