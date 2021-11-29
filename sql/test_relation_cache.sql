-- heap table
begin;
create table t(i int);
insert into t select generate_series(1, 100000);

select count(*) from diskquota.show_relation_cache_all_seg();
commit;

select pg_sleep(5);
select count(*) from diskquota.show_relation_cache_all_seg();
drop table t;

-- toast table
begin;
create table t(t text);
insert into t select array(select * from generate_series(1,1000)) from generate_series(1, 1000);

select count(*) from diskquota.show_relation_cache_all_seg();

select diskquota.check_relation_cache();
commit;

select pg_sleep(5);
select count(*) from diskquota.show_relation_cache_all_seg();
drop table t;

-- AO table
begin;
create table t(a int, b text) with(appendonly=true);
insert into t select generate_series(1,1000) as a, repeat('a', 1000) as b;

select count(*) from diskquota.show_relation_cache_all_seg();

select diskquota.check_relation_cache();
commit;

select pg_sleep(5);
select count(*) from diskquota.show_relation_cache_all_seg();
drop table t;

-- AOCS table
begin;
create table t(a int, b text) with(appendonly=true, orientation=column);
insert into t select generate_series(1,1000) as a, repeat('a', 1000) as b;

select count(*) from diskquota.show_relation_cache_all_seg();

select diskquota.check_relation_cache();
commit;

select pg_sleep(5);
select count(*) from diskquota.show_relation_cache_all_seg();
drop table t;
