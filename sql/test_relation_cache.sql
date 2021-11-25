begin;
create table t(i int);
insert into t select generate_series(1, 100000);

WITH table_size AS (
    SELECT diskquota.diskquota_fetch_table_stat(1, ARRAY['t'::regclass]) AS a
    FROM  gp_dist_random('gp_id')
)
SELECT (a).* FROM table_size order by table_size;

select pg_table_size('t') as ts from gp_dist_random('gp_id') order by ts;

abort;
