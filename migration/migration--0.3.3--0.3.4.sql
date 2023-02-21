/* contrib/lt_profile/lt_profile--0.3.3--0.3.4.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION lt_profile UPDATE TO '0.3.4'" to load this file. \quit

INSERT INTO import_queries_version_order VALUES
  ('lt_profile','0.3.4','lt_profile','0.3.3');

DROP PROCEDURE activity_collect;
DROP PROCEDURE activity_collect_minute;
DROP PROCEDURE activity_collect_minute_clear;
DROP TABLE pg_stat_activity_history;
DROP TABLE pg_stat_activity_history_minute;
DELETE FROM import_queries WHERE relname = 'pg_stat_activity_history';
DELETE FROM import_queries WHERE relname = 'pg_stat_activity_history_minute';

DROP FUNCTION cluster_stats_reset_diff_htbl(jsonb, integer, integer, integer, integer, integer);
DROP FUNCTION cluster_stats_diff_htbl(jsonb, integer, integer, integer, integer, integer);
DROP FUNCTION dbstats_reset_diff_htbl(jsonb, integer, integer, integer, integer, integer);
DROP FUNCTION dbstats_diff_htbl(jsonb, integer, integer, integer, integer, integer, integer);
DROP FUNCTION func_top_time_diff_htbl(jsonb, integer, integer, integer, integer, integer, integer);
DROP FUNCTION func_top_calls_diff_htbl(jsonb, integer, integer, integer, integer, integer, integer);
DROP FUNCTION func_top_trg_diff_htbl(jsonb, integer, integer, integer, integer, integer, integer);
DROP FUNCTION top_growth_indexes_diff_htbl(jsonb, integer, integer, integer, integer, integer, integer);
DROP FUNCTION top_vacuumed_indexes_diff_htbl(jsonb, integer, integer, integer, integer, integer, integer);
DROP FUNCTION top_cpu_time_diff_htbl(jsonb, integer, integer, integer, integer, integer, integer);
DROP FUNCTION top_io_filesystem_diff_htbl(jsonb, integer, integer, integer, integer, integer, integer);
DROP FUNCTION settings_and_changes_diff_htbl(jsonb, integer, integer, integer, integer, integer);
DROP FUNCTION top_elapsed_diff_htbl(jsonb, integer, integer, integer, integer, integer, integer);
DROP FUNCTION top_plan_time_diff_htbl(jsonb, integer, integer, integer, integer, integer, integer);
DROP FUNCTION top_exec_time_diff_htbl(jsonb, integer, integer, integer, integer, integer, integer);
DROP FUNCTION top_exec_diff_htbl(jsonb, integer, integer, integer, integer, integer, integer);
DROP FUNCTION top_iowait_diff_htbl(jsonb, integer, integer, integer, integer, integer, integer);
DROP FUNCTION top_shared_blks_fetched_diff_htbl(jsonb, integer, integer, integer, integer, integer, integer);
DROP FUNCTION top_shared_reads_diff_htbl(jsonb, integer, integer, integer, integer, integer, integer);
DROP FUNCTION top_shared_dirtied_diff_htbl(jsonb, integer, integer, integer, integer, integer, integer);
DROP FUNCTION top_shared_written_diff_htbl(jsonb, integer, integer, integer, integer, integer, integer);
DROP FUNCTION top_wal_size_diff_htbl(jsonb, integer, integer, integer, integer, integer, integer);
DROP FUNCTION top_temp_diff_htbl(jsonb, integer, integer, integer, integer, integer, integer);
DROP FUNCTION statements_stats_diff_htbl(jsonb, integer, integer, integer, integer, integer, integer);
DROP FUNCTION tablespaces_stats_diff_htbl(jsonb, integer, integer, integer, integer, integer);
DROP FUNCTION top_scan_tables_diff_htbl(jsonb, integer, integer, integer, integer, integer, integer);
DROP FUNCTION top_dml_tables_diff_htbl(jsonb, integer, integer, integer, integer, integer, integer);
DROP FUNCTION top_upd_vac_tables_diff_htbl(jsonb, integer, integer, integer, integer, integer, integer);
DROP FUNCTION top_growth_tables_diff_htbl(jsonb, integer, integer, integer, integer, integer, integer);
DROP FUNCTION top_vacuumed_tables_diff_htbl(jsonb, integer, integer, integer, integer, integer, integer);
DROP FUNCTION top_analyzed_tables_diff_htbl(jsonb, integer, integer, integer, integer, integer, integer);
DROP FUNCTION tbl_top_io_diff_htbl(jsonb, integer, integer, integer, integer, integer, integer);
DROP FUNCTION tbl_top_fetch_diff_htbl(jsonb, integer, integer, integer, integer, integer, integer);
DROP FUNCTION ix_top_io_diff_htbl(jsonb, integer, integer, integer, integer, integer, integer);
DROP FUNCTION ix_top_fetch_diff_htbl(jsonb, integer, integer, integer, integer, integer, integer);

DROP FUNCTION get_diffpwr(integer, integer, integer, integer, integer, text, boolean);
DROP FUNCTION get_diffpwr(name, integer, integer, integer, integer, text, boolean);
DROP FUNCTION get_diffpwr(integer, integer, integer, integer, text, boolean);
DROP FUNCTION get_diffpwr(name, varchar(25), varchar(25), text, boolean);
DROP FUNCTION get_diffpwr(varchar(25), varchar(25), text, boolean);
DROP FUNCTION get_diffpwr(name, varchar(25), integer, integer, text, boolean);
DROP FUNCTION get_diffpwr(varchar(25), integer, integer, text, boolean);
DROP FUNCTION get_diffpwr(name, integer, integer, varchar(25), text, boolean);
DROP FUNCTION get_diffpwr(integer, integer, varchar(25), text, boolean);
DROP FUNCTION get_diffpwr(name, tstzrange, tstzrange, text, boolean);
DROP FUNCTION get_diffpwr(name, varchar(25), tstzrange, text, boolean);
DROP FUNCTION get_diffpwr(name, tstzrange, varchar(25), text, boolean);