/* Testing drop server with data */
SELECT * FROM profile.drop_server('local');
DROP EXTENSION lt_profile;
DROP EXTENSION pg_stat_kcache;
DROP EXTENSION system_stats;
DROP ROLE monitor_system_stats;
DROP EXTENSION pg_wait_sampling;
DROP EXTENSION lt_stat_statements;
DROP EXTENSION dblink;
DROP SCHEMA profile;
DROP SCHEMA dblink;
DROP SCHEMA statements;
DROP SCHEMA kcache;
