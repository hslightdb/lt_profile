/* Testing drop server with data */
SELECT * FROM profile.drop_server('local');
DROP EXTENSION lt_profile;
DROP EXTENSION IF EXISTS system_stats;
DROP EXTENSION IF EXISTS lt_stat_activity;
DROP EXTENSION IF EXISTS lt_stat_statements;
DROP EXTENSION IF EXISTS dblink;
DROP SCHEMA profile;
DROP SCHEMA dblink;
DROP SCHEMA statements;
