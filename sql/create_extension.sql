CREATE SCHEMA IF NOT EXISTS profile;
CREATE SCHEMA IF NOT EXISTS dblink;
CREATE SCHEMA IF NOT EXISTS statements;
CREATE EXTENSION dblink SCHEMA dblink;
CREATE EXTENSION lt_stat_statements SCHEMA statements;
CREATE EXTENSION lt_stat_activity SCHEMA profile;
CREATE EXTENSION system_stats SCHEMA profile;
CREATE EXTENSION lt_profile SCHEMA profile;
