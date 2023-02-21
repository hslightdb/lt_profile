/* contrib/pg_profile/lt_profile--0.3.5--0.3.6.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION lt_profile UPDATE TO '0.3.6'" to load this file. \quit

INSERT INTO import_queries_version_order VALUES
  ('lt_profile','0.3.6','lt_profile','0.3.5');

CREATE TABLE sample_cpu_usage (
    server_id                   integer NOT NULL REFERENCES servers(server_id) ON DELETE CASCADE,
    sample_id                   integer NOT NULL,
    user_cpu                    float4,
    sys_cpu                     float4,
    idle_cpu                    float4,
    io_wait                     float4,
    CONSTRAINT pk_sample_cpu_usage PRIMARY KEY (server_id, sample_id)
);

CREATE TABLE sample_db_memory_usage (
    server_id                   integer NOT NULL REFERENCES servers(server_id) ON DELETE CASCADE,
    sample_id                   integer NOT NULL,
    shared_memory               bigint,  -- shared memory size in byte
    local_memory                bigint,  -- local memory size in byte
    CONSTRAINT pk_sample_db_memory_usage PRIMARY KEY (server_id, sample_id)
);