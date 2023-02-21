/* ========= Tables ========= */
CREATE TABLE servers (
    server_id           SERIAL PRIMARY KEY,
    server_name         name UNIQUE NOT NULL,
    server_description  text,
    server_created      timestamp with time zone DEFAULT now(),
    db_exclude          name[] DEFAULT NULL,
    enabled             boolean DEFAULT TRUE,
    connstr             text,
    max_sample_age      integer NULL,
    last_sample_id      integer DEFAULT 0 NOT NULL,
    size_smp_wnd_start  time with time zone,
    size_smp_wnd_dur    interval hour to second,
    size_smp_interval   interval day to minute
);
COMMENT ON TABLE servers IS 'lt_profile: Monitored servers (LightDB clusters) list';

INSERT INTO servers (server_name,enabled,connstr) VALUES ('local',true,'dbname='||current_database()||' port='||current_setting('port'));

CREATE TABLE sample_server_hardware (
    server_id        integer NOT NULL REFERENCES servers(server_id) ON DELETE CASCADE,
    sample_id        integer NOT NULL,
    architecture     text,
    byte_order       text,
    cpus             integer,
    threads_per_core integer,
    cores_per_socket integer,
    sockets          integer,
    vendor_id        text,
    cpu_family       integer,
    model            integer,
    model_name       text,
    cpu_mhz          text,
    l1dcache         text,
    l1icache         text,
    l2cache          text,
    l3cache          text,
    memory_total     int8,
    swap_total       int8
);
COMMENT ON TABLE sample_server_hardware IS 'lt_profile: Server hardware infomation (collected by system_stats)';

CREATE TABLE sample_cluster_instance (
    server_id        integer NOT NULL REFERENCES servers(server_id) ON DELETE CASCADE,
    sample_id        integer NOT NULL,
    hostname         text,
    cluster_name     text,
    role             text,
    release          text,
    startup_time     timestamp
);
COMMENT ON TABLE sample_cluster_instance IS 'lt_profile';

CREATE TABLE sample_kernel_settings (
    server_id                   integer NOT NULL REFERENCES servers(server_id) ON DELETE CASCADE,
    sample_id                   integer NOT NULL,
    name                        text,
    value                       text,
    sourcefile                  text
);
COMMENT ON TABLE sample_kernel_settings IS 'lt_profile';

CREATE TABLE sample_cpu_usage (
    server_id                   integer NOT NULL REFERENCES servers(server_id) ON DELETE CASCADE,
    sample_id                   integer NOT NULL,
    user_cpu                    float4,
    sys_cpu                     float4,
    idle_cpu                    float4,
    io_wait                     float4,
    CONSTRAINT pk_sample_cpu_usage PRIMARY KEY (server_id, sample_id)
);
COMMENT ON TABLE sample_cpu_usage IS 'lt_profile';
CREATE TABLE sample_db_memory_usage (
    server_id                   integer NOT NULL REFERENCES servers(server_id) ON DELETE CASCADE,
    sample_id                   integer NOT NULL,
    shared_memory               bigint,  -- shared memory size in byte
    local_memory                bigint,  -- local memory size in byte
    CONSTRAINT pk_sample_db_memory_usage PRIMARY KEY (server_id, sample_id)
);
COMMENT ON TABLE sample_db_memory_usage IS 'lt_profile';

CREATE TABLE samples (
    server_id integer NOT NULL REFERENCES servers(server_id) ON DELETE CASCADE,
    sample_id integer NOT NULL,
    sample_time timestamp (0) with time zone,
    CONSTRAINT pk_samples PRIMARY KEY (server_id, sample_id)
);

CREATE INDEX ix_sample_time ON samples(server_id, sample_time);
COMMENT ON TABLE samples IS 'lt_profile: Sample times list';

CREATE TABLE sample_settings (
    server_id          integer,
    first_seen          timestamp (0) with time zone,
    setting_scope      smallint, -- Scope of setting. Currently may be 1 for pg_settings and 2 for other adm functions (like version)
    name               text,
    setting            text,
    reset_val          text,
    boot_val           text,
    unit               text,
    sourcefile          text,
    sourceline         integer,
    pending_restart    boolean,
    CONSTRAINT pk_sample_settings PRIMARY KEY (server_id, first_seen, setting_scope, name),
    CONSTRAINT fk_sample_settings_servers FOREIGN KEY (server_id)
      REFERENCES servers(server_id) ON DELETE CASCADE
);
COMMENT ON TABLE sample_settings IS 'lt_profile: pg_settings values changes detected at time of sample';

CREATE TABLE sample_timings (
    server_id   integer NOT NULL,
    sample_id   integer NOT NULL,
    event       text,
    time_spent  interval MINUTE TO SECOND (2),
    CONSTRAINT pk_sample_timings PRIMARY KEY (server_id, sample_id, event),
    CONSTRAINT fk_sample_timings_sample FOREIGN KEY (server_id, sample_id) REFERENCES samples(server_id, sample_id) ON DELETE CASCADE
);
COMMENT ON TABLE sample_timings IS 'lt_profile: Sample taking time statistics';

CREATE TABLE baselines (
    server_id   integer NOT NULL REFERENCES servers(server_id) ON DELETE CASCADE,
    bl_id       SERIAL,
    bl_name     varchar (25) NOT NULL,
    keep_until  timestamp (0) with time zone,
    CONSTRAINT pk_baselines PRIMARY KEY (server_id, bl_id),
    CONSTRAINT uk_baselines UNIQUE (server_id,bl_name)
);
COMMENT ON TABLE baselines IS 'lt_profile: Baselines list';

CREATE TABLE bl_samples (
    server_id   integer NOT NULL,
    sample_id   integer NOT NULL,
    bl_id       integer NOT NULL,
    CONSTRAINT fk_bl_samples_samples FOREIGN KEY (server_id, sample_id) REFERENCES samples(server_id, sample_id) ON DELETE RESTRICT,
    CONSTRAINT fk_bl_samples_baselines FOREIGN KEY (server_id, bl_id) REFERENCES baselines(server_id, bl_id) ON DELETE CASCADE,
    CONSTRAINT pk_bl_samples PRIMARY KEY (server_id, bl_id, sample_id)
);
CREATE INDEX ix_bl_samples_blid ON bl_samples(bl_id);
COMMENT ON TABLE bl_samples IS 'lt_profile: Samples in baselines';

CREATE TABLE stmt_list(
    server_id      integer NOT NULL REFERENCES servers(server_id) ON DELETE CASCADE,
    queryid_md5    char(32),
    query          text,
    CONSTRAINT pk_stmt_list PRIMARY KEY (server_id, queryid_md5)
);
COMMENT ON TABLE stmt_list IS 'lt_profile: Statements, captured in samples';

CREATE TABLE sample_stat_database
(
    server_id           integer,
    sample_id           integer,
    datid               oid,
    datname             name NOT NULL,
    xact_commit         bigint,
    xact_rollback       bigint,
    blks_read           bigint,
    blks_hit            bigint,
    tup_returned        bigint,
    tup_fetched         bigint,
    tup_inserted        bigint,
    tup_updated         bigint,
    tup_deleted         bigint,
    conflicts           bigint,
    temp_files          bigint,
    temp_bytes          bigint,
    deadlocks           bigint,
    blk_read_time       double precision,
    blk_write_time      double precision,
    stats_reset         timestamp with time zone,
    datsize             bigint,
    datsize_delta       bigint,
    datistemplate       boolean,
    CONSTRAINT fk_statdb_samples FOREIGN KEY (server_id, sample_id)
      REFERENCES samples (server_id, sample_id) ON DELETE CASCADE,
    CONSTRAINT pk_sample_stat_database PRIMARY KEY (server_id,sample_id,datid)
);
COMMENT ON TABLE sample_stat_database IS 'lt_profile: Sample database statistics table (fields from pg_stat_database)';

CREATE TABLE last_stat_database AS SELECT * FROM sample_stat_database WHERE 0=1;
ALTER TABLE last_stat_database  ADD CONSTRAINT pk_last_stat_database PRIMARY KEY (server_id, sample_id, datid);
ALTER TABLE last_stat_database ADD CONSTRAINT fk_last_stat_database_samples
  FOREIGN KEY (server_id, sample_id) REFERENCES samples(server_id, sample_id) ON DELETE RESTRICT;
COMMENT ON TABLE last_stat_database IS 'lt_profile: Last sample data for calculating diffs in next sample';


CREATE TABLE sample_statements (
    server_id           integer,
    sample_id           integer,
    userid              oid,
    datid               oid,
    queryid             bigint,
    queryid_md5         char(32),
    plans               bigint,
    total_plan_time     double precision,
    min_plan_time       double precision,
    max_plan_time       double precision,
    mean_plan_time      double precision,
    stddev_plan_time    double precision,
    calls               bigint,
    total_exec_time     double precision,
    min_exec_time       double precision,
    max_exec_time       double precision,
    mean_exec_time      double precision,
    stddev_exec_time    double precision,
    rows                bigint,
    shared_blks_hit     bigint,
    shared_blks_read    bigint,
    shared_blks_dirtied bigint,
    shared_blks_written bigint,
    local_blks_hit      bigint,
    local_blks_read     bigint,
    local_blks_dirtied  bigint,
    local_blks_written  bigint,
    temp_blks_read      bigint,
    temp_blks_written   bigint,
    blk_read_time       double precision,
    blk_write_time      double precision,
    wal_records         bigint,
    wal_fpi             bigint,
    wal_bytes           numeric,
    CONSTRAINT pk_sample_statements_n PRIMARY KEY (server_id,sample_id,datid,userid,queryid),
    CONSTRAINT fk_stmt_list FOREIGN KEY (server_id,queryid_md5)
      REFERENCES stmt_list (server_id,queryid_md5) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_statments_dat FOREIGN KEY (server_id, sample_id, datid)
      REFERENCES sample_stat_database(server_id, sample_id, datid) ON DELETE CASCADE
);
CREATE INDEX ix_sample_stmts_qid ON sample_statements (queryid_md5);
COMMENT ON TABLE sample_statements IS 'lt_profile: Sample statement statistics table (fields from pg_stat_statements)';

CREATE TABLE sample_statements_total (
    server_id           integer,
    sample_id           integer,
    datid               oid,
    plans               bigint,
    total_plan_time     double precision,
    calls               bigint,
    total_exec_time     double precision,
    rows                bigint,
    shared_blks_hit     bigint,
    shared_blks_read    bigint,
    shared_blks_dirtied bigint,
    shared_blks_written bigint,
    local_blks_hit      bigint,
    local_blks_read     bigint,
    local_blks_dirtied  bigint,
    local_blks_written  bigint,
    temp_blks_read      bigint,
    temp_blks_written   bigint,
    blk_read_time       double precision,
    blk_write_time      double precision,
    wal_records         bigint,
    wal_fpi             bigint,
    wal_bytes           numeric,
    statements          bigint,
    CONSTRAINT pk_sample_statements_total PRIMARY KEY (server_id, sample_id, datid),
    CONSTRAINT fk_statments_t_dat FOREIGN KEY (server_id, sample_id, datid)
      REFERENCES sample_stat_database(server_id, sample_id, datid) ON DELETE CASCADE
);
COMMENT ON TABLE sample_statements_total IS 'lt_profile: Aggregated stats for sample, based on pg_stat_statements';

CREATE TABLE sample_kcache (
    server_id           integer,
    sample_id           integer,
    userid              oid,
    datid               oid,
    queryid             bigint,
    queryid_md5         char(32),
    plan_user_time      double precision, --  User CPU time used
    plan_system_time    double precision, --  System CPU time used
    plan_minflts         bigint, -- Number of page reclaims (soft page faults)
    plan_majflts         bigint, -- Number of page faults (hard page faults)
    plan_nswaps         bigint, -- Number of swaps
    plan_reads          bigint, -- Number of bytes read by the filesystem layer
    plan_writes         bigint, -- Number of bytes written by the filesystem layer
    plan_msgsnds        bigint, -- Number of IPC messages sent
    plan_msgrcvs        bigint, -- Number of IPC messages received
    plan_nsignals       bigint, -- Number of signals received
    plan_nvcsws         bigint, -- Number of voluntary context switches
    plan_nivcsws        bigint,
    exec_user_time      double precision, --  User CPU time used
    exec_system_time    double precision, --  System CPU time used
    exec_minflts         bigint, -- Number of page reclaims (soft page faults)
    exec_majflts         bigint, -- Number of page faults (hard page faults)
    exec_nswaps         bigint, -- Number of swaps
    exec_reads          bigint, -- Number of bytes read by the filesystem layer
    exec_writes         bigint, -- Number of bytes written by the filesystem layer
    exec_msgsnds        bigint, -- Number of IPC messages sent
    exec_msgrcvs        bigint, -- Number of IPC messages received
    exec_nsignals       bigint, -- Number of signals received
    exec_nvcsws         bigint, -- Number of voluntary context switches
    exec_nivcsws        bigint,
    CONSTRAINT pk_sample_kcache_n PRIMARY KEY (server_id,sample_id,datid,userid,queryid),
    CONSTRAINT fk_kcache_stmt_list FOREIGN KEY (server_id,queryid_md5)
      REFERENCES stmt_list (server_id,queryid_md5) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_kcache_st FOREIGN KEY (server_id, sample_id, datid,userid,queryid)
      REFERENCES sample_statements(server_id, sample_id, datid,userid,queryid) ON DELETE CASCADE
);
COMMENT ON TABLE sample_kcache IS 'lt_profile: Sample sample_kcache statistics table (fields from pg_stat_kcache)';

CREATE TABLE sample_kcache_total (
    server_id           integer,
    sample_id           integer,
    datid               oid,
    plan_user_time           double precision, --  User CPU time used
    plan_system_time         double precision, --  System CPU time used
    plan_minflts              bigint, -- Number of page reclaims (soft page faults)
    plan_majflts              bigint, -- Number of page faults (hard page faults)
    plan_nswaps              bigint, -- Number of swaps
    plan_reads               bigint, -- Number of bytes read by the filesystem layer
    --plan_reads_blks          bigint, -- Number of 8K blocks read by the filesystem layer
    plan_writes              bigint, -- Number of bytes written by the filesystem layer
    --plan_writes_blks         bigint, -- Number of 8K blocks written by the filesystem layer
    plan_msgsnds             bigint, -- Number of IPC messages sent
    plan_msgrcvs             bigint, -- Number of IPC messages received
    plan_nsignals            bigint, -- Number of signals received
    plan_nvcsws              bigint, -- Number of voluntary context switches
    plan_nivcsws             bigint,
    exec_user_time           double precision, --  User CPU time used
    exec_system_time         double precision, --  System CPU time used
    exec_minflts              bigint, -- Number of page reclaims (soft page faults)
    exec_majflts              bigint, -- Number of page faults (hard page faults)
    exec_nswaps              bigint, -- Number of swaps
    exec_reads               bigint, -- Number of bytes read by the filesystem layer
    --exec_reads_blks          bigint, -- Number of 8K blocks read by the filesystem layer
    exec_writes              bigint, -- Number of bytes written by the filesystem layer
    --exec_writes_blks         bigint, -- Number of 8K blocks written by the filesystem layer
    exec_msgsnds             bigint, -- Number of IPC messages sent
    exec_msgrcvs             bigint, -- Number of IPC messages received
    exec_nsignals            bigint, -- Number of signals received
    exec_nvcsws              bigint, -- Number of voluntary context switches
    exec_nivcsws             bigint,
    statements               bigint NOT NULL,
    CONSTRAINT pk_sample_kcache_total PRIMARY KEY (server_id, sample_id, datid),
    CONSTRAINT fk_kcache_t_st FOREIGN KEY (server_id, sample_id, datid)
      REFERENCES sample_stat_database(server_id, sample_id, datid) ON DELETE CASCADE
);
COMMENT ON TABLE sample_kcache_total IS 'lt_profile: Aggregated stats for kcache, based on pg_stat_kcache';

CREATE TABLE sample_wait_event_total (
    server_id          integer,
    sample_id          integer,
    pid                integer,
    queryid            bigint,
    wait_event_type    text,
    wait_event         text,
    wait_time          double precision,
    state              text
);
COMMENT ON TABLE sample_wait_event_total IS 'lt_profile: Aggregated stats for wait event, based on pg_wait_sampling';

CREATE TABLE tablespaces_list(
    server_id           integer REFERENCES servers(server_id) ON DELETE CASCADE,
    tablespaceid        oid,
    tablespacename      name NOT NULL,
    tablespacepath      text NOT NULL, -- cannot be changed without changing oid
    CONSTRAINT pk_tablespace_list PRIMARY KEY (server_id, tablespaceid)
);
COMMENT ON TABLE tablespaces_list IS 'lt_profile: Tablespaces, captured in samples';

CREATE TABLE sample_stat_tablespaces
(
    server_id           integer,
    sample_id           integer,
    tablespaceid        oid,
    size                bigint NOT NULL,
    size_delta          bigint NOT NULL,
    CONSTRAINT fk_stattbs_samples FOREIGN KEY (server_id, sample_id)
        REFERENCES samples (server_id, sample_id) ON DELETE CASCADE,
    CONSTRAINT fk_st_tablespaces_tablespaces FOREIGN KEY (server_id, tablespaceid)
        REFERENCES tablespaces_list(server_id, tablespaceid) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT pk_sample_stat_tablespaces PRIMARY KEY (server_id,sample_id,tablespaceid)
);
COMMENT ON TABLE sample_stat_tablespaces IS 'lt_profile: Sample tablespaces statistics (fields from pg_tablespace)';

CREATE VIEW v_sample_stat_tablespaces AS
    SELECT
        server_id,
        sample_id,
        tablespaceid,
        tablespacename,
        tablespacepath,
        size,
        size_delta
    FROM sample_stat_tablespaces JOIN tablespaces_list USING (server_id, tablespaceid);
COMMENT ON VIEW v_sample_stat_tablespaces IS 'lt_profile: Tablespaces stats view with tablespace names';

CREATE TABLE last_stat_tablespaces AS SELECT * FROM v_sample_stat_tablespaces WHERE 0=1;
ALTER TABLE last_stat_tablespaces ADD CONSTRAINT pk_last_stat_tablespaces PRIMARY KEY (server_id, sample_id, tablespaceid);
ALTER TABLE last_stat_tablespaces ADD CONSTRAINT fk_last_stat_tablespaces_samples
  FOREIGN KEY (server_id, sample_id) REFERENCES samples(server_id, sample_id) ON DELETE RESTRICT;
COMMENT ON TABLE last_stat_tablespaces IS 'lt_profile: Last sample data for calculating diffs in next sample';

CREATE TABLE tables_list(
    server_id           integer REFERENCES servers(server_id) ON DELETE CASCADE,
    datid               oid,
    relid               oid,
    relkind             char(1) NOT NULL,
    reltoastrelid       oid,
    schemaname          name NOT NULL,
    relname             name NOT NULL,
    CONSTRAINT pk_tables_list PRIMARY KEY (server_id, datid, relid),
    CONSTRAINT fk_toast_table FOREIGN KEY (server_id, datid, reltoastrelid)
      REFERENCES tables_list (server_id, datid, relid) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT uk_toast_table UNIQUE (server_id, datid, reltoastrelid)
);
CREATE UNIQUE INDEX ix_tables_list_reltoast ON tables_list(server_id, datid, reltoastrelid);
COMMENT ON TABLE tables_list IS 'lt_profile: Table names and schemas, captured in samples';

CREATE TABLE sample_stat_tables (
    server_id           integer,
    sample_id           integer,
    datid               oid,
    relid               oid,
    tablespaceid        oid,
    seq_scan            bigint,
    seq_tup_read        bigint,
    idx_scan            bigint,
    idx_tup_fetch       bigint,
    n_tup_ins           bigint,
    n_tup_upd           bigint,
    n_tup_del           bigint,
    n_tup_hot_upd       bigint,
    n_live_tup          bigint,
    n_dead_tup          bigint,
    n_mod_since_analyze bigint,
    n_ins_since_vacuum  bigint,
    last_vacuum         timestamp with time zone,
    last_autovacuum     timestamp with time zone,
    last_analyze        timestamp with time zone,
    last_autoanalyze    timestamp with time zone,
    vacuum_count        bigint,
    autovacuum_count    bigint,
    analyze_count       bigint,
    autoanalyze_count   bigint,
    heap_blks_read      bigint,
    heap_blks_hit       bigint,
    idx_blks_read       bigint,
    idx_blks_hit        bigint,
    toast_blks_read     bigint,
    toast_blks_hit      bigint,
    tidx_blks_read      bigint,
    tidx_blks_hit       bigint,
    relsize             bigint,
    relsize_diff        bigint,
    CONSTRAINT pk_sample_stat_tables PRIMARY KEY (server_id, sample_id, datid, relid),
    CONSTRAINT fk_st_tables_dat FOREIGN KEY (server_id, sample_id, datid)
      REFERENCES sample_stat_database(server_id, sample_id, datid) ON DELETE CASCADE,
    CONSTRAINT fk_st_tables_tablespace FOREIGN KEY (server_id, sample_id, tablespaceid)
      REFERENCES sample_stat_tablespaces(server_id, sample_id, tablespaceid) ON DELETE CASCADE,
    CONSTRAINT fk_st_tables_tables FOREIGN KEY (server_id, datid, relid)
      REFERENCES tables_list(server_id, datid, relid) ON DELETE RESTRICT ON UPDATE RESTRICT
);
COMMENT ON TABLE sample_stat_tables IS 'lt_profile: Stats increments for user tables in all databases by samples';

CREATE VIEW v_sample_stat_tables AS
    SELECT
        server_id,
        sample_id,
        datid,
        relid,
        schemaname,
        relname,
        seq_scan,
        seq_tup_read,
        idx_scan,
        idx_tup_fetch,
        n_tup_ins,
        n_tup_upd,
        n_tup_del,
        n_tup_hot_upd,
        n_live_tup,
        n_dead_tup,
        n_mod_since_analyze,
        n_ins_since_vacuum,
        last_vacuum,
        last_autovacuum,
        last_analyze,
        last_autoanalyze,
        vacuum_count,
        autovacuum_count,
        analyze_count,
        autoanalyze_count,
        heap_blks_read,
        heap_blks_hit,
        idx_blks_read,
        idx_blks_hit,
        toast_blks_read,
        toast_blks_hit,
        tidx_blks_read,
        tidx_blks_hit,
        relsize,
        relsize_diff,
        tablespaceid,
        reltoastrelid,
        relkind
    FROM sample_stat_tables JOIN tables_list USING (server_id, datid, relid);
COMMENT ON VIEW v_sample_stat_tables IS 'lt_profile: Tables stats view with table names and schemas';

CREATE TABLE last_stat_tables AS SELECT * FROM v_sample_stat_tables WHERE 0=1;
ALTER TABLE last_stat_tables ADD CONSTRAINT pk_last_stat_tables
  PRIMARY KEY (server_id, sample_id, datid, relid);
ALTER TABLE last_stat_tables ADD CONSTRAINT fk_last_stat_tables_dat
  FOREIGN KEY (server_id, sample_id, datid)
  REFERENCES last_stat_database(server_id, sample_id, datid) ON DELETE RESTRICT;
ALTER TABLE last_stat_tables ADD CONSTRAINT fk_last_stat_tablespaces
  FOREIGN KEY (server_id, sample_id, tablespaceid)
  REFERENCES last_stat_tablespaces(server_id, sample_id, tablespaceid) ON DELETE RESTRICT;
COMMENT ON TABLE last_stat_tables IS 'lt_profile: Last sample data for calculating diffs in next sample';

CREATE TABLE sample_stat_tables_total (
    server_id           integer,
    sample_id           integer,
    datid               oid,
    tablespaceid        oid,
    relkind             char(1) NOT NULL,
    seq_scan            bigint,
    seq_tup_read        bigint,
    idx_scan            bigint,
    idx_tup_fetch       bigint,
    n_tup_ins           bigint,
    n_tup_upd           bigint,
    n_tup_del           bigint,
    n_tup_hot_upd       bigint,
    vacuum_count        bigint,
    autovacuum_count    bigint,
    analyze_count       bigint,
    autoanalyze_count   bigint,
    heap_blks_read      bigint,
    heap_blks_hit       bigint,
    idx_blks_read       bigint,
    idx_blks_hit        bigint,
    toast_blks_read     bigint,
    toast_blks_hit      bigint,
    tidx_blks_read      bigint,
    tidx_blks_hit       bigint,
    relsize_diff        bigint,
    CONSTRAINT pk_sample_stat_tables_tot PRIMARY KEY (server_id, sample_id, datid, relkind, tablespaceid),
    CONSTRAINT fk_st_tables_tot_dat FOREIGN KEY (server_id, sample_id, datid)
      REFERENCES sample_stat_database(server_id, sample_id, datid) ON DELETE CASCADE,
    CONSTRAINT fk_st_tablespaces_tot_dat FOREIGN KEY (server_id, sample_id, tablespaceid)
      REFERENCES sample_stat_tablespaces(server_id, sample_id, tablespaceid) ON DELETE CASCADE
);
COMMENT ON TABLE sample_stat_tables_total IS 'lt_profile: Total stats for all tables in all databases by samples';

CREATE TABLE indexes_list(
    server_id       integer NOT NULL REFERENCES servers(server_id) ON DELETE CASCADE,
    datid           oid NOT NULL,
    indexrelid      oid NOT NULL,
    relid           oid NOT NULL,
    schemaname      name NOT NULL,
    indexrelname    name NOT NULL,
    CONSTRAINT pk_indexes_list PRIMARY KEY (server_id, datid, indexrelid),
    CONSTRAINT fk_indexes_tables FOREIGN KEY (server_id, datid, relid)
      REFERENCES tables_list(server_id, datid, relid)
);
COMMENT ON TABLE indexes_list IS 'lt_profile: Index names and schemas, captured in samples';

CREATE TABLE sample_stat_indexes (
    server_id           integer,
    sample_id           integer,
    datid               oid,
    indexrelid          oid,
    tablespaceid        oid,
    idx_scan            bigint,
    idx_tup_read        bigint,
    idx_tup_fetch       bigint,
    idx_blks_read       bigint,
    idx_blks_hit        bigint,
    relsize             bigint,
    relsize_diff         bigint,
    indisunique         bool,
    CONSTRAINT fk_stat_indexes_indexes FOREIGN KEY (server_id, datid, indexrelid)
      REFERENCES indexes_list(server_id, datid, indexrelid) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT fk_stat_indexes_dat FOREIGN KEY (server_id, sample_id, datid)
      REFERENCES sample_stat_database(server_id, sample_id, datid) ON DELETE CASCADE,
    CONSTRAINT fk_stat_indexes_tablespaces FOREIGN KEY (server_id, sample_id, tablespaceid)
      REFERENCES sample_stat_tablespaces(server_id, sample_id, tablespaceid) ON DELETE CASCADE,
    CONSTRAINT pk_sample_stat_indexes PRIMARY KEY (server_id, sample_id, datid, indexrelid)
);
COMMENT ON TABLE sample_stat_indexes IS 'lt_profile: Stats increments for user indexes in all databases by samples';

CREATE VIEW v_sample_stat_indexes AS
    SELECT
        server_id,
        sample_id,
        datid,
        relid,
        indexrelid,
        tl.schemaname,
        tl.relname,
        il.indexrelname,
        idx_scan,
        idx_tup_read,
        idx_tup_fetch,
        idx_blks_read,
        idx_blks_hit,
        relsize,
        relsize_diff,
        tablespaceid,
        indisunique
    FROM
        sample_stat_indexes s
        JOIN indexes_list il USING (datid, indexrelid, server_id)
        JOIN tables_list tl USING (datid, relid, server_id);
COMMENT ON VIEW v_sample_stat_indexes IS 'lt_profile: Reconstructed stats view with table and index names and schemas';

CREATE TABLE last_stat_indexes AS SELECT * FROM v_sample_stat_indexes WHERE 0=1;
ALTER TABLE last_stat_indexes ADD CONSTRAINT pk_last_stat_indexes PRIMARY KEY (server_id, sample_id, datid, relid, indexrelid);
ALTER TABLE last_stat_indexes ADD CONSTRAINT fk_last_stat_indexes_dat FOREIGN KEY (server_id, sample_id, datid)
-- Restrict deleting last data sample
  REFERENCES last_stat_database(server_id, sample_id, datid) ON DELETE RESTRICT;
COMMENT ON TABLE last_stat_indexes IS 'lt_profile: Last sample data for calculating diffs in next sample';

CREATE TABLE sample_stat_indexes_total (
    server_id           integer,
    sample_id           integer,
    datid               oid,
    tablespaceid        oid,
    idx_scan            bigint,
    idx_tup_read        bigint,
    idx_tup_fetch       bigint,
    idx_blks_read       bigint,
    idx_blks_hit        bigint,
    relsize_diff         bigint,
    CONSTRAINT fk_stat_indexes_tot_dat FOREIGN KEY (server_id, sample_id, datid)
      REFERENCES sample_stat_database(server_id, sample_id, datid) ON DELETE CASCADE,
    CONSTRAINT fk_stat_tablespaces_tot_dat FOREIGN KEY (server_id, sample_id, tablespaceid)
      REFERENCES sample_stat_tablespaces(server_id, sample_id, tablespaceid) ON DELETE CASCADE,
    CONSTRAINT pk_sample_stat_indexes_tot PRIMARY KEY (server_id, sample_id, datid, tablespaceid)
);
COMMENT ON TABLE sample_stat_indexes_total IS 'lt_profile: Total stats for indexes in all databases by samples';

CREATE TABLE funcs_list(
    server_id   integer NOT NULL REFERENCES servers(server_id) ON DELETE CASCADE,
    datid       oid,
    funcid      oid,
    schemaname  name NOT NULL,
    funcname    name NOT NULL,
    funcargs    text NOT NULL,
    CONSTRAINT pk_funcs_list PRIMARY KEY (server_id, datid, funcid)
);
COMMENT ON TABLE funcs_list IS 'lt_profile: Function names and schemas, captured in samples';

CREATE TABLE sample_stat_user_functions (
    server_id   integer,
    sample_id   integer,
    datid       oid,
    funcid      oid,
    calls       bigint,
    total_time  double precision,
    self_time   double precision,
    trg_fn      boolean,
    CONSTRAINT fk_user_functions_functions FOREIGN KEY (server_id, datid, funcid)
      REFERENCES funcs_list (server_id, datid, funcid) ON DELETE RESTRICT,
    CONSTRAINT fk_user_functions_dat FOREIGN KEY (server_id, sample_id, datid)
      REFERENCES sample_stat_database (server_id, sample_id, datid) ON DELETE CASCADE,
    CONSTRAINT pk_sample_stat_user_functions PRIMARY KEY (server_id, sample_id, datid, funcid)
);
COMMENT ON TABLE sample_stat_user_functions IS 'lt_profile: Stats increments for user functions in all databases by samples';

CREATE VIEW v_sample_stat_user_functions AS
    SELECT
        server_id,
        sample_id,
        datid,
        funcid,
        schemaname,
        funcname,
        funcargs,
        calls,
        total_time,
        self_time,
        trg_fn
    FROM sample_stat_user_functions JOIN funcs_list USING (server_id, datid, funcid);
COMMENT ON VIEW v_sample_stat_indexes IS 'lt_profile: Reconstructed stats view with function names and schemas';

CREATE TABLE last_stat_user_functions AS SELECT * FROM v_sample_stat_user_functions WHERE 0=1;
ALTER TABLE last_stat_user_functions ADD CONSTRAINT fk_last_stat_user_functions_dat
  FOREIGN KEY (server_id, sample_id, datid)
  -- Restrict deleting last data sample
  REFERENCES last_stat_database(server_id, sample_id, datid) ON DELETE RESTRICT;
COMMENT ON TABLE last_stat_user_functions IS 'lt_profile: Last sample data for calculating diffs in next sample';

CREATE TABLE sample_stat_user_func_total (
    server_id   integer,
    sample_id   integer,
    datid       oid,
    calls       bigint,
    total_time  double precision,
    trg_fn      boolean,
    CONSTRAINT fk_user_func_tot_dat FOREIGN KEY (server_id, sample_id, datid)
      REFERENCES sample_stat_database (server_id, sample_id, datid) ON DELETE CASCADE,
    CONSTRAINT pk_sample_stat_user_func_total PRIMARY KEY (server_id, sample_id, datid, trg_fn)
);
COMMENT ON TABLE sample_stat_user_func_total IS 'lt_profile: Total stats for user functions in all databases by samples';

CREATE TABLE sample_stat_cluster
(
    server_id                  integer,
    sample_id                  integer,
    checkpoints_timed          bigint,
    checkpoints_req            bigint,
    checkpoint_write_time      double precision,
    checkpoint_sync_time       double precision,
    buffers_checkpoint          bigint,
    buffers_clean               bigint,
    maxwritten_clean           bigint,
    buffers_backend             bigint,
    buffers_backend_fsync       bigint,
    buffers_alloc               bigint,
    stats_reset                timestamp with time zone,
    wal_size                   bigint,
    CONSTRAINT fk_statcluster_samples FOREIGN KEY (server_id, sample_id)
      REFERENCES samples (server_id, sample_id) ON DELETE CASCADE,
    CONSTRAINT pk_sample_stat_cluster PRIMARY KEY (server_id, sample_id)
);
COMMENT ON TABLE sample_stat_cluster IS 'lt_profile: Sample cluster statistics table (fields from pg_stat_bgwriter, etc.)';

CREATE TABLE last_stat_cluster AS SELECT * FROM sample_stat_cluster WHERE 0=1;
ALTER TABLE last_stat_cluster ADD CONSTRAINT fk_last_stat_cluster_samples
  FOREIGN KEY (server_id, sample_id) REFERENCES samples(server_id, sample_id) ON DELETE RESTRICT;
COMMENT ON TABLE last_stat_cluster IS 'lt_profile: Last sample data for calculating diffs in next sample';

CREATE TABLE sample_stat_archiver
(
    server_id                   integer,
    sample_id                   integer,
    archived_count              bigint,
    last_archived_wal           text,
    last_archived_time          timestamp with time zone,
    failed_count                bigint,
    last_failed_wal             text,
    last_failed_time            timestamp with time zone,
    stats_reset                 timestamp with time zone,
    CONSTRAINT fk_sample_stat_archiver_samples FOREIGN KEY (server_id, sample_id)
      REFERENCES samples (server_id, sample_id) ON DELETE CASCADE,
    CONSTRAINT pk_sample_stat_archiver PRIMARY KEY (server_id, sample_id)
);
COMMENT ON TABLE sample_stat_archiver IS 'lt_profile: Sample archiver statistics table (fields from pg_stat_archiver)';

CREATE TABLE last_stat_archiver AS SELECT * FROM sample_stat_archiver WHERE 0=1;
ALTER TABLE last_stat_archiver ADD CONSTRAINT fk_last_stat_archiver_samples
  FOREIGN KEY (server_id, sample_id) REFERENCES samples(server_id, sample_id) ON DELETE RESTRICT;
COMMENT ON TABLE last_stat_archiver IS 'lt_profile: Last sample data for calculating diffs in next sample';

CREATE TABLE import_queries_version_order (
  extension         text,
  version           text,
  parent_extension  text,
  parent_version    text,
  CONSTRAINT pk_import_queries_version_order PRIMARY KEY (extension, version),
  CONSTRAINT fk_import_queries_version_order FOREIGN KEY (parent_extension, parent_version)
    REFERENCES import_queries_version_order (extension,version)
);
COMMENT ON TABLE import_queries_version_order IS 'lt_profile: Version history used in import process';

CREATE TABLE import_queries (
  extension       text,
  from_version    text,
  exec_order      integer,
  relname         text,
  query           text NOT NULL,
  CONSTRAINT pk_import_queries PRIMARY KEY (extension, from_version, exec_order, relname),
  CONSTRAINT fk_import_queries_version FOREIGN KEY (extension, from_version)
    REFERENCES import_queries_version_order (extension,version)
);
COMMENT ON TABLE import_queries IS 'lt_profile: Queries, used in import process';

CREATE TABLE sample_stat_tables_failures (
    server_id           integer,
    sample_id           integer,
    datid               oid,
    relid               oid,
    size_failed         boolean,
    toastsize_failed    boolean,
    CONSTRAINT pk_sample_stat_tables_failures PRIMARY KEY (server_id, sample_id, datid, relid),
    CONSTRAINT fk_sample_tables_failures_samples FOREIGN KEY (server_id, sample_id)
      REFERENCES samples (server_id, sample_id) ON DELETE CASCADE,
    CONSTRAINT fk_sample_tables_failures_tables FOREIGN KEY (server_id, datid, relid)
      REFERENCES tables_list(server_id, datid, relid) ON DELETE CASCADE
);
COMMENT ON TABLE sample_stat_tables_failures IS 'lt_profile: For each sample lists tables with stats failed to collect';

CREATE TABLE sample_stat_indexes_failures (
    server_id           integer,
    sample_id           integer,
    datid               oid,
    indexrelid          oid,
    size_failed         boolean,
    CONSTRAINT pk_sample_stat_indexes_failures PRIMARY KEY (server_id, sample_id, datid, indexrelid),
    CONSTRAINT fk_sample_indexes_failures_samples FOREIGN KEY (server_id, sample_id)
      REFERENCES samples (server_id, sample_id) ON DELETE CASCADE,
    CONSTRAINT fk_sample_indexes_failures_indexes FOREIGN KEY (server_id, datid, indexrelid)
      REFERENCES indexes_list(server_id, datid, indexrelid) ON DELETE CASCADE
);
COMMENT ON TABLE sample_stat_indexes_failures IS 'lt_profile: For each sample lists tables with stats failed to collect';
