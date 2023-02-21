/* ========= Check available statement stats for report ========= */

CREATE FUNCTION profile_checkavail_statstatements(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS BOOLEAN
SET search_path=@extschema@ AS $$
-- Check if there was available lt_stat_statements statistics for report interval
  SELECT count(sn.sample_id) = count(st.sample_id)
  FROM samples sn LEFT OUTER JOIN sample_statements_total st USING (server_id, sample_id)
  WHERE sn.server_id = sserver_id AND sn.sample_id BETWEEN start_id + 1 AND end_id
$$ LANGUAGE sql;

CREATE FUNCTION profile_checkavail_planning_times(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS BOOLEAN
SET search_path=@extschema@ AS $$
-- Check if we have planning times collected for report interval
  SELECT COALESCE(sum(total_plan_time), 0) > 0
  FROM sample_statements_total sn
  WHERE sn.server_id = sserver_id AND sn.sample_id BETWEEN start_id + 1 AND end_id
$$ LANGUAGE sql;

CREATE FUNCTION profile_checkavail_wal_bytes(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS BOOLEAN
SET search_path=@extschema@ AS $$
-- Check if we have statement wal sizes collected for report interval
  SELECT COALESCE(sum(wal_bytes), 0) > 0
  FROM sample_statements_total sn
  WHERE sn.server_id = sserver_id AND sn.sample_id BETWEEN start_id + 1 AND end_id
$$ LANGUAGE sql;

/* ========= Statement stats functions ========= */

CREATE FUNCTION statements_stats(IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer)
RETURNS TABLE(
        dbname              name,
        datid               oid,
        calls               bigint,
        plans               bigint,
        total_exec_time     double precision,
        total_plan_time     double precision,
        blk_read_time       double precision,
        blk_write_time      double precision,
        trg_fn_total_time   double precision,
        shared_gets         bigint,
        local_gets          bigint,
        shared_blks_dirtied bigint,
        local_blks_dirtied  bigint,
        temp_blks_read      bigint,
        temp_blks_written   bigint,
        local_blks_read     bigint,
        local_blks_written  bigint,
        statements          bigint,
        wal_bytes           bigint
)
SET search_path=@extschema@ AS $$
    SELECT
        sample_db.datname AS dbname,
        sample_db.datid AS datid,
        sum(st.calls)::bigint AS calls,
        sum(st.plans)::bigint AS plans,
        sum(st.total_exec_time)/1000::double precision AS total_exec_time,
        sum(st.total_plan_time)/1000::double precision AS total_plan_time,
        sum(st.blk_read_time)/1000::double precision AS blk_read_time,
        sum(st.blk_write_time)/1000::double precision AS blk_write_time,
        (sum(trg.total_time)/1000)::double precision AS trg_fn_total_time,
        sum(st.shared_blks_hit)::bigint + sum(st.shared_blks_read)::bigint AS shared_gets,
        sum(st.local_blks_hit)::bigint + sum(st.local_blks_read)::bigint AS local_gets,
        sum(st.shared_blks_dirtied)::bigint AS shared_blks_dirtied,
        sum(st.local_blks_dirtied)::bigint AS local_blks_dirtied,
        sum(st.temp_blks_read)::bigint AS temp_blks_read,
        sum(st.temp_blks_written)::bigint AS temp_blks_written,
        sum(st.local_blks_read)::bigint AS local_blks_read,
        sum(st.local_blks_written)::bigint AS local_blks_written,
        sum(st.statements)::bigint AS statements,
        sum(st.wal_bytes)::bigint AS wal_bytes
    FROM sample_statements_total st
        LEFT OUTER JOIN sample_stat_user_func_total trg
          ON (st.server_id = trg.server_id AND st.sample_id = trg.sample_id AND st.datid = trg.datid AND trg.trg_fn)
        -- Database name
        JOIN sample_stat_database sample_db
        ON (st.server_id=sample_db.server_id AND st.sample_id=sample_db.sample_id AND st.datid=sample_db.datid)
    WHERE st.server_id = sserver_id AND st.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY sample_db.datname, sample_db.datid;
$$ LANGUAGE sql;

CREATE FUNCTION statements_stats_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for db stats
    c_dbstats CURSOR FOR
    SELECT
        COALESCE(dbname,'Total') as dbname_t,
        NULLIF(sum(calls), 0) as calls,
        NULLIF(sum(total_exec_time), 0.0) as total_exec_time,
        NULLIF(sum(total_plan_time), 0.0) as total_plan_time,
        NULLIF(sum(blk_read_time), 0.0) as blk_read_time,
        NULLIF(sum(blk_write_time), 0.0) as blk_write_time,
        NULLIF(sum(trg_fn_total_time), 0.0) as trg_fn_total_time,
        NULLIF(sum(shared_gets), 0) as shared_gets,
        NULLIF(sum(local_gets), 0) as local_gets,
        NULLIF(sum(shared_blks_dirtied), 0) as shared_blks_dirtied,
        NULLIF(sum(local_blks_dirtied), 0) as local_blks_dirtied,
        NULLIF(sum(temp_blks_read), 0) as temp_blks_read,
        NULLIF(sum(temp_blks_written), 0) as temp_blks_written,
        NULLIF(sum(local_blks_read), 0) as local_blks_read,
        NULLIF(sum(local_blks_written), 0) as local_blks_written,
        NULLIF(sum(statements), 0) as statements,
        NULLIF(sum(wal_bytes), 0) as wal_bytes
    FROM statements_stats(sserver_id,start_id,end_id,topn)
    GROUP BY ROLLUP(dbname)
    ORDER BY dbname NULLS LAST;

    r_result RECORD;
BEGIN
    -- Database stats TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th rowspan="2">Database</th>'
            '<th rowspan="2"title="Number of query executions">Calls</th>'
            '{time_hdr}'
            '<th colspan="2" title="Number of blocks fetched (hit + read)">Fetched blocks</th>'
            '<th colspan="2" title="Number of blocks dirtied">Dirtied blocks</th>'
            '<th colspan="2" title="Number of blocks, used in operations (like sorts and joins)">Temp blocks</th>'
            '<th colspan="2" title="Number of blocks, used for temporary tables">Local blocks</th>'
            '<th rowspan="2">Statements</th>'
            '{wal_bytes_hdr}'
          '</tr>'
          '<tr>'
            '{plan_time_hdr}'
            '<th title="Time spent executing queries">Exec</th>'
            '<th title="Time spent reading blocks">Read</th>'   -- I/O time
            '<th title="Time spent writing blocks">Write</th>'
            '<th title="Time spent in trigger functions">Trigger</th>'    -- Trigger functions time
            '<th>Shared</th>' -- Fetched
            '<th>Local</th>'
            '<th>Shared</th>' -- Dirtied
            '<th>Local</th>'
            '<th>Read</th>'   -- Work area read blks
            '<th>Write</th>'  -- Work area write blks
            '<th>Read</th>'   -- Local read blks
            '<th>Write</th>'  -- Local write blks
          '</tr>'
          '{rows}'
        '</table>',
      'stdb_tpl',
        '<tr>'
          '<td>%1$s</td>'
          '<td {value}>%2$s</td>'
          '{plan_time_cell}'
          '<td {value}>%4$s</td>'
          '<td {value}>%5$s</td>'
          '<td {value}>%6$s</td>'
          '<td {value}>%7$s</td>'
          '<td {value}>%8$s</td>'
          '<td {value}>%9$s</td>'
          '<td {value}>%10$s</td>'
          '<td {value}>%11$s</td>'
          '<td {value}>%12$s</td>'
          '<td {value}>%13$s</td>'
          '<td {value}>%14$s</td>'
          '<td {value}>%15$s</td>'
          '<td {value}>%16$s</td>'
          '{wal_bytes_cell}'
        '</tr>',
      'time_hdr', -- Time header for stat_statements less then v1.8
        '<th colspan="4">Time (s)</th>',
      'time_hdr_plan_time', -- Time header for stat_statements v1.8 - added plan time field
        '<th colspan="5">Time (s)</th>',
      'plan_time_hdr',
        '<th title="Time spent planning queries">Plan</th>',
      'plan_time_cell',
        '<td {value}>%3$s</td>',
      'wal_bytes_hdr',
        '<th rowspan="2">WAL size</th>',
      'wal_bytes_cell',
        '<td {value}>%17$s</td>');
    -- Conditional template
    -- Planning times
    IF jsonb_extract_path_text(jreportset, 'report_features', 'planning_times')::boolean THEN
      jtab_tpl := jsonb_set(jtab_tpl,'{tab_hdr}',to_jsonb(replace(jtab_tpl->>'tab_hdr','{time_hdr}',jtab_tpl->>'time_hdr_plan_time')));
      jtab_tpl := jsonb_set(jtab_tpl,'{tab_hdr}',to_jsonb(replace(jtab_tpl->>'tab_hdr','{plan_time_hdr}',jtab_tpl->>'plan_time_hdr')));
      jtab_tpl := jsonb_set(jtab_tpl,'{stdb_tpl}',to_jsonb(replace(jtab_tpl->>'stdb_tpl','{plan_time_cell}',jtab_tpl->>'plan_time_cell')));
    ELSE
      jtab_tpl := jsonb_set(jtab_tpl,'{tab_hdr}',to_jsonb(replace(jtab_tpl->>'tab_hdr','{time_hdr}',jtab_tpl->>'time_hdr')));
      jtab_tpl := jsonb_set(jtab_tpl,'{tab_hdr}',to_jsonb(replace(jtab_tpl->>'tab_hdr','{plan_time_hdr}','')));
      jtab_tpl := jsonb_set(jtab_tpl,'{stdb_tpl}',to_jsonb(replace(jtab_tpl->>'stdb_tpl','{plan_time_cell}','')));
    END IF;
    -- WAL size
    IF jsonb_extract_path_text(jreportset, 'report_features', 'statement_wal_bytes')::boolean THEN
      jtab_tpl := jsonb_set(jtab_tpl,'{tab_hdr}',to_jsonb(replace(jtab_tpl->>'tab_hdr','{wal_bytes_hdr}',jtab_tpl->>'wal_bytes_hdr')));
      jtab_tpl := jsonb_set(jtab_tpl,'{stdb_tpl}',to_jsonb(replace(jtab_tpl->>'stdb_tpl','{wal_bytes_cell}',jtab_tpl->>'wal_bytes_cell')));
    ELSE
      jtab_tpl := jsonb_set(jtab_tpl,'{tab_hdr}',to_jsonb(replace(jtab_tpl->>'tab_hdr','{wal_bytes_hdr}','')));
      jtab_tpl := jsonb_set(jtab_tpl,'{stdb_tpl}',to_jsonb(replace(jtab_tpl->>'stdb_tpl','{wal_bytes_cell}','')));
    END IF;

    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    -- Reporting summary databases stats
    FOR r_result IN c_dbstats LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['stdb_tpl'],
            r_result.dbname_t,
            r_result.calls,
            round(CAST(r_result.total_plan_time AS numeric),2),
            round(CAST(r_result.total_exec_time AS numeric),2),
            round(CAST(r_result.blk_read_time AS numeric),2),
            round(CAST(r_result.blk_write_time AS numeric),2),
            round(CAST(r_result.trg_fn_total_time AS numeric),2),
            r_result.shared_gets,
            r_result.local_gets,
            r_result.shared_blks_dirtied,
            r_result.local_blks_dirtied,
            r_result.temp_blks_read,
            r_result.temp_blks_written,
            r_result.local_blks_read,
            r_result.local_blks_written,
            r_result.statements,
            pg_size_pretty(r_result.wal_bytes)
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN report;
END;
$$ LANGUAGE plpgsql;