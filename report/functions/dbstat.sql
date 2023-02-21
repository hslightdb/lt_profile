/* ========= Reporting functions ========= */

/* ========= Cluster databases report functions ========= */

CREATE FUNCTION dbstats(IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer)
RETURNS TABLE(
    server_id         integer,
    datid             oid,
    dbname            name,
    xact_commit       bigint,
    xact_rollback     bigint,
    blks_read         bigint,
    blks_hit          bigint,
    tup_returned      bigint,
    tup_fetched       bigint,
    tup_inserted      bigint,
    tup_updated       bigint,
    tup_deleted       bigint,
    temp_files        bigint,
    temp_bytes        bigint,
    datsize_delta     bigint,
    deadlocks         bigint)
SET search_path=@extschema@ AS $$
    SELECT
        st.server_id AS server_id,
        st.datid AS datid,
        st.datname AS dbname,
        sum(xact_commit)::bigint AS xact_commit,
        sum(xact_rollback)::bigint AS xact_rollback,
        sum(blks_read)::bigint AS blks_read,
        sum(blks_hit)::bigint AS blks_hit,
        sum(tup_returned)::bigint AS tup_returned,
        sum(tup_fetched)::bigint AS tup_fetched,
        sum(tup_inserted)::bigint AS tup_inserted,
        sum(tup_updated)::bigint AS tup_updated,
        sum(tup_deleted)::bigint AS tup_deleted,
        sum(temp_files)::bigint AS temp_files,
        sum(temp_bytes)::bigint AS temp_bytes,
        sum(datsize_delta)::bigint AS datsize_delta,
        sum(deadlocks)::bigint AS deadlocks
    FROM sample_stat_database st
    WHERE st.server_id = sserver_id AND NOT datistemplate AND st.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY st.server_id, st.datid, st.datname
$$ LANGUAGE sql;

CREATE FUNCTION dbstats_reset(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
  datname       name,
  stats_reset   timestamp with time zone,
  sample_id       integer
)
SET search_path=@extschema@ AS $$
    SELECT
        st1.datname,
        st1.stats_reset,
        st1.sample_id
    FROM sample_stat_database st1
        LEFT JOIN sample_stat_database st0 ON
          (st0.server_id = st1.server_id AND st0.sample_id = st1.sample_id - 1 AND st0.datid = st1.datid)
    WHERE st1.server_id = sserver_id AND NOT st1.datistemplate AND st1.sample_id BETWEEN start_id + 1 AND end_id
      AND nullif(st1.stats_reset,st0.stats_reset) IS NOT NULL
    ORDER BY sample_id ASC
$$ LANGUAGE sql;

CREATE FUNCTION dbstats_reset_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for db stats
    c_dbstats CURSOR FOR
    SELECT
        datname,
        sample_id,
        stats_reset
    FROM dbstats_reset(sserver_id,start_id,end_id)
      ORDER BY stats_reset ASC;

    r_result RECORD;
BEGIN
    -- Database stats TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th>Database</th>'
            '<th>Sample</th>'
            '<th>Reset time</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'sample_tpl',
      '<tr>'
        '<td>%s</td>'
        '<td {value}>%s</td>'
        '<td {value}>%s</td>'
      '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    -- Reporting summary databases stats
    FOR r_result IN c_dbstats LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['sample_tpl'],
            r_result.datname,
            r_result.sample_id,
            r_result.stats_reset
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION dbstats_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for db stats
    c_dbstats CURSOR FOR
    SELECT
        COALESCE(st.dbname,'Total') as dbname,
        NULLIF(sum(st.xact_commit), 0) as xact_commit,
        NULLIF(sum(st.xact_rollback), 0) as xact_rollback,
        NULLIF(sum(st.blks_read), 0) as blks_read,
        NULLIF(sum(st.blks_hit), 0) as blks_hit,
        NULLIF(sum(st.tup_returned), 0) as tup_returned,
        NULLIF(sum(st.tup_fetched), 0) as tup_fetched,
        NULLIF(sum(st.tup_inserted), 0) as tup_inserted,
        NULLIF(sum(st.tup_updated), 0) as tup_updated,
        NULLIF(sum(st.tup_deleted), 0) as tup_deleted,
        NULLIF(sum(st.temp_files), 0) as temp_files,
        pg_size_pretty(NULLIF(sum(st.temp_bytes), 0)) AS temp_bytes,
        pg_size_pretty(NULLIF(sum(st_last.datsize), 0)) AS datsize,
        pg_size_pretty(NULLIF(sum(st.datsize_delta), 0)) AS datsize_delta,
        NULLIF(sum(st.deadlocks), 0) as deadlocks,
        (sum(st.blks_hit)*100/NULLIF(sum(st.blks_hit)+sum(st.blks_read),0))::double precision AS blks_hit_pct
    FROM dbstats(sserver_id,start_id,end_id,topn) st
      LEFT OUTER JOIN sample_stat_database st_last ON
        (st_last.server_id = st.server_id AND st_last.datid = st.datid AND st_last.sample_id = end_id)
    GROUP BY ROLLUP(st.dbname)
    ORDER BY st.dbname NULLS LAST;

    r_result RECORD;
BEGIN
    -- Database stats TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th rowspan="2">Database</th>'
            '<th colspan="3">Transactions</th>'
            '<th colspan="3">Block statistics</th>'
            '<th colspan="5">Tuples</th>'
            '<th colspan="2">Temp files</th>'
            '<th rowspan="2" title="Database size as is was at the moment of last sample in report interval">Size</th>'
            '<th rowspan="2" title="Database size increment during report interval">Growth</th>'
          '</tr>'
          '<tr>'
            '<th title="Number of transactions in this database that have been committed">Commits</th>'
            '<th title="Number of transactions in this database that have been rolled back">Rollbacks</th>'
            '<th title="Number of deadlocks detected in this database">Deadlocks</th>'
            '<th title="Buffer cache hit ratio">Hit(%)</th>'
            '<th title="Number of disk blocks read in this database">Read</th>'
            '<th title="Number of times disk blocks were found already in the buffer cache">Hit</th>'
            '<th title="Number of rows returned by queries in this database">Returned</th>'
            '<th title="Number of rows fetched by queries in this database">Fetched</th>'
            '<th title="Number of rows inserted by queries in this database">Inserted</th>'
            '<th title="Number of rows updated by queries in this database">Updated</th>'
            '<th title="Number of rows deleted">Deleted</th>'
            '<th title="Total amount of data written to temporary files by queries in this database">Size</th>'
            '<th title="Number of temporary files created by queries in this database">Files</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'db_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
          -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    -- Reporting summary databases stats
    FOR r_result IN c_dbstats LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['db_tpl'],
            r_result.dbname,
            r_result.xact_commit,
            r_result.xact_rollback,
            r_result.deadlocks,
            round(CAST(r_result.blks_hit_pct AS numeric),2),
            r_result.blks_read,
            r_result.blks_hit,
            r_result.tup_returned,
            r_result.tup_fetched,
            r_result.tup_inserted,
            r_result.tup_updated,
            r_result.tup_deleted,
            r_result.temp_bytes,
            r_result.temp_files,
            r_result.datsize,
            r_result.datsize_delta
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;