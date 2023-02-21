/* ===== Statements stats functions ===== */

CREATE FUNCTION top_statements(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    server_id               integer,
    datid                   oid,
    dbname                  name,
    userid                  oid,
    queryid                 bigint,
    queryid_md5             char(32),
    query                   text,
    plans                   bigint,
    plans_pct               float,
    calls                   bigint,
    calls_pct               float,
    total_time              double precision,
    total_time_pct          double precision,
    total_plan_time         double precision,
    plan_time_pct           float,
    total_exec_time         double precision,
    total_exec_time_pct     float,
    exec_time_pct           float,
    min_exec_time           double precision,
    max_exec_time           double precision,
    mean_exec_time          double precision,
    stddev_exec_time        double precision,
    min_plan_time           double precision,
    max_plan_time           double precision,
    mean_plan_time          double precision,
    stddev_plan_time        double precision,
    rows                    bigint,
    shared_blks_hit         bigint,
    shared_hit_pct          float,
    shared_blks_read        bigint,
    read_pct                float,
    shared_blks_fetched     bigint,
    shared_blks_fetched_pct float,
    shared_blks_dirtied     bigint,
    dirtied_pct             float,
    shared_blks_written     bigint,
    tot_written_pct         float,
    backend_written_pct     float,
    local_blks_hit          bigint,
    local_hit_pct           float,
    local_blks_read         bigint,
    local_blks_fetched      bigint,
    local_blks_dirtied      bigint,
    local_blks_written      bigint,
    temp_blks_read          bigint,
    temp_blks_written       bigint,
    blk_read_time           double precision,
    blk_write_time          double precision,
    io_time                 double precision,
    io_time_pct             float,
    temp_read_total_pct     float,
    temp_write_total_pct    float,
    local_read_total_pct    float,
    local_write_total_pct   float,
    wal_records             bigint,
    wal_fpi                 bigint,
    wal_bytes               numeric,
    wal_bytes_pct           float,
    user_time               double precision,
    system_time             double precision,
    reads                   bigint,
    writes                  bigint
) SET search_path=@extschema@ AS $$
    WITH
      tot AS (
        SELECT
            COALESCE(sum(total_plan_time), 0.0) + sum(total_exec_time) AS total_time,
            sum(blk_read_time) AS blk_read_time,
            sum(blk_write_time) AS blk_write_time,
            sum(shared_blks_hit) AS shared_blks_hit,
            sum(shared_blks_read) AS shared_blks_read,
            sum(shared_blks_dirtied) AS shared_blks_dirtied,
            sum(temp_blks_read) AS temp_blks_read,
            sum(temp_blks_written) AS temp_blks_written,
            sum(local_blks_read) AS local_blks_read,
            sum(local_blks_written) AS local_blks_written,
            sum(calls) AS calls,
            sum(plans) AS plans
        FROM sample_statements_total st
        WHERE st.server_id = sserver_id AND st.sample_id BETWEEN start_id + 1 AND end_id
      ),
      totbgwr AS (
        SELECT
          sum(buffers_checkpoint) + sum(buffers_clean) + sum(buffers_backend) AS written,
          sum(buffers_backend) AS buffers_backend,
          sum(wal_size) AS wal_size
        FROM sample_stat_cluster
        WHERE server_id = sserver_id AND sample_id BETWEEN start_id + 1 AND end_id
      )
    SELECT
        st.server_id as server_id,
        st.datid as datid,
        sample_db.datname as dbname,
        st.userid as userid,
        st.queryid as queryid,
        st.queryid_md5 as queryid_md5,
        (select sl.query from stmt_list sl where sl.queryid_md5 = st.queryid_md5)::varchar(100) as query,
        sum(st.plans)::bigint as plans,
        (sum(st.plans)*100/NULLIF(min(tot.plans), 0))::float as plans_pct,
        sum(st.calls)::bigint as calls,
        (sum(st.calls)*100/NULLIF(min(tot.calls), 0))::float as calls_pct,
        (sum(st.total_exec_time) + COALESCE(sum(st.total_plan_time), 0.0))/1000 as total_time,
        (sum(st.total_exec_time) + COALESCE(sum(st.total_plan_time), 0.0))*100/NULLIF(min(tot.total_time), 0) as total_time_pct,
        sum(st.total_plan_time)/1000 as total_plan_time,
        sum(st.total_plan_time)*100/NULLIF(sum(st.total_exec_time) + COALESCE(sum(st.total_plan_time), 0.0), 0) as plan_time_pct,
        sum(st.total_exec_time)/1000 as total_exec_time,
        sum(st.total_exec_time)*100/NULLIF(min(tot.total_time), 0) as total_exec_time_pct,
        sum(st.total_exec_time)*100/NULLIF(sum(st.total_exec_time) + COALESCE(sum(st.total_plan_time), 0.0), 0) as exec_time_pct,
        min(st.min_exec_time) as min_exec_time,
        max(st.max_exec_time) as max_exec_time,
        sum(st.mean_exec_time*st.calls)/NULLIF(sum(st.calls), 0) as mean_exec_time,
        sqrt(greatest(sum((power(st.stddev_exec_time,2)+power(st.mean_exec_time,2))*st.calls)/NULLIF(sum(st.calls),0)-power(sum(st.mean_exec_time*st.calls)/NULLIF(sum(st.calls),0),2), 0)) as stddev_exec_time,
        min(st.min_plan_time) as min_plan_time,
        max(st.max_plan_time) as max_plan_time,
        sum(st.mean_plan_time*st.plans)/NULLIF(sum(st.plans),0) as mean_plan_time,
        sqrt(greatest(sum((power(st.stddev_plan_time,2)+power(st.mean_plan_time,2))*st.plans)/NULLIF(sum(st.plans),0)-power(sum(st.mean_plan_time*st.plans)/NULLIF(sum(st.plans),0),2), 0)) as stddev_plan_time,
        sum(st.rows)::bigint as rows,
        sum(st.shared_blks_hit)::bigint as shared_blks_hit,
        (sum(st.shared_blks_hit) * 100 / NULLIF(sum(st.shared_blks_hit) + sum(st.shared_blks_read), 0))::float as shared_hit_pct,
        sum(st.shared_blks_read)::bigint as shared_blks_read,
        (sum(st.shared_blks_read) * 100 / NULLIF(min(tot.shared_blks_read), 0))::float as read_pct,
        (sum(st.shared_blks_hit) + sum(st.shared_blks_read))::bigint as shared_blks_fetched,
        ((sum(st.shared_blks_hit) + sum(st.shared_blks_read)) * 100 / NULLIF(min(tot.shared_blks_hit) + min(tot.shared_blks_read), 0))::float as shared_blks_fetched_pct,
        sum(st.shared_blks_dirtied)::bigint as shared_blks_dirtied,
        (sum(st.shared_blks_dirtied) * 100 / NULLIF(min(tot.shared_blks_dirtied), 0))::float as dirtied_pct,
        sum(st.shared_blks_written)::bigint as shared_blks_written,
        (sum(st.shared_blks_written) * 100 / NULLIF(min(totbgwr.written), 0))::float as tot_written_pct,
        (sum(st.shared_blks_written) * 100 / NULLIF(min(totbgwr.buffers_backend), 0))::float as backend_written_pct,
        sum(st.local_blks_hit)::bigint as local_blks_hit,
        (sum(st.local_blks_hit) * 100 / NULLIF(sum(st.local_blks_hit) + sum(st.local_blks_read),0))::float as local_hit_pct,
        sum(st.local_blks_read)::bigint as local_blks_read,
        (sum(st.local_blks_hit) + sum(st.local_blks_read))::bigint as local_blks_fetched,
        sum(st.local_blks_dirtied)::bigint as local_blks_dirtied,
        sum(st.local_blks_written)::bigint as local_blks_written,
        sum(st.temp_blks_read)::bigint as temp_blks_read,
        sum(st.temp_blks_written)::bigint as temp_blks_written,
        sum(st.blk_read_time)/1000 as blk_read_time,
        sum(st.blk_write_time)/1000 as blk_write_time,
        (sum(st.blk_read_time) + sum(st.blk_write_time))/1000 as io_time,
        (sum(st.blk_read_time) + sum(st.blk_write_time)) * 100 / NULLIF(min(tot.blk_read_time) + min(tot.blk_write_time),0) as io_time_pct,
        (sum(st.temp_blks_read) * 100 / NULLIF(min(tot.temp_blks_read), 0))::float as temp_read_total_pct,
        (sum(st.temp_blks_written) * 100 / NULLIF(min(tot.temp_blks_written), 0))::float as temp_write_total_pct,
        (sum(st.local_blks_read) * 100 / NULLIF(min(tot.local_blks_read), 0))::float as local_read_total_pct,
        (sum(st.local_blks_written) * 100 / NULLIF(min(tot.local_blks_written), 0))::float as local_write_total_pct,
        sum(st.wal_records)::bigint as wal_records,
        sum(st.wal_fpi)::bigint as wal_fpi,
        sum(st.wal_bytes) as wal_bytes,
        (sum(st.wal_bytes) * 100 / NULLIF(min(totbgwr.wal_size), 0))::float wal_bytes_pct,
        -- kcache stats
        COALESCE(sum(kc.exec_user_time), 0.0) + COALESCE(sum(kc.plan_user_time), 0.0) as user_time,
        COALESCE(sum(kc.exec_system_time), 0.0) + COALESCE(sum(kc.plan_system_time), 0.0) as system_time,
        (COALESCE(sum(kc.exec_reads), 0) + COALESCE(sum(kc.plan_reads), 0))::bigint as reads,
        (COALESCE(sum(kc.exec_writes), 0) + COALESCE(sum(kc.plan_writes), 0))::bigint as writes
    FROM sample_statements st
        -- kcache join
        LEFT OUTER JOIN sample_kcache kc USING(server_id, sample_id, userid, datid, queryid)
        -- Database name
        JOIN sample_stat_database sample_db
        ON (st.server_id=sample_db.server_id AND st.sample_id=sample_db.sample_id AND st.datid=sample_db.datid)
        -- Total stats
        CROSS JOIN tot CROSS JOIN totbgwr
    WHERE st.server_id = sserver_id AND st.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY st.server_id,st.datid,sample_db.datname,st.userid,st.queryid,st.queryid_md5
$$ LANGUAGE sql;

CREATE FUNCTION top_elapsed_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    tab_row     text := '';
    jtab_tpl    jsonb;

    --Cursor for top(cnt) queries ordered by elapsed time
    c_elapsed_time CURSOR FOR
    SELECT
        st.datid,
        st.userid,
        st.queryid,
        st.queryid_md5,
        st.query,
        st.dbname,
        NULLIF(st.total_time_pct, 0) as total_time_pct,
        NULLIF(st.total_time, 0) as total_time,
        NULLIF(st.total_plan_time, 0) as total_plan_time,
        NULLIF(st.total_exec_time, 0) as total_exec_time,
        NULLIF(st.blk_read_time, 0.0) as blk_read_time,
        NULLIF(st.blk_write_time, 0.0) as blk_write_time,
        NULLIF(st.user_time, 0.0) as user_time,
        NULLIF(st.system_time, 0.0) as system_time,
        NULLIF(st.calls, 0) as calls,
        NULLIF(st.plans, 0) as plans
    FROM top_statements(sserver_id, start_id, end_id) st
    ORDER BY st.total_time DESC,
      st.queryid ASC,
      st.datid ASC
    LIMIT topn;

    r_result RECORD;
BEGIN
    -- This report section is meaningful only when planning timing is available
    IF NOT jsonb_extract_path_text(jreportset, 'report_features', 'planning_times')::boolean THEN
      RETURN '';
    END IF;

    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th rowspan="2">Query ID</th>'
            '<th rowspan="2" title="First 100 characters of the complete sql text">SQL Text</th>'
            '<th rowspan="2">Database</th>'
            '<th rowspan="2" title="Elapsed time as a percentage of total cluster elapsed time">%Total</th>'
            '<th colspan="3">Time (s)</th>'
            '<th colspan="2">I/O time (s)</th>'
            '{kcache_hdr1}'
            '<th rowspan="2" title="Number of times the statement was planned">Plans</th>'
            '<th rowspan="2" title="Number of times the statement was executed">Executions</th>'
          '</tr>'
          '<tr>'
            '<th title="Time spent by the statement">Elapsed</th>'
            '<th title="Time spent planning statement">Plan</th>'
            '<th title="Time spent executing statement">Exec</th>'
            '<th title="Time spent reading blocks by statement">Read</th>'
            '<th title="Time spent writing blocks by statement">Write</th>'
            '{kcache_hdr2}'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr>'
          '<td {mono}><p><a HREF="#%1$s">%2$s</a></p>'
          '<p><small>%3$s</small></p></td>'
          '<td><p align="left">%4$s</p></td>'  -- SQL Text
          '<td>%5$s</td>'
          '<td {value}>%6$s</td>'
          '<td {value}>%7$s</td>'
          '<td {value}>%8$s</td>'
          '<td {value}>%9$s</td>'
          '<td {value}>%10$s</td>'
          '<td {value}>%11$s</td>'
          '{kcache_row}'
          '<td {value}>%14$s</td>'
          '<td {value}>%15$s</td>'
        '</tr>',
      'kcache_hdr1',
        '<th colspan="2">CPU time (s)</th>',
      'kcache_hdr2',
        '<th>Usr</th>'
        '<th>Sys</th>',
      'kcache_row',
        '<td {value}>%12$s</td>'
        '<td {value}>%13$s</td>'
      );
    -- Conditional template
    IF jsonb_extract_path_text(jreportset, 'report_features', 'kcachestatements')::boolean THEN
      jtab_tpl := jsonb_set(jtab_tpl,'{tab_hdr}',to_jsonb(replace(jtab_tpl->>'tab_hdr','{kcache_hdr1}',jtab_tpl->>'kcache_hdr1')));
      jtab_tpl := jsonb_set(jtab_tpl,'{tab_hdr}',to_jsonb(replace(jtab_tpl->>'tab_hdr','{kcache_hdr2}',jtab_tpl->>'kcache_hdr2')));
      jtab_tpl := jsonb_set(jtab_tpl,'{stmt_tpl}',to_jsonb(replace(jtab_tpl->>'stmt_tpl','{kcache_row}',jtab_tpl->>'kcache_row')));
    ELSE
      jtab_tpl := jsonb_set(jtab_tpl,'{tab_hdr}',to_jsonb(replace(jtab_tpl->>'tab_hdr','{kcache_hdr1}','')));
      jtab_tpl := jsonb_set(jtab_tpl,'{tab_hdr}',to_jsonb(replace(jtab_tpl->>'tab_hdr','{kcache_hdr2}','')));
      jtab_tpl := jsonb_set(jtab_tpl,'{stmt_tpl}',to_jsonb(replace(jtab_tpl->>'stmt_tpl','{kcache_row}','')));
    END IF;
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);
    -- Reporting on top queries by elapsed time
    FOR r_result IN c_elapsed_time LOOP
        tab_row := format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid_md5,
            md5(r_result.queryid::text),
            '',
            r_result.query,
            r_result.dbname,
            round(CAST(r_result.total_time_pct AS numeric),2),
            round(CAST(r_result.total_time AS numeric),2),
            round(CAST(r_result.total_plan_time AS numeric),2),
            round(CAST(r_result.total_exec_time AS numeric),2),
            round(CAST(r_result.blk_read_time AS numeric),2),
            round(CAST(r_result.blk_write_time AS numeric),2),
            round(CAST(r_result.user_time AS numeric),2),
            round(CAST(r_result.system_time AS numeric),2),
            r_result.plans,
            r_result.calls
        );

        report := report || tab_row;
        PERFORM collect_queries(
            r_result.userid,r_result.datid,r_result.queryid,r_result.queryid_md5
        );
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_plan_time_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    tab_row     text := '';
    jtab_tpl    jsonb;

    --Cursor for queries ordered by planning time
    c_elapsed_time CURSOR FOR
    SELECT
        st.datid,
        st.userid,
        st.queryid,
        st.queryid_md5,
        st.query,
        st.dbname,
        NULLIF(st.plans, 0) as plans,
        NULLIF(st.calls, 0) as calls,
        NULLIF(st.total_plan_time, 0.0) as total_plan_time,
        NULLIF(st.plan_time_pct, 0.0) as plan_time_pct,
        NULLIF(st.min_plan_time, 0.0) as min_plan_time,
        NULLIF(st.max_plan_time, 0.0) as max_plan_time,
        NULLIF(st.mean_plan_time, 0.0) as mean_plan_time,
        NULLIF(st.stddev_plan_time, 0.0) as stddev_plan_time
    FROM top_statements(sserver_id, start_id, end_id) st
    ORDER BY st.total_plan_time DESC,
      st.total_exec_time DESC,
      st.queryid ASC,
      st.datid ASC
    LIMIT topn;

    r_result RECORD;
BEGIN
    -- This report section is meaningful only when planning timing is available
    IF NOT jsonb_extract_path_text(jreportset, 'report_features', 'planning_times')::boolean THEN
      RETURN '';
    END IF;

    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th rowspan="2">Query ID</th>'
            '<th rowspan="2" title="First 100 characters of the complete sql text">SQL Text</th>'
            '<th rowspan="2">Database</th>'
            '<th rowspan="2" title="Time spent planning statement">Plan elapsed (s)</th>'
            '<th rowspan="2" title="Plan elapsed as a percentage of statement elapsed time">%Elapsed</th>'
            '<th colspan="4" title="Planning time statistics">Plan times (ms)</th>'
            '<th rowspan="2" title="Number of times the statement was planned">Plans</th>'
            '<th rowspan="2" title="Number of times the statement was executed">Executions</th>'
          '</tr>'
          '<tr>'
            '<th>Mean</th>'
            '<th>Min</th>'
            '<th>Max</th>'
            '<th>StdErr</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr>'
          '<td {mono}><p><a HREF="#%1$s">%2$s</a></p>'
          '<p><small>%3$s</small></p></td>'
          '<td><p align="left">%4$s</p></td>'  -- SQL Text
          '<td>%5$s</td>'
          '<td {value}>%6$s</td>'
          '<td {value}>%7$s</td>'
          '<td {value}>%8$s</td>'
          '<td {value}>%9$s</td>'
          '<td {value}>%10$s</td>'
          '<td {value}>%11$s</td>'
          '<td {value}>%12$s</td>'
          '<td {value}>%13$s</td>'
        '</tr>'
      );
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);
    -- Reporting on top queries by elapsed time
    FOR r_result IN c_elapsed_time LOOP
        tab_row := format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid_md5,
            md5(r_result.queryid::text),
            '',
            r_result.query,
            r_result.dbname,
            round(CAST(r_result.total_plan_time AS numeric),2),
            round(CAST(r_result.plan_time_pct AS numeric),2),
            round(CAST(r_result.mean_plan_time AS numeric),3),
            round(CAST(r_result.min_plan_time AS numeric),3),
            round(CAST(r_result.max_plan_time AS numeric),3),
            round(CAST(r_result.stddev_plan_time AS numeric),3),
            r_result.plans,
            r_result.calls
        );

        report := report || tab_row;
        PERFORM collect_queries(
            r_result.userid,r_result.datid,r_result.queryid,r_result.queryid_md5
        );
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_exec_time_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    tab_row     text := '';
    jtab_tpl    jsonb;

    --Cursor for queries ordered by execution time
    c_elapsed_time CURSOR FOR
    SELECT
        st.datid,
        st.userid,
        st.queryid,
        st.queryid_md5,
        st.query,
        st.dbname,
        NULLIF(st.calls, 0) as calls,
        NULLIF(st.total_exec_time, 0.0) as total_exec_time,
        NULLIF(st.total_exec_time_pct, 0.0) as total_exec_time_pct,
        NULLIF(st.exec_time_pct, 0.0) as exec_time_pct,
        NULLIF(st.blk_read_time, 0.0) as blk_read_time,
        NULLIF(st.blk_write_time, 0.0) as blk_write_time,
        NULLIF(st.min_exec_time, 0.0) as min_exec_time,
        NULLIF(st.max_exec_time, 0.0) as max_exec_time,
        NULLIF(st.mean_exec_time, 0.0) as mean_exec_time,
        NULLIF(st.stddev_exec_time, 0.0) as stddev_exec_time,
        NULLIF(st.rows, 0) as rows,
        NULLIF(st.user_time, 0.0) as user_time,
        NULLIF(st.system_time, 0.0) as system_time
    FROM top_statements(sserver_id, start_id, end_id) st
    ORDER BY st.total_exec_time DESC,
      st.total_time DESC,
      st.queryid ASC,
      st.datid ASC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th rowspan="2">Query ID</th>'
            '<th rowspan="2" title="First 100 characters of the complete sql text">SQL Text</th>'
            '<th rowspan="2">Database</th>'
            '<th rowspan="2" title="Time spent executing statement">Exec (s)</th>'
            '{elapsed_pct_hdr}'
            '<th rowspan="2" title="Exec time as a percentage of total cluster elapsed time">%Total</th>'
            '<th colspan="2">I/O time (s)</th>'
            '{kcache_hdr1}'
            '<th rowspan="2" title="Total number of rows retrieved or affected by the statement">Rows</th>'
            '<th colspan="4" title="Execution time statistics">Execution times (ms)</th>'
            '<th rowspan="2" title="Number of times the statement was executed">Executions</th>'
          '</tr>'
          '<tr>'
            '<th title="Time spent reading blocks by statement">Read</th>'
            '<th title="Time spent writing blocks by statement">Write</th>'
            '{kcache_hdr2}'
            '<th>Mean</th>'
            '<th>Min</th>'
            '<th>Max</th>'
            '<th>StdErr</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr>'
          '<td {mono}><p><a HREF="#%1$s">%2$s</a></p>'
          '<p><small>%3$s</small></p></td>'
          '<td><p align="left">%4$s</p></td>'  -- SQL Text
          '<td>%5$s</td>'
          '<td {value}>%6$s</td>'
          '{elapsed_pct_row}'
          '<td {value}>%7$s</td>'
          '<td {value}>%8$s</td>'
          '<td {value}>%9$s</td>'
          '{kcache_row}'
          '<td {value}>%12$s</td>'
          '<td {value}>%13$s</td>'
          '<td {value}>%14$s</td>'
          '<td {value}>%15$s</td>'
          '<td {value}>%16$s</td>'
          '<td {value}>%17$s</td>'
        '</tr>',
      'elapsed_pct_hdr',
        '<th rowspan="2" title="Exec time as a percentage of statement elapsed time">%Elapsed</th>',
      'elapsed_pct_row',
        '<td {value}>%18$s</td>',
      'kcache_hdr1',
        '<th colspan="2">CPU time (s)</th>',
      'kcache_hdr2',
        '<th>Usr</th>'
        '<th>Sys</th>',
      'kcache_row',
        '<td {value}>%10$s</td>'
        '<td {value}>%11$s</td>'
      );
    -- Conditional template
    -- kcache
    IF jsonb_extract_path_text(jreportset, 'report_features', 'kcachestatements')::boolean AND
      NOT jsonb_extract_path_text(jreportset, 'report_features', 'planning_times')::boolean THEN
      /* We won't show kcache CPU stats here if planning times are available as CPU stats is then
      shown in "Top SQL by elapsed time" section */
      jtab_tpl := jsonb_set(jtab_tpl,'{tab_hdr}',to_jsonb(replace(jtab_tpl->>'tab_hdr','{kcache_hdr1}',jtab_tpl->>'kcache_hdr1')));
      jtab_tpl := jsonb_set(jtab_tpl,'{tab_hdr}',to_jsonb(replace(jtab_tpl->>'tab_hdr','{kcache_hdr2}',jtab_tpl->>'kcache_hdr2')));
      jtab_tpl := jsonb_set(jtab_tpl,'{stmt_tpl}',to_jsonb(replace(jtab_tpl->>'stmt_tpl','{kcache_row}',jtab_tpl->>'kcache_row')));
    ELSE
      jtab_tpl := jsonb_set(jtab_tpl,'{tab_hdr}',to_jsonb(replace(jtab_tpl->>'tab_hdr','{kcache_hdr1}','')));
      jtab_tpl := jsonb_set(jtab_tpl,'{tab_hdr}',to_jsonb(replace(jtab_tpl->>'tab_hdr','{kcache_hdr2}','')));
      jtab_tpl := jsonb_set(jtab_tpl,'{stmt_tpl}',to_jsonb(replace(jtab_tpl->>'stmt_tpl','{kcache_row}','')));
    END IF;
    -- Planning times
    IF jsonb_extract_path_text(jreportset, 'report_features', 'planning_times')::boolean THEN
      jtab_tpl := jsonb_set(jtab_tpl,'{tab_hdr}',to_jsonb(replace(jtab_tpl->>'tab_hdr','{elapsed_pct_hdr}',jtab_tpl->>'elapsed_pct_hdr')));
      jtab_tpl := jsonb_set(jtab_tpl,'{stmt_tpl}',to_jsonb(replace(jtab_tpl->>'stmt_tpl','{elapsed_pct_row}',jtab_tpl->>'elapsed_pct_row')));
    ELSE
      jtab_tpl := jsonb_set(jtab_tpl,'{tab_hdr}',to_jsonb(replace(jtab_tpl->>'tab_hdr','{elapsed_pct_hdr}','')));
      jtab_tpl := jsonb_set(jtab_tpl,'{stmt_tpl}',to_jsonb(replace(jtab_tpl->>'stmt_tpl','{elapsed_pct_row}','')));
    END IF;
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);
    -- Reporting on top queries by elapsed time
    FOR r_result IN c_elapsed_time LOOP
        tab_row := format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid_md5,
            md5(r_result.queryid::text),
            '',
            r_result.query,
            r_result.dbname,
            round(CAST(r_result.total_exec_time AS numeric),2),
            round(CAST(r_result.total_exec_time_pct AS numeric),2),
            round(CAST(r_result.blk_read_time AS numeric),2),
            round(CAST(r_result.blk_write_time AS numeric),2),
            round(CAST(r_result.user_time AS numeric),2),
            round(CAST(r_result.system_time AS numeric),2),
            r_result.rows,
            round(CAST(r_result.mean_exec_time AS numeric),3),
            round(CAST(r_result.min_exec_time AS numeric),3),
            round(CAST(r_result.max_exec_time AS numeric),3),
            round(CAST(r_result.stddev_exec_time AS numeric),3),
            r_result.calls,
            round(CAST(r_result.exec_time_pct AS numeric),2)
        );

        report := report || tab_row;
        PERFORM collect_queries(
            r_result.userid,r_result.datid,r_result.queryid,r_result.queryid_md5
        );
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_exec_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    tab_row     text := '';
    jtab_tpl    jsonb;

    -- Cursor for topn querues ordered by executions
    c_calls CURSOR FOR
    SELECT
        st.datid,
        st.userid,
        st.queryid,
        st.queryid_md5,
        st.query,
        st.dbname,
        NULLIF(st.calls, 0) as calls,
        NULLIF(st.calls_pct, 0.0) as calls_pct,
        NULLIF(st.total_exec_time, 0.0) as total_exec_time,
        NULLIF(st.min_exec_time, 0.0) as min_exec_time,
        NULLIF(st.max_exec_time, 0.0) as max_exec_time,
        NULLIF(st.mean_exec_time, 0.0) as mean_exec_time,
        NULLIF(st.stddev_exec_time, 0.0) as stddev_exec_time,
        NULLIF(st.rows, 0) as rows
    FROM top_statements(sserver_id, start_id, end_id) st
    ORDER BY st.calls DESC,
      st.total_time DESC,
      st.queryid ASC,
      st.datid ASC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th>Query ID</th>'
            '<th title="First 100 characters of the complete sql text">SQL Text</th>'
            '<th>Database</th>'
            '<th title="Number of times the statement was executed">Executions</th>'
            '<th title="Executions of this statement as a percentage of total executions of all statements in a cluster">%Total</th>'
            '<th title="Total number of rows retrieved or affected by the statement">Rows</th>'
            '<th>Mean(ms)</th>'
            '<th>Min(ms)</th>'
            '<th>Max(ms)</th>'
            '<th>StdErr(ms)</th>'
            '<th title="Time spent by the statement">Elapsed(s)</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr>'
          '<td {mono}><p><a HREF="#%1$s">%2$s</a></p>'
          '<p><small>%3$s</small></p></td>'
          '<td><p align="left">%4$s</p></td>'  -- SQL Text
          '<td>%s</td>'
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
    -- Reporting on top 10 queries by executions
    FOR r_result IN c_calls LOOP
        tab_row := format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid_md5,
            md5(r_result.queryid::text),
            '',
            r_result.query,
            r_result.dbname,
            r_result.calls,
            round(CAST(r_result.calls_pct AS numeric),2),
            r_result.rows,
            round(CAST(r_result.mean_exec_time AS numeric),3),
            round(CAST(r_result.min_exec_time AS numeric),3),
            round(CAST(r_result.max_exec_time AS numeric),3),
            round(CAST(r_result.stddev_exec_time AS numeric),3),
            round(CAST(r_result.total_exec_time AS numeric),1)
        );

        report := report || tab_row;
        PERFORM collect_queries(
            r_result.userid,r_result.datid,r_result.queryid,r_result.queryid_md5
        );
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_iowait_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    tab_row     text := '';
    jtab_tpl    jsonb;

    --Cursor for top(cnt) querues ordered by I/O Wait time
    c_iowait_time CURSOR FOR
    SELECT
        st.datid,
        st.userid,
        st.queryid,
        st.queryid_md5 AS queryid_md5,
        st.query,
        st.dbname,
        NULLIF(st.total_time, 0.0) as total_time,
        NULLIF(st.io_time, 0.0) as io_time,
        NULLIF(st.blk_read_time, 0.0) as blk_read_time,
        NULLIF(st.blk_write_time, 0.0) as blk_write_time,
        NULLIF(st.io_time_pct, 0.0) as io_time_pct,
        NULLIF(st.shared_blks_read, 0) as shared_blks_read,
        NULLIF(st.local_blks_read, 0) as local_blks_read,
        NULLIF(st.temp_blks_read, 0) as temp_blks_read,
        NULLIF(st.shared_blks_written, 0) as shared_blks_written,
        NULLIF(st.local_blks_written, 0) as local_blks_written,
        NULLIF(st.temp_blks_written, 0) as temp_blks_written,
        NULLIF(st.calls, 0) as calls
    FROM top_statements(sserver_id, start_id, end_id) st
    WHERE st.io_time > 0
    ORDER BY st.io_time DESC,
      st.total_time DESC,
      st.queryid ASC,
      st.datid ASC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th rowspan="2">Query ID</th>'
            '<th rowspan="2" title="First 100 characters of the complete sql text">SQL Text</th>'
            '<th rowspan="2">Database</th>'
            '<th rowspan="2" title="Time spent by the statement reading and writing blocks">IO(s)</th>'
            '<th rowspan="2" title="Time spent by the statement reading blocks">Read(s)</th>'
            '<th rowspan="2" title="Time spent by the statement writing blocks">Write(s)</th>'
            '<th rowspan="2" title="I/O time of this statement as a percentage of total I/O time for all statements in a cluster">%Total</th>'
            '<th colspan="3" title="Number of blocks read by the statement">Reads</th>'
            '<th colspan="3" title="Number of blocks written by the statement">Writes</th>'
            '<th rowspan="2" title="Time spent by the statement">Elapsed(s)</th>'
            '<th rowspan="2" title="Number of times the statement was executed">Executions</th>'
          '</tr>'
          '<tr>'
            '<th title="Number of shared blocks read by the statement">Shared</th>'
            '<th title="Number of local blocks read by the statement (usually used for temporary tables)">Local</th>'
            '<th title="Number of temp blocks read by the statement (usually used for operations like sorts and joins)">Temp</th>'
            '<th title="Number of shared blocks written by the statement">Shared</th>'
            '<th title="Number of local blocks written by the statement (usually used for temporary tables)">Local</th>'
            '<th title="Number of temp blocks written by the statement (usually used for operations like sorts and joins)">Temp</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr>'
          '<td {mono}><p><a HREF="#%1$s">%2$s</a></p>'
          '<p><small>%3$s</small></p></td>'
          '<td><p align="left">%4$s</p></td>'  -- SQL Text
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
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);
    -- Reporting on top 10 queries by I/O wait time
    FOR r_result IN c_iowait_time LOOP
        tab_row := format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid_md5,
            md5(r_result.queryid::text),
            '',
            r_result.query,
            r_result.dbname,
            round(CAST(r_result.io_time AS numeric),3),
            round(CAST(r_result.blk_read_time AS numeric),3),
            round(CAST(r_result.blk_write_time AS numeric),3),
            round(CAST(r_result.io_time_pct AS numeric),2),
            round(CAST(r_result.shared_blks_read AS numeric)),
            round(CAST(r_result.local_blks_read AS numeric)),
            round(CAST(r_result.temp_blks_read AS numeric)),
            round(CAST(r_result.shared_blks_written AS numeric)),
            round(CAST(r_result.local_blks_written AS numeric)),
            round(CAST(r_result.temp_blks_written AS numeric)),
            round(CAST(r_result.total_time AS numeric),1),
            r_result.calls
        );

        report := report || tab_row;
        PERFORM collect_queries(
            r_result.userid,r_result.datid,r_result.queryid,r_result.queryid_md5
        );
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_shared_blks_fetched_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer)
  RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    tab_row     text := '';
    jtab_tpl    jsonb;

    --Cursor for top(cnt) queries ordered by shared_blks_fetched
    c_shared_blks_fetched CURSOR FOR
    SELECT
        st.datid,
        st.userid,
        st.queryid,
        st.queryid_md5 AS queryid_md5,
        st.query,
        st.dbname,
        NULLIF(st.total_time, 0.0) as total_time,
        NULLIF(st.rows, 0) as rows,
        NULLIF(st.shared_blks_fetched, 0) as shared_blks_fetched,
        NULLIF(st.shared_blks_fetched_pct, 0.0) as shared_blks_fetched_pct,
        NULLIF(st.shared_hit_pct, 0.0) as shared_hit_pct,
        NULLIF(st.calls, 0) as calls
    FROM top_statements(sserver_id, start_id, end_id) st
    WHERE shared_blks_fetched > 0
    ORDER BY st.shared_blks_fetched DESC,
      st.queryid ASC,
      st.datid ASC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th>Query ID</th>'
            '<th title="First 100 characters of the complete sql text">SQL Text</th>'
            '<th>Database</th>'
            '<th title="Shared blocks fetched (read and hit) by the statement">Fetched</th>'
            '<th title="Shared blocks fetched by this statement as a percentage of all shared blocks fetched in a cluster">%Total</th>'
            '<th title="Shared blocks hits as a percentage of shared blocks fetched (read + hit)">Hits(%)</th>'
            '<th title="Time spent by the statement">Elapsed(s)</th>'
            '<th title="Total number of rows retrieved or affected by the statement">Rows</th>'
            '<th title="Number of times the statement was executed">Executions</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr>'
          '<td {mono}><p><a HREF="#%1$s">%2$s</a></p>'
          '<p><small>%3$s</small></p></td>'
          '<td><p align="left">%4$s</p></td>'  -- SQL Text
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    -- Reporting on top queries by shared_blks_fetched
    FOR r_result IN c_shared_blks_fetched LOOP
        tab_row := format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid_md5,
            md5(r_result.queryid::text),
            '',
            r_result.query,
            r_result.dbname,
            r_result.shared_blks_fetched,
            round(CAST(r_result.shared_blks_fetched_pct AS numeric),2),
            round(CAST(r_result.shared_hit_pct AS numeric),2),
            round(CAST(r_result.total_time AS numeric),1),
            r_result.rows,
            r_result.calls
        );

        report := report || tab_row;
        PERFORM collect_queries(
            r_result.userid,r_result.datid,r_result.queryid,r_result.queryid_md5
        );
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_shared_reads_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer)
  RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    tab_row     text := '';
    jtab_tpl    jsonb;

    --Cursor for top queries ordered by reads
    c_sh_reads CURSOR FOR
    SELECT
        st.datid,
        st.userid,
        st.queryid,
        st.queryid_md5 AS queryid_md5,
        st.query,
        st.dbname,
        NULLIF(st.total_time, 0.0) as total_time,
        NULLIF(st.rows, 0) as rows,
        NULLIF(st.shared_blks_read, 0) as shared_blks_read,
        NULLIF(st.read_pct, 0.0) as read_pct,
        NULLIF(st.shared_hit_pct, 0.0) as shared_hit_pct,
        NULLIF(st.calls, 0) as calls
    FROM top_statements(sserver_id, start_id, end_id) st
    WHERE st.shared_blks_read > 0
    ORDER BY st.shared_blks_read DESC,
      st.queryid ASC,
      st.datid ASC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th>Query ID</th>'
            '<th title="First 100 characters of the complete sql text">SQL Text</th>'
            '<th>Database</th>'
            '<th title="Total number of shared blocks read by the statement">Reads</th>'
            '<th title="Shared blocks read by this statement as a percentage of all shared blocks read in a cluster">%Total</th>'
            '<th title="Shared blocks hits as a percentage of shared blocks fetched (read + hit)">Hits(%)</th>'
            '<th title="Time spent by the statement">Elapsed(s)</th>'
            '<th title="Total number of rows retrieved or affected by the statement">Rows</th>'
            '<th title="Number of times the statement was executed">Executions</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr>'
          '<td {mono}><p><a HREF="#%1$s">%2$s</a></p>'
          '<p><small>%3$s</small></p></td>'
          '<td><p align="left">%4$s</p></td>'  -- SQL Text
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    -- Reporting on top queries by reads
    FOR r_result IN c_sh_reads LOOP
        tab_row := format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid_md5,
            md5(r_result.queryid::text),
            '',
            r_result.query,
            r_result.dbname,
            r_result.shared_blks_read,
            round(CAST(r_result.read_pct AS numeric),2),
            round(CAST(r_result.shared_hit_pct AS numeric),2),
            round(CAST(r_result.total_time AS numeric),1),
            r_result.rows,
            r_result.calls
        );

        report := report || tab_row;
        PERFORM collect_queries(
            r_result.userid,r_result.datid,r_result.queryid,r_result.queryid_md5
        );
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_shared_dirtied_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer)
  RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    tab_row     text := '';
    jtab_tpl    jsonb;

    --Cursor for top queries ordered by shared dirtied
    c_sh_dirt CURSOR FOR
    SELECT
        st.datid,
        st.userid,
        st.queryid,
        st.queryid_md5 AS queryid_md5,
        st.query,
        st.dbname,
        NULLIF(st.total_time, 0.0) as total_time,
        NULLIF(st.rows, 0) as rows,
        NULLIF(st.shared_blks_dirtied, 0) as shared_blks_dirtied,
        NULLIF(st.dirtied_pct, 0.0) as dirtied_pct,
        NULLIF(st.shared_hit_pct, 0.0) as shared_hit_pct,
        NULLIF(st.calls, 0) as calls
    FROM top_statements(sserver_id, start_id, end_id) st
    WHERE st.shared_blks_dirtied > 0
    ORDER BY st.shared_blks_dirtied DESC,
      st.queryid ASC,
      st.datid ASC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th>Query ID</th>'
            '<th title="First 100 characters of the complete sql text">SQL Text</th>'
            '<th>Database</th>'
            '<th title="Total number of shared blocks dirtied by the statement">Dirtied</th>'
            '<th title="Shared blocks dirtied by this statement as a percentage of all shared blocks dirtied in a cluster">%Total</th>'
            '<th title="Shared blocks hits as a percentage of shared blocks fetched (read + hit)">Hits(%)</th>'
            '<th title="Time spent by the statement">Elapsed(s)</th>'
            '<th title="Total number of rows retrieved or affected by the statement">Rows</th>'
            '<th title="Number of times the statement was executed">Executions</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr>'
          '<td {mono}><p><a HREF="#%1$s">%2$s</a></p>'
          '<p><small>%3$s</small></p></td>'
          '<td><p align="left">%4$s</p></td>'  -- SQL Text
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    -- Reporting on top queries by shared dirtied
    FOR r_result IN c_sh_dirt LOOP
        tab_row := format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid_md5,
            md5(r_result.queryid::text),
            '',
            r_result.query,
            r_result.dbname,
            r_result.shared_blks_dirtied,
            round(CAST(r_result.dirtied_pct AS numeric),2),
            round(CAST(r_result.shared_hit_pct AS numeric),2),
            round(CAST(r_result.total_time AS numeric),1),
            r_result.rows,
            r_result.calls
        );

        report := report || tab_row;
        PERFORM collect_queries(
            r_result.userid,r_result.datid,r_result.queryid,r_result.queryid_md5
        );
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_shared_written_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer)
  RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    tab_row     text := '';
    jtab_tpl    jsonb;

    --Cursor for top queries ordered by shared written
    c_sh_wr CURSOR FOR
    SELECT
        st.datid,
        st.userid,
        st.queryid,
        st.queryid_md5 AS queryid_md5,
        st.query,
        st.dbname,
        NULLIF(st.total_time, 0.0) as total_time,
        NULLIF(st.rows, 0) as rows,
        NULLIF(st.shared_blks_written, 0) as shared_blks_written,
        NULLIF(st.tot_written_pct, 0.0) as tot_written_pct,
        NULLIF(st.backend_written_pct, 0.0) as backend_written_pct,
        NULLIF(st.shared_hit_pct, 0.0) as shared_hit_pct,
        NULLIF(st.calls, 0) as calls
    FROM top_statements(sserver_id, start_id, end_id) st
    WHERE st.shared_blks_written > 0
    ORDER BY st.shared_blks_written DESC,
      st.queryid ASC,
      st.datid ASC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th>Query ID</th>'
            '<th title="First 100 characters of the complete sql text">SQL Text</th>'
            '<th>Database</th>'
            '<th title="Total number of shared blocks written by the statement">Written</th>'
            '<th title="Shared blocks written by this statement as a percentage of all shared blocks written in a cluster (sum of pg_stat_bgwriter fields buffers_checkpoint, buffers_clean and buffers_backend)">%Total</th>'
            '<th title="Shared blocks written by this statement as a percentage total buffers written directly by a backends (buffers_backend of pg_stat_bgwriter view)">%BackendW</th>'
            '<th title="Shared blocks hits as a percentage of shared blocks fetched (read + hit)">Hits(%)</th>'
            '<th title="Time spent by the statement">Elapsed(s)</th>'
            '<th title="Total number of rows retrieved or affected by the statement">Rows</th>'
            '<th title="Number of times the statement was executed">Executions</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr>'
          '<td {mono}><p><a HREF="#%1$s">%2$s</a></p>'
          '<p><small>%3$s</small></p></td>'
          '<td><p align="left">%4$s</p></td>'  -- SQL Text
          '<td>%s</td>'
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

    -- Reporting on top queries by shared written
    FOR r_result IN c_sh_wr LOOP
        tab_row := format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid_md5,
            md5(r_result.queryid::text),
            '',
            r_result.query,
            r_result.dbname,
            r_result.shared_blks_written,
            round(CAST(r_result.tot_written_pct AS numeric),2),
            round(CAST(r_result.backend_written_pct AS numeric),2),
            round(CAST(r_result.shared_hit_pct AS numeric),2),
            round(CAST(r_result.total_time AS numeric),1),
            r_result.rows,
            r_result.calls
        );

        report := report || tab_row;
        PERFORM collect_queries(
            r_result.userid,r_result.datid,r_result.queryid,r_result.queryid_md5
        );
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_wal_size_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    tab_row     text := '';
    jtab_tpl    jsonb;

    --Cursor for queries ordered by WAL bytes
    c_wal_size CURSOR FOR
    SELECT
        st.datid,
        st.userid,
        st.queryid,
        st.queryid_md5,
        st.query,
        st.dbname,
        NULLIF(st.wal_bytes, 0) as wal_bytes,
        NULLIF(st.wal_bytes_pct, 0.0) as wal_bytes_pct,
        NULLIF(st.shared_blks_dirtied, 0) as shared_blks_dirtied,
        NULLIF(st.wal_fpi, 0) as wal_fpi,
        NULLIF(st.wal_records, 0) as wal_records
    FROM top_statements(sserver_id, start_id, end_id) st
    WHERE st.wal_bytes > 0
    ORDER BY st.wal_bytes DESC,
      st.queryid ASC,
      st.datid ASC
    LIMIT topn;

    r_result RECORD;
BEGIN
    -- This report section is meaningful only when WAL stats is available
    IF NOT jsonb_extract_path_text(jreportset, 'report_features', 'statement_wal_bytes')::boolean THEN
      RETURN '';
    END IF;

    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th>Query ID</th>'
            '<th title="First 100 characters of the complete sql text">SQL Text</th>'
            '<th>Database</th>'
            '<th title="Total amount of WAL bytes generated by the statement">WAL</th>'
            '<th title="WAL bytes of this statement as a percentage of total WAL bytes generated by a cluster">%Total</th>'
            '<th title="Total number of shared blocks dirtied by the statement">Dirtied</th>'
            '<th title="Total number of WAL full page images generated by the statement">WAL full page images</th>'
            '<th title="Total number of WAL records generated by the statement">WAL records</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr>'
          '<td {mono}><p><a HREF="#%1$s">%2$s</a></p>'
          '<p><small>%3$s</small></p></td>'
          '<td><p align="left">%4$s</p></td>'  -- SQL Text
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
      );
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);
    -- Reporting on top queries by elapsed time
    FOR r_result IN c_wal_size LOOP
        tab_row := format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid_md5,
            md5(r_result.queryid::text),
            '',
            r_result.query,
            r_result.dbname,
            pg_size_pretty(r_result.wal_bytes),
            round(CAST(r_result.wal_bytes_pct AS numeric),2),
            r_result.shared_blks_dirtied,
            r_result.wal_fpi,
            r_result.wal_records
        );

        report := report || tab_row;
        PERFORM collect_queries(
            r_result.userid,r_result.datid,r_result.queryid,r_result.queryid_md5
        );
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_temp_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    tab_row     text := '';
    jtab_tpl    jsonb;

    --Cursor for top(cnt) querues ordered by temp usage
    c_temp CURSOR FOR
    SELECT
        st.datid,
        st.userid,
        st.queryid,
        st.queryid_md5 AS queryid_md5,
        st.query,
        st.dbname,
        NULLIF(st.total_time, 0.0) as total_time,
        NULLIF(st.rows, 0) as rows,
        NULLIF(st.local_blks_fetched, 0) as local_blks_fetched,
        NULLIF(st.local_hit_pct, 0.0) as local_hit_pct,
        NULLIF(st.temp_blks_written, 0) as temp_blks_written,
        NULLIF(st.temp_write_total_pct, 0.0) as temp_write_total_pct,
        NULLIF(st.temp_blks_read, 0) as temp_blks_read,
        NULLIF(st.temp_read_total_pct, 0.0) as temp_read_total_pct,
        NULLIF(st.local_blks_written, 0) as local_blks_written,
        NULLIF(st.local_write_total_pct, 0.0) as local_write_total_pct,
        NULLIF(st.local_blks_read, 0) as local_blks_read,
        NULLIF(st.local_read_total_pct, 0.0) as local_read_total_pct,
        NULLIF(st.calls, 0) as calls
    FROM top_statements(sserver_id, start_id, end_id) st
    WHERE COALESCE(st.temp_blks_read, 0) + COALESCE(st.temp_blks_written, 0) +
        COALESCE(st.local_blks_read, 0) + COALESCE(st.local_blks_written, 0) > 0
    ORDER BY COALESCE(st.temp_blks_read, 0) + COALESCE(st.temp_blks_written, 0) +
        COALESCE(st.local_blks_read, 0) + COALESCE(st.local_blks_written, 0) DESC,
      st.queryid ASC,
      st.datid ASC
    LIMIT topn;

    r_result RECORD;
BEGIN

    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th rowspan="2">Query ID</th>'
            '<th rowspan="2" title="First 100 characters of the complete sql text">SQL Text</th>'
            '<th rowspan="2">Database</th>'
            '<th rowspan="2" title="Number of local blocks fetched (hit + read)">Local fetched</th>'
            '<th rowspan="2" title="Local blocks hit percentage">Hits(%)</th>'
            '<th colspan="4" title="Number of blocks, used for temporary tables">Local blocks</th>'
            '<th colspan="4" title="Number of blocks, used in operations (like sorts and joins)">Temp blocks</th>'
            '<th rowspan="2" title="Time spent by the statement">Elapsed(s)</th>'
            '<th rowspan="2" title="Total number of rows retrieved or affected by the statement">Rows</th>'
            '<th rowspan="2" title="Number of times the statement was executed">Executions</th>'
          '</tr>'
          '<tr>'
            '<th title="Number of written local blocks">Write</th>'
            '<th title="Percentage of all local blocks written">%Total</th>'
            '<th title="Number of read local blocks">Read</th>'
            '<th title="Percentage of all local blocks read">%Total</th>'
            '<th title="Number of written temp blocks">Write</th>'
            '<th title="Percentage of all temp blocks written">%Total</th>'
            '<th title="Number of read temp blocks">Read</th>'
            '<th title="Percentage of all temp blocks read">%Total</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr>'
          '<td {mono}><p><a HREF="#%1$s">%2$s</a></p>'
          '<p><small>%3$s</small></p></td>'
          '<td><p align="left">%4$s</p></td>'  -- SQL Text
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
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    -- Reporting on top queries by temp usage
    FOR r_result IN c_temp LOOP
        tab_row := format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid_md5,
            md5(r_result.queryid::text),
            '',
            r_result.query,
            r_result.dbname,
            r_result.local_blks_fetched,
            round(CAST(r_result.local_hit_pct AS numeric),2),
            r_result.local_blks_written,
            round(CAST(r_result.local_write_total_pct AS numeric),2),
            r_result.local_blks_read,
            round(CAST(r_result.local_read_total_pct AS numeric),2),
            r_result.temp_blks_written,
            round(CAST(r_result.temp_write_total_pct AS numeric),2),
            r_result.temp_blks_read,
            round(CAST(r_result.temp_read_total_pct AS numeric),2),
            round(CAST(r_result.total_time AS numeric),1),
            r_result.rows,
            r_result.calls
        );

        report := report || tab_row;
        PERFORM collect_queries(
            r_result.userid,r_result.datid,r_result.queryid,r_result.queryid_md5
        );
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION collect_queries(IN userid oid, IN datid oid, queryid bigint,
  IN queryid_md5 char(32)) RETURNS integer SET search_path=@extschema@ AS $$
BEGIN
    INSERT INTO queries_list(
      userid,
      datid,
      queryid,
      queryid_md5
    )
    VALUES (
      collect_queries.userid,
      collect_queries.datid,
      collect_queries.queryid,
      collect_queries.queryid_md5
    )
    ON CONFLICT DO NOTHING;

    RETURN 0;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION report_queries(IN jreportset jsonb, IN sserver_id integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    c_queries CURSOR FOR
    SELECT
      '<p>' ||
        string_agg(md5(ql.queryid::text),
        '</p><p>' ORDER BY ql.userid, ql.datid, ql.queryid)
      || '</p>' AS queryids,
      queryid_md5,
      sl.query AS querytext
    FROM
      queries_list ql
      JOIN stmt_list sl USING (queryid_md5)
    WHERE
      server_id = sserver_id
    GROUP BY queryid_md5, querytext
    ORDER BY queryid_md5;

    qr_result   RECORD;
    report      text := '';
    query_text  text := '';
    jtab_tpl    jsonb;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th>QueryID</th>'
            '<th>Query Text</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr id="%1$s">'
          '<td {mono}>%2$s</td>'
          '<td {mono}>%3$s</td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    FOR qr_result IN c_queries LOOP
        query_text := replace(qr_result.querytext,'<','&lt;');
        query_text := replace(query_text,'>','&gt;');
        report := report||format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            qr_result.queryid_md5,
            qr_result.queryids,
            query_text
        );
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_exec_time_with_top_events_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    tab_row     text := '';
    jtab_tpl    jsonb;

    --Cursor for queries ordered by execution time
    c_top_sql CURSOR FOR
    SELECT
        t1.datid as datid,
        t1.userid as userid,
        t1.queryid as queryid,
        t1.queryid_md5 as queryid_md5,
        t1.query as query,
        t1.total_exec_time as total_exec_time,
        t1.total_exec_time_pct as total_exec_time_pct,
        t2.wait_event as wait_event,
        sum(t2.wait_time) as wait_time
    FROM (
            SELECT
                st.datid as datid,
                st.userid as userid,
                st.queryid as queryid,
                st.queryid_md5 as queryid_md5,
                st.query as query,
                NULLIF(st.total_exec_time, 0.0) as total_exec_time,
                NULLIF(st.total_exec_time_pct, 0.0) as total_exec_time_pct
            FROM top_statements(sserver_id, start_id, end_id) st
            ORDER BY st.total_exec_time DESC,
                     st.queryid ASC,
                     st.datid ASC
            LIMIT 5
         ) t1,
         sample_wait_event_total t2
    WHERE t1.queryid = t2.queryid
    AND   t2.server_id = sserver_id
    AND   t2.sample_id >= start_id
    AND   t2.sample_id <= end_id
    GROUP BY t1.datid,
             t1.userid,
             t1.queryid,
             t1.queryid_md5,
             t1.query,
             t1.total_exec_time,
             t1.total_exec_time_pct,
             t2.wait_event
    ORDER BY t1.queryid ASC,
             wait_time DESC;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th>Query ID</th>'
            '<th title="First 100 characters of the complete sql text">SQL Text</th>'
            '<th title="Time spent executing statement">Exec (s)</th>'
            '<th title="Exec time as a percentage of total cluster elapsed time">%Total</th>'
            '<th>Wait Event</th>'
            '<th>Wait Time (s)</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr>'
          '<td {mono}><p><a HREF="#%1$s">%2$s</a></p>'
          '<p><small>%3$s</small></p></td>'
          '<td><p align="left">%4$s</p></td>'  -- SQL Text
          '<td {value}>%5$s</td>'
          '<td {value}>%6$s</td>'
          '<td {value}>%7$s</td>'
          '<td {value}>%8$s</td>'
        '</tr>'
      );

    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    -- Reporting on top queries by elapsed time
    FOR r_result IN c_top_sql LOOP
        tab_row := format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid_md5,
            md5(r_result.queryid::text),
            '',
            r_result.query,
            round(CAST(r_result.total_exec_time AS numeric),2),
            round(CAST(r_result.total_exec_time_pct AS numeric),2),
            r_result.wait_event,
            r_result.wait_time
        );

        report := report || tab_row;
        PERFORM collect_queries(
            r_result.userid,r_result.datid,r_result.queryid,r_result.queryid_md5
        );
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;