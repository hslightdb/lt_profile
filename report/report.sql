/* ===== Main report function ===== */

CREATE FUNCTION host_hardware_info_htbl(IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    jtab_tpl    jsonb;

    --Cursor for stats
    c_tbl_stats CURSOR FOR
    SELECT
        'Linux ' || sh.architecture as platform,
        sh.cpus as cpus,
        sh.cpus as cores,
        sh.sockets as sockets,
        round(sh.memory_total / 1024.0 / 1024.0 / 1024.0, 1) as memory,
        round(sh.swap_total / 1024.0 / 1024.0 / 1024.0, 1) as swap
    FROM sample_server_hardware sh
    WHERE sh.server_id = sserver_id;

    r_result    RECORD;
BEGIN
    --- Populate templates
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th>Server Name</th>'
            '<th>Platform</th>'
            '<th>CPUs</th>'
            '<th>Cores</th>'
            '<th>Sockets</th>'
            '<th>Memory (GB)</th>'
            '<th>Swap (GB)</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'ts_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');

    FOR r_result IN c_tbl_stats LOOP
              report := report || format(
                  jtab_tpl #>> ARRAY['ts_tpl'],
                  '{server_name}',
                  r_result.platform,
                  r_result.cpus,
                  r_result.cores,
                  r_result.sockets,
                  r_result.memory,
                  r_result.swap
              );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'], '{rows}', report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION total_cpu_time(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS SETOF double precision SET search_path=@extschema@ AS $$
DECLARE
    start_time timestamptz;
    end_time   timestamptz;
BEGIN
    SELECT sample_time INTO start_time
    FROM samples
    WHERE server_id = sserver_id
    AND sample_id = start_id + 1;

    SELECT sample_time INTO end_time
    FROM samples
    WHERE server_id = sserver_id
    AND sample_id = end_id;

    RETURN QUERY
        SELECT COALESCE(sum(count)::double precision, 0) AS cpu_time
        FROM sample_activity_profile
        WHERE sample_time >= start_time
        AND sample_time <= end_time
        AND wait_event_type = 'DBCpu';
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION get_db_time(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS SETOF double precision SET search_path=@extschema@ AS $$
BEGIN
    RETURN QUERY
        SELECT total_wait_time(sserver_id, start_id, end_id, 'user');
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION report_sample_range_htbl(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    jtab_tpl    jsonb;
    report_start timestamp;
    report_end   timestamp;
    elapsed      integer;
    dbtime       double precision;
    --Cursor and variable for checking existance of samples
    c_sample CURSOR (csample_id integer) FOR SELECT * FROM samples WHERE server_id = sserver_id AND sample_id = csample_id;
    sample_rec samples%rowtype;
BEGIN
    OPEN c_sample(start_id);
        FETCH c_sample INTO sample_rec;
        IF sample_rec IS NULL THEN
            RAISE 'Start sample % does not exists', start_id;
        END IF;
        report_start := sample_rec.sample_time::timestamp(0) without time zone;
    CLOSE c_sample;

    OPEN c_sample(end_id);
        FETCH c_sample INTO sample_rec;
        IF sample_rec IS NULL THEN
            RAISE 'End sample % does not exists', end_id;
        END IF;
        report_end := sample_rec.sample_time::timestamp(0) without time zone;
    CLOSE c_sample;

    --- Populate templates
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th></th>'
            '<th>Snap ID</th>'
            '<th>Snap Time</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'ts_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td><p align="middle">%s</p></td>'
        '</tr>');

    report := report || format(
        jtab_tpl #>> ARRAY['ts_tpl'],
        'Begin Snap',
        start_id,
        report_start::text
    );

    report := report || format(
        jtab_tpl #>> ARRAY['ts_tpl'],
        'End Snap',
        end_id,
        report_end::text
    );

    elapsed := EXTRACT(EPOCH FROM(report_end - report_start))::integer;

    report := report || format(
        jtab_tpl #>> ARRAY['ts_tpl'],
        'Elapsed',
        to_char((elapsed || 'second')::interval, 'HH24:MI:SS'),
        '--'
    );

    dbtime := get_db_time(sserver_id, start_id, end_id);

    report := report || format(
        jtab_tpl #>> ARRAY['ts_tpl'],
        'DB Time',
        to_char((dbtime || 'second')::interval, 'HH24:MI:SS'),
        '--'
    );

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'], '{rows}', report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION cluster_instance_htbl(IN sserver_id integer)
    RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    jtab_tpl    jsonb;

    --Cursor for stats
    c_tbl_stats CURSOR FOR
    SELECT
        st.cluster_name as cluster_name,
        st.role as role,
        st.release as release,
        st.startup_time as startup_time
    FROM sample_cluster_instance st
    WHERE st.server_id = sserver_id;

    r_result    RECORD;
BEGIN
    --- Populate templates
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th>Cluster Name</th>'
            '<th>Role</th>'
            '<th>Release</th>'
            '<th>Startup Time</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'ts_tpl',
        '<tr>'
          '<td><p align="middle">%s</p></td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
        '</tr>');

    -- Reporting wait event type stats
    FOR r_result IN c_tbl_stats LOOP
              report := report || format(
                  jtab_tpl #>> ARRAY['ts_tpl'],
                  CASE WHEN r_result.cluster_name = '' THEN '--' ELSE r_result.cluster_name END,
                  r_result.role,
                  r_result.release,
                  r_result.startup_time
              );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'], '{rows}', report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION cpu_usage_htbl(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    jtab_tpl    jsonb;

    --Cursor for stats
    c_tbl_stats_begin CURSOR FOR
    SELECT
        st.user_cpu as user_cpu,
        st.sys_cpu as sys_cpu,
        st.idle_cpu as idle_cpu,
        st.io_wait as io_wait
    FROM sample_cpu_usage st
    WHERE st.server_id = sserver_id
    AND st.sample_id = start_id;

    c_tbl_stats_end CURSOR FOR
    SELECT
        st.user_cpu as user_cpu,
        st.sys_cpu as sys_cpu,
        st.idle_cpu as idle_cpu,
        st.io_wait as io_wait
    FROM sample_cpu_usage st
    WHERE st.server_id = sserver_id
    AND st.sample_id = end_id;

    r_result_begin    RECORD;
    r_result_end      RECORD;
BEGIN
    --- Populate templates
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th> </th>'
            '<th>%User</th>'
            '<th>%System</th>'
            '<th>%Idle</th>'
            '<th>%IO Wait</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'ts_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
        '</tr>');

    FOR r_result_begin IN c_tbl_stats_begin LOOP
              report := report || format(
                  jtab_tpl #>> ARRAY['ts_tpl'],
                  'Begin Snap',
                  r_result_begin.user_cpu,
                  r_result_begin.sys_cpu,
                  r_result_begin.idle_cpu,
                  r_result_begin.io_wait
              );
    END LOOP;

    FOR r_result_end IN c_tbl_stats_end LOOP
              report := report || format(
                  jtab_tpl #>> ARRAY['ts_tpl'],
                  'End Snap',
                  r_result_end.user_cpu,
                  r_result_end.sys_cpu,
                  r_result_end.idle_cpu,
                  r_result_end.io_wait
              );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'], '{rows}', report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION db_memory_usage_htbl(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    jtab_tpl    jsonb;

    --Cursor for stats
    c_tbl_stats_begin CURSOR FOR
    SELECT
        st.shared_memory as shared_memory,
        st.local_memory as local_memory
    FROM sample_db_memory_usage st
    WHERE st.server_id = sserver_id
    AND st.sample_id = start_id;

    c_tbl_stats_end CURSOR FOR
    SELECT
        st.shared_memory as shared_memory,
        st.local_memory as local_memory
    FROM sample_db_memory_usage st
    WHERE st.server_id = sserver_id
    AND st.sample_id = end_id;

    host_memory       bigint;
    r_result_begin    RECORD;
    r_result_end      RECORD;
BEGIN
    --- Populate templates
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th> </th>'
            '<th>Host memory (MB)</th>'
            '<th>Shared memory use (MB)</th>'
            '<th>Local memory use (MB)</th>'
            '<th>Host memory used for shared memory+local memory (%)</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'ts_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
        '</tr>');

    SELECT memory_total FROM sample_server_hardware WHERE server_id = sserver_id INTO host_memory;

    FOR r_result_begin IN c_tbl_stats_begin LOOP
              report := report || format(
                  jtab_tpl #>> ARRAY['ts_tpl'],
                  'Begin Snap',
                  (host_memory / 1024 / 1024)::text,
                  (r_result_begin.shared_memory / 1024 / 1024)::text,
                  (r_result_begin.local_memory / 1024 / 1024)::text,
                  ((r_result_begin.shared_memory + r_result_begin.local_memory) * 100 / host_memory)::text
              );
    END LOOP;

    FOR r_result_end IN c_tbl_stats_end LOOP
              report := report || format(
                  jtab_tpl #>> ARRAY['ts_tpl'],
                  'End Snap',
                  (host_memory / 1024 / 1024)::text,
                  (r_result_end.shared_memory / 1024 / 1024)::text,
                  (r_result_end.local_memory / 1024 / 1024)::text,
                  ((r_result_end.shared_memory + r_result_end.local_memory) * 100 / host_memory)::text
              );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'], '{rows}', report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION get_pwr(IN sserver_id integer, IN start_id integer, IN end_id integer,
  IN description text = NULL, IN with_growth boolean = false) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    tmp_text    text;
    tmp_report  text;
    report      text;
    topn        integer;
    stmt_all_cnt    integer;
    -- HTML elements templates
    report_tpl CONSTANT text := '<html><head><style>{css}</style><title>LightDB profile report</title></head><body><H1>LightDB profile report</H1>'
    '<br /><p>{report_server_hardware}</p>'
    '<br /><p>{report_sample_range}</p>'
    '<br /><p>{report_cluster_instance}</p>'
    '{report_description}{report}</body></html>';
    report_css CONSTANT text := 'table, th, td {border: 1px solid black; border-collapse: collapse; padding: 4px;} '
    'table tr td.value, table tr td.mono {font-family: Monospace;} '
    'table tr td.value {text-align: right;} '
    'table p {margin: 0.2em;}'
    'table tr.parent td:not(.relhdr) {background-color: #D8E8C2;} '
    'table tr.child td {background-color: #BBDD97; border-top-style: hidden;} '
    'table tr:nth-child(even) {background-color: #eee;} '
    'table tr:nth-child(odd) {background-color: #fff;} '
    'table tr:hover td:not(.relhdr) {background-color:#d9ffcc} '
    'table th {color: black; background-color: #ffcc99;}'
    'table tr:target {border: solid; border-width: medium; border-color: limegreen;}'
    'table tr:target td:first-of-type {font-weight: bold;}';
    description_tpl CONSTANT text := '<h2>Report description</h2><p>{description_text}</p>';
    jreportset  jsonb;
    r_result    RECORD;
BEGIN
    -- Interval expanding in case of growth stats requested
    IF with_growth THEN
      BEGIN
        SELECT left_bound, right_bound INTO STRICT start_id, end_id
        FROM get_sized_bounds(sserver_id, start_id, end_id);
      EXCEPTION
        WHEN OTHERS THEN
          RAISE 'Samples with sizes collected for requested interval (%) not found',
            format('%s - %s',start_id, end_id);
      END;
    END IF;

    -- Check if all samples of requested interval are available
    --IF (
    --  SELECT count(*) != end_id - start_id + 1 FROM samples
    --  WHERE server_id = sserver_id AND sample_id BETWEEN start_id AND end_id
    --) THEN
    --  RAISE 'There is a gap in sample sequence between %',
    --    format('%s AND %s', start_id, end_id);
    --END IF;

    -- Creating temporary table for reported queries
    CREATE TEMPORARY TABLE IF NOT EXISTS queries_list (
      userid              oid,
      datid               oid,
      queryid             bigint,
      queryid_md5       char(32),
      CONSTRAINT pk_queries_list PRIMARY KEY (userid, datid, queryid))
    ON COMMIT DELETE ROWS;

    -- CSS
    report := replace(report_tpl,'{css}',report_css);

    -- Add host hardware
    report := replace(report,'{report_server_hardware}',host_hardware_info_htbl(sserver_id));

    -- Add report sample range
    report := replace(report,'{report_sample_range}',report_sample_range_htbl(sserver_id, start_id, end_id));

    -- Add cluster instance
    report := replace(report,'{report_cluster_instance}',cluster_instance_htbl(sserver_id));

    -- Add provided description
    IF description IS NOT NULL THEN
      report := replace(report,'{report_description}',replace(description_tpl,'{description_text}',description));
    ELSE
      report := replace(report,'{report_description}','');
    END IF;

    -- Server name and description substitution
    SELECT server_name,server_description INTO STRICT r_result
    FROM servers WHERE server_id = sserver_id;

    -- replace default server_name from 'local' to real hostname
    IF sserver_id = 1 AND r_result.server_name = 'local'
    THEN
        report := replace(report,'{server_name}',pg_read_file('/proc/sys/kernel/hostname', 0, 1024, true));
    ELSE
        report := replace(report,'{server_name}',r_result.server_name);
    END IF;

    IF r_result.server_description IS NOT NULL AND r_result.server_description != ''
    THEN
      report := replace(report,'{server_description}','<p>'||r_result.server_description||'</p>');
    ELSE
      report := replace(report,'{server_description}','');
    END IF;

    -- Getting TopN setting
    BEGIN
        topn := current_setting('{lt_profile}.topn')::integer;
    EXCEPTION
        WHEN OTHERS THEN topn := 10;
    END;

    tmp_text := '';

    -- Populate report settings
    jreportset := jsonb_build_object(
    'htbl',jsonb_build_object(
      'reltr','class="parent"',
      'toasttr','class="child"',
      'reltdhdr','class="relhdr"',
      'value','class="value"',
      'mono','class="mono"',
      'reltdspanhdr','rowspan="2" class="relhdr"'
    ),
    'report_features',jsonb_build_object(
      'statstatements',profile_checkavail_statstatements(sserver_id, start_id, end_id),
      'planning_times',profile_checkavail_planning_times(sserver_id, start_id, end_id),
      'statement_wal_bytes',profile_checkavail_wal_bytes(sserver_id, start_id, end_id),
      'function_stats',profile_checkavail_functions(sserver_id, start_id, end_id),
      'trigger_function_stats',profile_checkavail_trg_functions(sserver_id, start_id, end_id),
      'table_sizes',profile_checkavail_tablesizes(sserver_id, start_id, end_id),
      'table_growth',profile_checkavail_tablegrowth(sserver_id, start_id, end_id),
      'kcachestatements',profile_checkavail_rusage(sserver_id,start_id,end_id),
      'rusage.planstats',profile_checkavail_rusage_planstats(sserver_id,start_id,end_id)
    ));

    -- Reporting possible statements overflow
    tmp_report := check_stmt_cnt(sserver_id, start_id, end_id);
    IF tmp_report != '' THEN
        tmp_text := tmp_text || '<H2>Warning!</H2>';
        tmp_text := tmp_text || '<p>This interval contains sample(s) with captured statements count more than 90% of lt_stat_statements.max setting. Consider increasing this parameter.</p>';
        tmp_text := tmp_text || tmp_report;
    END IF;

    -- lt_stat_statements.track warning
    stmt_all_cnt := check_stmt_all_setting(sserver_id, start_id, end_id);
    tmp_report := '';
    IF stmt_all_cnt > 0 THEN
        tmp_report := 'Report includes '||stmt_all_cnt||' sample(s) with setting <i>lt_stat_statements.track = all</i>.'||
        'Value of %Total columns may be incorrect.';
    END IF;
    IF tmp_report != '' THEN
        tmp_text := tmp_text || '<p><b>Warning!</b>'||tmp_report||'</p>';
    END IF;

    -- Table of Contents
    tmp_text := tmp_text ||'<H2>Report sections</H2><ul>';
    tmp_text := tmp_text || '<li><a HREF=#cl_stat>Server statistics</a></li>';
    tmp_text := tmp_text || '<ul>';
    tmp_text := tmp_text || '<li><a HREF=#cpu_usage_stat>CPU usage statistics</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#db_memory_usage_stat>Database memory usage statistics</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#db_stat>Database statistics</a></li>';
    IF jsonb_extract_path_text(jreportset, 'report_features', 'statstatements')::boolean THEN
      tmp_text := tmp_text || '<li><a HREF=#st_stat>Statement statistics by database</a></li>';
    END IF;
    tmp_text := tmp_text || '<li><a HREF=#clu_stat>Cluster statistics</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#tablespace_stat>Tablespace statistics</a></li>';
    tmp_text := tmp_text || '</ul>';

    tmp_text := tmp_text || '<li><a HREF=#wait_event_stat>Wait event statistics</a></li>';
    tmp_text := tmp_text || '<ul>';
    tmp_text := tmp_text || '<li><a HREF=#top_wait_event_type>Top wait event type by wait time</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#top_wait_event>Top wait event by wait time</a></li>';
    tmp_text := tmp_text || '</ul>';

    IF jsonb_extract_path_text(jreportset, 'report_features', 'statstatements')::boolean THEN
      tmp_text := tmp_text || '<li><a HREF=#sql_stat>SQL Query statistics</a></li>';
      tmp_text := tmp_text || '<ul>';
      IF jsonb_extract_path_text(jreportset, 'report_features', 'planning_times')::boolean THEN
        tmp_text := tmp_text || '<li><a HREF=#top_ela>Top SQL by elapsed time</a></li>';
        tmp_text := tmp_text || '<li><a HREF=#top_plan>Top SQL by planning time</a></li>';
      END IF;
      tmp_text := tmp_text || '<li><a HREF=#top_exec>Top SQL by execution time</a></li>';
      tmp_text := tmp_text || '<li><a HREF=#top_calls>Top SQL by executions</a></li>';
      tmp_text := tmp_text || '<li><a HREF=#top_iowait>Top SQL by I/O wait time</a></li>';
      tmp_text := tmp_text || '<li><a HREF=#top_pgs_fetched>Top SQL by shared blocks fetched</a></li>';
      tmp_text := tmp_text || '<li><a HREF=#top_shared_reads>Top SQL by shared blocks read</a></li>';
      tmp_text := tmp_text || '<li><a HREF=#top_shared_dirtied>Top SQL by shared blocks dirtied</a></li>';
      tmp_text := tmp_text || '<li><a HREF=#top_shared_written>Top SQL by shared blocks written</a></li>';
      IF jsonb_extract_path_text(jreportset, 'report_features', 'statement_wal_bytes')::boolean THEN
        tmp_text := tmp_text || '<li><a HREF=#top_wal_bytes>Top SQL by WAL size</a></li>';
      END IF;
      tmp_text := tmp_text || '<li><a HREF=#top_temp>Top SQL by temp usage</a></li>';
      IF jsonb_extract_path_text(jreportset, 'report_features', 'kcachestatements')::boolean THEN
        tmp_text := tmp_text || '<li><a HREF=#kcache_stat>Rusage statistics</a></li>';
        tmp_text := tmp_text || '<ul>';
        tmp_text := tmp_text || '<li><a HREF=#kcache_time>Top SQL by system and user time </a></li>';
        tmp_text := tmp_text || '<li><a HREF=#kcache_reads_writes>Top SQL by reads/writes done by filesystem layer </a></li>';
        tmp_text := tmp_text || '</ul>';
      END IF;
      -- SQL texts
      tmp_text := tmp_text || '<li><a HREF=#sql_list>Complete list of SQL texts</a></li>';
      tmp_text := tmp_text || '</ul>';
    END IF;

    tmp_text := tmp_text || '<li><a HREF=#schema_stat>Schema object statistics</a></li>';
    tmp_text := tmp_text || '<ul>';
    tmp_text := tmp_text || '<li><a HREF=#scanned_tbl>Top tables by estimated sequentially scanned volume</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#fetch_tbl>Top tables by blocks fetched</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#read_tbl>Top tables by blocks read</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#dml_tbl>Top DML tables</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#vac_tbl>Top tables by updated/deleted tuples</a></li>';
    IF jsonb_extract_path_text(jreportset, 'report_features', 'table_growth')::boolean THEN
      tmp_text := tmp_text || '<li><a HREF=#growth_tbl>Top growing tables</a></li>';
    END IF;
    tmp_text := tmp_text || '<li><a HREF=#fetch_idx>Top indexes by blocks fetched</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#read_idx>Top indexes by blocks read</a></li>';
    IF jsonb_extract_path_text(jreportset, 'report_features', 'table_growth')::boolean THEN
      tmp_text := tmp_text || '<li><a HREF=#growth_idx>Top growing indexes</a></li>';
    END IF;
    tmp_text := tmp_text || '<li><a HREF=#ix_unused>Unused indexes</a></li>';
    tmp_text := tmp_text || '</ul>';

    IF jsonb_extract_path_text(jreportset, 'report_features', 'function_stats')::boolean THEN
      tmp_text := tmp_text || '<li><a HREF=#func_stat>User function statistics</a></li>';
      tmp_text := tmp_text || '<ul>';
      tmp_text := tmp_text || '<li><a HREF=#funcs_time_stat>Top functions by total time</a></li>';
      tmp_text := tmp_text || '<li><a HREF=#funcs_calls_stat>Top functions by executions</a></li>';
      IF jsonb_extract_path_text(jreportset, 'report_features', 'trigger_function_stats')::boolean THEN
        tmp_text := tmp_text || '<li><a HREF=#trg_funcs_time_stat>Top trigger functions by total time</a></li>';
      END IF;
      tmp_text := tmp_text || '</ul>';
    END IF;


    tmp_text := tmp_text || '<li><a HREF=#vacuum_stats>Vacuum-related statistics</a></li>';
    tmp_text := tmp_text || '<ul>';
    tmp_text := tmp_text || '<li><a HREF=#top_vacuum_cnt_tbl>Top tables by vacuum operations</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#top_analyze_cnt_tbl>Top tables by analyze operations</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#top_ix_vacuum_bytes_cnt_tbl>Top indexes by estimated vacuum I/O load</a></li>';

    tmp_text := tmp_text || '<li><a HREF=#dead_tbl>Top tables by dead tuples ratio</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#mod_tbl>Top tables by modified tuples ratio</a></li>';
    tmp_text := tmp_text || '</ul>';
    tmp_text := tmp_text || '<li><a HREF=#pg_settings>Cluster settings during the report interval</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#kernel_settings>Kernel settings during the report interval</a></li>';
    tmp_text := tmp_text || '</ul>';

    --Reporting cluster stats
    tmp_text := tmp_text || '<H2><a NAME=cl_stat>Server statistics</a></H2>';
    tmp_text := tmp_text || '<H3><a NAME=cpu_usage_stat>CPU usage statistics</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(cpu_usage_htbl(sserver_id, start_id, end_id));
    tmp_text := tmp_text || '<H3><a NAME=db_memory_usage_stat>Database memory usage statistics</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(db_memory_usage_htbl(sserver_id, start_id, end_id));
    tmp_text := tmp_text || '<H3><a NAME=db_stat>Database statistics</a></H3>';
    tmp_report := dbstats_reset_htbl(jreportset, sserver_id, start_id, end_id);
    IF tmp_report != '' THEN
      tmp_text := tmp_text || '<p><b>Warning!</b> Database statistics reset detected during report period!</p>'||tmp_report||
        '<p>Statistics for listed databases and contained objects might be affected</p>';
    END IF;
    tmp_text := tmp_text || nodata_wrapper(dbstats_htbl(jreportset, sserver_id, start_id, end_id, topn));

    IF jsonb_extract_path_text(jreportset, 'report_features', 'statstatements')::boolean THEN
      tmp_text := tmp_text || '<H3><a NAME=st_stat>Statement statistics by database</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(statements_stats_htbl(jreportset, sserver_id, start_id, end_id, topn));
    END IF;

    tmp_text := tmp_text || '<H3><a NAME=clu_stat>Cluster statistics</a></H3>';
    tmp_report := cluster_stats_reset_htbl(jreportset, sserver_id, start_id, end_id);
    IF tmp_report != '' THEN
      tmp_text := tmp_text || '<p><b>Warning!</b> Cluster statistics reset detected during report period!</p>'||tmp_report||
        '<p>Cluster statistics might be affected</p>';
    END IF;
    tmp_text := tmp_text || nodata_wrapper(cluster_stats_htbl(jreportset, sserver_id, start_id, end_id));

    tmp_text := tmp_text || '<H3><a NAME=tablespace_stat>Tablespace statistics</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(tablespaces_stats_htbl(jreportset, sserver_id, start_id, end_id));

    --Reporting wait event statistics
    tmp_text := tmp_text || '<H2><a NAME=wait_event_stat>Wait event statistics</a></H2>';
    tmp_text := tmp_text || '<H3><a NAME=top_wait_event_type>Top wait event type by wait time</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_user_event_type_htbl(jreportset, sserver_id, start_id, end_id, topn));
    tmp_text := tmp_text || '<H3><a NAME=top_wait_event>Top wait event by wait time</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_user_event_htbl(jreportset, sserver_id, start_id, end_id, topn));

    --Reporting on top queries by elapsed time
    IF jsonb_extract_path_text(jreportset, 'report_features', 'statstatements')::boolean THEN
      tmp_text := tmp_text || '<H2><a NAME=sql_stat>SQL Query statistics</a></H2>';
      IF jsonb_extract_path_text(jreportset, 'report_features', 'planning_times')::boolean THEN
        tmp_text := tmp_text || '<H3><a NAME=top_ela>Top SQL by elapsed time</a></H3>';
        tmp_text := tmp_text || nodata_wrapper(top_elapsed_htbl(jreportset, sserver_id, start_id, end_id, topn));
        tmp_text := tmp_text || '<H3><a NAME=top_plan>Top SQL by planning time</a></H3>';
        tmp_text := tmp_text || nodata_wrapper(top_plan_time_htbl(jreportset, sserver_id, start_id, end_id, topn));
      END IF;
      tmp_text := tmp_text || '<H3><a NAME=top_exec>Top SQL by execution time</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(top_exec_time_htbl(jreportset, sserver_id, start_id, end_id, topn));

      -- Reporting on top queries by executions
      tmp_text := tmp_text || '<H3><a NAME=top_calls>Top SQL by executions</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(top_exec_htbl(jreportset, sserver_id, start_id, end_id, topn));

      -- Reporting on top queries by I/O wait time
      tmp_text := tmp_text || '<H3><a NAME=top_iowait>Top SQL by I/O wait time</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(top_iowait_htbl(jreportset, sserver_id, start_id, end_id, topn));

      -- Reporting on top queries by fetched blocks
      tmp_text := tmp_text || '<H3><a NAME=top_pgs_fetched>Top SQL by shared blocks fetched</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(top_shared_blks_fetched_htbl(jreportset, sserver_id, start_id, end_id, topn));

      -- Reporting on top queries by shared reads
      tmp_text := tmp_text || '<H3><a NAME=top_shared_reads>Top SQL by shared blocks read</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(top_shared_reads_htbl(jreportset, sserver_id, start_id, end_id, topn));

      -- Reporting on top queries by shared dirtied
      tmp_text := tmp_text || '<H3><a NAME=top_shared_dirtied>Top SQL by shared blocks dirtied</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(top_shared_dirtied_htbl(jreportset, sserver_id, start_id, end_id, topn));

      -- Reporting on top queries by shared written
      tmp_text := tmp_text || '<H3><a NAME=top_shared_written>Top SQL by shared blocks written</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(top_shared_written_htbl(jreportset, sserver_id, start_id, end_id, topn));

      -- Reporting on top queries by WAL bytes
      IF jsonb_extract_path_text(jreportset, 'report_features', 'statement_wal_bytes')::boolean THEN
        tmp_text := tmp_text || '<H3><a NAME=top_wal_bytes>Top SQL by WAL size</a></H3>';
        tmp_text := tmp_text || nodata_wrapper(top_wal_size_htbl(jreportset, sserver_id, start_id, end_id, topn));
      END IF;

      -- Reporting on top queries by temp usage
      tmp_text := tmp_text || '<H3><a NAME=top_temp>Top SQL by temp usage</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(top_temp_htbl(jreportset, sserver_id, start_id, end_id, topn));

      --Kcache section
     IF jsonb_extract_path_text(jreportset, 'report_features', 'kcachestatements')::boolean THEN
      -- Reporting kcache queries
        tmp_text := tmp_text||'<H3><a NAME=kcache_stat>Rusage statistics</a></H3>';
        tmp_text := tmp_text||'<H4><a NAME=kcache_time>Top SQL by system and user time </a></H4>';
        tmp_text := tmp_text || nodata_wrapper(top_cpu_time_htbl(jreportset, sserver_id, start_id, end_id, topn));
        tmp_text := tmp_text||'<H4><a NAME=kcache_reads_writes>Top SQL by reads/writes done by filesystem layer </a></H4>';
        tmp_text := tmp_text || nodata_wrapper(top_io_filesystem_htbl(jreportset, sserver_id, start_id, end_id, topn));
     END IF;

      -- Listing queries
      tmp_text := tmp_text || '<H3><a NAME=sql_list>Complete list of SQL texts</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(report_queries(jreportset, sserver_id));
    END IF;

    -- Reporting Object stats
    -- Reporting scanned table
    tmp_text := tmp_text || '<H2><a NAME=schema_stat>Schema object statistics</a></H2>';
    tmp_text := tmp_text || '<H3><a NAME=scanned_tbl>Top tables by estimated sequentially scanned volume</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_scan_tables_htbl(jreportset, sserver_id, start_id, end_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=fetch_tbl>Top tables by blocks fetched</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(tbl_top_fetch_htbl(jreportset, sserver_id, start_id, end_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=read_tbl>Top tables by blocks read</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(tbl_top_io_htbl(jreportset, sserver_id, start_id, end_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=dml_tbl>Top DML tables</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_dml_tables_htbl(jreportset, sserver_id, start_id, end_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=vac_tbl>Top tables by updated/deleted tuples</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_upd_vac_tables_htbl(jreportset, sserver_id, start_id, end_id, topn));

    IF jsonb_extract_path_text(jreportset, 'report_features', 'table_growth')::boolean THEN
      tmp_text := tmp_text || '<H3><a NAME=growth_tbl>Top growing tables</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(top_growth_tables_htbl(jreportset, sserver_id, start_id, end_id, topn));
    END IF;

    tmp_text := tmp_text || '<H3><a NAME=fetch_idx>Top indexes by blocks fetched</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(ix_top_fetch_htbl(jreportset, sserver_id, start_id, end_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=read_idx>Top indexes by blocks read</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(ix_top_io_htbl(jreportset, sserver_id, start_id, end_id, topn));

    IF jsonb_extract_path_text(jreportset, 'report_features', 'table_growth')::boolean THEN
      tmp_text := tmp_text || '<H3><a NAME=growth_idx>Top growing indexes</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(top_growth_indexes_htbl(jreportset, sserver_id, start_id, end_id, topn));
    END IF;

    tmp_text := tmp_text || '<H3><a NAME=ix_unused>Unused indexes</a></H3>';
    tmp_text := tmp_text || '<p>This table contains non-scanned indexes (during report period), ordered by number of DML operations on underlying tables. Constraint indexes are excluded.</p>';
    tmp_text := tmp_text || nodata_wrapper(ix_unused_htbl(jreportset, sserver_id, start_id, end_id, topn));

    IF jsonb_extract_path_text(jreportset, 'report_features', 'function_stats')::boolean THEN
      tmp_text := tmp_text || '<H2><a NAME=func_stat>User function statistics</a></H2>';
      tmp_text := tmp_text || '<H3><a NAME=funcs_time_stat>Top functions by total time</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(func_top_time_htbl(jreportset, sserver_id, start_id, end_id, topn));

      tmp_text := tmp_text || '<H3><a NAME=funcs_calls_stat>Top functions by executions</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(func_top_calls_htbl(jreportset, sserver_id, start_id, end_id, topn));

      IF jsonb_extract_path_text(jreportset, 'report_features', 'trigger_function_stats')::boolean THEN
        tmp_text := tmp_text || '<H3><a NAME=trg_funcs_time_stat>Top trigger functions by total time</a></H3>';
        tmp_text := tmp_text || nodata_wrapper(func_top_trg_htbl(jreportset, sserver_id, start_id, end_id, topn));
      END IF;
    END IF;

    -- Reporting vacuum related stats
    tmp_text := tmp_text || '<H2><a NAME=vacuum_stats>Vacuum-related statistics</a></H2>';
    tmp_text := tmp_text || '<H3><a NAME=top_vacuum_cnt_tbl>Top tables by vacuum operations</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_vacuumed_tables_htbl(jreportset, sserver_id, start_id, end_id, topn));
    tmp_text := tmp_text || '<H3><a NAME=top_analyze_cnt_tbl>Top tables by analyze operations</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_analyzed_tables_htbl(jreportset, sserver_id, start_id, end_id, topn));
    tmp_text := tmp_text || '<H3><a NAME=top_ix_vacuum_bytes_cnt_tbl>Top indexes by estimated vacuum I/O load</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_vacuumed_indexes_htbl(jreportset, sserver_id, start_id, end_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=dead_tbl>Top tables by dead tuples ratio</a></H3>';
    tmp_text := tmp_text || '<p>Data in this section is not differential. This data is valid for last report sample only.</p>';
    tmp_text := tmp_text || nodata_wrapper(tbl_top_dead_htbl(jreportset, sserver_id, start_id, end_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=mod_tbl>Top tables by modified tuples ratio</a></H3>';
    tmp_text := tmp_text || '<p>Table shows modified tuples statistics since last analyze.</p>';
    tmp_text := tmp_text || '<p>Data in this section is not differential. This data is valid for last report sample only.</p>';
    tmp_text := tmp_text || nodata_wrapper(tbl_top_mods_htbl(jreportset, sserver_id, start_id, end_id, topn));

    -- Database settings report
    tmp_text := tmp_text || '<H2><a NAME=pg_settings>Cluster settings during the report interval</a></H2>';
    tmp_text := tmp_text || nodata_wrapper(settings_and_changes_htbl(jreportset, sserver_id, start_id, end_id));

    -- Server host settings report
    tmp_text := tmp_text || '<H2><a NAME=kernel_settings>Kernel settings during the report interval</a></H2>';
    tmp_text := tmp_text || nodata_wrapper(kernel_settings_htbl(jreportset, sserver_id, start_id, end_id));

    -- Reporting possible statements overflow
    tmp_report := check_stmt_cnt(sserver_id);
    IF tmp_report != '' THEN
        tmp_text := tmp_text || '<H2>Warning!</H2>';
        tmp_text := tmp_text || '<p>Sample repository contains samples with captured statements count more than 90% of lt_stat_statements.max setting. Consider increasing this parameter.</p>';
        tmp_text := tmp_text || tmp_report;
    END IF;

    RETURN replace(report,'{report}',tmp_text);
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION get_pwr(IN sserver_id integer, IN start_id integer, IN end_id integer,
  IN description text, IN with_growth boolean)
IS 'Statistics PWR report generation function. Takes server_id and IDs of start and end sample (inclusive).';

CREATE FUNCTION get_pwr(IN server name, IN start_id integer, IN end_id integer,
  IN description text = NULL, IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_pwr(get_server_by_name(server), start_id, end_id,
    description, with_growth);
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_pwr(IN server name, IN start_id integer, IN end_id integer,
  IN description text, IN with_growth boolean)
IS 'Statistics PWR report generation function. Takes server name and IDs of start and end sample (inclusive).';

CREATE FUNCTION get_pwr(IN start_id integer, IN end_id integer,
  IN description text = NULL, IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_pwr('local',start_id,end_id,description,with_growth);
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_pwr(IN start_id integer, IN end_id integer,
  IN description text, IN with_growth boolean)
IS 'Statistics PWR report generation function for local server. Takes IDs of start and end sample (inclusive).';

CREATE FUNCTION get_pwr(IN sserver_id integer, IN time_range tstzrange,
  IN description text = NULL, IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_pwr(sserver_id, start_id, end_id, description, with_growth)
  FROM get_sampleids_by_timerange(sserver_id, time_range)
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_pwr(IN sserver_id integer, IN time_range tstzrange,
  IN description text, IN with_growth boolean)
IS 'Statistics PWR report generation function. Takes server ID and time interval.';

CREATE FUNCTION get_pwr(IN server name, IN time_range tstzrange,
  IN description text = NULL, IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_pwr(get_server_by_name(server), start_id, end_id, description,with_growth)
  FROM get_sampleids_by_timerange(get_server_by_name(server), time_range)
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_pwr(IN server name, IN time_range tstzrange,
  IN description text, IN with_growth boolean)
IS 'Statistics PWR report generation function. Takes server name and time interval.';

CREATE FUNCTION get_pwr(IN time_range tstzrange, IN description text = NULL,
  IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_pwr(get_server_by_name('local'), start_id, end_id, description, with_growth)
  FROM get_sampleids_by_timerange(get_server_by_name('local'), time_range)
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_pwr(IN time_range tstzrange,
  IN description text, IN with_growth boolean)
IS 'Statistics PWR report generation function for local server. Takes time interval.';

CREATE FUNCTION get_pwr(IN server name, IN baseline varchar(25),
  IN description text = NULL, IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_pwr(get_server_by_name(server), start_id, end_id, description, with_growth)
  FROM get_baseline_samples(get_server_by_name(server), baseline)
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_pwr(IN server name, IN baseline varchar(25),
  IN description text, IN with_growth boolean)
IS 'Statistics PWR report generation function for server baseline. Takes server name and baseline name.';

CREATE FUNCTION get_pwr(IN baseline varchar(25), IN description text = NULL,
  IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
BEGIN
    RETURN get_pwr('local',baseline,description,with_growth);
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION get_pwr(IN baseline varchar(25),
  IN description text, IN with_growth boolean)
IS 'Statistics PWR report generation function for local server baseline. Takes baseline name.';

CREATE FUNCTION get_pwr_latest(IN server name = NULL)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_pwr(srv.server_id, s.sample_id, e.sample_id, NULL)
  FROM samples s JOIN samples e ON (s.server_id = e.server_id AND s.sample_id = e.sample_id - 1)
    JOIN servers srv ON (e.server_id = srv.server_id AND e.sample_id = srv.last_sample_id)
  WHERE srv.server_name = COALESCE(server, 'local')
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_pwr_latest(IN server name) IS 'Statistics PWR report generation function for last two samples';






