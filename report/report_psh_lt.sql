/* ===== Main report function ===== */

CREATE FUNCTION psh_cluster_instance_htbl(IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    jtab_tpl    jsonb;

    --Cursor for stats
    c_tbl_stats CURSOR FOR
    SELECT
        st.cluster_name as cluster_name,
        st.release as release,
        st.role as role,
        st.hostname as hostname
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
            '<th>Release</th>'
            '<th>Role</th>'
            '<th>Host</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'ts_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
        '</tr>');

    FOR r_result IN c_tbl_stats LOOP
        IF sserver_id = 1 AND r_result.hostname = 'local' THEN
            report := report || format(
              jtab_tpl #>> ARRAY['ts_tpl'],
              r_result.cluster_name,
              r_result.release,
              r_result.role,
              pg_read_file('/proc/sys/kernel/hostname', 0, 1024, true)
            );
        ELSE
            report := report || format(
              jtab_tpl #>> ARRAY['ts_tpl'],
              r_result.cluster_name,
              r_result.release,
              r_result.role,
              r_result.hostname
            );
        END IF;
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'], '{rows}', report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION psh_cpu_dbbuffer_info_htbl(IN sserver_id integer)
    RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    jtab_tpl    jsonb;

    --Cursor for stats
    c_tbl_stats CURSOR FOR
    SELECT
        st.cpus as cpus,
        (SELECT (reset_val::integer * 8 / 1024)::text || 'MB' FROM sample_settings WHERE server_id = sserver_id AND name = 'shared_buffers' ORDER BY first_seen DESC LIMIT 1) as shared_buffers,
        (SELECT (reset_val::integer / 1024)::text || 'MB' FROM sample_settings WHERE server_id = sserver_id AND name = 'work_mem' ORDER BY first_seen DESC LIMIT 1) as work_mem,
        (SELECT (reset_val::integer * 8 / 1024)::text || 'MB' FROM sample_settings WHERE server_id = sserver_id AND name = 'wal_buffers' ORDER BY first_seen DESC LIMIT 1) as wal_buffers,
        (SELECT reset_val::integer::text || 'MB' FROM sample_settings WHERE server_id = sserver_id AND name = 'min_wal_size' ORDER BY first_seen DESC LIMIT 1) as min_wal_size,
        (SELECT reset_val::integer::text || 'MB' FROM sample_settings WHERE server_id = sserver_id AND name = 'max_wal_size' ORDER BY first_seen DESC LIMIT 1) as max_wal_size
    FROM sample_server_hardware st
    WHERE st.server_id = sserver_id;

    r_result    RECORD;
BEGIN
    --- Populate templates
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th>CPUs</th>'
            '<th>shared_buffers</th>'
            '<th>work_mem</th>'
            '<th>wal_buffers</th>'
            '<th>min_wal_size</th>'
            '<th>max_wal_size</th>'
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
          '<td>%s</td>'
        '</tr>');

    -- apply settings to templates
    --jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    -- Reporting wait event type stats
    FOR r_result IN c_tbl_stats LOOP
              report := report || format(
                  jtab_tpl #>> ARRAY['ts_tpl'],
                  r_result.cpus,
                  r_result.shared_buffers,
                  r_result.work_mem,
                  r_result.wal_buffers,
                  r_result.min_wal_size,
                  r_result.max_wal_size
              );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'], '{rows}', report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION get_avg_active_sessions(IN start_time timestamp, IN end_time timestamp)
RETURNS SETOF double precision SET search_path=@extschema@ AS $$
BEGIN
    IF (SELECT count(1) FROM pg_catalog.pg_class WHERE relname = 'sample_active_session_profile') > 0 THEN
        RETURN QUERY
            SELECT
                CASE WHEN (sum(t.active_sessions)::double precision / count(t.active_sessions)) IS NULL
                THEN 0
                ELSE (sum(t.active_sessions)::double precision / count(t.active_sessions))
                END
            FROM  sample_active_session_profile t
            WHERE t.sample_time >= start_time
            AND   t.sample_time <= end_time;
    ELSE
        RETURN QUERY
            SELECT 0::double precision;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION psh_report_sample_range_htbl(IN sserver_id integer, IN start_id integer, IN end_id integer)
    RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    jtab_tpl    jsonb;
    report_start timestamp;
    report_end   timestamp;
    elapsed      integer;
    avg_active_sessions double precision;
    cpus integer := (SELECT st.cpus FROM sample_server_hardware st WHERE st.server_id = sserver_id);
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

    elapsed := EXTRACT(EPOCH FROM(report_end - report_start))::integer;
    avg_active_sessions :=  round(get_avg_active_sessions(report_start, report_end)::numeric, 2);

    --- Populate templates
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th></th>'
            '<th>Sample Time</th>'
            '<th>Data Source</th>'
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
        'Analysis Begin Time',
        report_start::text,
        'pg_stat_activity'
    );

    report := report || format(
        jtab_tpl #>> ARRAY['ts_tpl'],
        'Analysis End Time',
        report_end::text,
        'pg_stat_activity'
    );

    IF elapsed < 60 THEN
        report := report || format(
            jtab_tpl #>> ARRAY['ts_tpl'],
            'Elapsed Time',
            elapsed::text || ' (secs)',
            '--'
        );
    ELSE
        report := report || format(
            jtab_tpl #>> ARRAY['ts_tpl'],
            'Elapsed Time',
            round(elapsed / 60.0, 2)::text || ' (mins)',
            '--'
        );
    END IF;

    report := report || format(
        jtab_tpl #>> ARRAY['ts_tpl'],
        'Sample Count',
        (end_id - start_id + 1)::text,
        '--'
    );

    report := report || format(
        jtab_tpl #>> ARRAY['ts_tpl'],
        'Average Active Sessions',
        avg_active_sessions::text,
        '--'
    );

    report := report || format(
        jtab_tpl #>> ARRAY['ts_tpl'],
        'Avg. Active Session per CPU',
        round((avg_active_sessions / cpus)::numeric, 2)::text,
        '--'
    );

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'], '{rows}', report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION get_psh(IN sserver_id integer, IN start_id integer, IN end_id integer,
  IN description text = NULL, IN with_growth boolean = false) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    tmp_text    text;
    tmp_report  text;
    report      text;
    topn        integer;
    stmt_all_cnt    integer;
    -- HTML elements templates
    report_tpl CONSTANT text := '<html><head><style>{css}</style><title>PSH report for {cluster_name}</title></head><body><H1>PSH report for {cluster_name}</H1>'
    '<p>{report_cluster_instance}</p>'
    '<p>{report_cpu_dbbuffer}</p>'
    '<p>{report_sample_range}</p>'
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

    -- Add cluster instance
    report := replace(report,'{report_cluster_instance}',psh_cluster_instance_htbl(sserver_id));

    -- Add host cpu and database buffer info
    report := replace(report,'{report_cpu_dbbuffer}',psh_cpu_dbbuffer_info_htbl(sserver_id));

    -- Add report sample range
    report := replace(report,'{report_sample_range}',psh_report_sample_range_htbl(sserver_id, start_id, end_id));

    -- Add provided description
    IF description IS NOT NULL THEN
      report := replace(report,'{report_description}',replace(description_tpl,'{description_text}',description));
    ELSE
      report := replace(report,'{report_description}','');
    END IF;

    -- cluster_name substitution
    SELECT cluster_name INTO STRICT r_result
    FROM sample_cluster_instance WHERE server_id = sserver_id;
    report := replace(report,'{cluster_name}',r_result.cluster_name);

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
    tmp_text := tmp_text || '<li><a HREF=#top_wait_events>Top Wait Events</a></li>';
    tmp_text := tmp_text || '<ul>';
    tmp_text := tmp_text || '<li><a HREF=#top_user_events>Top User Events</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#top_background_events>Top Background Events</a></li>';
    tmp_text := tmp_text || '</ul>';

    IF jsonb_extract_path_text(jreportset, 'report_features', 'statstatements')::boolean THEN
      tmp_text := tmp_text || '<li><a HREF=#top_sql>Top SQL</a></li>';
      tmp_text := tmp_text || '<ul>';
      tmp_text := tmp_text || '<li><a HREF=#top_sql_with_top_event>Top SQL with Top Events</a></li>';
      tmp_text := tmp_text || '<li><a HREF=#sql_list>Complete list of SQL texts</a></li>';
      tmp_text := tmp_text || '</ul>';
    END IF;

    tmp_text := tmp_text || '</ul>';

    --Reporting wait event statistics
    tmp_text := tmp_text || '<H2><a NAME=top_wait_events>Top Wait Events</a></H2>';
    tmp_text := tmp_text || '<H3><a NAME=top_user_events>Top User Events</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_user_event_htbl(jreportset, sserver_id, start_id, end_id, topn));
    tmp_text := tmp_text || '<H3><a NAME=top_background_events>Top Background Events</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_background_event_htbl(jreportset, sserver_id, start_id, end_id, topn));

    --Reporting on top queries by elapsed time
    IF jsonb_extract_path_text(jreportset, 'report_features', 'statstatements')::boolean THEN
      tmp_text := tmp_text || '<H2><a NAME=top_sql>Top SQL</a></H2>';
      tmp_text := tmp_text || '<H3><a NAME=top_sql_with_top_event>Top SQL with Top Events</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(top_exec_time_with_top_events_htbl(jreportset, sserver_id, start_id, end_id, topn));

      -- Listing queries
      tmp_text := tmp_text || '<H3><a NAME=sql_list>Complete list of SQL texts</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(report_queries(jreportset, sserver_id));
    END IF;

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

COMMENT ON FUNCTION get_psh(IN sserver_id integer, IN start_id integer, IN end_id integer,
  IN description text, IN with_growth boolean)
IS 'Statistics PSH report generation function. Takes server_id and IDs of start and end sample (inclusive).';

CREATE FUNCTION get_psh(IN server name, IN start_id integer, IN end_id integer,
  IN description text = NULL, IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_psh(get_server_by_name(server), start_id, end_id,
    description, with_growth);
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_psh(IN server name, IN start_id integer, IN end_id integer,
  IN description text, IN with_growth boolean)
IS 'Statistics PSH report generation function. Takes server name and IDs of start and end sample (inclusive).';

CREATE FUNCTION get_psh(IN start_id integer, IN end_id integer,
  IN description text = NULL, IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_psh('local',start_id,end_id,description,with_growth);
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_psh(IN start_id integer, IN end_id integer,
  IN description text, IN with_growth boolean)
IS 'Statistics PSH report generation function for local server. Takes IDs of start and end sample (inclusive).';

CREATE FUNCTION get_psh(IN sserver_id integer, IN time_range tstzrange,
  IN description text = NULL, IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_psh(sserver_id, start_id, end_id, description, with_growth)
  FROM get_sampleids_by_timerange(sserver_id, time_range)
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_psh(IN sserver_id integer, IN time_range tstzrange,
  IN description text, IN with_growth boolean)
IS 'Statistics PSH report generation function. Takes server ID and time interval.';

CREATE FUNCTION get_psh(IN server name, IN time_range tstzrange,
  IN description text = NULL, IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_psh(get_server_by_name(server), start_id, end_id, description,with_growth)
  FROM get_sampleids_by_timerange(get_server_by_name(server), time_range)
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_psh(IN server name, IN time_range tstzrange,
  IN description text, IN with_growth boolean)
IS 'Statistics PSH report generation function. Takes server name and time interval.';

CREATE FUNCTION get_psh(IN time_range tstzrange, IN description text = NULL,
  IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_psh(get_server_by_name('local'), start_id, end_id, description, with_growth)
  FROM get_sampleids_by_timerange(get_server_by_name('local'), time_range)
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_psh(IN time_range tstzrange,
  IN description text, IN with_growth boolean)
IS 'Statistics PSH report generation function for local server. Takes time interval.';

CREATE FUNCTION get_psh(IN server name, IN baseline varchar(25),
  IN description text = NULL, IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_psh(get_server_by_name(server), start_id, end_id, description, with_growth)
  FROM get_baseline_samples(get_server_by_name(server), baseline)
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_psh(IN server name, IN baseline varchar(25),
  IN description text, IN with_growth boolean)
IS 'Statistics PSH report generation function for server baseline. Takes server name and baseline name.';

CREATE FUNCTION get_psh(IN baseline varchar(25), IN description text = NULL,
  IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
BEGIN
    RETURN get_psh('local',baseline,description,with_growth);
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION get_psh(IN baseline varchar(25),
  IN description text, IN with_growth boolean)
IS 'Statistics PSH report generation function for local server baseline. Takes baseline name.';

CREATE FUNCTION get_psh_latest(IN server name = NULL)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_psh(srv.server_id, s.sample_id, e.sample_id, NULL)
  FROM samples s JOIN samples e ON (s.server_id = e.server_id AND s.sample_id = e.sample_id - 1)
    JOIN servers srv ON (e.server_id = srv.server_id AND e.sample_id = srv.last_sample_id)
  WHERE srv.server_name = COALESCE(server, 'local')
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_psh_latest(IN server name) IS 'Statistics PSH report generation function for last two samples';