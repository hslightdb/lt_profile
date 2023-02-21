/* ===== Function stats functions ===== */
CREATE FUNCTION profile_checkavail_functions(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS BOOLEAN
SET search_path=@extschema@ AS $$
-- Check if we have planning times collected for report interval
  SELECT COALESCE(sum(calls), 0) > 0
  FROM sample_stat_user_func_total sn
  WHERE sn.server_id = sserver_id AND sn.sample_id BETWEEN start_id + 1 AND end_id
$$ LANGUAGE sql;

CREATE FUNCTION profile_checkavail_trg_functions(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS BOOLEAN
SET search_path=@extschema@ AS $$
-- Check if we have planning times collected for report interval
  SELECT COALESCE(sum(calls), 0) > 0
  FROM sample_stat_user_func_total sn
  WHERE sn.server_id = sserver_id AND sn.sample_id BETWEEN start_id + 1 AND end_id
    AND sn.trg_fn
$$ LANGUAGE sql;
/* ===== Function stats functions ===== */

CREATE FUNCTION top_functions(IN sserver_id integer, IN start_id integer, IN end_id integer, IN trigger_fn boolean)
RETURNS TABLE(
    server_id     integer,
    datid       oid,
    funcid      oid,
    dbname      name,
    schemaname  name,
    funcname    name,
    funcargs    text,
    calls       bigint,
    total_time  double precision,
    self_time   double precision,
    m_time      double precision,
    m_stime     double precision
)
SET search_path=@extschema@ AS $$
    SELECT
        st.server_id,
        st.datid,
        st.funcid,
        sample_db.datname AS dbname,
        st.schemaname,
        st.funcname,
        st.funcargs,
        sum(st.calls)::bigint AS calls,
        sum(st.total_time)/1000 AS total_time,
        sum(st.self_time)/1000 AS self_time,
        sum(st.total_time)/NULLIF(sum(st.calls),0)/1000 AS m_time,
        sum(st.self_time)/NULLIF(sum(st.calls),0)/1000 AS m_stime
    FROM v_sample_stat_user_functions st
        -- Database name
        JOIN sample_stat_database sample_db
        ON (st.server_id=sample_db.server_id AND st.sample_id=sample_db.sample_id AND st.datid=sample_db.datid)
    WHERE
      st.server_id = sserver_id
      AND st.trg_fn = trigger_fn
      AND NOT sample_db.datistemplate
      AND st.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY st.server_id,st.datid,st.funcid,sample_db.datname,st.schemaname,st.funcname,st.funcargs
$$ LANGUAGE sql;

CREATE FUNCTION func_top_time_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    c_fun_stats CURSOR FOR
    SELECT
        dbname,
        schemaname,
        funcname,
        funcargs,
        NULLIF(calls, 0) as calls,
        NULLIF(total_time, 0.0) as total_time,
        NULLIF(self_time, 0.0) as self_time,
        NULLIF(m_time, 0.0) as m_time,
        NULLIF(m_stime, 0.0) as m_stime
    FROM top_functions(sserver_id, start_id, end_id, false)
    WHERE total_time > 0
    ORDER BY
      total_time DESC,
      datid ASC,
      funcid ASC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th rowspan="2">Database</th>'
            '<th rowspan="2">Schema</th>'
            '<th rowspan="2">Function</th>'
            '<th rowspan="2" title="Number of times this function has been called">Executions</th>'
            '<th colspan="4" title="Function execution timing statistics">Time (s)</th>'
          '</tr>'
          '<tr>'
            '<th title="Total time spent in this function and all other functions called by it">Total</th>'
            '<th title="Total time spent in this function itself, not including other functions called by it">Self</th>'
            '<th title="Mean total time per execution">Mean</th>'
            '<th title="Mean self time per execution">Mean self</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td title="%s">%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);
    FOR r_result IN c_fun_stats LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.schemaname,
            r_result.funcargs,
            r_result.funcname,
            r_result.calls,
            round(CAST(r_result.total_time AS numeric),2),
            round(CAST(r_result.self_time AS numeric),2),
            round(CAST(r_result.m_time AS numeric),3),
            round(CAST(r_result.m_stime AS numeric),3)
        );
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION func_top_calls_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    c_fun_stats CURSOR FOR
    SELECT
        dbname,
        schemaname,
        funcname,
        funcargs,
        NULLIF(calls, 0) as calls,
        NULLIF(total_time, 0.0) as total_time,
        NULLIF(self_time, 0.0) as self_time,
        NULLIF(m_time, 0.0) as m_time,
        NULLIF(m_stime, 0.0) as m_stime
    FROM top_functions(sserver_id, start_id, end_id, false)
    WHERE calls > 0
    ORDER BY
      calls DESC,
      datid ASC,
      funcid ASC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th rowspan="2">Database</th>'
            '<th rowspan="2">Schema</th>'
            '<th rowspan="2">Function</th>'
            '<th rowspan="2" title="Number of times this function has been called">Executions</th>'
            '<th colspan="4" title="Function execution timing statistics">Time (s)</th>'
          '</tr>'
          '<tr>'
            '<th title="Total time spent in this function and all other functions called by it">Total</th>'
            '<th title="Total time spent in this function itself, not including other functions called by it">Self</th>'
            '<th title="Mean total time per execution">Mean</th>'
            '<th title="Mean self time per execution">Mean self</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td title="%s">%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);
    FOR r_result IN c_fun_stats LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.schemaname,
            r_result.funcargs,
            r_result.funcname,
            r_result.calls,
            round(CAST(r_result.total_time AS numeric),2),
            round(CAST(r_result.self_time AS numeric),2),
            round(CAST(r_result.m_time AS numeric),3),
            round(CAST(r_result.m_stime AS numeric),3)
        );
    END LOOP;

   IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
   ELSE
        RETURN '';
   END IF;
END;
$$ LANGUAGE plpgsql;

/* ==== Trigger report functions ==== */

CREATE FUNCTION func_top_trg_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    c_fun_stats CURSOR FOR
    SELECT
        dbname,
        schemaname,
        funcname,
        funcargs,
        NULLIF(calls, 0) as calls,
        NULLIF(total_time, 0.0) as total_time,
        NULLIF(self_time, 0.0) as self_time,
        NULLIF(m_time, 0.0) as m_time,
        NULLIF(m_stime, 0.0) as m_stime
    FROM top_functions(sserver_id, start_id, end_id, true)
    WHERE total_time > 0
    ORDER BY
      total_time DESC,
      datid ASC,
      funcid ASC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th rowspan="2">Database</th>'
            '<th rowspan="2">Schema</th>'
            '<th rowspan="2">Function</th>'
            '<th rowspan="2" title="Number of times this function has been called">Executions</th>'
            '<th colspan="4" title="Function execution timing statistics">Time (s)</th>'
          '</tr>'
          '<tr>'
            '<th title="Total time spent in this function and all other functions called by it">Total</th>'
            '<th title="Total time spent in this function itself, not including other functions called by it">Self</th>'
            '<th title="Mean total time per execution">Mean</th>'
            '<th title="Mean self time per execution">Mean self</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td title="%s">%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);
    FOR r_result IN c_fun_stats LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.schemaname,
            r_result.funcargs,
            r_result.funcname,
            r_result.calls,
            round(CAST(r_result.total_time AS numeric),2),
            round(CAST(r_result.self_time AS numeric),2),
            round(CAST(r_result.m_time AS numeric),3),
            round(CAST(r_result.m_stime AS numeric),3)
        );
    END LOOP;

   IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
   ELSE
        RETURN '';
   END IF;
END;
$$ LANGUAGE plpgsql;
