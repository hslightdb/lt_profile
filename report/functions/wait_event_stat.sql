/* ===== Wait event stats functions ===== */

CREATE FUNCTION total_wait_time(IN sserver_id integer, IN start_id integer, IN end_id integer, IN mode text)
RETURNS SETOF double precision SET search_path=@extschema@ AS $$
BEGIN
    IF mode = 'user' THEN
        RETURN QUERY
            SELECT COALESCE(sum(t.wait_time), 0)
            FROM sample_wait_event_total t
            WHERE t.server_id = sserver_id
            AND t.sample_id BETWEEN start_id + 1 AND end_id
            AND t.state = 'active'
            AND t.wait_event != 'Null'
            AND (t.wait_event_type != 'Activity' AND t.wait_event_type != 'Extension');
    END IF;

    IF mode = 'background' THEN
        RETURN QUERY
            SELECT sum(t.wait_time)
            FROM sample_wait_event_total t
            WHERE t.server_id = sserver_id
            AND t.sample_id BETWEEN start_id + 1 AND end_id
            AND t.wait_event != 'Null'
            AND (t.wait_event_type = 'Activity' OR t.wait_event_type = 'Extension');
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_event(IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer, IN mode text)
RETURNS TABLE(
    wait_event        text,
    wait_event_type   text,
    wait_time         double precision,
    time_percent      text
) SET search_path=@extschema@ AS $$
DECLARE
    total_wait_time double precision;
BEGIN
    total_wait_time := total_wait_time(sserver_id, start_id, end_id, mode);

    IF mode = 'user' THEN
    RETURN QUERY
        SELECT
            t.wait_event,
            t.wait_event_type,
            sum(t.wait_time) as wait_time,
            to_char((sum(t.wait_time) / total_wait_time) * 100, '999.9') AS time_percent
        FROM sample_wait_event_total t
        WHERE t.server_id = sserver_id
        AND t.sample_id BETWEEN start_id + 1 AND end_id
        AND t.state = 'active'
        AND t.wait_event != 'Null'
        AND (t.wait_event_type != 'Activity' AND t.wait_event_type != 'Extension')
        GROUP BY t.wait_event, t.wait_event_type
        ORDER BY wait_time DESC
        LIMIT topn;
    END IF;

    IF mode = 'background' THEN
    RETURN QUERY
        SELECT
            t.wait_event,
            t.wait_event_type,
            sum(t.wait_time) as wait_time,
            to_char((sum(t.wait_time) / total_wait_time) * 100, '999.9') AS time_percent
        FROM sample_wait_event_total t
        WHERE t.server_id = sserver_id
        AND t.sample_id BETWEEN start_id + 1 AND end_id
        AND t.wait_event != 'Null'
        AND (t.wait_event_type = 'Activity' OR t.wait_event_type = 'Extension')
        GROUP BY t.wait_event, t.wait_event_type
        ORDER BY wait_time DESC
        LIMIT topn;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_event_type(IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer, IN mode text)
RETURNS TABLE(
    wait_event_type   text,
    wait_time         double precision,
    time_percent      text
) SET search_path=@extschema@ AS $$
DECLARE
    total_wait_time double precision;
BEGIN
    total_wait_time := total_wait_time(sserver_id, start_id, end_id, mode);

    IF mode = 'user' THEN
    RETURN QUERY
        SELECT
            t.wait_event_type,
            sum(t.wait_time) as wait_time,
            to_char((sum(t.wait_time) / total_wait_time) * 100, '999.9') AS time_percent
        FROM sample_wait_event_total t
        WHERE t.server_id = sserver_id
        AND t.sample_id BETWEEN start_id + 1 AND end_id
        AND t.state = 'active'
        AND t.wait_event != 'Null'
        AND (t.wait_event_type != 'Activity' AND t.wait_event_type != 'Extension')
        GROUP BY t.wait_event_type
        ORDER BY wait_time DESC
        LIMIT topn;
    END IF;

    IF mode = 'background' THEN
    RETURN QUERY
        SELECT
            t.wait_event_type,
            sum(t.wait_time) as wait_time,
            to_char((sum(t.wait_time) / total_wait_time) * 100, '999.9') AS time_percent
        FROM sample_wait_event_total t
        WHERE t.server_id = sserver_id
        AND t.sample_id BETWEEN start_id + 1 AND end_id
        AND t.wait_event != 'Null'
        AND (t.wait_event_type = 'Activity' OR t.wait_event_type = 'Extension')
        GROUP BY t.wait_event_type
        ORDER BY wait_time DESC
        LIMIT topn;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_event_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer, IN mode text)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    jtab_tpl    jsonb;

    --Cursor for stats
    c_tbl_stats CURSOR FOR
    SELECT
        st.wait_event,
        st.wait_event_type,
        st.wait_time,
        st.time_percent
    FROM top_event(sserver_id, start_id, end_id, topn, mode) st;

    r_result    RECORD;
BEGIN
    --- Populate templates
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th>Wait Event</th>'
            '<th>Wait Event Type</th>'
            '<th>Wait Time (s)</th>'
            '<th>%Total Wait Time</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'ts_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');

    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    -- Reporting wait event type stats
    FOR r_result IN c_tbl_stats LOOP
          report := report || format(
              jtab_tpl #>> ARRAY['ts_tpl'],
              r_result.wait_event,
              r_result.wait_event_type,
              r_result.wait_time,
              r_result.time_percent
          );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'], '{rows}', report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_event_type_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer, IN mode text)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    jtab_tpl    jsonb;

    --Cursor for stats
    c_tbl_stats CURSOR FOR
    SELECT
        st.wait_event_type,
        st.wait_time,
        st.time_percent
    FROM top_event_type(sserver_id, start_id, end_id, topn, mode) st;

    r_result    RECORD;
BEGIN
    --- Populate templates
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th>Wait Event Type</th>'
            '<th>Wait Time (s)</th>'
            '<th>%Total Wait Time</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'ts_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');

    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    -- Reporting wait event type stats
    FOR r_result IN c_tbl_stats LOOP
          report := report || format(
              jtab_tpl #>> ARRAY['ts_tpl'],
              r_result.wait_event_type,
              r_result.wait_time,
              r_result.time_percent
          );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'], '{rows}', report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_user_event_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer)
RETURNS text SET search_path=@extschema@ AS $$
BEGIN
    RETURN top_event_htbl(jreportset, sserver_id, start_id, end_id, topn, 'user');
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_background_event_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer)
RETURNS text SET search_path=@extschema@ AS $$
BEGIN
    RETURN top_event_htbl(jreportset, sserver_id, start_id, end_id, topn, 'background');
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_user_event_type_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer)
RETURNS text SET search_path=@extschema@ AS $$
BEGIN
    RETURN top_event_type_htbl(jreportset, sserver_id, start_id, end_id, topn, 'user');
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_background_event_type_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer)
RETURNS text SET search_path=@extschema@ AS $$
BEGIN
    RETURN top_event_type_htbl(jreportset, sserver_id, start_id, end_id, topn, 'background');
END;
$$ LANGUAGE plpgsql;