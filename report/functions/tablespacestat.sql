/* ===== Tables stats functions ===== */

CREATE FUNCTION tablespace_stats(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    server_id integer,
    tablespaceid oid,
    tablespacename name,
    tablespacepath text,
    size_delta bigint
) SET search_path=@extschema@ AS $$
    SELECT
        st.server_id,
        st.tablespaceid,
        st.tablespacename,
        st.tablespacepath,
        sum(st.size_delta)::bigint AS size_delta
    FROM v_sample_stat_tablespaces st
    WHERE st.server_id = sserver_id
      AND st.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY st.server_id, st.tablespaceid, st.tablespacename, st.tablespacepath
$$ LANGUAGE sql;

CREATE FUNCTION tablespaces_stats_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for stats
    c_tbl_stats CURSOR FOR
    SELECT
        st.tablespacename,
        st.tablespacepath,
        pg_size_pretty(NULLIF(st_last.size, 0)) as size,
        pg_size_pretty(NULLIF(st.size_delta, 0)) as size_delta
    FROM tablespace_stats(sserver_id,start_id,end_id) st
      LEFT OUTER JOIN v_sample_stat_tablespaces st_last ON
        (st_last.server_id = st.server_id AND st_last.sample_id = end_id AND st_last.tablespaceid = st.tablespaceid)
    ORDER BY st.tablespacename ASC;

    r_result RECORD;
BEGIN
       --- Populate templates

    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th>Tablespace</th>'
            '<th>Path</th>'
            '<th title="Tablespace size as it was at the moment of last sample in report interval">Size</th>'
            '<th title="Tablespace size increment during report interval">Growth</th>'
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

    -- Reporting table stats
    FOR r_result IN c_tbl_stats LOOP
          report := report||format(
              jtab_tpl #>> ARRAY['ts_tpl'],
              r_result.tablespacename,
              r_result.tablespacepath,
              r_result.size,
              r_result.size_delta
          );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;


    RETURN  report;
END;
$$ LANGUAGE plpgsql;