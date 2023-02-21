/*===== Settings reporting functions =====*/
CREATE FUNCTION settings_and_changes(IN sserver_id integer, IN start_id integer, IN end_id integer)
  RETURNS TABLE(
    first_seen          timestamp(0) with time zone,
    setting_scope       smallint,
    name                text,
    setting             text,
    reset_val           text,
    boot_val            text,
    unit                text,
    sourcefile          text,
    sourceline          integer,
    pending_restart     boolean,
    changed             boolean,
    default_val         boolean
  )
SET search_path=@extschema@ AS $$
  SELECT
    first_seen,
    setting_scope,
    name,
    setting,
    reset_val,
    boot_val,
    unit,
    sourcefile,
    sourceline,
    pending_restart,
    false,
    COALESCE(boot_val = reset_val, false)
  FROM v_sample_settings
  WHERE server_id = sserver_id AND sample_id = start_id
  UNION ALL
  SELECT
    first_seen,
    setting_scope,
    name,
    setting,
    reset_val,
    boot_val,
    unit,
    sourcefile,
    sourceline,
    pending_restart,
    true,
    COALESCE(boot_val = reset_val, false)
  FROM sample_settings s
    JOIN samples s_start ON (s_start.server_id = s.server_id AND s_start.sample_id = start_id)
    JOIN samples s_end ON (s_end.server_id = s.server_id AND s_end.sample_id = end_id)
  WHERE s.server_id = sserver_id AND s.first_seen > s_start.sample_time AND s.first_seen <= s_end.sample_time
$$ LANGUAGE SQL;

CREATE FUNCTION settings_and_changes_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer)
  RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report_defined text := '';
    report_default text := '';
    defined_tpl    text := '';
    default_tpl    text := '';

    jtab_tpl       jsonb;
    notes          text[];

    --Cursor for top(cnt) queries ordered by elapsed time
    c_settings CURSOR FOR
    SELECT
      first_seen,
      setting_scope,
      name,
      setting,
      reset_val,
      CASE WHEN unit IS NULL THEN '--' ELSE unit END,
      sourcefile,
      sourceline,
      pending_restart,
      changed,
      default_val
    FROM settings_and_changes(sserver_id, start_id, end_id) st
    ORDER BY default_val AND NOT changed ASC, name,setting_scope,first_seen,pending_restart ASC NULLS FIRST;

    r_result RECORD;
BEGIN

    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '{defined_tpl}'
          '{default_tpl}'
        '</table>',
      'defined_tpl',
        '<tr><th colspan="5">Defined settings</th></tr>'
        '<tr>'
          '<th>Setting</th>'
          '<th>reset_val</th>'
          '<th>Unit</th>'
          '<th>Source</th>'
          '<th>Notes</th>'
        '</tr>'
        '{rows_defined}',
      'default_tpl',
        '<tr><th colspan="5">Default settings</th></tr>'
        '<tr>'
           '<th>Setting</th>'
           '<th>reset_val</th>'
           '<th>Unit</th>'
           '<th>Source</th>'
           '<th>Notes</th>'
         '</tr>'
         '{rows_default}',
      'init_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}><p align="middle">%s</p></td>'
          '<td {value}>%s</td>'
          '<td {value}><p align="middle">%s</p></td>'
        '</tr>',
      'new_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td {value}><strong>%s</strong></td>'
          '<td {value}><p align="middle">%s</p></td>'
          '<td {value}>%s</td>'
          '<td {value}><p align="middle"><strong>%s</strong></p></td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    FOR r_result IN c_settings LOOP
        notes := ARRAY['--'];
        IF r_result.changed THEN
          notes := array_append(notes,r_result.first_seen::text);
        END IF;
        IF r_result.pending_restart THEN
          notes := array_append(notes,'Pending restart');
        END IF;
        notes := array_remove(notes,'');
        IF r_result.default_val AND NOT r_result.changed THEN
            report_default := report_default||format(
              jtab_tpl #>> ARRAY['init_tpl'],
              r_result.name,
              r_result.reset_val,
              r_result.unit,
              r_result.sourcefile || ':' || r_result.sourceline::text,
              array_to_string(notes,', ')
          );
        ELSIF NOT r_result.changed THEN
            report_defined := report_defined ||format(
              jtab_tpl #>> ARRAY['init_tpl'],
              r_result.name,
              r_result.reset_val,
              r_result.unit,
              r_result.sourcefile || ':' || r_result.sourceline::text,
              array_to_string(notes,',  ')
          );
        ELSE
            report_defined := report_defined ||format(
              jtab_tpl #>> ARRAY['new_tpl'],
              r_result.name,
              r_result.reset_val,
              r_result.unit,
              r_result.sourcefile || ':' || r_result.sourceline::text,
              array_to_string(notes,',  ')
          );
        END IF;
    END LOOP;

    IF report_default = '' and report_defined = '' THEN
        RETURN '!!!';
    ELSE
        -- apply settings to templates
        defined_tpl := replace(jtab_tpl #>> ARRAY['defined_tpl'],'{rows_defined}', report_defined);
        defined_tpl := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{defined_tpl}', defined_tpl);

        IF report_default != '' THEN
          default_tpl := replace(jtab_tpl #>> ARRAY['default_tpl'],'{rows_default}', report_default);
          RETURN replace(defined_tpl,'{default_tpl}',default_tpl);
        END IF;
        RETURN defined_tpl;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION kernel_settings_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer)
  RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    jtab_tpl    jsonb;

    --Cursor for stats
    c_tbl_stats CURSOR FOR
    SELECT
        name,
        value,
        sourcefile
    FROM sample_kernel_settings
    WHERE server_id = sserver_id;

    r_result    RECORD;
BEGIN
    --- Populate templates
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th>Setting</th>'
            '<th>Value</th>'
            '<th>Source</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'ts_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
        '</tr>'
    );

    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    -- Reporting wait event type stats
    FOR r_result IN c_tbl_stats LOOP
              report := report || format(
                  jtab_tpl #>> ARRAY['ts_tpl'],
                  r_result.name,
                  r_result.value,
                  r_result.sourcefile
              );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'], '{rows}', report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;
