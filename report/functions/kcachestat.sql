/* ===== Statements stats functions ===== */

CREATE FUNCTION top_kcache_statements(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    server_id                integer,
    datid                    oid,
    dbname                   name,
    userid                   oid,
    queryid                  bigint,
    queryid_md5              char(32),
    query                    text,
    exec_user_time           double precision, --  User CPU time used
    user_time_pct            float, --  User CPU time used percentage
    exec_system_time         double precision, --  System CPU time used
    system_time_pct          float, --  System CPU time used percentage
    exec_minflts              bigint, -- Number of page reclaims (soft page faults)
    exec_majflts              bigint, -- Number of page faults (hard page faults)
    exec_nswaps              bigint, -- Number of swaps
    exec_reads               bigint, -- Number of bytes read by the filesystem layer
    exec_writes              bigint, -- Number of bytes written by the filesystem layer
    exec_msgsnds             bigint, -- Number of IPC messages sent
    exec_msgrcvs             bigint, -- Number of IPC messages received
    exec_nsignals            bigint, -- Number of signals received
    exec_nvcsws              bigint, -- Number of voluntary context switches
    exec_nivcsws             bigint,
    reads_total_pct          float,
    writes_total_pct         float,
    plan_user_time           double precision, --  User CPU time used
    plan_system_time         double precision, --  System CPU time used
    plan_minflts              bigint, -- Number of page reclaims (soft page faults)
    plan_majflts              bigint, -- Number of page faults (hard page faults)
    plan_nswaps              bigint, -- Number of swaps
    plan_reads               bigint, -- Number of bytes read by the filesystem layer
    plan_writes              bigint, -- Number of bytes written by the filesystem layer
    plan_msgsnds             bigint, -- Number of IPC messages sent
    plan_msgrcvs             bigint, -- Number of IPC messages received
    plan_nsignals            bigint, -- Number of signals received
    plan_nvcsws              bigint, -- Number of voluntary context switches
    plan_nivcsws             bigint
) SET search_path=@extschema@ AS $$
  WITH tot AS (
        SELECT
            COALESCE(sum(exec_user_time), 0.0) + COALESCE(sum(plan_user_time), 0.0) AS user_time,
            COALESCE(sum(exec_system_time), 0.0) + COALESCE(sum(plan_system_time), 0.0)  AS system_time,
            COALESCE(sum(exec_reads), 0) + COALESCE(sum(plan_reads), 0) AS reads,
            COALESCE(sum(exec_writes), 0) + COALESCE(sum(plan_writes), 0) AS writes
        FROM sample_kcache_total
        WHERE server_id = sserver_id AND sample_id BETWEEN start_id + 1 AND end_id)
    SELECT
        kc.server_id as server_id,
        kc.datid as datid,
        sample_db.datname as dbname,
        kc.userid as userid,
        kc.queryid as queryid,
        kc.queryid_md5 as queryid_md5,
        (select sl.query from stmt_list sl where sl.queryid_md5 = kc.queryid_md5)::varchar(100) as query,
        sum(kc.exec_user_time) as exec_user_time,
        ((COALESCE(sum(kc.exec_user_time), 0.0) + COALESCE(sum(kc.plan_user_time), 0.0))
          *100/NULLIF(min(tot.user_time),0.0))::float AS user_time_pct,
        sum(kc.exec_system_time) as exec_system_time,
        ((COALESCE(sum(kc.exec_system_time), 0.0) + COALESCE(sum(kc.plan_system_time), 0.0))
          *100/NULLIF(min(tot.system_time), 0.0))::float AS system_time_pct,
        sum(kc.exec_minflts)::bigint as exec_minflts,
        sum(kc.exec_majflts)::bigint as exec_majflts,
        sum(kc.exec_nswaps)::bigint as exec_nswaps,
        sum(kc.exec_reads)::bigint as exec_reads,
        sum(kc.exec_writes)::bigint as exec_writes,
        sum(kc.exec_msgsnds)::bigint as exec_msgsnds,
        sum(kc.exec_msgrcvs)::bigint as exec_msgrcvs,
        sum(kc.exec_nsignals)::bigint as exec_nsignals,
        sum(kc.exec_nvcsws)::bigint as exec_nvcsws,
        sum(kc.exec_nivcsws)::bigint as exec_nivcsws,
        ((COALESCE(sum(kc.exec_reads), 0) + COALESCE(sum(kc.plan_reads), 0))
          *100/NULLIF(min(tot.reads),0))::float AS reads_total_pct,
        ((COALESCE(sum(kc.exec_writes), 0) + COALESCE(sum(kc.plan_writes), 0))
          *100/NULLIF(min(tot.writes),0))::float AS writes_total_pct,
        sum(kc.plan_user_time) as plan_user_time,
        sum(kc.plan_system_time) as plan_system_time,
        sum(kc.plan_minflts)::bigint as plan_minflts,
        sum(kc.plan_majflts)::bigint as plan_majflts,
        sum(kc.plan_nswaps)::bigint as plan_nswaps,
        sum(kc.plan_reads)::bigint as plan_reads,
        sum(kc.plan_writes)::bigint as plan_writes,
        sum(kc.plan_msgsnds)::bigint as plan_msgsnds,
        sum(kc.plan_msgrcvs)::bigint as plan_msgrcvs,
        sum(kc.plan_nsignals)::bigint as plan_nsignals,
        sum(kc.plan_nvcsws)::bigint as plan_nvcsws,
        sum(kc.plan_nivcsws)::bigint as plan_nivcsws
   FROM sample_kcache kc
        -- Database name
        JOIN sample_stat_database sample_db
        ON (kc.server_id=sample_db.server_id AND kc.sample_id=sample_db.sample_id AND kc.datid=sample_db.datid)
        -- Total stats
        CROSS JOIN tot
    WHERE kc.server_id = sserver_id AND kc.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY kc.server_id,kc.datid,sample_db.datname,kc.userid,kc.queryid,kc.queryid_md5
$$ LANGUAGE sql;


CREATE FUNCTION top_cpu_time_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    tab_row     text := '';
    jtab_tpl    jsonb;

    --Cursor for top(cnt) queries ordered by elapsed time
    c_elapsed_time CURSOR FOR
    SELECT
        kc.datid as datid,
        kc.userid as userid,
        kc.queryid as queryid,
        kc.queryid_md5 as queryid_md5,
        kc.query as query,
        kc.dbname,
        NULLIF(kc.plan_user_time, 0.0) as plan_user_time,
        NULLIF(kc.exec_user_time, 0.0) as exec_user_time,
        NULLIF(kc.user_time_pct, 0.0) as user_time_pct,
        NULLIF(kc.plan_system_time, 0.0) as plan_system_time,
        NULLIF(kc.exec_system_time, 0.0) as exec_system_time,
        NULLIF(kc.system_time_pct, 0.0) as system_time_pct
    FROM top_kcache_statements(sserver_id, start_id, end_id) kc
    WHERE COALESCE(kc.plan_user_time, 0.0) + COALESCE(kc.plan_system_time, 0.0) +
      COALESCE(kc.exec_user_time, 0.0) + COALESCE(kc.exec_system_time, 0.0) > 0
    ORDER BY COALESCE(kc.plan_user_time, 0.0) + COALESCE(kc.plan_system_time, 0.0) +
      COALESCE(kc.exec_user_time, 0.0) + COALESCE(kc.exec_system_time, 0.0) DESC,
      kc.datid,
      kc.queryid,
      kc.queryid_md5
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
          '<th title="Userspace CPU" colspan="{cputime_colspan}">User Time</th>'
          '<th title="Kernelspace CPU" colspan="{cputime_colspan}">System Time</th>'
        '</tr>'
        '<tr>'
          '{user_plan_time_hdr}'
          '<th title="User CPU time elapsed during execution">Exec (s)</th>'
          '<th title="User CPU time elapsed by this statement as a percentage of total user CPU time">%Total</th>'
          '{system_plan_time_hdr}'
          '<th title="System CPU time elapsed during execution">Exec (s)</th>'
          '<th title="System CPU time elapsed by this statement as a percentage of total system CPU time">%Total</th>'
        '</tr>'
        '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr>'
          '<td {mono}><p><a HREF="#%1$s">%2$s</a></p>'
          '<p><small>[%3$s]</small></p></td>'
          '<td><p align="left">%4$s</p></td>'  -- SQL Text
          '<td>%5$s</td>'
          '{user_plan_time_tpl}'
          '<td {value}>%7$s</td>'
          '<td {value}>%8$s</td>'
          '{system_plan_time_tpl}'
          '<td {value}>%10$s</td>'
          '<td {value}>%11$s</td>'
        '</tr>',
      'user_plan_time_hdr',
        '<th title="User CPU time elapsed during planning">Plan (s)</th>',
      'system_plan_time_hdr',
        '<th title="System CPU time elapsed during planning">Plan (s)</th>',
      'user_plan_time_tpl',
        '<td {value}>%6$s</td>',
      'system_plan_time_tpl',
        '<td {value}>%9$s</td>'
    );
    -- Conditional template
    IF jsonb_extract_path_text(jreportset, 'report_features', 'rusage.planstats')::boolean THEN
      jtab_tpl := jsonb_set(jtab_tpl,'{tab_hdr}',to_jsonb(replace(jtab_tpl->>'tab_hdr','{cputime_colspan}','3')));
      jtab_tpl := jsonb_set(jtab_tpl,'{tab_hdr}',to_jsonb(replace(jtab_tpl->>'tab_hdr',
        '{user_plan_time_hdr}',jtab_tpl->>'user_plan_time_hdr')));
      jtab_tpl := jsonb_set(jtab_tpl,'{tab_hdr}',to_jsonb(replace(jtab_tpl->>'tab_hdr',
        '{system_plan_time_hdr}',jtab_tpl->>'system_plan_time_hdr')));
      jtab_tpl := jsonb_set(jtab_tpl,'{stmt_tpl}',to_jsonb(replace(jtab_tpl->>'stmt_tpl',
        '{user_plan_time_tpl}',jtab_tpl->>'user_plan_time_tpl')));
      jtab_tpl := jsonb_set(jtab_tpl,'{stmt_tpl}',to_jsonb(replace(jtab_tpl->>'stmt_tpl',
        '{system_plan_time_tpl}',jtab_tpl->>'system_plan_time_tpl')));
    ELSE
      jtab_tpl := jsonb_set(jtab_tpl,'{tab_hdr}',to_jsonb(replace(jtab_tpl->>'tab_hdr','{cputime_colspan}','2')));
      jtab_tpl := jsonb_set(jtab_tpl,'{tab_hdr}',to_jsonb(replace(jtab_tpl->>'tab_hdr',
        '{user_plan_time_hdr}','')));
      jtab_tpl := jsonb_set(jtab_tpl,'{tab_hdr}',to_jsonb(replace(jtab_tpl->>'tab_hdr',
        '{system_plan_time_hdr}','')));
      jtab_tpl := jsonb_set(jtab_tpl,'{stmt_tpl}',to_jsonb(replace(jtab_tpl->>'stmt_tpl',
        '{user_plan_time_tpl}','')));
      jtab_tpl := jsonb_set(jtab_tpl,'{stmt_tpl}',to_jsonb(replace(jtab_tpl->>'stmt_tpl',
        '{system_plan_time_tpl}','')));
    END IF;
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);
    -- Reporting on top queries by elapsed time
    FOR r_result IN c_elapsed_time LOOP
        tab_row := format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid_md5,
            left(md5(r_result.userid::text || r_result.datid::text || r_result.queryid::text), 10),
            to_hex(r_result.queryid),
            r_result.query,
            r_result.dbname,
            round(CAST(r_result.plan_user_time AS numeric),2),
            round(CAST(r_result.exec_user_time AS numeric),2),
            round(CAST(r_result.user_time_pct AS numeric),2),
            round(CAST(r_result.plan_system_time AS numeric),2),
            round(CAST(r_result.exec_system_time AS numeric),2),
            round(CAST(r_result.system_time_pct AS numeric),2)
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

CREATE FUNCTION top_io_filesystem_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    tab_row     text := '';
    jtab_tpl    jsonb;

    --Cursor for top(cnt) queries ordered by elapsed time
    c_elapsed_time CURSOR FOR
    SELECT
        kc.datid as datid,
        kc.userid as userid,
        kc.queryid as queryid,
        kc.queryid_md5 as queryid_md5,
        kc.query as query,
        kc.dbname,
        NULLIF(kc.plan_reads, 0) as plan_reads,
        NULLIF(kc.exec_reads, 0) as exec_reads,
        NULLIF(kc.reads_total_pct, 0.0) as reads_total_pct,
        NULLIF(kc.plan_writes, 0)  as plan_writes,
        NULLIF(kc.exec_writes, 0)  as exec_writes,
        NULLIF(kc.writes_total_pct, 0.0) as writes_total_pct
    FROM top_kcache_statements(sserver_id, start_id, end_id) kc
    WHERE COALESCE(kc.plan_reads, 0) + COALESCE(kc.plan_writes, 0) +
      COALESCE(kc.exec_reads, 0) + COALESCE(kc.exec_writes, 0) > 0
    ORDER BY COALESCE(kc.plan_reads, 0) + COALESCE(kc.plan_writes, 0) +
      COALESCE(kc.exec_reads, 0) + COALESCE(kc.exec_writes, 0) DESC,
      kc.datid,
      kc.queryid,
      kc.queryid_md5
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
            '<th title="Filesystem reads" colspan="{fs_colspan}">Read Bytes</th>'
            '<th title="Filesystem writes" colspan="{fs_colspan}">Write Bytes</th>'
          '</tr>'
          '<tr>'
            '{plan_reads_hdr}'
            '<th title="Filesystem read amount during execution">Exec</th>'
            '<th title="Filesystem read amount of this statement as a percentage of all statements FS read amount">%Total</th>'
            '{plan_writes_hdr}'
            '<th title="Filesystem write amount during execution">Exec</th>'
            '<th title="Filesystem write amount of this statement as a percentage of all statements FS write amount">%Total</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr>'
          '<td {mono}><p><a HREF="#%1$s">%2$s</a></p>'
          '<p><small>[%3$s]</small></p></td>'
          '<td><p align="left">%4$s</p></td>'  -- SQL Text
          '<td>%5$s</td>'
          '{plan_reads_tpl}'
          '<td {value}>%7$s</td>'
          '<td {value}>%8$s</td>'
          '{plan_writes_tpl}'
          '<td {value}>%10$s</td>'
          '<td {value}>%11$s</td>'
        '</tr>',
      'plan_reads_hdr',
        '<th title="Filesystem read amount during planning">Plan</th>',
      'plan_writes_hdr',
        '<th title="Filesystem write amount during planning">Plan</th>',
      'plan_reads_tpl',
        '<td {value}>%6$s</td>',
      'plan_writes_tpl',
        '<td {value}>%9$s</td>'
    );

    -- Conditional template
    IF jsonb_extract_path_text(jreportset, 'report_features', 'rusage.planstats')::boolean THEN
      jtab_tpl := jsonb_set(jtab_tpl,'{tab_hdr}',to_jsonb(replace(jtab_tpl->>'tab_hdr','{fs_colspan}','3')));
      jtab_tpl := jsonb_set(jtab_tpl,'{tab_hdr}',to_jsonb(replace(jtab_tpl->>'tab_hdr',
        '{plan_reads_hdr}',jtab_tpl->>'plan_reads_hdr')));
      jtab_tpl := jsonb_set(jtab_tpl,'{tab_hdr}',to_jsonb(replace(jtab_tpl->>'tab_hdr',
        '{plan_writes_hdr}',jtab_tpl->>'plan_writes_hdr')));
      jtab_tpl := jsonb_set(jtab_tpl,'{stmt_tpl}',to_jsonb(replace(jtab_tpl->>'stmt_tpl',
        '{plan_reads_tpl}',jtab_tpl->>'plan_reads_tpl')));
      jtab_tpl := jsonb_set(jtab_tpl,'{stmt_tpl}',to_jsonb(replace(jtab_tpl->>'stmt_tpl',
        '{plan_writes_tpl}',jtab_tpl->>'plan_writes_tpl')));
    ELSE
      jtab_tpl := jsonb_set(jtab_tpl,'{tab_hdr}',to_jsonb(replace(jtab_tpl->>'tab_hdr','{fs_colspan}','2')));
      jtab_tpl := jsonb_set(jtab_tpl,'{tab_hdr}',to_jsonb(replace(jtab_tpl->>'tab_hdr',
        '{plan_reads_hdr}','')));
      jtab_tpl := jsonb_set(jtab_tpl,'{tab_hdr}',to_jsonb(replace(jtab_tpl->>'tab_hdr',
        '{plan_writes_hdr}','')));
      jtab_tpl := jsonb_set(jtab_tpl,'{stmt_tpl}',to_jsonb(replace(jtab_tpl->>'stmt_tpl',
        '{plan_reads_tpl}','')));
      jtab_tpl := jsonb_set(jtab_tpl,'{stmt_tpl}',to_jsonb(replace(jtab_tpl->>'stmt_tpl',
        '{plan_writes_tpl}','')));
    END IF;
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);
    -- Reporting on top queries by elapsed time
    FOR r_result IN c_elapsed_time LOOP
        tab_row := format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid_md5,
            left(md5(r_result.userid::text || r_result.datid::text || r_result.queryid::text), 10),
            to_hex(r_result.queryid),
            r_result.query,
            r_result.dbname,
            pg_size_pretty(r_result.plan_reads),
            pg_size_pretty(r_result.exec_reads),
            round(CAST(r_result.reads_total_pct AS numeric),2),
            pg_size_pretty(r_result.plan_writes),
            pg_size_pretty(r_result.exec_writes),
            round(CAST(r_result.writes_total_pct AS numeric),2)
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
