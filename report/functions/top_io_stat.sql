/* ===== Top IO objects ===== */

CREATE FUNCTION top_io_tables(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    server_id                     integer,
    datid                       oid,
    relid                       oid,
    dbname                      name,
    tablespacename              name,
    schemaname                  name,
    relname                     name,
    heap_blks_read              bigint,
    heap_blks_read_pct          numeric,
    heap_blks_fetch             bigint,
    heap_blks_proc_pct          numeric,
    idx_blks_read               bigint,
    idx_blks_read_pct           numeric,
    idx_blks_fetch              bigint,
    idx_blks_proc_pct           numeric,
    toast_blks_read             bigint,
    toast_blks_read_pct         numeric,
    toast_blks_fetch            bigint,
    toast_blks_proc_pct         numeric,
    tidx_blks_read              bigint,
    tidx_blks_read_pct          numeric,
    tidx_blks_fetch             bigint,
    tidx_blks_proc_pct          numeric,
    seq_scan                    bigint,
    idx_scan                    bigint
) SET search_path=@extschema@ AS $$
    WITH total AS (SELECT
      COALESCE(sum(heap_blks_read), 0) + COALESCE(sum(idx_blks_read), 0) AS total_blks_read,
      COALESCE(sum(heap_blks_read), 0) + COALESCE(sum(idx_blks_read), 0) +
      COALESCE(sum(heap_blks_hit), 0) + COALESCE(sum(idx_blks_hit), 0) AS total_blks_fetch
    FROM sample_stat_tables_total
    WHERE server_id = sserver_id AND sample_id BETWEEN start_id + 1 AND end_id
    )
    SELECT
        st.server_id,
        st.datid,
        st.relid,
        sample_db.datname AS dbname,
        tablespaces_list.tablespacename,
        st.schemaname,
        st.relname,
        sum(st.heap_blks_read)::bigint AS heap_blks_read,
        sum(st.heap_blks_read) * 100 / NULLIF(min(total.total_blks_read), 0) AS heap_blks_read_pct,
        COALESCE(sum(st.heap_blks_read), 0)::bigint + COALESCE(sum(st.heap_blks_hit), 0)::bigint AS heap_blks_fetch,
        (COALESCE(sum(st.heap_blks_read), 0) + COALESCE(sum(st.heap_blks_hit), 0)) * 100 / NULLIF(min(total.total_blks_fetch), 0) AS heap_blks_proc_pct,
        sum(st.idx_blks_read)::bigint AS idx_blks_read,
        sum(st.idx_blks_read) * 100 / NULLIF(min(total.total_blks_read), 0) AS idx_blks_read_pct,
        COALESCE(sum(st.idx_blks_read), 0)::bigint + COALESCE(sum(st.idx_blks_hit), 0)::bigint AS idx_blks_fetch,
        (COALESCE(sum(st.idx_blks_read), 0) + COALESCE(sum(st.idx_blks_hit), 0)) * 100 / NULLIF(min(total.total_blks_fetch), 0) AS idx_blks_proc_pct,
        sum(st.toast_blks_read)::bigint AS toast_blks_read,
        sum(st.toast_blks_read) * 100 / NULLIF(min(total.total_blks_read), 0) AS toast_blks_read_pct,
        COALESCE(sum(st.toast_blks_read), 0)::bigint + COALESCE(sum(st.toast_blks_hit), 0)::bigint AS toast_blks_fetch,
        (COALESCE(sum(st.toast_blks_read), 0) + COALESCE(sum(st.toast_blks_hit), 0)) * 100 / NULLIF(min(total.total_blks_fetch), 0) AS toast_blks_proc_pct,
        sum(st.tidx_blks_read)::bigint AS tidx_blks_read,
        sum(st.tidx_blks_read) * 100 / NULLIF(min(total.total_blks_read), 0) AS tidx_blks_read_pct,
        COALESCE(sum(st.tidx_blks_read), 0)::bigint + COALESCE(sum(st.tidx_blks_hit), 0)::bigint AS tidx_blks_fetch,
        (COALESCE(sum(st.tidx_blks_read), 0) + COALESCE(sum(st.tidx_blks_hit), 0)) * 100 / NULLIF(min(total.total_blks_fetch), 0) AS tidx_blks_proc_pct,
        sum(st.seq_scan)::bigint AS seq_scan,
        sum(st.idx_scan)::bigint AS idx_scan
    FROM v_sample_stat_tables st
        -- Database name
        JOIN sample_stat_database sample_db
          USING (server_id, sample_id, datid)
        JOIN tablespaces_list USING(server_id,tablespaceid)
        CROSS JOIN total
    WHERE st.server_id = sserver_id
      AND st.relkind IN ('r','m')
      AND NOT sample_db.datistemplate
      AND st.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY st.server_id,st.datid,st.relid,sample_db.datname,tablespaces_list.tablespacename, st.schemaname,st.relname
    HAVING min(sample_db.stats_reset) = max(sample_db.stats_reset)
$$ LANGUAGE sql;

CREATE FUNCTION top_io_indexes(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    server_id             integer,
    datid               oid,
    relid               oid,
    dbname              name,
    tablespacename      name,
    schemaname          name,
    relname             name,
    indexrelid          oid,
    indexrelname        name,
    idx_scan            bigint,
    idx_blks_read       bigint,
    idx_blks_read_pct   numeric,
    idx_blks_hit_pct    numeric,
    idx_blks_fetch  bigint,
    idx_blks_proc_pct   numeric
) SET search_path=@extschema@ AS $$
    WITH total AS (SELECT
      sum(heap_blks_read + idx_blks_read) AS total_blks_read,
      sum(heap_blks_read + idx_blks_read + heap_blks_hit + idx_blks_hit) AS total_blks_fetch
    FROM sample_stat_tables_total
    WHERE server_id = sserver_id AND sample_id BETWEEN start_id + 1 AND end_id
    )
    SELECT
        st.server_id,
        st.datid,
        st.relid,
        sample_db.datname AS dbname,
        tablespaces_list.tablespacename,
        COALESCE(mtbl.schemaname,st.schemaname)::name AS schemaname,
        COALESCE(mtbl.relname||'(TOAST)',st.relname)::name AS relname,
        st.indexrelid,
        st.indexrelname,
        sum(st.idx_scan)::bigint AS idx_scan,
        sum(st.idx_blks_read)::bigint AS idx_blks_read,
        sum(st.idx_blks_read) * 100 / NULLIF(min(total.total_blks_read), 0) AS idx_blks_read_pct,
        sum(st.idx_blks_hit) * 100 / NULLIF(COALESCE(sum(st.idx_blks_hit), 0) + COALESCE(sum(st.idx_blks_read), 0), 0) AS idx_blks_hit_pct,
        COALESCE(sum(st.idx_blks_read), 0)::bigint + COALESCE(sum(st.idx_blks_hit), 0)::bigint AS idx_blks_fetch,
        (COALESCE(sum(st.idx_blks_read), 0) + COALESCE(sum(st.idx_blks_hit), 0)) * 100 / NULLIF(min(total_blks_fetch), 0) AS idx_blks_proc_pct
    FROM v_sample_stat_indexes st
        -- Database name
        JOIN sample_stat_database sample_db
        ON (st.server_id=sample_db.server_id AND st.sample_id=sample_db.sample_id AND st.datid=sample_db.datid)
        JOIN tablespaces_list ON  (st.server_id=tablespaces_list.server_id AND st.tablespaceid=tablespaces_list.tablespaceid)
        -- join main table for indexes on toast
        LEFT OUTER JOIN tables_list mtbl ON (st.server_id = mtbl.server_id AND st.datid = mtbl.datid AND st.relid = mtbl.reltoastrelid)
        CROSS JOIN total
    WHERE st.server_id = sserver_id AND NOT sample_db.datistemplate AND st.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY st.server_id,st.datid,st.relid,sample_db.datname,
      COALESCE(mtbl.schemaname,st.schemaname), COALESCE(mtbl.relname||'(TOAST)',st.relname),
      st.schemaname,st.relname,tablespaces_list.tablespacename, st.indexrelid,st.indexrelname
    HAVING min(sample_db.stats_reset) = max(sample_db.stats_reset)
$$ LANGUAGE sql;

CREATE FUNCTION tbl_top_io_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    c_tbl_stats CURSOR FOR
    SELECT
        dbname,
        tablespacename,
        schemaname,
        relname,
        NULLIF(heap_blks_read, 0) as heap_blks_read,
        NULLIF(heap_blks_read_pct, 0.0) as heap_blks_read_pct,
        NULLIF(idx_blks_read, 0) as idx_blks_read,
        NULLIF(idx_blks_read_pct, 0.0) as idx_blks_read_pct,
        NULLIF(toast_blks_read, 0) as toast_blks_read,
        NULLIF(toast_blks_read_pct, 0.0) as toast_blks_read_pct,
        NULLIF(tidx_blks_read, 0) as tidx_blks_read,
        NULLIF(tidx_blks_read_pct, 0.0) as tidx_blks_read_pct
    FROM top_io_tables(sserver_id,start_id,end_id)
    WHERE COALESCE(heap_blks_read, 0) + COALESCE(idx_blks_read, 0) + COALESCE(toast_blks_read, 0) + COALESCE(tidx_blks_read, 0) > 0
    ORDER BY
      COALESCE(heap_blks_read, 0) + COALESCE(idx_blks_read, 0) + COALESCE(toast_blks_read, 0) + COALESCE(tidx_blks_read, 0) DESC,
      datid ASC,
      relid ASC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th rowspan="2">Database</th>'
            '<th rowspan="2">Tablespace</th>'
            '<th rowspan="2">Schema</th>'
            '<th rowspan="2">Table</th>'
            '<th colspan="2">Heap</th>'
            '<th colspan="2">Index</th>'
            '<th colspan="2">TOAST</th>'
            '<th colspan="2">TOAST-Index</th>'
          '</tr>'
          '<tr>'
            '<th title="Number of disk blocks read from this table">Blocks</th>'
            '<th title="Heap block reads for this table as a percentage of all blocks read in a cluster">%Total</th>'
            '<th title="Number of disk blocks read from all indexes on this table">Blocks</th>'
            '<th title="Indexes block reads for this table as a percentage of all blocks read in a cluster">%Total</th>'
            '<th title="Number of disk blocks read from this table''s TOAST table (if any)">Blocks</th>'
            '<th title="TOAST block reads for this table as a percentage of all blocks read in a cluster">%Total</th>'
            '<th title="Number of disk blocks read from this table''s TOAST table indexes (if any)">Blocks</th>'
            '<th title="TOAST table index block reads for this table as a percentage of all blocks read in a cluster">%Total</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
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

    FOR r_result IN c_tbl_stats LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.tablespacename,
            r_result.schemaname,
            r_result.relname,
            r_result.heap_blks_read,
            round(r_result.heap_blks_read_pct,2),
            r_result.idx_blks_read,
            round(r_result.idx_blks_read_pct,2),
            r_result.toast_blks_read,
            round(r_result.toast_blks_read_pct,2),
            r_result.tidx_blks_read,
            round(r_result.tidx_blks_read_pct,2)
        );
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION tbl_top_fetch_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer,
  IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    c_tbl_stats CURSOR FOR
    SELECT
        dbname,
        tablespacename,
        schemaname,
        relname,
        NULLIF(heap_blks_fetch, 0) as heap_blks_fetch,
        NULLIF(heap_blks_proc_pct, 0.0) as heap_blks_proc_pct,
        NULLIF(idx_blks_fetch, 0) as idx_blks_fetch,
        NULLIF(idx_blks_proc_pct, 0.0) as idx_blks_proc_pct,
        NULLIF(toast_blks_fetch, 0) as toast_blks_fetch,
        NULLIF(toast_blks_proc_pct, 0.0) as toast_blks_proc_pct,
        NULLIF(tidx_blks_fetch, 0) as tidx_blks_fetch,
        NULLIF(tidx_blks_proc_pct, 0.0) as tidx_blks_proc_pct
    FROM top_io_tables(sserver_id,start_id,end_id)
    WHERE COALESCE(heap_blks_fetch, 0) + COALESCE(idx_blks_fetch, 0) + COALESCE(toast_blks_fetch, 0) + COALESCE(tidx_blks_fetch, 0) > 0
    ORDER BY
      COALESCE(heap_blks_fetch, 0) + COALESCE(idx_blks_fetch, 0) + COALESCE(toast_blks_fetch, 0) + COALESCE(tidx_blks_fetch, 0) DESC,
      datid ASC,
      relid ASC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th rowspan="2">Database</th>'
            '<th rowspan="2">Tablespace</th>'
            '<th rowspan="2">Schema</th>'
            '<th rowspan="2">Table</th>'
            '<th colspan="2">Heap</th>'
            '<th colspan="2">Index</th>'
            '<th colspan="2">TOAST</th>'
            '<th colspan="2">TOAST-Index</th>'
          '</tr>'
          '<tr>'
            '<th title="Number of blocks fetched (read+hit) from this table">Blocks</th>'
            '<th title="Heap blocks fetched for this table as a percentage of all blocks fetched in a cluster">%Total</th>'
            '<th title="Number of blocks fetched (read+hit) from all indexes on this table">Blocks</th>'
            '<th title="Indexes blocks fetched for this table as a percentage of all blocks fetched in a cluster">%Total</th>'
            '<th title="Number of blocks fetched (read+hit) from this table''s TOAST table (if any)">Blocks</th>'
            '<th title="TOAST blocks fetched for this table as a percentage of all blocks fetched in a cluster">%Total</th>'
            '<th title="Number of blocks fetched (read+hit) from this table''s TOAST table indexes (if any)">Blocks</th>'
            '<th title="TOAST table index blocks fetched for this table as a percentage of all blocks fetched in a cluster">%Total</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
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

    FOR r_result IN c_tbl_stats LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.tablespacename,
            r_result.schemaname,
            r_result.relname,
            r_result.heap_blks_fetch,
            round(r_result.heap_blks_proc_pct,2),
            r_result.idx_blks_fetch,
            round(r_result.idx_blks_proc_pct,2),
            r_result.toast_blks_fetch,
            round(r_result.toast_blks_proc_pct,2),
            r_result.tidx_blks_fetch,
            round(r_result.tidx_blks_proc_pct,2)
        );
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION ix_top_io_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    c_tbl_stats CURSOR FOR
    SELECT
        dbname,
        tablespacename,
        schemaname,
        relname,
        indexrelname,
        NULLIF(idx_scan, 0) as idx_scan,
        NULLIF(idx_blks_read, 0) as idx_blks_read,
        NULLIF(idx_blks_read_pct, 0.0) as idx_blks_read_pct,
        NULLIF(idx_blks_hit_pct, 0.0) as idx_blks_hit_pct
    FROM top_io_indexes(sserver_id,start_id,end_id)
    WHERE idx_blks_read > 0
    ORDER BY
      idx_blks_read DESC,
      datid ASC,
      relid ASC,
      indexrelid ASC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th>Database</th>'
            '<th>Tablespace</th>'
            '<th>Schema</th>'
            '<th>Table</th>'
            '<th>Index</th>'
            '<th title="Number of scans performed on index">Scans</th>'
            '<th title="Number of disk blocks read from this index">Block Reads</th>'
            '<th title="Disk blocks read from this index as a percentage of all blocks read in a cluster">%Total</th>'
            '<th title="Index blocks buffer cache hit percentage">Hits(%)</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    FOR r_result IN c_tbl_stats LOOP
    report := report||format(
        jtab_tpl #>> ARRAY['row_tpl'],
        r_result.dbname,
        r_result.tablespacename,
        r_result.schemaname,
        r_result.relname,
        r_result.indexrelname,
        r_result.idx_scan,
        r_result.idx_blks_read,
        round(r_result.idx_blks_read_pct,2),
        round(r_result.idx_blks_hit_pct,2)
    );
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION ix_top_fetch_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    c_tbl_stats CURSOR FOR
    SELECT
        dbname,
        tablespacename,
        schemaname,
        relname,
        indexrelname,
        NULLIF(idx_scan, 0) as idx_scan,
        NULLIF(idx_blks_fetch, 0) as idx_blks_fetch,
        NULLIF(idx_blks_proc_pct, 0.0) as idx_blks_proc_pct
    FROM top_io_indexes(sserver_id,start_id,end_id)
    WHERE idx_blks_fetch > 0
    ORDER BY
      idx_blks_fetch DESC,
      datid ASC,
      relid ASC,
      indexrelid ASC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th>Database</th>'
            '<th>Tablespace</th>'
            '<th>Schema</th>'
            '<th>Table</th>'
            '<th>Index</th>'
            '<th title="Number of scans performed on index">Scans</th>'
            '<th title="Number of blocks fetched (read+hit) from this index">Blocks</th>'
            '<th title="Blocks fetched from this index as a percentage of all blocks fetched in a cluster">%Total</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    FOR r_result IN c_tbl_stats LOOP
    report := report||format(
        jtab_tpl #>> ARRAY['row_tpl'],
        r_result.dbname,
        r_result.tablespacename,
        r_result.schemaname,
        r_result.relname,
        r_result.indexrelname,
        r_result.idx_scan,
        r_result.idx_blks_fetch,
        round(r_result.idx_blks_proc_pct,2)
    );
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;