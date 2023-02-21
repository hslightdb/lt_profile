/* ===== Indexes stats functions ===== */

CREATE FUNCTION top_indexes(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    server_id             integer,
    datid               oid,
    relid               oid,
    indexrelid          oid,
    indisunique         boolean,
    dbname              name,
    tablespacename      name,
    schemaname          name,
    relname             name,
    indexrelname        name,
    idx_scan            bigint,
    growth              bigint,
    tbl_n_tup_ins       bigint,
    tbl_n_tup_upd       bigint,
    tbl_n_tup_del       bigint,
    tbl_n_tup_hot_upd   bigint
)
SET search_path=@extschema@ AS $$
    SELECT
        st.server_id,
        st.datid,
        st.relid,
        st.indexrelid,
        st.indisunique,
        sample_db.datname,
        tablespaces_list.tablespacename,
        COALESCE(mtbl.schemaname,st.schemaname)::name AS schemaname,
        COALESCE(mtbl.relname||'(TOAST)',st.relname)::name as relname,
        st.indexrelname,
        sum(st.idx_scan)::bigint as idx_scan,
        sum(st.relsize_diff)::bigint as growth,
        sum(tbl.n_tup_ins)::bigint as tbl_n_tup_ins,
        sum(tbl.n_tup_upd)::bigint as tbl_n_tup_upd,
        sum(tbl.n_tup_del)::bigint as tbl_n_tup_del,
        sum(tbl.n_tup_hot_upd)::bigint as tbl_n_tup_hot_upd
    FROM v_sample_stat_indexes st JOIN v_sample_stat_tables tbl USING (server_id, sample_id, datid, relid)
        -- Database name
        JOIN sample_stat_database sample_db
        ON (st.server_id=sample_db.server_id AND st.sample_id=sample_db.sample_id AND st.datid=sample_db.datid)
        JOIN tablespaces_list ON  (st.server_id=tablespaces_list.server_id AND st.tablespaceid=tablespaces_list.tablespaceid)
        -- join main table for indexes on toast
        LEFT OUTER JOIN tables_list mtbl ON (st.server_id = mtbl.server_id AND st.datid = mtbl.datid AND st.relid = mtbl.reltoastrelid)
    WHERE st.server_id=sserver_id AND NOT sample_db.datistemplate AND st.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY st.server_id,st.datid,st.relid,st.indexrelid,st.indisunique,sample_db.datname,
      COALESCE(mtbl.schemaname,st.schemaname),COALESCE(mtbl.relname||'(TOAST)',st.relname), tablespaces_list.tablespacename,st.indexrelname
$$ LANGUAGE sql;

/*
  index_size_failures() function is used for detecting indexes with possibly
  incorrect growth stats due to failed relation size collection
  on either bound of an interval
*/
CREATE FUNCTION index_size_failures(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    server_id         integer,
    datid             oid,
    indexrelid        oid,
    size_failed       boolean
) SET search_path=@extschema@ AS $$
  SELECT
    server_id,
    datid,
    indexrelid,
    bool_or(size_failed) as size_failed
  FROM
    sample_stat_indexes_failures
  WHERE
    server_id = sserver_id AND sample_id IN (start_id, end_id)
  GROUP BY
    server_id,
    datid,
    indexrelid
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION ix_size_interpolated(IN server_id integer, sample_id integer,
  IN datid oid, IN indexrelid oid
) RETURNS bigint
STABLE
RETURNS NULL ON NULL INPUT
AS
$$
DECLARE
  r_current   record;
  r_left      record;
  r_right     record;
  timediff    double precision;

  c_before CURSOR FOR
  SELECT sample_time,relsize
  FROM sample_stat_indexes i
    JOIN samples s USING (server_id, sample_id)
  WHERE (i.server_id, i.datid, i.indexrelid) =
    (ix_size_interpolated.server_id,
    ix_size_interpolated.datid, ix_size_interpolated.indexrelid)
    AND relsize IS NOT NULL
    AND i.sample_id < ix_size_interpolated.sample_id
  ORDER BY i.sample_id DESC
  LIMIT 2;

  c_after CURSOR FOR
  SELECT sample_time,relsize
  FROM sample_stat_indexes i
    JOIN samples s USING (server_id, sample_id)
  WHERE (i.server_id, i.datid, i.indexrelid) =
    (ix_size_interpolated.server_id,
    ix_size_interpolated.datid, ix_size_interpolated.indexrelid)
    AND relsize IS NOT NULL
    AND i.sample_id > ix_size_interpolated.sample_id
  ORDER BY i.sample_id ASC
  LIMIT 2;
BEGIN
	/* If raw data exists, return it as is */
	SELECT relsize INTO r_current
	FROM sample_stat_indexes i
	WHERE (i.server_id,i.sample_id,i.datid,i.indexrelid) =
		(ix_size_interpolated.server_id,ix_size_interpolated.sample_id,
		ix_size_interpolated.datid, ix_size_interpolated.indexrelid);
	IF FOUND THEN
		IF r_current.relsize IS NOT NULL THEN
			RETURN r_current.relsize;
		END IF;
	ELSE
		RETURN NULL;
	END IF;

	/* We need to use interpolation here */
	OPEN c_before;
	FETCH c_before INTO r_left;

	OPEN c_after;
	FETCH c_after INTO r_right;

	SELECT s.sample_time,relsize INTO STRICT r_current
	FROM sample_stat_indexes i
	JOIN samples s USING (server_id, sample_id)
	WHERE (i.server_id, i.sample_id, i.datid, i.indexrelid) =
		(ix_size_interpolated.server_id, ix_size_interpolated.sample_id,
		ix_size_interpolated.datid, ix_size_interpolated.indexrelid);

	CASE
		WHEN r_left.sample_time IS NOT NULL AND r_right.sample_time IS NULL THEN
			r_right := r_left;
			FETCH c_before INTO r_left;
		WHEN r_left.sample_time IS NULL AND r_right.sample_time IS NOT NULL THEN
			r_left := r_right;
			FETCH c_after INTO r_right;
		ELSE
			NULL;
	END CASE;

	CLOSE c_after;
	CLOSE c_before;

	timediff := extract(epoch from r_right.sample_time - r_left.sample_time);
	IF timediff <= 0 THEN
		RETURN r_left.relsize +
			round(extract(epoch from r_current.sample_time - r_left.sample_time) *
				(r_right.relsize - r_left.relsize)
			);
	ELSE
		RETURN r_left.relsize +
			round(extract(epoch from r_current.sample_time - r_left.sample_time) *
				(r_right.relsize - r_left.relsize) / timediff
			);
	END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_growth_indexes_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer,
  IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';

    -- Indexes stats template
    jtab_tpl    jsonb;

    --Cursor for indexes stats
    c_ix_stats CURSOR FOR
    SELECT
        st.dbname,
        st.tablespacename,
        st.schemaname,
        st.relname,
        st.indexrelname,
        CASE WHEN sf.size_failed THEN 'N/A'
          ELSE pg_size_pretty(NULLIF(st.growth, 0)) END AS growth,
        pg_size_pretty(NULLIF(st_last.relsize, 0)) as relsize,
        NULLIF(tbl_n_tup_ins, 0) as tbl_n_tup_ins,
        NULLIF(tbl_n_tup_upd - COALESCE(tbl_n_tup_hot_upd,0), 0) as tbl_n_tup_upd,
        NULLIF(tbl_n_tup_del, 0) as tbl_n_tup_del
    FROM top_indexes(sserver_id, start_id, end_id) st
        JOIN v_sample_stat_indexes st_last using (server_id,datid,relid,indexrelid)
        -- Is there any failed size collections on indexes?
        LEFT OUTER JOIN index_size_failures(sserver_id, start_id, end_id) sf
          USING (server_id, datid, indexrelid)
    WHERE st_last.sample_id = end_id
      AND st.growth > 0
    ORDER BY st.growth DESC,
      COALESCE(tbl_n_tup_ins,0) + COALESCE(tbl_n_tup_upd,0) + COALESCE(tbl_n_tup_del,0) DESC,
      st.datid ASC,
      st.relid ASC,
      st.indexrelid ASC
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
            '<th rowspan="2">Index</th>'
            '<th colspan="2">Index</th>'
            '<th colspan="3">Table</th>'
          '</tr>'
          '<tr>'
            '<th title="Index size, as it was at the moment of last sample in report interval">Size</th>'
            '<th title="Index size increment during report interval">Growth</th>'
            '<th title="Number of rows inserted">Inserted</th>'
            '<th title="Number of rows updated (without HOT updated rows)">Updated</th>'
            '<th title="Number of rows deleted">Deleted</th>'
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
          '<td {value}>%s</td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);
    -- Reporting table stats
    FOR r_result IN c_ix_stats LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.tablespacename,
            r_result.schemaname,
            r_result.relname,
            r_result.indexrelname,
            r_result.relsize,
            r_result.growth,
            r_result.tbl_n_tup_ins,
            r_result.tbl_n_tup_upd,
            r_result.tbl_n_tup_del
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION ix_unused_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';

    jtab_tpl    jsonb;

    --Cursor for indexes stats
    c_ix_stats CURSOR FOR
    SELECT
        st.dbname,
        st.tablespacename,
        st.schemaname,
        st.relname,
        st.indexrelname,
        pg_size_pretty(NULLIF(st.growth, 0)) as growth,
        pg_size_pretty(NULLIF(st_last.relsize, 0)) as relsize,
        NULLIF(tbl_n_tup_ins, 0) as tbl_n_tup_ins,
        NULLIF(tbl_n_tup_upd - COALESCE(tbl_n_tup_hot_upd,0), 0) as tbl_n_ind_upd,
        NULLIF(tbl_n_tup_del, 0) as tbl_n_tup_del
    FROM top_indexes(sserver_id, start_id, end_id) st
        JOIN v_sample_stat_indexes st_last using (server_id,datid,relid,indexrelid)
    WHERE st_last.sample_id=end_id AND COALESCE(st.idx_scan, 0) = 0 AND NOT st.indisunique
      AND COALESCE(tbl_n_tup_ins, 0) + COALESCE(tbl_n_tup_upd, 0) + COALESCE(tbl_n_tup_del, 0) > 0
    ORDER BY
      COALESCE(tbl_n_tup_ins, 0) + COALESCE(tbl_n_tup_upd, 0) + COALESCE(tbl_n_tup_del, 0) DESC,
      st.datid ASC,
      st.relid ASC,
      st.indexrelid ASC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th rowspan="2">Database</th>'
            '<th rowspan="2">Tablespaces</th>'
            '<th rowspan="2">Schema</th>'
            '<th rowspan="2">Table</th>'
            '<th rowspan="2">Index</th>'
            '<th colspan="2">Index</th>'
            '<th colspan="3">Table</th>'
          '</tr>'
          '<tr>'
            '<th title="Index size, as it was at the moment of last sample in report interval">Size</th>'
            '<th title="Index size increment during report interval">Growth</th>'
            '<th title="Number of rows inserted">Inserted</th>'
            '<th title="Number of rows updated (without HOT updated rows)">Updated</th>'
            '<th title="Number of rows deleted">Deleted</th>'
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
          '<td {value}>%s</td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);
    -- Reporting on top queries by elapsed time
    FOR r_result IN c_ix_stats LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.tablespacename,
            r_result.schemaname,
            r_result.relname,
            r_result.indexrelname,
            r_result.relsize,
            r_result.growth,
            r_result.tbl_n_tup_ins,
            r_result.tbl_n_ind_upd,
            r_result.tbl_n_tup_del
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;


CREATE FUNCTION top_vacuumed_indexes_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer,
  IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';

    -- Indexes stats template
    jtab_tpl    jsonb;

    --Cursor for indexes stats
    c_ix_stats CURSOR FOR
    SELECT
        st.dbname,
        st.tablespacename,
        st.schemaname,
        st.relname,
        st.indexrelname,
        NULLIF(vac.vacuum_count, 0) as vacuum_count,
        NULLIF(vac.autovacuum_count, 0) as autovacuum_count,
        NULLIF(vac.vacuum_bytes, 0) as vacuum_bytes,
        NULLIF(vac.avg_indexrelsize, 0) as avg_ix_relsize,
        NULLIF(vac.avg_relsize, 0) as avg_relsize
    FROM top_indexes(sserver_id, start_id, end_id) st
      JOIN (
        SELECT
          server_id,
          datid,
          indexrelid,
          sum(vacuum_count) as vacuum_count,
          sum(autovacuum_count) as autovacuum_count,
          round(sum(COALESCE(i.relsize,ix_size_interpolated(server_id,sample_id,datid,indexrelid))
			* (COALESCE(vacuum_count,0) + COALESCE(autovacuum_count,0))))::bigint as vacuum_bytes,
          round(avg(COALESCE(i.relsize,ix_size_interpolated(server_id,sample_id,datid,indexrelid))))::bigint as avg_indexrelsize,
          round(avg(COALESCE(t.relsize,tab_size_interpolated(server_id,sample_id,datid,relid))))::bigint as avg_relsize
        FROM sample_stat_indexes i
			JOIN indexes_list il USING (server_id,datid,indexrelid)
			JOIN sample_stat_tables t USING
				(server_id, sample_id, datid, relid)
        WHERE
          server_id = sserver_id AND
          sample_id BETWEEN start_id + 1 AND end_id
        GROUP BY
          server_id, datid, indexrelid
      ) vac USING (server_id, datid, indexrelid)
    WHERE vac.vacuum_bytes > 0
    ORDER BY
      vacuum_bytes DESC,
      st.datid ASC,
      st.relid ASC,
      st.indexrelid ASC
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
            '<th title="Estimated implicit vacuum load caused by table indexes">~Vacuum bytes</th>'
            '<th title="Vacuum count on underlying table">Vacuum count</th>'
            '<th title="Autovacuum count on underlying table">Autovacuum count</th>'
            '<th title="Average index size during report interval">Index size</th>'
            '<th title="Average relation size during report interval">Relsize</th>'
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
          '<td {value}>%s</td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);
    -- Reporting table stats
    FOR r_result IN c_ix_stats LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.tablespacename,
            r_result.schemaname,
            r_result.relname,
            r_result.indexrelname,
            pg_size_pretty(r_result.vacuum_bytes),
            r_result.vacuum_count,
            r_result.autovacuum_count,
            pg_size_pretty(r_result.avg_ix_relsize),
            pg_size_pretty(r_result.avg_relsize)
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;
