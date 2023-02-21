/* contrib/pg_profile/lt_profile--0.3.4--0.3.5.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION lt_profile UPDATE TO '0.3.5'" to load this file. \quit

INSERT INTO import_queries_version_order VALUES
  ('lt_profile','0.3.5','lt_profile','0.3.4');

ALTER TABLE sample_wait_event_total ADD COLUMN state text;

UPDATE sample_wait_event_total SET state = 'active'
WHERE wait_event_type != 'Activity'
AND (wait_event_type != 'Client' AND wait_event != 'ClientRead');