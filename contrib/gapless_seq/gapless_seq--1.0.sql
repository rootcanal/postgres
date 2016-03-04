-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION gapless_seq" to load this file. \quit

CREATE OR REPLACE FUNCTION seqam_gapless_handler(internal)
RETURNS SEQ_AM_HANDLER
LANGUAGE C
STABLE STRICT
AS 'MODULE_PATHNAME'
;

CREATE OR REPLACE FUNCTION seqam_gapless_state_in(cstring)
RETURNS seqam_gapless_state
LANGUAGE C
STABLE STRICT
AS 'MODULE_PATHNAME'
;

CREATE OR REPLACE FUNCTION seqam_gapless_state_out(seqam_gapless_state)
RETURNS cstring
LANGUAGE C
STABLE STRICT
AS 'MODULE_PATHNAME'
;

CREATE TYPE seqam_gapless_state (
	INPUT   = seqam_gapless_state_in,
	OUTPUT  = seqam_gapless_state_out,
	INTERNALLENGTH = 16
);
COMMENT ON TYPE seqam_gapless_state IS 'state for gapless sequence am';

CREATE TABLE seqam_gapless_values (
	seqid oid PRIMARY KEY,
	last_value bigint NOT NULL,
	is_called bool NOT NULL
);

CREATE OR REPLACE FUNCTION gapless_seq_clean_sequence_value()
RETURNS event_trigger
LANGUAGE plpgsql
AS $$
BEGIN
	-- just delete all the value data that don't have corresponding
	-- gapless sequence (either DELETEd or ALTERed to different AM)
	DELETE FROM gapless_seq.seqam_gapless_values WHERE seqid NOT IN (
		SELECT oid FROM pg_class WHERE relkind = 'S' AND relam = (
			SELECT oid FROM pg_am WHERE amname = 'gapless' AND amtype = 'S'
		)
	);
END;
$$;

CREATE EVENT TRIGGER gapless_seq_clean_sequence_value ON sql_drop
	WHEN TAG IN ('DROP SEQUENCE')
	EXECUTE PROCEDURE gapless_seq_clean_sequence_value();

CREATE ACCESS METHOD gapless TYPE SEQUENCE HANDLER seqam_gapless_handler;
