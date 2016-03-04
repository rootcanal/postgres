CREATE EXTENSION gapless_seq;

CREATE SEQUENCE test_gapless USING gapless;

SELECT nextval('test_gapless'::regclass);

BEGIN;
	SELECT nextval('test_gapless'::regclass);
	SELECT nextval('test_gapless'::regclass);
	SELECT nextval('test_gapless'::regclass);
ROLLBACK;

SELECT nextval('test_gapless'::regclass);

CREATE SEQUENCE test_alter_seq USING local;

SELECT nextval('test_alter_seq'::regclass);

ALTER SEQUENCE test_alter_seq USING gapless;

SELECT nextval('test_alter_seq'::regclass);

BEGIN;
	SELECT nextval('test_alter_seq'::regclass);
ROLLBACK;

SELECT nextval('test_alter_seq'::regclass);

BEGIN;
	SELECT nextval('test_alter_seq'::regclass);
	SAVEPOINT mysp;
	SELECT nextval('test_alter_seq'::regclass);
	ROLLBACK TO SAVEPOINT mysp;
	SAVEPOINT mysp2;
	SELECT nextval('test_alter_seq'::regclass);
	RELEASE SAVEPOINT mysp2;
	SELECT nextval('test_alter_seq'::regclass);
COMMIT;

ALTER SEQUENCE test_alter_seq RESTART 100 USING local;

SELECT nextval('test_alter_seq'::regclass);

-- check dump/restore
SELECT pg_sequence_get_state('test_gapless');
SELECT pg_sequence_set_state('test_gapless', pg_sequence_get_state('test_gapless'));
SELECT pg_sequence_get_state('test_gapless');

-- check that event trigger works correctly
SELECT last_value FROM gapless_seq.seqam_gapless_values ORDER BY seqid;
DROP SEQUENCE test_gapless;
SELECT last_value FROM gapless_seq.seqam_gapless_values ORDER BY seqid;

CREATE SEQUENCE test_gapless USING gapless;

-- should fail due to deps
DROP ACCESS METHOD gapless;
-- likewise
DROP EXTENSION gapless_seq;
-- success
DROP EXTENSION gapless_seq CASCADE;
