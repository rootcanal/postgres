--
-- Create access method tests
--

-- Make gist2 over gisthandler. In fact, it would be a synonym to gist.
CREATE ACCESS METHOD gist2 TYPE INDEX HANDLER gisthandler;

-- Drop old index on fast_emp4000
DROP INDEX grect2ind;

-- Try to create gist2 index on fast_emp4000: fail because opclass doesn't exist
CREATE INDEX grect2ind ON fast_emp4000 USING gist2 (home_base);

-- Make operator class for boxes using gist2
CREATE OPERATOR CLASS box_ops DEFAULT
	FOR TYPE box USING gist2 AS
	OPERATOR 1	<<,
	OPERATOR 2	&<,
	OPERATOR 3	&&,
	OPERATOR 4	&>,
	OPERATOR 5	>>,
	OPERATOR 6	~=,
	OPERATOR 7	@>,
	OPERATOR 8	<@,
	OPERATOR 9	&<|,
	OPERATOR 10	<<|,
	OPERATOR 11	|>>,
	OPERATOR 12	|&>,
	OPERATOR 13	~,
	OPERATOR 14	@,
	FUNCTION 1	gist_box_consistent(internal, box, smallint, oid, internal),
	FUNCTION 2	gist_box_union(internal, internal),
	FUNCTION 3	gist_box_compress(internal),
	FUNCTION 4	gist_box_decompress(internal),
	FUNCTION 5	gist_box_penalty(internal, internal, internal),
	FUNCTION 6	gist_box_picksplit(internal, internal),
	FUNCTION 7	gist_box_same(box, box, internal),
	FUNCTION 9	gist_box_fetch(internal);

-- Create gist2 index on fast_emp4000
CREATE INDEX grect2ind ON fast_emp4000 USING gist2 (home_base);

-- Now check the results from plain indexscan
SET enable_seqscan = OFF;
SET enable_indexscan = ON;
SET enable_bitmapscan = OFF;

EXPLAIN (COSTS OFF)
SELECT * FROM fast_emp4000
    WHERE home_base @ '(200,200),(2000,1000)'::box
    ORDER BY (home_base[0])[0];
SELECT * FROM fast_emp4000
    WHERE home_base @ '(200,200),(2000,1000)'::box
    ORDER BY (home_base[0])[0];

EXPLAIN (COSTS OFF)
SELECT count(*) FROM fast_emp4000 WHERE home_base && '(1000,1000,0,0)'::box;
SELECT count(*) FROM fast_emp4000 WHERE home_base && '(1000,1000,0,0)'::box;

EXPLAIN (COSTS OFF)
SELECT count(*) FROM fast_emp4000 WHERE home_base IS NULL;
SELECT count(*) FROM fast_emp4000 WHERE home_base IS NULL;

-- Try to drop access method: fail because of depending objects
DROP ACCESS METHOD gist2;

-- Drop access method cascade
DROP ACCESS METHOD gist2 CASCADE;

-- Reset optimizer options
RESET enable_seqscan;
RESET enable_indexscan;
RESET enable_bitmapscan;

-- Checks for sequence access methods

-- Create new sequence access method which uses standard local handler
CREATE ACCESS METHOD local2 TYPE SEQUENCE HANDLER seqam_local_handler;

-- Create and use sequence
CREATE SEQUENCE test_seqam USING local2;
SELECT nextval('test_seqam'::regclass);

-- Try to drop and fail on dependency
DROP ACCESS METHOD local2;

-- And cleanup
DROP SEQUENCE test_seqam;
DROP ACCESS METHOD local2;