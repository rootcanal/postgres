setup
{
	CREATE EXTENSION IF NOT EXISTS gapless_seq;
    DROP SEQUENCE IF EXISTS test_gapless;
	CREATE SEQUENCE test_gapless USING gapless;
}

teardown
{
    DROP SEQUENCE test_gapless;
}

session "s1"
step "s1_begin" { BEGIN; }
step "s1_nextval" { SELECT nextval('test_gapless'::regclass); }
step "s1_commit" { COMMIT; }

session "s2"
step "s2_begin" { BEGIN; }
step "s2_nextval" { SELECT nextval('test_gapless'::regclass); }
step "s2_commit" { COMMIT; }

session "s3"
step "s3_begin" { BEGIN; }
step "s3_nextval" { SELECT nextval('test_gapless'::regclass); }
step "s3_rollback" { ROLLBACK; }

permutation "s1_begin" "s1_nextval" "s2_begin" "s2_nextval" "s1_commit" "s2_commit"
permutation "s3_begin" "s3_nextval" "s2_begin" "s2_nextval" "s3_rollback" "s2_commit"
