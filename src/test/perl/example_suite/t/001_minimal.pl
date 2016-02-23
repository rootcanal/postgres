# This is a minimal example showing how to use the Perl-based test framework for
# writing tests that are more complicated than pg_regress or the isolation tester
# can handle.
#
# Test scripts should all begin with:
#
#     use strict;
#     use warnings;
#     use PostgresNode;
#     use TestLib;
#
# The script must include:
#
#     use Test::More tests => 1;
#
# where the number of tests expected to run is the value of 'tests'.
#
# All tests must support running under Perl 5.8.8. "use 5.8.8" does NOT
# ensure that.

use strict;
use warnings;
use 5.8.8;
use PostgresNode;
use TestLib;
use Test::More tests => 5;

my $verbose = 0;

# Set up a node
#
# Unlike pg_regress tests, Perl-based tests must create their own PostgreSQL
# node (s). The nodes are managed with the PostgresNode module; see that module
# for methods available for the node.

diag 'Setting up node "master"' if $verbose;
my $node = get_new_node('master');
$node->init;
$node->start;

diag $node->info() if $verbose;

# Trivial SELECT test
#
# A very simple test that just runs a SELECT and checks the result.
#
# Obviously you should write tests like this with pg_regress, but this
# serves to demonstrate the minimal setup required.

my $ret = $node->psql('postgres', 'SELECT 1;');
is($ret, '1', 'simple SELECT');

# Create a read-replica
#
# One of the reasons to write Perl tests is to do work that touches
# multiple nodes, so lets set up a second node.

diag 'Reconfiguring master for replication' if $verbose;
$node->append_conf('postgresql.conf', "max_wal_senders = 2\n");
$node->append_conf('postgresql.conf', "wal_level = logical\n");
$node->append_conf('postgresql.conf', "hot_standby = on\n");
$node->restart('fast');

diag 'Making base backup of current node' if $verbose;
$ret = $node->backup('makereplica',
	write_recovery_conf => 1,
	xlog_stream => 1);
ok($ret, 'base backup of master');

diag 'Setting up node "replica"' if $verbose;
my $replica = get_new_node('replica');
$replica->init_from_backup($node, 'makereplica');
$replica->start;

diag $replica->info() if $verbose;

# Wait for replica to catch up
#
# A simple example of the sort of tests that can be written with this
# toolset: write to a master server then wait until the replica server
# replays the change.

diag 'Setting up for catchup tests' if $verbose;

$node->psql('postgres', q|
	CREATE TABLE replica_test(blah text);
	INSERT INTO replica_test(blah) VALUES ('fruit bat');
	|);

# method one: poll until the new row becomes visible on the replica
diag 'Waiting for replica to catch up - polling' if $verbose;
$ret = $replica->poll_query_until('postgres',
	q|SELECT 't' FROM replica_test WHERE blah = 'fruit bat';|);

ok($ret, 'caught up - polling for row');

# Method two: wait for the replica replay position to overtake the master
# by querying the replica.
$node->psql('postgres', q|INSERT INTO replica_test(blah) VALUES ('donkey');|);

my $lsn = $node->psql('postgres', 'SELECT pg_current_xlog_location()');

diag "Waiting for replica to catch up to $lsn - replica xlog location" if $verbose;
$ret = $replica->poll_query_until('postgres',
	qq|SELECT pg_xlog_location_diff('$lsn', pg_last_xlog_replay_location()) <= 0;|);

ok($ret, 'caught up - polling for replay on replica');

# Method three: wait for the replica to catch up according to
# pg_stat_replication on the master.
$node->psql('postgres', q|INSERT INTO replica_test(blah) VALUES ('turkey');|);

$lsn = $node->psql('postgres', 'SELECT pg_current_xlog_location()');

# This assumes only one replica...
diag "Waiting for replica to catch up to $lsn - pg_stat_replication" if $verbose;
$ret = $node->poll_query_until('postgres',
	qq|SELECT pg_xlog_location_diff('$lsn', replay_location) <= 0
	   FROM pg_stat_replication|);

ok($ret, 'caught up - polling pg_stat_replication on master');

# Test teardown
#
# Every test should end by explicitly tearing down the node (s) it created.

$node->teardown_node;
$replica->teardown_node;

1;
