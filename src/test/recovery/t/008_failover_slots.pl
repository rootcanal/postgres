#
# Test failover slots
#
use strict;
use warnings;
use bigint;
use PostgresNode;
use TestLib;
use Test::More;
use RecursiveCopy;
use File::Copy;
use File::Basename qw(basename);
use List::Util qw();
use Data::Dumper;
use IPC::Run qw();


use Carp 'verbose';
$SIG{ __DIE__ } = sub { Carp::confess( @_ ) };

my $verbose = 0;

sub lsn_to_bigint
{
	my ($lsn) = @_;
	my ($high, $low) = split("/",$lsn);
	return hex($high) * 2**32 + hex($low);
}

sub get_slot_info
{
	my ($node, $slot_name) = @_;

	my $esc_slot_name = $slot_name;
	$esc_slot_name =~ s/'/''/g;
	my @selectlist = ('slot_name', 'plugin', 'slot_type', 'database', 'active_pid', 'xmin', 'catalog_xmin', 'restart_lsn', 'confirmed_flush_lsn', 'failover');
	my $row = $node->safe_psql('postgres', "SELECT " . join(', ', @selectlist) . " FROM pg_replication_slots WHERE slot_name = '$esc_slot_name';");
	chomp $row;
	my @fields = split('\|', $row, -1);
	if (scalar @fields != scalar @selectlist)
	{
		diag "Invalid row is: '$row'";
		die "Select-list '@selectlist'(".scalar(@selectlist).") didn't match length of result-list '@fields'(".scalar(@fields).")";
	}
	my %slotinfo;
	for (my $i = 0; $i < scalar @selectlist; $i++)
	{
		$slotinfo{$selectlist[$i]} = $fields[$i];
	}
	return \%slotinfo;
}

sub diag_slotinfo
{
	my ($info, $msg) = @_;
	return unless $verbose;
	diag "slot " . $info->{slot_name} . ": " . Dumper($info);
}

sub wait_for_catchup
{
	my ($node_master, $node_replica) = @_;

	my $master_lsn = $node_master->safe_psql('postgres', 'SELECT pg_current_xlog_insert_location()');
	diag "waiting for " . $node_replica->name . " to catch up to $master_lsn on " . $node_master->name if $verbose;
	my $ret = $node_replica->poll_query_until('postgres',
		"SELECT pg_last_xlog_replay_location() >= '$master_lsn'::pg_lsn;");
	BAIL_OUT('replica failed to catch up') unless $ret;
	my $replica_lsn = $node_replica->safe_psql('postgres', 'SELECT pg_last_xlog_replay_location()');
	diag "Replica is caught up to $replica_lsn, past required LSN $master_lsn" if $verbose;
}

sub read_slot_updates_from_xlog
{
	my ($node, $timeline) = @_;
	my ($stdout, $stderr) = ('', '');
	# Look at master xlogs and examine sequence advances
	my $wal_pattern = sprintf("%s/pg_xlog/%08X" . ("?" x 16), $node->data_dir, $timeline);
	my @wal = glob $wal_pattern;
	my $firstwal = List::Util::minstr(@wal);
	my $lastwal = basename(List::Util::maxstr(@wal));
	diag "decoding xlog on " . $node->name . " from $firstwal to $lastwal" if $verbose;
	IPC::Run::run ['pg_xlogdump', $firstwal, $lastwal], '>', \$stdout, '2>', \$stderr;
	like($stderr, qr/invalid record length at [0-9A-F]+\/[0-9A-F]+: wanted 24, got 0/,
		'pg_xlogdump exits with expected error');
	my @slots = grep(/ReplicationSlot/, split(/\n/, $stdout));

	# Parse the dumped xlog data
	my @slot_updates = ();
	for my $slot (@slots) {
		if (my @matches = ($slot =~ /lsn: ([[:xdigit:]]{1,8}\/[[:xdigit:]]{1,8}), prev [[:xdigit:]]{1,8}\/[[:xdigit:]]{1,8}, desc: UPDATE of slot (\w+) with restart ([[:xdigit:]]{1,8}\/[[:xdigit:]]{1,8}) and xid ([[:digit:]]+) confirmed to ([[:xdigit:]]{1,8}\/[[:xdigit:]]{1,8})/))
		{
			my %slot_update = (
				action => 'UPDATE',
				log_lsn => $1, slot_name => $2, restart_lsn => $3,
				xid => $4, confirm_lsn => $5
				);
			diag "Replication slot create/advance: $slot_update{slot_name} advanced to $slot_update{confirm_lsn} with restart $slot_update{restart_lsn} and $slot_update{xid} in xlog entry $slot_update{log_lsn}" if $verbose;
			push @slot_updates, \%slot_update;
		}
		elsif ($slot =~ /DELETE/)
		{
			diag "Replication slot delete: $slot" if $verbose;
		}
		else
		{
			die "Slot xlog entry didn't match pattern: $slot";
		}
	}
	return \@slot_updates;
}

sub check_slot_wal_update
{
	my ($entry, $slotname, %params) = @_;

	ok(defined($entry), 'xlog entry exists for slot $slotname');
	SKIP: {
		skip 'Expected xlog entry was undef' unless defined($entry);
		my %entry = %{$entry}; undef $entry;
		diag "Examining decoded slot update xlog entry: " . Dumper(\%entry) if $verbose;
		is($entry{action}, 'UPDATE', "$slotname: action is an update");
		is($entry{slot_name}, $slotname, "$slotname: action affects slot " . $slotname);

		cmp_ok(lsn_to_bigint($entry{restart_lsn}), "le",
		       lsn_to_bigint($entry{log_lsn}),
		       "$slotname: restart_lsn is no greater than LSN when logged");

		cmp_ok(lsn_to_bigint($entry{confirm_lsn}), "le",
		       lsn_to_bigint($entry{log_lsn}),
		       "$slotname: confirm_lsn is no greater than LSN when logged");

		cmp_ok(lsn_to_bigint($entry{confirm_lsn}), "ge",
			lsn_to_bigint($entry{restart_lsn}),
			"$slotname: confirm_lsn equal to or ahead of restart_lsn")
		      if $entry{confirm_lsn} && $entry{confirm_lsn} ne '0/0';

		cmp_ok(lsn_to_bigint($entry{restart_lsn}), "le",
			lsn_to_bigint($params{expect_max_restart_lsn}),
			"$slotname: restart_lsn is at or before expected")
			if ($params{expect_max_restart_lsn});

		cmp_ok(lsn_to_bigint($entry{restart_lsn}), "ge",
			lsn_to_bigint($params{expect_min_restart_lsn}),
			"$slotname: restart_lsn is at or after expected")
			if ($params{expect_min_restart_lsn});

		cmp_ok(lsn_to_bigint($entry{confirm_lsn}), "le",
			lsn_to_bigint($params{expect_max_confirm_lsn}),
			"$slotname: confirm_lsn is at or before expected")
			if ($params{expect_max_confirm_lsn});

		cmp_ok(lsn_to_bigint($entry{confirm_lsn}), "ge",
			lsn_to_bigint($params{expect_min_confirm_lsn}),
			"$slotname: confirm_lsn is at or after expected")
			if ($params{expect_min_confirm_lsn});
	}
}

sub test_read_from_slot
{
	my ($node, $slot, $expected) = @_;
	my $slot_quoted = $slot;
	$slot_quoted =~ s/'/''/g;
	my ($ret, $stdout, $stderr) = $node->psql('postgres',
		"SELECT data FROM pg_logical_slot_peek_changes('$slot_quoted', NULL, NULL, 'include-xids', '0', 'skip-empty-xacts', '1');"
	);
	is($ret, 0, "replaying from slot $slot is successful");
	is($stderr, '', "replay from slot $slot produces no stderr");
	if (defined($expected)) {
		is($stdout, $expected, "slot $slot returned expected output");
	}
	return $stderr;
}

sub wait_for_end_of_recovery
{
	my ($node) = @_;
	$node->poll_query_until('postgres',
		"SELECT NOT pg_is_in_recovery();");
}

# Launch pg_xlogdump as a background proc and return the IPC::Run handle for it
# as well as the proc's stdout and stderr scalar refs as well as the path to
# where the xlogs are written.
sub start_pg_receivexlog
{
  my ($node, $slotname) = @_;
  my ($stdout, $stderr);

  my $outdir = $node->basedir . '/xl_' . $slotname;
  mkdir($outdir);

  my @cmd = ("pg_receivexlog", "--verbose", "-S", $slotname, "-D", $outdir, "--no-loop", "--dbname", $node->connstr);
  diag "Running '@cmd'" if $verbose;

  my $proc = IPC::Run::start \@cmd, '>', \$stdout, '2>', \$stderr;

  die $! unless defined($proc);

  return ($proc, \$stdout, \$stderr, $outdir);
}

sub test_phys_replay
{
  my ($node, $slotname, $start_tli) = @_;
  my ($recvxlog, $stdout, $stderr, $outdir) = start_pg_receivexlog($node, $slotname);
  # pg_receivexlog doesn't give us a --nowait option so we have to just wait a
  # bit then kill it.
  sleep(1);
  $recvxlog->signal("TERM");
  sleep(1);
  $recvxlog->finish;
  # FIXME: Not portable, we should use IPC::Signal but that's in CPAN because
  # apparently Perl doesn't have a signo/signame mapping built-in. WTF...
  is($recvxlog->full_result, "15", 'pg_recvlog exited due to SIGTERM');
  chomp $$stderr;
  my $expected_stderr_re = "^pg_receivexlog: starting log streaming at ([[:xdigit:]]{1,8})/([[:xdigit:]]{1,8}) \\(timeline ($start_tli)\\)";
  like($$stderr, "/$expected_stderr_re/", "reported start location to stderr");
  if ($$stderr =~ $expected_stderr_re)
  {
    my ($cap_lsn_high, $cap_lsn_low, $cap_tli) = ($1, $2, $3);
    diag "pg_xlogdump streamed xlog from node " . $node->name . " starting at $cap_lsn_high/$cap_lsn_low on timeline $cap_tli" if $verbose;
    is($cap_tli, $start_tli, 'replay started on expected timeline') if ($start_tli);
  }
  is($$stdout, '', "no stdout");
  my @xlogs = glob $outdir . "/*";
  cmp_ok(scalar(@xlogs), "ge", 1, "Received at least one segment from $slotname");
}


my ($stdout, $stderr, $ret, $slotinfo, $outdir, $proc);

# Initialize master node
my $node_master = get_new_node('master');
$node_master->init(allows_streaming => 1, has_archiving => 1);
$node_master->append_conf('postgresql.conf', "wal_level = 'logical'\n");
$node_master->append_conf('postgresql.conf', "max_replication_slots = 12\n");
$node_master->append_conf('postgresql.conf', "max_wal_senders = 12\n");
$node_master->append_conf('postgresql.conf', "max_connections = 20\n");
#$node_master->append_conf('postgresql.conf', "log_min_messages = 'debug2'\n");
$node_master->dump_info;
$node_master->start;

my $master_beforecreate_bb_lsn = $node_master->safe_psql('postgres',
	"SELECT pg_current_xlog_insert_location()");

$node_master->safe_psql('postgres',
"SELECT pg_create_logical_replication_slot('bb_failover', 'test_decoding', true);"
);
my $bb_beforeconsume_si = get_slot_info($node_master, 'bb_failover');
diag_slotinfo $bb_beforeconsume_si, 'bb_beforeconsume';

# Create non-failover slot to make sure it isn't replicated
$node_master->safe_psql('postgres',
"SELECT pg_create_logical_replication_slot('bb', 'test_decoding');"
);

# Failover slots work for physical slots too
$node_master->safe_psql('postgres',
"SELECT pg_create_physical_replication_slot('bb_phys_failover', false, true);");
$node_master->safe_psql('postgres',
"SELECT pg_create_physical_replication_slot('bb_phys');");

my $bb_phys_beforeconsume_si = get_slot_info($node_master, 'bb_phys_failover');
diag_slotinfo $bb_phys_beforeconsume_si, 'bb_phys_beforeconsume';

$node_master->safe_psql('postgres', "CREATE TABLE decoding(blah text);");
$node_master->safe_psql('postgres',
	"INSERT INTO decoding(blah) VALUES ('consumed');");
($ret, $stdout, $stderr) = $node_master->psql('postgres',
	"SELECT data FROM pg_logical_slot_get_changes('bb_failover', NULL, NULL, 'include-xids', '0', 'skip-empty-xacts', '1');");
is($ret, 0, 'replaying from bb_failover on master is successful');
is( $stdout, q(BEGIN
table public.decoding: INSERT: blah[text]:'consumed'
COMMIT), 'decoded expected data from slot bb_failover on master');
is($stderr, '', 'replay from slot bb_failover produces no stderr');

my $bb_afterconsume_si = get_slot_info($node_master, 'bb_failover');
diag_slotinfo $bb_afterconsume_si, 'bb_afterconsume';

($ret, $stdout, $stderr) = $node_master->psql('postgres',
	"SELECT data FROM pg_logical_slot_get_changes('bb_failover', NULL, NULL, 'include-xids', '0', 'skip-empty-xacts', '1');");
is ($ret, 0, 'no error reading empty slot changes after get');
is ($stdout, '', 'no new changes to read from slot after get');

cmp_ok(lsn_to_bigint($bb_afterconsume_si->{confirmed_flush_lsn}),
      "gt",
      lsn_to_bigint($bb_beforeconsume_si->{confirmed_flush_lsn}),
      "confirm lsn on bb_failover advanced on master after replay");

$node_master->safe_psql('postgres', 'CHECKPOINT;');

$node_master->safe_psql('postgres',
	"INSERT INTO decoding(blah) VALUES ('beforebb');");
$node_master->safe_psql('postgres', 'CHECKPOINT;');

my $backup_name = 'b1';
$node_master->backup_fs_hot($backup_name);

my $node_replica = get_new_node('replica');
$node_replica->init_from_backup(
	$node_master, $backup_name,
	has_streaming => 1,
	has_restoring => 1);
$node_replica->start;

my $master_beforecreate_ab_lsn = $node_master->safe_psql('postgres',
	"SELECT pg_current_xlog_insert_location()");

$node_master->safe_psql('postgres',
"SELECT pg_create_logical_replication_slot('ab_failover', 'test_decoding', true);"
);

my $ab_beforeconsume_si = get_slot_info($node_master, 'ab_failover');
diag_slotinfo $ab_beforeconsume_si, 'ab_beforeconsume';

$node_master->safe_psql('postgres',
"SELECT pg_create_logical_replication_slot('ab', 'test_decoding');"
);

$node_master->safe_psql('postgres',
"SELECT pg_create_physical_replication_slot('ab_phys_failover', false, true);"
);

$node_master->safe_psql('postgres',
"SELECT pg_create_physical_replication_slot('ab_phys');"
);

my $ab_phys_beforeconsume_si = get_slot_info($node_master, 'ab_phys_failover');
diag_slotinfo $ab_phys_beforeconsume_si, 'ab_phys_beforeconsume';

# We can also create physical slots on replicas if they aren't failover slots
$node_replica->safe_psql('postgres',
"SELECT pg_create_physical_replication_slot('onreplica');"
);

($ret, $stdout, $stderr) = $node_replica->psql('postgres',
"SELECT pg_create_physical_replication_slot('onreplica', false, true);"
);
is($ret, 3, "failed to create failover slot on replica");
like($stderr, qr/a failover slot may not be created on a replica/, "got expected error creating failover slot on replica");

$node_master->safe_psql('postgres',
	"INSERT INTO decoding(blah) VALUES ('afterbb');");

wait_for_catchup($node_master, $node_replica);

# Can't replay from a failover slot on a replica
($proc, $stdout, $stderr, $outdir) = start_pg_receivexlog($node_replica, 'bb_phys_failover');
$proc->finish;
is($proc->result, 1, 'pg_receivexlog exited with error code when attempting replay from failover slot on replica');
is($$stdout, '', 'no stdout');
like($$stderr, qr/ERROR:.*replication slot "bb_phys_failover" is reserved for use after failover/, 'pg_receivexlog exited with expected error');

$stdout = $node_master->safe_psql('postgres', 'SELECT slot_name FROM pg_replication_slots ORDER BY slot_name');
is($stdout, q(ab
ab_failover
ab_phys
ab_phys_failover
bb
bb_failover
bb_phys
bb_phys_failover), 'Expected slots exist on master')
  or BAIL_OUT('Remaining tests meaningless');


# Verify that only the failover slots and the physical slot we created
# directly are present on the replica
$stdout = $node_replica->safe_psql('postgres', 'SELECT slot_name FROM pg_replication_slots ORDER BY slot_name');
is($stdout, q(ab_failover
ab_phys_failover
bb_failover
bb_phys_failover
onreplica), 'Expected slots exist on replica')
  or BAIL_OUT('Remaining tests meaningless');

# Make sure we can replay from the physical failover slot on the master
my $master_beforereplay_bbphys_si = get_slot_info($node_master, 'bb_phys_failover');
is($master_beforereplay_bbphys_si->{restart_lsn}, '',
  'restart_lsn on slot bb_phys_failover is empty before replay');
test_phys_replay($node_master, 'bb_phys_failover', 1);
my $master_afterreplay_bbphys_si = get_slot_info($node_master, 'bb_phys_failover');

cmp_ok(lsn_to_bigint($master_afterreplay_bbphys_si->{restart_lsn}),
       "gt",
       0,
       "bb_phys_failover restart_lsn advanced after replay");

$node_master->stop('fast');

my $log = TestLib::slurp_file($node_master->logfile);
unlike($log, '/PANIC:/', 'No PANIC in master logs');

my @slot_updates = @{ read_slot_updates_from_xlog($node_master, 1) };

#
# Decode the WAL from the master and make sure the expected entries and only the
# expected entries are present.
#
# We want to see two WAL entries, one for each slot. There won't be another entry
# for the slot advance because right now we don't write out WAL when a slot's confirmed
# location advances, only when the flush location or xmin advance. The restart lsn
# and confirmed flush LSN in the slot's WAL record must not be less than the LSN
# of the master before we created the slot and not greater than the position we saw
# in pg_replication_slots after slot creation.
#

# bb_failover created
check_slot_wal_update($slot_updates[0], 'bb_failover',
	expect_min_restart_lsn => $master_beforecreate_bb_lsn,
	expect_min_confirm_lsn => $master_beforecreate_bb_lsn,
	expect_max_restart_lsn => $bb_beforeconsume_si->{restart_lsn},
	expect_max_confirm_lsn => $bb_beforeconsume_si->{confirmed_flush_lsn});

# bb_phys_failover created
check_slot_wal_update($slot_updates[1], 'bb_phys_failover',
	expect_min_restart_lsn => '0/0',
	expect_min_confirm_lsn => '0/0',
	expect_max_restart_lsn => '0/0',
	expect_max_confirm_lsn => '0/0');

# bb_failover updated after replay. This only happens because we
# force a checkpoint to flush the dirtied but not written-out
# slot.
check_slot_wal_update($slot_updates[2], 'bb_failover',
	expect_min_restart_lsn => $master_beforecreate_bb_lsn,
	expect_min_confirm_lsn => $master_beforecreate_bb_lsn,
	expect_max_restart_lsn => $bb_afterconsume_si->{restart_lsn},
	expect_max_confirm_lsn => $bb_afterconsume_si->{confirmed_flush_lsn});

# Creation of ab_failover
check_slot_wal_update($slot_updates[3], 'ab_failover',
	expect_min_restart_lsn => $master_beforecreate_ab_lsn,
	expect_min_confirm_lsn => $master_beforecreate_ab_lsn,
	expect_max_restart_lsn => $ab_beforeconsume_si->{restart_lsn},
	expect_max_confirm_lsn => $ab_beforeconsume_si->{confirmed_flush_lsn});

# Creation of ab_phys_failover
check_slot_wal_update($slot_updates[4], 'ab_phys_failover',
	expect_min_restart_lsn => '0/0',
	expect_min_confirm_lsn => '0/0',
	expect_max_restart_lsn => '0/0',
	expect_max_confirm_lsn => '0/0');

# created after we replayed from bb_failover on the master
check_slot_wal_update($slot_updates[5], 'bb_phys_failover',
	expect_min_restart_lsn => $master_afterreplay_bbphys_si->{restart_lsn},
	expect_min_confirm_lsn => '0/0',
	expect_max_restart_lsn => $master_afterreplay_bbphys_si->{restart_lsn},
	expect_max_confirm_lsn => '0/0');

# Consuming from a slot does not cause the slot to be written out even on
# CHECKPOINT. Since nothing else would have dirtied the slot, there should
# be no more WAL entries for failover slots.
#
# The client is expected to keep track of the confirmed LSN and skip replaying
# data it's already seen.
ok(!defined($slot_updates[6]), 'No more slot updates');



# Can replay from physical failover slot on promoted replica


$node_replica->promote;

wait_for_end_of_recovery($node_replica);

$node_replica->safe_psql('postgres',
	"INSERT INTO decoding(blah) VALUES ('after failover');");

my $bb_afterpromote_si = get_slot_info($node_replica, 'bb_failover');
diag_slotinfo $bb_afterpromote_si, 'bb_afterpromote';
# Because we forced a checkpoint to flush the slot to disk after replaying from
# bb_failover it should have the new confirmed flush point on the replica.
is($bb_afterpromote_si->{confirmed_flush_lsn}, $bb_afterconsume_si->{confirmed_flush_lsn},
	'slot bb_failover confirmed pos on replica matches master');
# We haven't replayed much, so the restartpoint probably didn't change, but
# it should be wherever it was after we replayed anyway.
is($bb_afterpromote_si->{restart_lsn}, $bb_afterconsume_si->{restart_lsn},
	'slot bb_failover restart pos on replica matches master');

# We never replayed from the after-basebackup slot on the master so it
# should be right where it was created.
my $ab_afterpromote_si = get_slot_info($node_replica, 'ab_failover');
diag_slotinfo $ab_afterpromote_si, 'ab_afterpromote';
is($ab_afterpromote_si->{confirmed_flush_lsn}, $ab_beforeconsume_si->{confirmed_flush_lsn},
	'slot ab_failover confirmed pos is unchanged');
is($ab_afterpromote_si->{restart_lsn}, $ab_beforeconsume_si->{restart_lsn},
	'slot ab_failover restart pos is unchanged');




# Can replay from slot ab, following the timeline switch
test_read_from_slot($node_replica, 'ab_failover', q(BEGIN
table public.decoding: INSERT: blah[text]:'afterbb'
COMMIT
BEGIN
table public.decoding: INSERT: blah[text]:'after failover'
COMMIT));

# Can replay from slot bb too, and we only see data after
# what we replayed on the master.
#
# Note that if we didn't force a checkpoint on the master then did an unclean
# shutdown we would expect to see data that we already replayed on the master
# here.  The confirm lsn wouldn't be flushed on the master and would therefore
# effectively go backwards on failover.
#
# See http://www.postgresql.org/message-id/CAMsr+YGSaTRGqPcx9qx4eOcizWsa27XjKEiPSOtAJE8OfiXT-g@mail.gmail.com
#
test_read_from_slot($node_replica, 'bb_failover', q(BEGIN
table public.decoding: INSERT: blah[text]:'beforebb'
COMMIT
BEGIN
table public.decoding: INSERT: blah[text]:'afterbb'
COMMIT
BEGIN
table public.decoding: INSERT: blah[text]:'after failover'
COMMIT));


# Can replay from physical failover slot on promoted replica
test_phys_replay($node_replica, 'bb_phys_failover', 2);

$node_replica->stop('fast');

$log = TestLib::slurp_file($node_replica->logfile);
unlike($log, '/PANIC:/', 'No PANIC in replica logs');

# We don't need the standby anymore
$node_replica->teardown_node();



# Now make sure slot drop works correctly and replays correctly by restoring
# a fresh backup of the standby and having it replay the slot drops. We'll
# also test dropping a physical slot that's currently in-use.
$node_master->start;

# restore the replica again
$node_replica = get_new_node('replica2');
$node_replica->init_from_backup(
	$node_master, $backup_name,
	has_streaming => 1,
	has_restoring => 1);
$node_replica->start;


# start pg_receivexlog from a local slot on the replica. Then create a failover
# slot with the same name on the master. pg_receivexlog will be automatically
# killed when we drop the slot it's replaying from and replace it with a failover
# slot.
$node_replica->safe_psql('postgres',
"SELECT pg_create_physical_replication_slot('replace_me', false, false);");

my $si = get_slot_info($node_replica, 'replace_me');
diag_slotinfo($si);
is($si->{failover}, 'f', 'created as slot replace_me as non-failover');

($proc, $stdout, $stderr, $outdir) = start_pg_receivexlog($node_replica, 'replace_me');

$node_master->safe_psql('postgres',
"SELECT pg_create_physical_replication_slot('replace_me', false, true);");

wait_for_catchup($node_master, $node_replica);

# pg_receivexlog should've died
$proc->finish;
is($proc->result, 1, 'pg_receivexlog exited with error code after its slot was dropped');
like($$stdout, '', 'no stdout');
like($$stderr, qr/by administrative command/, 'pg_receivexlog exited with admin command');

# The slot is now a failover slot
$si = get_slot_info($node_replica, 'replace_me');
is($si->{failover}, 't', 'failover slot successfully replaces local slot');

# OK, make sure slot drops replay correctly

$node_master->safe_psql("postgres", "SELECT pg_drop_replication_slot('bb_failover');");
$node_master->safe_psql("postgres", "SELECT pg_drop_replication_slot('ab_failover');");
$node_master->safe_psql("postgres", "SELECT pg_drop_replication_slot('bb_phys_failover');");
$node_master->safe_psql("postgres", "SELECT pg_drop_replication_slot('ab_phys_failover');");
$node_master->safe_psql("postgres", "SELECT pg_drop_replication_slot('replace_me');");

wait_for_catchup($node_master, $node_replica);


$stdout = $node_replica->safe_psql('postgres', 'SELECT slot_name FROM pg_replication_slots ORDER BY slot_name');
is($stdout, '', 'No slots exist on replica')
  or BAIL_OUT('Remaining tests meaningless');


# OK, now we need to test replay of a big enough chunk of data to advance the restart_lsn
# and make the master do a checkpoint.
#
# We create two copies of the slot so we can advance one of them and get the changes
# checkpointed out, while leaving the other unchanged for replay after failover.
# This just lets us test two things in one: checkpointing of failover slots and
# failover with big chunks of data.

$node_master->safe_psql('postgres',
"SELECT pg_create_logical_replication_slot('big', 'test_decoding', true); SELECT pg_create_logical_replication_slot('big_adv', 'test_decoding', true);"
);

$node_master->safe_psql('postgres',
  "CREATE TABLE big_inserts (id serial primary key, text padding);"
);

$node_master->safe_psql('postgres',
  "INSERT into big_inserts(padding) SELECT repeat('x', n % 100) FROM generate_series(1, 1000000) n;"
);

($ret, $stdout, $stderr) = $node_master->psql('postgres',
	"SELECT data FROM pg_logical_slot_get_changes('big_adv', NULL, NULL, 'include-xids', '0', 'skip-empty-xacts', '1');");
is($ret, 0, 'replaying from slot big_adv on master is successful');
my $data_replayed_from_master = $stdout;
is($stderr, '', 'replay from slot big_adv produces no stderr');

wait_for_catchup($node_master, $node_replica);
$node_master->stop('fast');
$node_replica->promote;
wait_for_end_of_recovery($node_replica);

($ret, $stdout, $stderr) = $node_replica->psql('postgres',
	"SELECT data FROM pg_logical_slot_peek_changes('big', NULL, NULL, 'include-xids', '0', 'skip-empty-xacts', '1');");
is($ret, 0, 'replaying from slot big on replica is successful');
is($stdout, $data_replayed_from_master, 'Got same data from replica as master');
is($stderr, '', 'replay from slot big produces no stderr');

$node_replica->stop('fast');

# Make sure there's no crash complaint in the master or replica logs
$log = TestLib::slurp_file($node_master->logfile);
unlike($log, '/PANIC:/', 'No PANIC in master logs');

$log = TestLib::slurp_file($node_replica->logfile);
unlike($log, '/PANIC:/', 'No PANIC in replica logs');

$node_master->teardown_node;
$node_replica->teardown_node;

done_testing();
