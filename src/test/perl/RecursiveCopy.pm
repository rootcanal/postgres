
=pod

=head1 NAME

RecursiveCopy - simple recursive copy implementation

=head1 SYNOPSIS

use RecursiveCopy;

RecursiveCopy::copypath($from, $to);

=cut

package RecursiveCopy;

use strict;
use warnings;

use File::Basename;
use File::Copy;

=pod

=head2 copypath($from, $to)

Copy all files and directories from $from to $to. Raises an exception
if a file would be overwritten, the source dir can't be read, or any
I/O operation fails.  Always returns true. On failure the copy may be
in some incomplete state; no cleanup is attempted.

If the keyword param 'filterfn' is defined it's invoked as a sub that
returns true if the file/directory should be copied, false otherwise.
The passed path is the full path to the file relative to the source
directory.

e.g.

RecursiveCopy::copypath('/some/path', '/empty/dir',
	filterfn => sub {^
		# omit children of pg_log
		my $src = shift;
		return ! $src ~= /\/pg_log\//
	}
);

=cut

sub copypath
{
	my ($srcpath, $destpath, %params) = @_;

	die("if specified, 'filterfn' must be a sub ref")
	  if defined $params{filterfn} && !ref $params{filterfn};

	my $filterfn;
	if (defined $params{filterfn})
	{
		$filterfn = $params{filterfn};
	}
	else
	{
		$filterfn = sub { return 1; };
	}

	return _copypath_recurse($srcpath, $destpath, $filterfn);
}

# Recursive private guts of copypath
sub _copypath_recurse
{
	my ($srcpath, $destpath, $filterfn) = @_;

	die "Cannot operate on symlinks" if -l $srcpath or -l $destpath;

	# This source path is a file, simply copy it to destination with the
	# same name.
	die "Destination path $destpath exists as file" if -f $destpath;
	if (-f $srcpath)
	{
		if ($filterfn->($srcpath))
		{
			copy($srcpath, $destpath)
			  or die "copy $srcpath -> $destpath failed: $!";
		}
		return 1;
	}

	die "Destination needs to be a directory" unless -d $srcpath;
	mkdir($destpath) or die "mkdir($destpath) failed: $!";

	# Scan existing source directory and recursively copy everything.
	opendir(my $directory, $srcpath) or die "could not opendir($srcpath): $!";
	while (my $entry = readdir($directory))
	{
		next if ($entry eq '.' || $entry eq '..');
		RecursiveCopy::_copypath_recurse("$srcpath/$entry",
			"$destpath/$entry", $filterfn)
		  or die "copypath $srcpath/$entry -> $destpath/$entry failed";
	}
	closedir($directory);
	return 1;
}

1;
