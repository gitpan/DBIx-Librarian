#!/usr/bin/perl -w

use strict;
use Data::Dumper;
use Test::Simple qw/no_plan/;

use lib ".";

use DBIx::Librarian;
use DBIx::Librarian::Library::ManyPerFile;

$ENV{DBI_DSN}="dbi:mysql:test";

my $data = {};

######################################################################

# Erect test tables
system ("mysql -v -D test < tests/bugdb.ddl") and do {
    print STDERR "mysql not operational on this system.  Aborting tests.\n";
    exit;
};

my $dblbn = new DBIx::Librarian ({
				  LIB => ["tests"],
#				  EXTENSION => "sql",
				  TRACE => 1
				 });

# test series with default archiver
runtest();

$dblbn->disconnect;

my $archiver = new DBIx::Librarian::Library::ManyPerFile({
							  LIB => ["tests"],
							  EXTENSION => "msql",
							  });
$dblbn = new DBIx::Librarian ({
			       ARCHIVER => $archiver,
			       TRACE => 1,
			      });

# test series with ManyPerFile archiver
runtest();

$dblbn->disconnect;

# Dismantle test tables
system ("echo 'drop table BUG' | mysql -v -D test") and die;

exit;

sub runtest {

my @toc = $dblbn->{SQL}->toc;
#print join(",", @toc), "\n";
ok (scalar(@toc) == 6, "toc");

######################################################################
# DELETE to prepare for test scenario

eval { $dblbn->execute("t_delete", $data); };
ok (!$@, "delete");

######################################################################
# test prepare

eval { $dblbn->prepare("t_insert_bind") };
ok (!$@, "successful prepare");

eval { $dblbn->prepare("t_no_such_query",
		       "t_select_bug") };
ok ($@, "successful prepare");

######################################################################
# INSERT with no bind variable
# SELECT check to verify that one row was inserted

my @results;
eval { @results = $dblbn->execute("t_insert", $data); };
print STDERR $@ if $@;

ok($data->{bugid} == 5, "insert, no bind variables");
ok($results[0] == 2
   && $results[1]->[0] == 1
   && $results[1]->[1] == 1, "rowcounts");


# force disconnect to make sure Librarian reconnects correctly
$dblbn->{DBH}->disconnect;

######################################################################
# INSERT with bind variables
# SELECT check to verify that one row was inserted

$data->{groupset} = 17;
$data->{assigned_to} = 9;
$data->{product} = "Perl";
#$data->{product} = [ "foo" ];

eval { $dblbn->execute("t_insert_bind", $data); };
print STDERR $@ if $@;

ok($data->{bugid} == 7, "insert, with bind variables");

######################################################################
# multi-column SELECT

$data->{groupset} = 42;

eval { $dblbn->execute("t_select_row", $data); };
print STDERR $@ if $@;

ok($data->{bugid} == 5, "single-row select");

######################################################################
# multi-row SELECT

eval { $dblbn->execute("t_select_all", $data); };
print STDERR $@ if $@;

ok(scalar(@{$data->{bug}}) == 2, "multi-row select");

######################################################################
# repeat SELECT

my $i = 0;
foreach my $bug (@{$data->{bug}}) {
    eval { $dblbn->execute("t_select_bug", $bug); };
    print STDERR $@ if $@;
    $i++ if length $bug->{product} > 0;
}
ok($i == 2, "fetching rows from sub-level of data");

######################################################################
# try an non-existent query

eval { $dblbn->execute("t_does_not_exist", $data) };
ok($@, "t_does_not_exist not found");

ok (! $dblbn->can("t_does_not_exist"), "cannot");
ok ($dblbn->can("t_select_bug"), "can");

######################################################################

eval { $dblbn->execute("t_select_all"); };
print STDERR $@ if $@;
ok ($@, "missing data");

######################################################################
######################################################################

}
