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
system ("mysql -v -D test < t/bugdb.ddl") and die;

my $dblbn = new DBIx::Librarian ({
				  LIB => ["t"],
#				  EXTENSION => "sql",
#				  TRACE => 1
				 });

# test series with default archiver
runtest();

$dblbn->disconnect;

my $archiver = new DBIx::Librarian::Library::ManyPerFile({
							  LIB => ["t"],
							  EXTENSION => "msql",
							  });
$dblbn = new DBIx::Librarian ({
			       ARCHIVER => $archiver,
			      });

# test series with ManyPerFile archiver
runtest();

$dblbn->disconnect;

# Dismantle test tables
system ("echo 'drop table BUG' | mysql -v -D test") and die;

exit;

sub runtest {

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

eval { $dblbn->execute("t_insert", $data); };
print STDERR $@ if $@;

ok($data->{bugid} == 5, "insert, no bind variables");

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
######################################################################
######################################################################
######################################################################

}
