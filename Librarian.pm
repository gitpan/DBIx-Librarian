package DBIx::Librarian;

require 5.005;
use strict;
#use warnings;			# needs 5.6
use vars qw($VERSION);

$VERSION = '0.1';

use DBIx::Librarian::Statement;

=head1 NAME

DBIx::Librarian - Manage SQL in repository outside code

=head1 SYNOPSIS

  use DBIx::Librarian;

  my $dblbn = new DBIx::Librarian;

  my $data = { id => 473 };
  eval { $dblbn->execute("lookup_employee", $data); };
  die $@ if $@;
  print "Employee $data->{id} is $data->{name}\n";

  $dblbn->disconnect;

=head1 OBJECTIVES

Separation of database logic from application logic (SQL from Perl)

Simple interface - sacrifices some flexibility in exchange for
code readability and development speed

Leave SQL syntax untouched if possible; support any extensions that are
supported by the underlying database

Support transaction capability if the database allows it

This is NOT an object-to-relational-mapping toolkit or a persistence
framework.  For that sort of thing, see SPOPS or any of several other
excellent modules.  The combination of DBIx::Librarian and Template
Toolkit or one of the other templating packages will give the basis
of a fairly comprehensive database-driven application framework.

=head1 FEATURES

=over

=item *

Support full complexity of Perl associative data structures

=item *

Multiple SQL statements chained in a single execute() invocation.
Use results from one call as inputs to the next.

=item *

Each execute() is automatically a transaction, comprising one or
more statements per the above.  Optional delayed commit to
collect multiple invocations into a transaction.  Note that if your
database doesn't support transactions (e.g. vanilla mySQL), then
you're still out of luck here.

=item *

Processing modes for select statements: exactly one row, zero-or-one,
multiple rows (zero to many); optional exception on receiving multiple
rows when expecting just one.  SQL syntax is extended to provide these
controls.

=item *

Support bind variables, and on-the-fly SQL generation through substitution
of entire SQL fragments.

=item *

Supports multiple repositories for queries - currently supports
individual files, multiple-query files, and SQL::Catalog.

=item *

Database connection can be passed into the Librarian initializer, or
it will create it internally.

=back

=head1 ENVIRONMENT VARIABLES

DBIx::Librarian will use the following:

  DBI_DSN       standard DBI connection parameters
  DBI_USER
  DBI_PASS

  DBIX_LIBRARIAN_TRACE  turns on basic internal logging


=head1 DESCRIPTION

This is for data manipulation (SELECT, INSERT, UPDATE, DELETE), not for
data definition (CREATE, DROP, ALTER).  Some DDL statements may work
inside this module, but correct behavior is not guaranteed.

Results of "SELECT1 colname FROM table", expected to return a single row:

    {
      colname => "value"
    }

  Access via $data->{colname}

  If more than one row is returned, raise an exception.

Results of "SELECT* colname FROM table", expected to return multiple rows
(note alteration to standard SQL syntax):

  [
    {
      colname => "vala"
    },
    {
      colname => "valb"
    },
    {
      colname => "valc"
    }
  ]

  Access via $data->[n]->{colname}

Results of "SELECT1 col1, col2 FROM table", expected to return a single row:

    {
      col1 => "valA",
      col2 => "valB",
    }

  Access via $data->{colname}

  If more than one row is returned, raise an exception.

Results of

    SELECT*  col1 "record.col1",
             col2 "record.col2",
             col3 "record.col3"
    FROM table

expected to return multiple rows:

  {
    record =>
      [
        {
          col1 => "val1a",
          col2 => "val2a",
          col3 => "val3a"
        },
        {
          col1 => "val1b",
          col2 => "val2b",
          col3 => "val3b"
        },
        {
          col1 => "val1c",
          col2 => "val2c",
          col3 => "val3c"
        },
      ]
  }

  Access via $data->{record}[n]->{colname}

=head1 TO DO

=over

=item *

Endeavor to consolidate some of this work with other similar modules

=item *

Optional constraint on number of rows returned by SELECT statements

=item *

Optional cancellation of long-running queries

=item *

Verbosity controls for logging during initialization and query execution;
tie in with DBI tracing

=item *

Limits on number of cached statement handles.  Some databases may place
limits on the number of concurrent handles.  Some sort of LRU stack of
handles would be useful for this.

=item *

Consider whether DBI Taint mode would be appropriate here.

=item *

Make sure this works properly with threads.

=item *

Improve regex matching for substitution variables in SQL statements so
they handle quoting and comments.

=item *

Support for Oracle PL/SQL and similar database language extensions.

=back

=head1 WARNINGS

You must call $dblbn->disconnect explicitly before your program terminates.


This module uses strict throughout.  There is one notable side-effect;
if you have a scalar value in a hash element:

    $data->{name} = "John"

and you run a multi-row SELECT with the same field as a target:

    select* name,
            department
    from    EMPLOYEE

then you are likely to get an error like this:

    Can't use string ("John") as an ARRAY ref while "strict refs"
    in use at .../DBIx/Librarian/Statement/SelectMany.pm line XXX.

This is because it is trying to write values into

    $data->{name}[0]
    $data->{name}[1]
    etc.

Recommended syntax for multi-row, multi-column SELECTs is:

    select* name "employee.name",
            department "employee.dept"
    from    EMPLOYEE

so then you can access the information via

    $data->{employee}[0]->{name}
    $data->{employee}[0]->{dept}
    $data->{employee}[1]->{name}
    etc.

=head1 METHODS

=cut

use DBI;
use Carp;

my %select_mode = (
		   "*"	=> "zero_or_more",
		   "?"	=> "zero_or_one",
		   "1"	=> "exactly_one",
		   ""	=> "zero_or_more",
		  );

my %parameters = (
		  "ARCHIVER" => undef,
		  "LIB"	=> undef,
		  "EXTENSION" => undef,
		  "AUTOCOMMIT" => 1,
		  "ALLARRAYS" => 0,
		  "DBH" => undef,
		  "DBI_DSN" => undef,
		  "DBI_USER" => undef,
		  "DBI_PASS" => undef,
		  "TRACE" => undef,
		 );

=item B<new>

  my $dblbn = new DBIx::Librarian({ name => "value" ... });

Supported Librarian parameters:

  ARCHIVER    Reference to class responsible for caching SQL statements.
              Default is DBIx::Librarian::Library::OnePerFile.

  LIB         If set, passed through to archiver

  EXTENSION   If set, passed through to archiver

  AUTOCOMMIT  If set, will commit() upon completion of all the SQL
              statements in a tag.  If not set, the application must
              call $dblbn->commit directly.  Default is set.

  ALLARRAYS   If set, all bind and direct substition variables will
              be obtained from element 0 of the named array, rather
              than from scalars.  Default is off.

  DBH         If set, Librarian will use this database handle and
              will not open one itself.

  DBI_DSN     passed directly to DBI::connect
  DBI_USER    passed directly to DBI::connect
  DBI_PASS    passed directly to DBI::connect

  TRACE       Turns on function tracing in this package.
              Can be set via environment variable DBIX_LIBRARIAN_TRACE.
              Passed through to archiver if set.

=cut

sub new {
    my ($proto, $config) = @_;
    my $class = ref ($proto) || $proto;

    my $self  = $config || {};

    bless ($self, $class);

    $self->_init;

    return $self;
}


sub _init {
    my ($self) = shift;

    # verify input params and set defaults
    # dies on any unknown parameter
    # fills in the default for anything that is not provided

    foreach my $key (keys %$self) {
	if (!exists $parameters{$key}) {
	    croak "Undefined Librarian parameter $key";
	}
    }

    foreach my $key (keys %parameters) {
	$self->{$key} = $parameters{$key} unless defined $self->{$key};
    }

    $self->{TRACE} = $ENV{DBIX_LIBRARIAN_TRACE} unless $self->{TRACE};

    if (! defined $self->{DBH}) {
	$self->_connect;
    }

    $self->_init_archiver;
}


sub _init_archiver {
    my ($self) = shift;

    my $archiver = $self->{ARCHIVER};
    my $config = {};
    $config->{LIB} = $self->{LIB} if $self->{LIB};
    $config->{EXTENSION} = $self->{EXTENSION} if $self->{EXTENSION};
    $config->{TRACE} = $self->{TRACE} if $self->{TRACE};

    if (!$archiver) {
	# use default archiver

	use DBIx::Librarian::Library::OnePerFile;
	$archiver = new DBIx::Librarian::Library::OnePerFile($config);
    }

    $self->{SQL} = $archiver;
}


sub _connect {
    my ($self) = shift;

    my $dbh = DBI->connect (
			    $self->{DBI_DSN},
			    $self->{DBI_USER},
			    $self->{DBI_PASS},
			    {
			     RaiseError => 0,
			     PrintError => 0,
			     AutoCommit => 0
			    }
			   );
    if (!$dbh) {
	croak $DBI::errstr;
    }

    $self->{DBH} = $dbh;
}


sub prepare {
    my ($self, @tags) = @_;

    foreach my $tag (@tags) {
	if (! $self->{SQL}->lookup($tag)) {
	    $self->_prepare($tag);
	}
    }
}


=item B<execute>

  $dblbn->execute("label", $data);

$data is assumed to be a hash reference.  Inputs for bind variables will
be obtained from $data.  SELECT results will be written back to $data.

The SQL block is obtained from the repository specified above.

Return value is the number of non-SELECT SQL statements that were
executed, if you find that useful.

=cut

sub execute {
    my ($self, $tag, $data) = @_;

    my $prepped = $self->{SQL}->lookup($tag);
    if (!$prepped) {
	$prepped = $self->_prepare($tag);
    }

    print STDERR "EXECUTE $tag\n" if $self->{TRACE};

    return $self->_execute($prepped, $data);
}


sub _prepare {
    my ($self, $tag) = @_;

    my $sql = $self->{SQL}->find($tag);

    print STDERR "PREPARE $tag\n" if $self->{TRACE};

    my @stmts = grep { !/^\s*$/ } split (/\s*(\n\s*){2,}/, $sql);

    my @preps;

    foreach my $stmt (@stmts) {
	$stmt =~ s/;$//o;	# erase any trailing semicolons
	if ($stmt =~ /^include\s+/io) {
	    my ($include) = $stmt =~ /^include\s+(\S+)/;
	    push @preps, $include;
	    $self->_prepare($include);
	} else {
	    my $statement = new DBIx::Librarian::Statement ($self->{DBH},
							    $stmt);
	    $statement->{ALLARRAYS} = $self->{ALLARRAYS};
	    push @preps, $statement;
	}
    }

    $self->{SQL}->cache($tag, \@preps);

    return \@preps;
}


sub _execute {
    my ($self, $prep, $data) = @_;

    my $update_count = 0;

    foreach my $stmt_prep (@{$prep}) {
	if (!ref($stmt_prep)) {
	    # found an include
	    $update_count += $self->execute($stmt_prep, $data);
	} else {
	    eval { $update_count += $stmt_prep->execute($data); };
	    if ($@) {
		$self->rollback;
		die $@;
	    }
	}
    }

    if ($update_count && $self->{AUTOCOMMIT}) {
	# there was at least one non-SELECT, so better commit here
	$self->commit;
    }

    return $update_count;
}


=item B<commit>

Invokes commit() on the database handle.  Not needed unless
$dblbn->delaycommit() has been called.

=cut

sub commit {
    my ($self) = @_;

    $self->{DBH}->commit;
}

=item B<rollback>

Invokes rollback() on the database handle.  Not needed unless
$dblbn->delaycommit() has been called.

=cut

sub rollback {
    my ($self) = @_;

    $self->{DBH}->rollback;
}

=item B<autocommit>

Sets the AUTOCOMMIT flag.  Once set, explicit commit and rollback
are not needed.

=cut

sub autocommit {
    my ($self) = @_;

    $self->{AUTOCOMMIT} = 1;
}

=item B<delaycommit>

Clears the AUTOCOMMIT flag.  Explicit commit and rollback will be
needed to apply changes to the database.

=cut

sub delaycommit {
    my ($self) = @_;

    $self->{AUTOCOMMIT} = 0;
}

=item B<disconnect>

  $dblbn->disconnect;

Disconnect from the database.  Database handle and any active statements
are discarded.

=cut

sub disconnect {
    my ($self) = @_;

    $self->{DBH}->disconnect if $self->{DBH};
    undef $self->{DBH};
    undef $self->{SQL};
}

1;

=head1 AUTHOR

Jason W. May <jmay@pobox.com>

=head1 COPYRIGHT

Copyright (C) 2001 Jason W. May.  All rights reserved.
This module is free software; you can redistribute it and/or
modify it under the same terms as Perl itself.

=head1 TEST SUITE

Under development.

=head1 SEE ALSO

  Class:Phrasebook::SQL
  Ima::DBI
  SQL::Catalog
  DBIx::SearchProfiles
  DBIx::Abstract

  Relevant links stolen from SQL::Catalog documentation:
    http://perlmonks.org/index.pl?node_id=96268&lastnode_id=96273
    http://perlmonks.org/index.pl?node=Leashing%20DBI&lastnode_id=96268
