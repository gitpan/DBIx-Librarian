package DBIx::Librarian::Statement;

require 5.004;
use strict;
use Carp;

=head1 NAME

DBIx::Librarian::Statement - an active SQL statement in a Librarian

=head1 SYNOPSIS

Internal class used by DBIx::Librarian.  Implementation of BUILDER
pattern (Librarian is the Director, Statement is the Builder).

Recognizes the following extensions to the SQL SELECT directive:

    SELECT*	return zero or more rows as an array
    SELECT?	return zero or one rows as a scalar
    SELECT1	return exactly one row as a scalar

For the SELECT? and SELECT1 flavors, an exception will be raised if
more than one row is returned.  For the SELECT1 flavor, an exception
will be raised if no rows are found.

The default behavior for an unadorned SELECT is multi-row SELECT*.

=head1 METHODS

=cut

my %select_mode = (
		   "*"	=> "SelectMany",
		   "?"	=> "SelectOne",
		   "1"	=> "SelectExactlyOne",
		   ""	=> "SelectMany",
		  );

=item B<new>

  my $stmt = new DBIx::Librarian::Statement ($dbh, $sql);

Prepares the SQL statement in $sql against the database connection
in $dbh.  Handles bind variables and direct substitution.

=cut

sub new {
    my ($proto, $dbh, $sql, %config) = @_;

    my $class = ref ($proto) || $proto;
    my $self = {
		DBH => $dbh
	       };
    while (my ($key, $val) = each %config) {
	$self->{$key} = $val;
    }


    # WARNING: Oracle does not like ? placeholders inside comments.
    #   If Statement thinks that the ? in the comment is a bind value
    #   and includes a value for it in the execute() list, Oracle receives
    #   more values than it expects.
    #   mysql seems to handle this correctly.
    #   May need to strip comments from SQL before converting placeholders.
    #   Yuck.  Is there a cross-platform way to do this?


    my @bindvars = $sql =~ /[^A-Za-z0-9:]:(\w+)/mog;
    if (@bindvars) {
	$sql =~ s/([^A-Za-z0-9:]):\w+/$1?/og;
    }
    $self->{BINDVARS} = \@bindvars;

    if ($sql =~ /^select/io) {
	my ($mode) = $sql =~ /^select(\S*)/io;

	croak "Unrecognized select mode $mode in\n$sql\n"
	  unless $select_mode{$mode};

	# delegate SELECT processing based on expected rows returned

	$class = __PACKAGE__ . "::$select_mode{$mode}";
	my $classpath = $class;
	$classpath =~ s{::}{/}g;
	require "$classpath.pm";

	$sql =~ s/^select\S*/select/io;

	$self->{IS_SELECT} = 1;
    }


    bless ($self, $class);


    my @directvars;
    if ($sql =~ /\$\w+/o) {
	# requires on-demand parsing
	@directvars = $sql =~ /\$(\w+)/og;
	$self->{DIRECTVARS} = \@directvars;
	$self->{SQL} = $sql;
    }
    else {
	# can prepare in advance
	$self->_prepare($sql);
    }

    return $self;
}


sub _prepare {
    my ($self, $sql) = @_;

    print STDERR "PREPARE SQL:\n", $sql, "\n====================\n"
      if $self->{TRACE};

    my $sth = $self->{DBH}->prepare($sql);
    if (!$sth) {
	croak $self->{DBH}->errstr;
    }
    $self->{STH} = $sth;
}


=item B<execute>

  $stmt->execute($data);

Returns the number of rows affected for INSERTs, UPDATEs and DELETES;
zero for SELECTs.
Croaks on any database error or if any SELECT criteria are violated.

=cut

sub execute {
    my ($self, $data) = @_;

    $self->_substitutions($data);

    my @bindlist = $self->_bind($data);

    if ($self->{IS_SELECT} && !$data) {
	croak "Missing target data reference in SQL\n$self->{STH}->{Statement}\n";
    }

    if (! $self->{STH}->execute(@bindlist)) {
	croak $self->{DBH}->errstr. " in SQL\n$self->{STH}->{Statement}\n";
    }

    if ($self->{IS_SELECT}) {
	return $self->fetch($data);
#	return 0;
    }
    else {
	return $self->{STH}->rows;
    }
}


sub _substitutions {
    my ($self, $data) = @_;

    return unless $self->{DIRECTVARS};

    # The SQL contains "$parameter" substitutions.
    # Must be re-prepared before every execution.

    my $sql = $self->{SQL};
    foreach my $directvar (@{$self->{DIRECTVARS}}) {
	my $val;
	if ($self->{ALLARRAYS}) {
	    $val = $data->{$directvar}[0];
	} else {
	    croak "Expected scalar for $directvar" if ref($data->{$directvar});
	    $val = $data->{$directvar};
	}
	printf STDERR ("\tSUB \$%s = %s\n",
		       $directvar,
		       $val || '(null)') if $self->{TRACE};
	$sql =~ s/\$$directvar(\W|$)/$val$1/g;
    }

    my $sth = $self->{DBH}->prepare($sql);
    if (!$sth) {
	croak $self->{DBH}->errstr . " in SQL\n$sql\n";
    }
    $self->{STH} = $sth;

#    print STDERR "SUBSITUTION COMPLETE:\n", $sql, "\n";
}


sub _bind {
    my ($self, $data) = @_;

    return unless $self->{BINDVARS};

    # The SQL contains ":parameter" placeholders, which have already
    # been converted to standard ? markers by prepare().  Pull the
    # list of bind variables.

    my @bindlist;

    foreach my $bindvar (@{$self->{BINDVARS}}) {
	my $val;
	if ($self->{ALLARRAYS}) {
	    $val = $data->{$bindvar}[0];
	} else {
	    croak "Expected scalar for $bindvar" if ref($data->{$bindvar});
	    $val = $data->{$bindvar};
	}

	printf STDERR ("\tBIND :%s = %s\n",
		       $bindvar,
		       $val || '(null)') if $self->{TRACE};
	push @bindlist, $val;
    }

    return @bindlist;
}

1;

=head1 AUTHOR

Jason W. May <jmay@pobox.com>

=head1 COPYRIGHT

Copyright (C) 2001 Jason W. May.  All rights reserved.
This module is free software; you can redistribute it and/or
modify it under the same terms as Perl itself.
