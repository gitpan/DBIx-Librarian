package DBIx::Librarian::Statement;

require 5.004;
use strict;
use Carp;

=head1 NAME

DBIx::Librarian::Statement - an active SQL statement in a Librarian

=head1 SYNOPSIS

Internal class used by DBIx::Librarian.

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
    my ($proto, $dbh, $sql) = @_;

    my $class = ref ($proto) || $proto;
    my $self = {};


    $self->{DBH} = $dbh;


    my @bindvars = $sql =~ /[^\\]:(\w+)/og;
    if (@bindvars) {
	$sql =~ s/([^\\]):\w+/$1?/og;
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

    if (! $self->{STH}->execute(@bindlist)) {
	croak $self->{DBH}->errstr. " in SQL\n$self->{STH}->{Statement}\n";
    }

    if ($self->{IS_SELECT}) {
	$self->fetch($data);
	return 0;
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
	if ($self->{ALLARRAYS}) {
	    $sql =~ s/\$$directvar(\W|$)/$data->{$directvar}[0]/g;
	} else {
	    croak "Expected scalar for $directvar" if ref($data->{$directvar});
	    $sql =~ s/\$$directvar(\W|$)/$data->{$directvar}/g;
	}
    }

    my $sth = $self->{DBH}->prepare($sql);
    if (!$sth) {
	croak $self->{DBH}->errstr . " in SQL\n$sql\n";
    }
    $self->{STH} = $sth;
}


sub _bind {
    my ($self, $data) = @_;

    return unless $self->{BINDVARS};

    # The SQL contains ":parameter" placeholders, which have already
    # been converted to standard ? markers by prepare().  Pull the
    # list of bind variables.

    my @bindlist;

    foreach my $bindvar (@{$self->{BINDVARS}}) {
	if ($self->{ALLARRAYS}) {
	    push @bindlist, $data->{$bindvar}[0];
	} else {
	    croak "Expected scalar for $bindvar" if ref($data->{$bindvar});
	    push @bindlist, $data->{$bindvar};
	}
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
