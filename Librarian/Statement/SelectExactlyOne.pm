package DBIx::Librarian::Statement::SelectExactlyOne;

require 5.004;
@ISA = "DBIx::Librarian::Statement";
use strict;
use Carp;

=head1 NAME

DBIx::Librarian::Statement::SelectExactlyOne - strict one-row SELECT statement

=head1 DESCRIPTION

SELECT statement that expects to retrieve exactly one record from
the database.  An exception is raised if zero rows are found or
if more than one row is found.

All values fetched will be stored in top-level scalars in the data hash.

=cut

sub fetch {
    my ($self, $data) = @_;

    my $hash_ref = $self->{STH}->fetchrow_hashref;

    if (!$hash_ref) {
	croak "Expected exactly one row but received none for\n" . $self->{STH}->{Statement};
    }

    while (my ($key, $val) = each %$hash_ref) {
	if ($self->{ALLARRAYS}) {
	    $data->{$key}[0] = $val;
	} else {
	    $data->{$key} = $val;
	}
    }

    if ($self->{STH}->fetchrow_hashref) {
	croak "Expected exactly one row; received more than one for\n" . $self->{STH}->{Statement};
    }
}


1;

=head1 AUTHOR

Jason W. May <jmay@pobox.com>

=head1 COPYRIGHT

Copyright (C) 2001 Jason W. May.  All rights reserved.
This module is free software; you can redistribute it and/or
modify it under the same terms as Perl itself.
