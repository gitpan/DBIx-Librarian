package DBIx::Librarian::Library::OnePerFile;

=head1 NAME

DBIx::Librarian::Library::OnePerFile - one-query-per-file support class
for DBIx::Librarian

=head1 SYNOPSIS

Provides repository service to DBIx::Librarian.  This package
supports SQL in template files, where each file contains a
single query block.  A query tag corresponds to a filename
(tag.EXTENSION where EXTENSION is specified at initialization
and defaults to ".sql").  Searching will be done through a
list of directories.  The first matching file will be used.
Conflicts are not detected.

OnePerFile recognizes when a query file is changed, and will
instruct DBIx::Librarian to reload the query.

=head1 METHODS

=cut

require 5.004;
use strict;
use Carp;

my %parameters = (
		  "LIB"	=> [ "sql" ],
		  "EXTENSION" => "sql",
		  "TRACE" => undef,
		 );

=item B<new>

  my $archiver = new DBIx::Librarian::Library::OnePerFile
		  ({ name => "value" ... });

Supported Library::OnePerFile parameters:

  LIB         Search path for SQL files.  Defaults to [ "sql" ]

  EXTENSION   Filename extension for SQL files.  Defaults to ".sql"

  TRACE       Turns on function tracing in this package.

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
	    croak "Undefined parameter $key";
	}
    }

    foreach my $key (keys %parameters) {
	$self->{$key} = $parameters{$key} unless defined $self->{$key};
    }
}


=item B<new>

  $archiver->lookup($tag);

Returns cached statement handles.  If the source has changed since
it was cached, returns false.

=cut

sub lookup {
    my ($self, $tag) = @_;

    print STDERR "LOOKUP $tag\n" if $self->{TRACE};

    if (! $self->_cache_valid($tag)) {
	return;
    }

    return $self->{$tag}->{STMTS};
}


sub _cache_valid {
    my ($self, $tag) = @_;

    return unless defined $self->{$tag};
    return unless defined $self->{$tag}->{STMTS};

    return unless ($self->{$tag}->{LOADTS}
		   >= (stat($self->{$tag}->{FILE}))[9]);

    return 1;
}


=item B<new>

  $archiver->find($tag);

Searches through the directory path in LIB for a query file named
"$tag.EXTENSION".  Returns the contents of that file if successful,
and records the path for subsequent checking by lookup().

=cut

sub find {
    my ($self, $tag) = @_;

    print STDERR "FIND $tag\n" if $self->{TRACE};

    my $file;
    foreach my $lib (@{$self->{LIB}}) {
	$file = "$lib/$tag.$self->{EXTENSION}";
	next unless -r $file;
    }
    if (! -r $file) {
	# never found a matching readable file
	croak "Unable to read .$self->{EXTENSION} file for tag $tag";
    }

    open(INPUT, $file) or croak "Open $file failed: $!";
    local $/ = undef;
    my $sql = <INPUT>;
    close INPUT;

    print STDERR "FOUND $tag in $file\n" if $self->{TRACE};

    $self->{$tag}->{FILE} = $file;
    $self->{$tag}->{LOADTS} = (stat($self->{$tag}->{FILE}))[9];

    return $sql;
}


=item B<cache>

  $archiver->cache($tag, $data);

Caches statement handles for later fetching via lookup().

=cut

sub cache {
    my ($self, $tag, $data) = @_;

    print STDERR "CACHE $tag\n" if $self->{TRACE};

    $self->{$tag}->{STMTS} = $data;
}


=item B<toc>

  my @array = $archiver->toc();

Search through the library and return a list of all available entries.
Does not import any of the items.

=cut

sub toc {
    my ($self) = @_;

    my %items;
    foreach my $lib (@{$self->{LIB}}) {
	opendir DIR, $lib or die "open $lib failed: $!";
	foreach my $file (readdir DIR) {
	    next unless $file =~ /\.$self->{EXTENSION}$/;
	    $items{$file}++;
	}
	closedir DIR;
    }

    return sort keys %items;
}


1;

=head1 AUTHOR

Jason W. May <jmay@pobox.com>

=head1 COPYRIGHT

Copyright (C) 2001 Jason W. May.  All rights reserved.
This module is free software; you can redistribute it and/or
modify it under the same terms as Perl itself.
