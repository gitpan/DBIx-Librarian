package DBIx::Librarian::Library::ManyPerFile;

=head1 NAME

DBIx::Librarian::Library::ManyPerFile - multiple-queries-per-file
support class for DBIx::Librarian

=head1 SYNOPSIS

Provides repository service to DBIx::Librarian.  This package
supports SQL in template files, where each file contains one
or more query blocks.

=head1 DESCRIPTION

Format of queries in a template file is as follows:

queryname1:

[One or more SQL statements]

;;

Query name must start at beginning of line and end with a colon.
Terminate is a pair of semicolons on a line by itself.

When searching through the repository for a matching tag, the first
match will be used.  Conflicts are not detected.

ManyPerFile recognizes when a query file is changed, and will
instruct DBIx::Librarian to reload the query from the file.

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

  my $archiver = new DBIx::Librarian::Library::ManyPerFile
		  ({ name => "value" ... });

Supported Library::ManyPerFile parameters:

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


sub find {
    my ($self, $tag) = @_;

    print STDERR "FIND $tag\n" if $self->{TRACE};

    my $sql;
    my $thefile;
    foreach my $lib (@{$self->{LIB}}) {
	opendir (DIR, $lib) or croak "opendir $lib failed: $!";
	my @files = sort grep { /^[^\.]/ && /\.$self->{EXTENSION}$/ && -r "$lib/$_" } readdir(DIR);
	closedir (DIR);

	foreach my $file (@files) {
	    open (FILE, "$lib/$file") or croak "open $file failed: $!";
	    local $/ = undef;
	    my $body = <FILE>;
	    if ($body =~ /^$tag:/ms) {
		($sql) = $body =~ /^$tag:\s*(.*?)\s*^;;/ms;
		$thefile = "$lib/$file";
	    }
	    close (FILE);
	    last if $sql;
	}
	last if $sql;
    }
    if (! $sql) {
	# never found the sql
	croak "Unable to find tag $tag";
    }

    print STDERR "FOUND $tag in $thefile\n" if $self->{TRACE};

    $self->{$tag}->{FILE} = $thefile;
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
	opendir (DIR, $lib) or croak "opendir $lib failed: $!";
	my @files = sort grep { /^[^\.]/ && /\.$self->{EXTENSION}$/ && -r "$lib/$_" } readdir(DIR);
	closedir (DIR);

	foreach my $file (@files) {
	    open (FILE, "$lib/$file") or croak "open $file failed: $!";
	    local $/ = undef;
	    my $body = <FILE>;
	    close FILE;
	    foreach my $tag ($body =~ /^(\w+):/msg) {
		$items{$tag}++;
	    }
	}
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
