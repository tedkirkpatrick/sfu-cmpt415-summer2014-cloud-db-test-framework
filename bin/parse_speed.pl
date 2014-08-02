#!/usr/bin/perl
# parse the output of lib/speed.py
use JSON qw/encode_json decode_json/;
use Data::Dumper;
$Data::Dumper::Indent = 1;
use strict;

my ($lib, $test, $oldtest, $data);

while (my $_ = <>) {
	chomp;
	if (/^running (\w+) (\w)/) {
		($lib, $test) = ($1,$2);
		my $stats = <>;
		chomp $stats;
		my $histograms = <>;
		chomp $histograms;
		$stats =~ s/'/"/g;
		$histograms =~ s/'/"/g;
		$stats =~ s/(\d+):/"$1":/g;
		$histograms =~ s/(\d+):/"$1":/g;
		# print "lib $lib\ntest $test\nstats $stats\nhistograms $histograms\n";
		$data->{stats}{$test}{$lib} = decode_json($stats);
		$data->{histograms}{$test}{$lib} = decode_json($histograms);
	} 
}

print "stats\n";
foreach my $test (sort keys %{$data->{stats}}) {
	print "workload $test\n";
	my $tdata = $data->{stats}{$test};
	print "db,operation,min (ms),avg (ms),max (ms),count\n";
	foreach my $op (qw/reads writes/) {
		foreach my $lib (sort keys %{$tdata}) {
			my $stats = $tdata->{$lib};
			print "$lib,$op,$stats->{$op}{min},$stats->{$op}{avg},$stats->{$op}{max},$stats->{$op}{count}\n"
		}
	}
}
print "histograms\n";
foreach my $test (sort keys %{$data->{histograms}}) {
	print "workload $test\n";
	my $tdata = $data->{histograms}{$test};
	my $max = -1;
	foreach my $lib (keys %{$tdata}) {
		foreach my $op (qw/reads writes/) {
			my $hgrams = $tdata->{$lib}{$op};
			if (scalar keys %$hgrams > $max) { $max = scalar keys %$hgrams; }
		}
	}
	print "db,operation,",(join ',', map { $_.' ms' } (0..$max)),"\n";
	foreach my $op (qw/reads writes/) {
		foreach my $lib (sort keys %{$tdata}) {
			my $hgrams = $tdata->{$lib};
			print "$lib,$op,";
			foreach my $b (0..$max) {
				print "$hgrams->{$op}{$b},";
			}
			print "\n";
		}
	}
}

