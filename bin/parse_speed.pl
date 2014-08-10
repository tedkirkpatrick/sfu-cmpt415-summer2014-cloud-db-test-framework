#!/usr/bin/perl
# parse the output of lib/speed.py and lib/jepsen.py for latency information
use JSON qw/encode_json decode_json/;
use Data::Dumper;
$Data::Dumper::Indent = 1;
use strict;

my ($lib, $test, $oldtest, $data, %seen);

while (<>) {
	chomp;
	if (/^running (\w+) (\w+)/) {
		($lib, $test) = ($1,$2);
		my $testtest = "$test $ARGV";
		my $count;
		while ($seen{$lib}{$testtest}) {
			$testtest = "$test $ARGV $count";
			$count++;
		}
		$test = $testtest;
		$seen{$lib}{$test} = 1;
		my ($decoded_stats, $decoded_histograms);
		do {
			my ($stats, $histograms) = &getlines();
			# just ignore lines in the file that aren't json
			eval {
				$decoded_stats = decode_json($stats);
				$decoded_histograms = decode_json($histograms);
				# print "lib $lib\ntest $test\nstats $stats\nhistograms $histograms\n";
			}
		} while $@;
		$data->{stats}{$test}{$lib} = $decoded_stats;
		$data->{histograms}{$test}{$lib} = $decoded_histograms;
	} 
}

sub getlines {
	my $stats = <>;
	my $histograms = <>;
	chomp $stats;
	chomp $histograms;
	$stats =~ s/'/"/g;
	$histograms =~ s/'/"/g;
	$stats =~ s/(\d+):/"$1":/g;
	$histograms =~ s/(\d+):/"$1":/g;
	($stats, $histograms);
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
			my $maxkey = (sort keys %$hgrams)[-1];
			if ($maxkey > $max) { $max = $maxkey; }
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

