#!/usr/bin/perl
# parse the output of lib/speed.py and lib/jepsen.py for latency information
use JSON qw/encode_json decode_json/;
use Data::Dumper;
$Data::Dumper::Indent = 1;
use strict;

my ($lib, $test, $oldtest, $data, %seen);

while (<>) {
	chomp;
	RUNNING:
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
		$_ = &getlines();
		goto RUNNING if not eof;
	} 
	# will stall at the <> forever if we don't check here
	last if eof;
}

sub getlines {
	while (my $line = <>) {
		chomp $line;
		if ($line =~ /^running (\w+) (\w+)/) {
			return $line;
		}
		my $pat = qr/^\s*{\s*['"](reads|writes|unknown)['"].*}/;
		if ($line =~ $pat) {
			my $statstype = ($1 eq 'reads' ? 'stats':'histograms');
			$line =~ s/'/"/g;
			$line =~ s/(\d+):/"$1":/g;
			my $decoded = decode_json($line);
			if (defined $decoded) {
				$data->{$statstype}{$test}{$lib} = $decoded;
			}
		}
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
			my $maxkey = (sort { $a <=> $b } keys %$hgrams)[-1];
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

