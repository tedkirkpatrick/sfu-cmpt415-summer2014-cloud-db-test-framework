#!/bin/bash
echo "content-type: text/html"
echo
if [ -f results/TESTING ]
then
	echo "testing in progress"
else
	arg=`perl -e 'use CGI qw/:standard/; print param("test");'`
	ts=`php -r 'print date("Ymd_His");'`
	status=`ps -ef | grep $arg`
	outfile=results/$arg-test-$ts.txt
	touch results/TESTING
	(sudo /srv/cal/bin/startjepsen.py $arg; echo FINISHED) > $outfile 2>&1 &
fi
title=Results
heading="Testing $arg"
. ../lib/startfuncs.sh

htmlpage

