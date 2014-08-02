#!/bin/bash
echo "content-type: text/html"
echo
arg=`perl -e 'use CGI qw/:standard/; print param("test");'`
ts=`php -r 'print date("Ymd_His");'`
status=`ps -ef | grep $arg`
outfile=results/$arg-server-$ts.txt
sudo /srv/cal/bin/startlxc.sh $arg > $outfile 2>&1 &
title=Server
heading="Restarting server for $arg"
. ../lib/startfuncs.sh

htmlpage
