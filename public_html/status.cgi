#!/bin/bash
echo "content-type: text/plain"
echo
arg=`perl -e 'use CGI qw/:standard/; print param("test");'`
valid=`echo "$arg" | grep -P '^(cassandra|riak|mongo|voldemort)$'`
if [[ $? -eq 0 ]]
then
	procs=`ps -ef | grep $arg | grep -v grep` 
fi
if [ "$procs" = "" ]
then
	echo "no $arg processes"
else
	echo "$procs"
fi
