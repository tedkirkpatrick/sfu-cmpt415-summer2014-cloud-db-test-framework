#!/bin/bash
echo "content-type: text/plain"
echo
arg=`perl -e 'use CGI qw/:standard/; print param("test");'`
valid=`echo "$arg" | grep -P '^(cassandra|riak|mongo|voldemort)$'`
if [[ $? -eq 0 ]]
then
	ps -ef | grep $arg | grep -v grep
fi
