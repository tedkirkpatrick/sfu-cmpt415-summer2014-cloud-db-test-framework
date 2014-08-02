#!/bin/bash
for i in `seq 5`
do
	echo
	echo "running '$1' on cloudsmall$i"
	ssh cloudsmall$i "$1"
done
