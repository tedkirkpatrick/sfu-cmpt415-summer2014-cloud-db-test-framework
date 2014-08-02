#!/bin/bash
for i in `seq 5`
do
	echo
	echo "running '$1' on lxc container n$i"
	sudo ssh n$i "$1"
done
