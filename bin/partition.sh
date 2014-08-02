#!/bin/sh

# block traffic from 
for tgt in n1 n2
do
	for src in n3 n4 n5
	do
		ssh root@$tgt "iptables -A INPUT -s $src -j DROP"
	done
done
sleep 60

# clear rules for hosts
for tgt in n1 n2 n3 n4 n5
do
	ssh root@$tgt "iptables -F"
done

