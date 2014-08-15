#!/usr/bin/env python
import sys
import re
import os
sys.path.append("/srv/cal/lib")
from jepsen import Jepsen

TESTING = os.environ['HOME']+"/public_html/results/TESTING"

if len(sys.argv) <= 1:
	print "startest needs an argument"
	exit(1)

if re.match("cassandra|riak|mongo|voldemort", sys.argv[1]):
	test = None
	if len(sys.argv) >= 3 and sys.argv[2] != 'all':
		test = sys.argv[2]
	excludeall = False
	if len(sys.argv) >= 4:
		if sys.argv[3] == 'excludeall':
			excludeall = True
	try:
		f = file(TESTING,"w")
	finally:
		f.close()
	# for longerdelay riak tests - only riak uses partition_wait at the moment
	# j = Jepsen(sys.argv[1],test,{'excludeall':excludeall,'wait': 1000,'count': 100, 'partition_wait': 0.1})
	# normal test settings 
	j = Jepsen(sys.argv[1],test,{'excludeall':excludeall,'wait': 1000,'count': 40, 'partition_wait': 0.33})
	if test == None:
		j.start_all()
	os.remove(TESTING)
else:
	print "don't understand "+sys.argv[1]
