#!/usr/bin/env python
import sys
import re
import os
sys.path.append("/srv/cal/lib")
from jepsen import Jepsen

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
	j = Jepsen(sys.argv[1],test,{'excludeall':excludeall,'wait': 100,'count': 40})
	if test == None:
		j.start_all()
else:
	print "don't understand "+sys.argv[1]
