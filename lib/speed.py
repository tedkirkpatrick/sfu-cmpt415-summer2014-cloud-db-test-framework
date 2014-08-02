#!/usr/bin/env python
# recreate yahoo cloud services benchmark tests with python
# instead of having separate files describing each workload 
# define them here
# workloads are proportions of reads and writes
# and may contain other parameters
# see the Workload class below for details

from results import Results
import importlib
import sys
import traceback
import time
import nanotime
import random
import copy
debug = False

class Speed:
	"""
	Class that loads up a module and runs a workload using that module.
	The module defines setup, read, write and cleanup functions 
	that can be run from here.
	"""
	# opaque (mostly) data to send to actual test
	# add custom workloads here to extend the framework
	props = None
	def_props = {'host': 'cloudsmall1'}
	test = None

	def __init__(self, lib, test=None, props=None):
		if isinstance(props, dict): 
			self.props = dict(self.def_props.items() + props.items())
		else: 
			self.props = self.def_props

		if lib != None: 
			self.set_lib(lib)
			if test != None:
				self.set_test(test)
				self.start()

	# setters
	def set_count(self, count):
		"""
		count is the number of test runs to do
		"""
		if count < 0: raise Exception("set_count: less than zero!")
		self.props['count'] = count

	def set_destroy(self, destroy):
		"""
		tell test framework to remove generated data
		this is just passed on to the test framework
		has no effect in a Speed object
		"""
		if isinstance(destroy, bool): self.props['destroy'] = destroy

	def set_lib(self, lib):
		"""
		find a client library 
		these all start with "speed_" and define startup,cleanup,read,write
		"""
		# need to have PYTHONPATH defined for this to work correctly
		self.mod = importlib.import_module('speed_'+lib)
		self.mod_name = 'speed_'+lib
		self.lib = lib

	def set_test(self, test):
		# Workload now checks for existence of test parameters
		self.test = test

	def setup(self):
		"""
		generic db set up routine
		defined in test implementation
		in cassandra this was so unreliable that cqlsh was used to set up table instead
		"""
		getattr(self.mod, 'setup')(self)
		
	def cleanup(self):
		"""
		generic db teardown routine
		this is where destroy should be checked
		"""
		getattr(self.mod, 'cleanup')(self)

	def start(self):
		try:
			print "running "+self.lib+" "+self.test
			w = Workload(self)
			w.dowork()
			w.history.dump()
		except Exception as e:
			print "failed to run "+str(self.lib)+" "+str(self.test)+": "+str(e)
			print traceback.format_exc()
			sys.exit(2)

	def start_all(self, timeout=0):
		# add in any custom defined workloads
		if 'workloads' in self.props:
			workloads = dict(Workload.workloads.items() + self.props['workloads'].items())
		else: workloads = Workload.workloads

		for wl in sorted(workloads):
			self.set_test(wl)
			self.start()
			if timeout > 0: 
				print "sleeping for "+str(timeout)+" seconds"
				time.sleep(timeout)


class WorkloadResults(Results):
	"""
	manage test results for speed tests
	in these tests the assumptions about the order of operations
	are different, some may be reads, some may be writes 
	and the order of tests that do both is reversed from the jepsen tests
	
	Better off writing a custom module to do this? Results needs to be more general?
	"""

	def __init__(self, props, start=None):
		Results.__init__(self, start)
		self.props = props
		# remember what we were doing
		self.ops = {}

	def add(self, op, args):
		"""
		adapt speed testing inputs to jepsen syntax
		"""
		if 'key' in args: key = args['key']
		elif 'range' in args: key = str(args['range'])
		else: key = 'unknown'

		if 'value' in args: value = args['value']
		else: value = 'unknown'

		# for individual writes assume read took no time
		if op == 'write': self.end = self.start

		idx = Results.add(self, self.props['host'], key, value)

		self.ops[idx] = op
		return idx

	def update(self, idx, op, result):
		"""
		interface to Results.update
		mangles the times so we get sensible read and write times
		given that these are out of order compared to the jepsen tests Results was designed for
		TODO: make Results general enough that it doesn't need this
		"""
		if idx in self.ops:
			if self.ops[idx] != op: 
				self.ops[idx] = self.ops[idx]+" "+op
		else:
			self.ops[idx] = op

		now = nanotime.now()
		evt = self.events[idx]
		if self.ops[idx] == 'write': 
			Results.update(self, idx, {'end': now, 'rawtime': now, 'found': result, 'notes': self.ops[idx]})
		elif self.ops[idx] == 'read': 
			Results.update(self, idx, {'end': evt.start, 'rawtime': now, 'found': result, 'notes': self.ops[idx]})
		elif self.ops[idx] == 'read write':
			re = evt.rawelapsed()
			we = now - evt.rawtime
			wtime = evt.start + we
			rtime = wtime + re
			Results.update(self, idx, {'end': wtime, 'rawtime': rtime, 'found': result, 'notes': self.ops[idx]})

	def dump(self,bucketsz=1000000):
		"""
		bucketsz is a divisor of nanoseconds
		by default this converts nanoseconds to milliseconds
		mongo in particular seems to be quicker than nanosecond scale
		"""
		print str(self.stats(bucketsz))
		print str(self.histograms(bucketsz))

class Workload:
	"""
	manage running a workload
	this actually runs the read/write funcs defined in a test harness module

	Workload A: 50% reads, 50% writes described as "update heavy"
	Workload B: 95% reads, 5% writes
	Workload C: 100% reads
	Workload D: simulation of status update reads, like A but latest inserted records are heavily read
	Workload E: read short ranges. Like workload C except that multiple records are returned
	Workload F: read-modify-write
	
	create a st.props['workloads'] key with additional workloads to run - or redefine the existing ones
	each is a workload id (e.g. "A") and a dict with at least a 
	"read" (probability from 0.0 to 1.0)
	and optionally 
	"latest" - only read last updated value
	"range" - read range from 0,count with every read
	"change" - read/modify/write cycle

	by default random keys are generated

	"""
	# define what a workload is
	# may want to define this externally
	workloads = {
		'A': { 'read': 0.5, },
		'B': { 'read': 0.95, },
		'C': { 'read': 1.0, },
		'D': { 'read': 0.5, 'latest': True },
		'E': { 'read': 1.0, 'range': True },
		'F': { 'read': 1.0, 'change': True },
	}
	# how many operations to attempt
	def_count = 100

	# module to use for read/write
	mod = None
	# which workload test to run
	test = None
	# log of what happened
	history = None

	def __init__(self, st):
		"""
		Takes in a speedtest object (or other related one)
		and sets up the environment to run a test
		Creates a new history instance
		"""
		self.mod = st.mod
		self.test = st.test
		self.props = st.props

		# add in any custom defined workloads
		if 'workloads' in st.props:
			self.workloads = dict(self.workloads.items() + st.props['workloads'].items())

		if 'count' in st.props:
			self.count = st.props['count']
		else:
			self.count = self.def_count

		self.history = WorkloadResults(st.props)

	def dowork(self, workload=None):
		"""
		run through a test based on a workload's parameters
		self.test is the workloads workload key 
		can also be defined when running function
		"""
		if workload == None:
			if self.test not in self.workloads:
				raise Exception(str(self.test)+" missing from workload list")
			workload = self.workloads[self.test]

		# give ourselves somethign to read
		if workload['read'] == 1.0:
			for c in range(self.count):
				result = getattr(self.mod, 'write')({'key': str(c),'value':time.time()})

		last_write_key = 0
		for c in range(self.count):
			if 'latest' in workload and workload['latest']:
				key = str(last_write_key)
			else:
				key = str(random.randint(0,self.count-1))

			if 'change' in workload and workload['change']:
				idx = self.log('read', {'key': key})
				self.log('write', {'key': key,'value':time.time()})
			else:
				read = (True if random.random() < workload['read'] else False)
				if read:
					# not every db can support reading a range of values so this may not always work
					if 'range' in workload and workload['range']:
						self.log('read', {'range':[max(0,c-10),c]})
					else:
						self.log('read', {'key': key})
				else:
					last_write_key = key
					self.log('write', {'key': key,'value':time.time()})

	def log(self, op, args, idx=None):
		"""
		actually do something and make or update a log record
		"""
		try:
			global debug
			if idx == None: 
				idx = self.history.add(op, args)
			self.result = getattr(self.mod, op)(args)
			self.history.update(idx, op, self.result)
			if debug: self.history.printEvt(idx)
		except Exception as e:
			if debug: print self.lib+" "+self.test+" "+op+" "+str(e)

		return idx
	
def main():
	import sys
	"""
	s = Speed("mongo")
	s = Speed("voldemort")
	s = Speed("cassandra")
	s = Speed("riak")
	s = Speed("hbase")
	"""
	global debug
	debug = True
	platform = sys.argv[1]
	count = int(sys.argv[2])
	print "start tests for "+platform+" (count "+str(count)+")"
	s = Speed(platform, None, {'count': count})
	s.setup()
	if len(sys.argv) == 4:
		s.set_test(sys.argv[3])
		s.start()
	else:
		s.start_all(5)
	s.cleanup()

if __name__ == "__main__":
	main()
