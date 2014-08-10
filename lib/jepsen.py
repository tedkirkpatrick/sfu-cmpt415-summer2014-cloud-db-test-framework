#!/usr/bin/env python
# attempt at reproducing the "old" version of jepsen tests
# we assume sudo access on the lxc containers

import importlib
import subprocess
import sys
import os
import string
import threading
import traceback
import results
import iptables
import time

class Jepsen: 
	"""
	module to manage setting up and running jepsen-like tests
	jepsen is a test framework for testing network partitions
	originally by Kyle Kingsbury (aephyr)
	we assume the cloud database is up and running 
	however, all configuration of the db should be done from the test
	
	various opaque properties can be passed to tests some of which are:
		count - number of times per thread to run a test
		wait - how long to wait between each iteration in ms
		user - ssh user to use
		ssh - ssh command to use
		destroy - whether or not to remove data after test (not always implemented by tests)
		partition - first group of hosts to use as a partition [otherwise its 0->(n/2) vs ((n/2)+1)->(n-1)]
		port - port to connect on (optional)
		portmap - map of host/port pairs (required for voldemort only)
	"""
	tests = []
	host = None
	hosts = []
	# how hosts are partitioned
	first_group = []
	last_group = []
	test = None
	lib = None
	history = None
	# opaque data to send to test suite
	props = {}
	def_props = {'count': 40, 'wait': 500, 'user': 'root', 'ssh': 'ssh', 'destroy': True}
	threads = None
	hostprefix = 'n'

	def __init__(self, lib=None, test=None, props=None, threads=None, hostprefix='n'):
		"""
		set up and possibly run a test
		lib is the jepsen_*.py library to invoke
		test is optionally which of the tests to run
		threads and hostprefix are for situations where 
			the number of threads or hosts is not based 
			on the lxc hosts running on the system
			e.g. the amazon tests
		"""
		self.threads = threads
		self.hostprefix = hostprefix
		if isinstance(props, dict): 
			# order matters here: last one takes precedence on key conflict
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

	def set_wait(self, wait):
		"""
		wait is the time in ms to wait between runs
		"""
		if wait < 0: raise Exception("set_wait: less than zero!")
		self.props['wait'] = wait

	def set_user(self, user):
		"""
		user is the ssh user to access the lxc containers
		this user must have sudo access on the container
		"""
		self.props['user'] = user

	def set_ssh(self, ssh):
		"""
		ssh command to use to access the lxc containers
		"""
		self.props['ssh'] = ssh

	def set_destroy(self, destroy):
		"""
		tell test framework to remove generated data
		this is just passed on to the test framework
		has no effect in a Jepsen object
		"""
		if isinstance(destroy, bool): self.props['destroy'] = destroy

	def set_lib(self, lib):
		# need to have PYTHONPATH defined for this to work correctly
		self.mod = importlib.import_module('jepsen_'+lib)
		self.mod_name = 'jepsen_'+lib
		self.lib = lib
		self.tests = {self.lib: self.mod.tests}
		print "tests for "+self.mod_name+": "+str(self.tests)

	def start_all(self):
		for test in self.tests[self.lib]:
			self.set_test(test)
			self.start()

	def set_test(self, test):
		try:
			which = self.tests[self.lib].index(test)
			self.test = self.tests[self.lib][which]
		except:
			self.test = "noop"

	def start(self):
		try:
			print "running "+self.lib+" "+self.test
			self.run_test()
			self.history.dump()
		except Exception as e:
			print "failed to run "+str(self.lib)+" "+str(self.test)+": "+str(e)
			print traceback.format_exc()
			sys.exit(2)

	def _find_hosts_(self):
		"""
		find every lxc container
		we assume all of them are going to be used in tests
		this function requires sudo access to work
		"""
		if self.threads != None and self.threads > 0:
			self.hosts = [ self.hostprefix+str(i) for i in range(self.threads) ]
			print "threads: "+str(self.hosts)
		else:
			self.hosts = subprocess.check_output(["sudo","lxc-ls"]).split()
			print "hosts: "+str(self.hosts)
			self._split_hosts_()
			ipt = iptables.Iptables(None, self)
			print "flushing iptables rules"
			print ipt.flushAll()
			print ipt.listAll()

	def _split_hosts_(self):
		self.first_group = []
		self.last_group = []
		hosts = self.hosts

		# allow end users to partition hosts in various ways
		# arbitrary partition in two
		if 'partition' in self.props:
			self.first_group = self.props['partition']
			for host in hosts:
				if host not in self.first_group:
					self.last_group.append(host)
		# group into two halves based number (0,1,2/3,4 etc)
		else:
			n = len(hosts)
			half = n/2
			i = 0
			for host in hosts:
				if i <= half:
					self.first_group.append(host)
				else:
					self.last_group.append(host)
				i = i + 1
		if 'excludeall' in self.props:
			print "exclude all "+str(self.props['excludeall'])
		else:
			print "first group: "+str(self.first_group)
			print "last group: "+str(self.last_group)

	def run_test(self):
		self._find_hosts_()
		self.history = results.Results()

		getattr(self.mod, 'setup')(self)

		threads = []
		for host in self.hosts:
			self.host = host
			thread = JepsenThread(self)
			thread.start()
			threads.append(thread)
		self.host = None

		for thread in threads:
			thread.join()


		# this func may or may not exist
		try:
			result = getattr(self.mod, 'cleanup')(self)
		except Exception as e:
			pass

class JepsenThread(threading.Thread):
	host = None
	mod = None
	test = None
	# number of runs to do
	count = 0
	# time in ms to wait between runs
	wait = 0
	hosts = []
	history = None
	user = None
	ssh = None

	def __init__(self, jep):
		threading.Thread.__init__(self)
		self.host = jep.host
		self.mod = jep.mod
		self.test = jep.test
		self.hosts = jep.hosts
		self.first_group = jep.first_group
		self.last_group = jep.last_group
		self.props = jep.props
		self.history = jep.history

	def run(self):
		result = getattr(self.mod, self.test)(self)

	def pause(self):
		try:
			# wait a specific number of milliseconds between runs
			if self.props['wait'] > 0:
				time.sleep(self.props['wait']/1e3)
		except:
			pass

def main():
	"""
	j = Jepsen("mongo")
	j = Jepsen("voldemort")
	j = Jepsen("cassandra")
	j = Jepsen("riak")
	j.start_all()
	"""
	j = Jepsen("riak","basic",{'excludeall': True})
	j.history.dump()

if __name__ == "__main__":
	main()
