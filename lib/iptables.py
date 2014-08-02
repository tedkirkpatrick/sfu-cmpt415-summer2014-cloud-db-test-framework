import subprocess
import string
import time
from multiprocessing import Lock

class Iptables:
	"""
	for doing ip tables on other hosts
	specific to the lxc environment for jepsen
	assumes passwordless ssh access and sudo access in the container
	"""
	user = None
	host = None
	hosts = []
	first = []
	last = []
	exclude = []
	tgt = None
	ssh = None
	blocked = False
	# for blocking/unblocking all at once
	allblocked = False
	healed = False
	lock = Lock()

	def __init__(self, host, jep, strict=False):
		"""
		apart from setting up our identity 
		also figure out what part of the partition we are in
		allow for flexibly sized partitions 
		"""
		self.host = host
		self.hosts = jep.hosts
		if strict and host not in hosts: 
			raise Exception("can't find host "+host+" in host list "+hosts)

		if host in jep.first_group:
			self.exclude = jep.last_group
		else: self.exclude = jep.first_group
			
		self.user = jep.props['user']
		self.tgt = self.login(host)
		self.ssh = jep.props['ssh']
		# if self.host != None: self.flush()

	def login(self, host):
		if host != None:
			return str(self.user)+'@'+str(host)
		return None

	def flush(self):
		"""
		cleanup rules - note this will delete ALL iptables rules on that host
		"""
		print "healing "+self.tgt
		subprocess.check_call([self.ssh, self.tgt, "sudo iptables --flush"])
		self.blocked = False

	def flushOne(self, host):
		tgt = self.login(host)
		if tgt != None:
			subprocess.check_call([self.ssh, tgt, "sudo iptables --flush"])

	def flushAll(self):
		Iptables.healed = False
		Iptables.allblocked = False
		for host in self.hosts:
			self.flushOne(host)

	def block(self, hosts):
		"""
		block group of hosts from this machine - indiscriminately drops all incoming traffic
		"""
		print "partioning "+self.tgt
		for h in hosts:
			subprocess.check_call([self.ssh, self.tgt, "sudo iptables -A INPUT -s "+h+" -j DROP"])
		self.blocked = True

	@staticmethod
	def blockOne(ssh, tgt, blocked):
		for h in blocked:
			subprocess.check_call([ssh, tgt, "sudo iptables -A INPUT -s "+h+" -j DROP"])
		
	@staticmethod
	def listOne(ssh, tgt):
		return subprocess.check_output([ssh, tgt, "sudo iptables -L"])


	def listAll(self):
		out = "iptables\n"
		for h in self.hosts:
			out = out + "\n" + h + "\n" + self.list(h)
		return out

	def list(self, host):
		return subprocess.check_output([self.ssh, self.login(host), "sudo iptables -L"])

	def split_unsplit(self, count, i, wait=0.33333):
		"""
		block or unblock based on iteration count during test
		hosts are partitioned based on where host is in list
		want proportion of test to be blocked to be about 1/3?
		"""
		pos = float(i)/float(count)
		if pos > wait and pos < 2.0*wait:
			if not self.blocked: 
				self.block(self.exclude)

		elif pos >= 2.0*wait:
			if self.blocked: 
				self.flush()

		return self.blocked

	@staticmethod
	def partitioned(blocked=False):
		return "PARTITIONED " if Iptables.allblocked or blocked else ""

	def isblocked(self):
		return Iptables.partitioned(self.blocked)

	def split_unsplit_all(self, i, jep, wait=0.33333):
		return Iptables.split_unsplit_all_static(i, jep, wait)

	@staticmethod
	def split_unsplit_all_static(i, jep, wait=0.33333):
		"""
		block or unblock everybody at once
		"""
		count = jep.props['count']
		hosts = jep.hosts
		ssh = jep.props['ssh']
		user = jep.props['user']
		pos = float(i)/float(count)
		if pos > wait and pos < 2.0*wait:
			if not Iptables.healed and not Iptables.allblocked: 
				Iptables.lock.acquire()
				Iptables.allblocked = True
				last = jep.last_group
				first = jep.first_group
				for host in hosts:
					if 'excludeall' in jep.props and jep.props['excludeall']:
						exclude = []
						for eh in hosts:
							if eh != host:
								exclude.append(eh)
					else:
						if host in first:
							exclude = last
						else: exclude = first
					print "partioning "+host+" exclude "+str(exclude)
					Iptables.blockOne(ssh, user+'@'+host, exclude)
					print host+"\n"+Iptables.listOne(ssh, user+'@'+host)
				Iptables.lock.release()

		elif pos >= 2.0*wait:
			if Iptables.allblocked: 
				Iptables.lock.acquire()
				for host in hosts:
					print "healing "+host
					subprocess.check_call([ssh, user+'@'+host, "sudo iptables --flush"])
				Iptables.allblocked = False
				Iptables.healed = True
				Iptables.lock.release()

		return Iptables.allblocked
		
	def split_unsplit_by_time(self, start, maxtime, wait=0.33333):
		"""
		block or unblock based on iteration count during test
		hosts are partitioned based on where host is in list
		want proportion of test to be blocked to be about 1/3?
		"""
		elapsed = time.time() - start
		pos = elapsed/maxtime
		print str(elapsed)+"/"+str(maxtime)+" = "+str(pos)+" between "+str(wait)+" and "+str(2.0*wait)+" ? "
		if pos > wait and pos < 2.0*wait:
			if not self.blocked: 
				self.block(self.exclude)

		elif pos >= 2.0*wait:
			if self.blocked: 
				self.flush()


