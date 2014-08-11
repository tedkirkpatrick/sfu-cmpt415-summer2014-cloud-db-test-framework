from cassandra import ConsistencyLevel
from cassandra import Timeout
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import time
import nanotime
import random
import string
# jepsen specific modules
import iptables
import results

keyspace = 'jepsen'
initsleep = 5

# used by Jepsen to validate tests that can be run
# these vary depending on the storage technology
tests = [ "basic", "counter", "set", "isolation", "transaction", "transaction_dup" ]

def tbname(table):
	return keyspace+'.'+table

def noop(jep):
	"""
	default test
	don't do anything
	"""
	print "running noop test for host "+jep.host

def setup(jep):
	"""
	called from Jepsen class
	these are all done as part of the tests themselves
	"""
	pass

def cleanup(jep):
	"""
	called from Jepsen class
	this should be optional as we may want to look at the db after the test 
	"""
	try:
		if jep.destroy == True:
			global session, keyspace, initwait
			session.execute("DROP KEYSPACE "+keyspace)
			time.sleep(initwait)
	except Exception as e:
		print jep.host+": "+str(e)

def prep_session(jep, canonical, createtb=None, createks=None):
	"""
	create keyspace and table
	strategy is to serialize keyspace and table creation
	doing this here as it may be the case that we don't need / want a setup step 
	for some test suites
	"""
	global initsleep, keyspace

	if 'port' in jep.props:
		cluster = Cluster([jep.host+':'+str(jep.props['port'])])
	else: 
		cluster = Cluster([jep.host])
	session = cluster.connect()
	if createks == None: 
		# from cql3 as reported on http://stackoverflow.com/questions/9656371/cql-how-to-check-if-keyspace-exists
		createks = "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class':'SimpleStrategy', 'replication_factor':3}" % keyspace
	if createtb == None:
		createtb = 'CREATE TABLE %s (id int PRIMARY KEY, stuff varchar)' % canonical

	# only allow one host to do this setup step? doesn't seem to work well with cassandra
	print "%s setting up keyspace.table %s" % (jep.host, canonical)
	session.execute(createks)
	# make sure keyspace exists on this host
	tries = 0
	while tries < 100:
		time.sleep(initsleep)
		rows = session.execute("SELECT keyspace_name FROM system.schema_keyspaces")
		for row in rows:
			if row.keyspace_name == keyspace: 
				tries = 11
				break
		tries = tries + 1
		
	session.execute('USE %s' % keyspace)
	try:
		session.execute('DROP TABLE %s' % canonical)
	except:
		print jep.host+": no table %s" % canonical
	try:
		# this can fail if the table already exists
		session.execute(createtb)
	except Exception as e:
		print jep.host+": "+str(e)	

	# make sure table exists on this host
	tries = 0
	while tries < 100:
		time.sleep(initsleep)
		rows = session.execute("SELECT keyspace_name,columnfamily_name FROM system.schema_columnfamilies")
		for row in rows:
			if row.keyspace_name+'.'+row.columnfamily_name == canonical:
				tries = 11
				break
		tries = tries + 1

	return session

# these are checkers for history records
# look for the jep.history.set_checker() to see which is used by which test
def samestuff(event):
	if event.found == None:
		event.resultmsg = "event "+str(event.idx)+" empty."
		event.result = False
	elif event.value != event.found[0].stuff:
		event.resultmsg =  "event "+str(event.idx)+" different."
		event.result = False
	else:
		event.resultmsg = "event "+str(event.idx)+" ok."
		event.result = True

def isonemore(event):
	if event.found == None:
		event.resultmsg = "event "+str(event.idx)+" empty."
		event.result = False
		return
	f = event.found[0]
	v = event.value[0]
	if int(v.k)+1 != int(f.k):
		event.resultmsg =  "event "+str(event.idx)+" failed."
		event.result = False
	else:
		event.resultmsg = "event "+str(event.idx)+" ok."
		event.result = True

def isempty(event):
	if event.found == None or len(event.found):
		event.resultmsg = "event "+str(event.idx)+" failed."
		event.result = False
	else:
		event.resultmsg = "event "+str(event.idx)+" ok."
		event.result = True

def same(event):
	if event.found == None:
		event.resultmsg = "event "+str(event.idx)+" empty."
		event.result = False
	elif event.found == event.value:
		event.resultmsg = "event "+str(event.idx)+" ok."
		event.result = True
	else:
		event.resultmsg = "event "+str(event.idx)+" different."
		event.result = False

def sequence(event):
	if event.found == None:
		event.resultmsg = "event "+str(event.idx)+" empty."
		event.result = False
		return

	seq = [i for i in string.split(event.found[0].stuff, " ")]
	
	for i, v in enumerate(seq[1:]):
		if i != int(v):
			event.resultmsg = "event "+str(event.idx)+" out of order at "+str(i)
			event.result = False
			return

	event.resultmsg = "event "+str(event.idx)+" ok."
	event.result = True
	
# the actual tests
def basic(jep): 
	"""
	just do a basic key value update to a table
	note that kingsbury's test monkeys with the timestamps for each host
	also need to check what value actually got written
	"""
	host = jep.host
	count = jep.props['count']
	hosts = jep.hosts
	history = jep.history
	print "start basic test on "+host
	canonical = tbname('basic')

	session = prep_session(jep, canonical)

	fudge = random.randint(0, 1000)
	sel = SimpleStatement(
		"SELECT stuff FROM "+canonical+" WHERE id=0 LIMIT 1",
			consistency_level=ConsistencyLevel.ALL
		)
	ipt = iptables.Iptables(host, jep)
	history.set_checker(getattr(jep.mod, 'samestuff'))

	i = 0
	teststart = time.time()
	while i < count:
		i = i + 1

		blocked = ipt.split_unsplit_all(i, jep)

		value = host+" "+str(time.time())
		idx = history.add(host, 0, value, ipt.isblocked())
		try:
			instr = "INSERT INTO %s (id, stuff) VALUES (%s, '%s') USING TIMESTAMP %d" \
					% (canonical, 0, value, (nanotime.now().microseconds()+fudge))
			print jep.host+": "+instr
			ins = SimpleStatement( instr, consistency_level=ConsistencyLevel.QUORUM)
			session.execute(ins)
			history.update(idx, {'end': nanotime.now()})
			rows = session.execute(sel)
			history.update(idx, {'found': rows, 'rawtime': nanotime.now()})
			history.printEvt(idx)
		except Exception as e:
			print jep.host+" "+str(nanotime.now())+": "+str(e)
		jep.pause()

def counter(jep):
	"""
	All writes are increments. 
	Recovers [0...n] where n is the current value of the counter.
	"""
	print "starting counter test for "+jep.host
	canonical = tbname('counter_app')
	createtb = 'CREATE TABLE %s (id int PRIMARY KEY, k counter)' % canonical
	session = prep_session(jep, canonical, createtb)

	sel = SimpleStatement(
		"SELECT k FROM %s WHERE id=0 LIMIT 1" % canonical,
			consistency_level=ConsistencyLevel.ALL
		)
	updstr = "UPDATE %s SET k = k + 1 WHERE id=0" % canonical
	upd = SimpleStatement(updstr, consistency_level=ConsistencyLevel.ONE)
	ipt = iptables.Iptables(jep.host, jep)
	jep.history.set_checker(getattr(jep.mod, 'isonemore'))

	i = 0
	while i < jep.props['count']:
		blocked = ipt.split_unsplit_all(i, jep)
		try:
			value = session.execute(sel)
			idx = jep.history.add(jep.host, 0, value, ipt.isblocked())

			print jep.host+": "+updstr
			session.execute(upd)
			jep.history.update(idx, {'end': nanotime.now()})

			rows = session.execute(sel)
			jep.history.update(idx, {'found': rows, 'rawtime': nanotime.now()})
			jep.history.printEvt(idx)
		except Exception as e:
			print jep.host+" "+str(nanotime.now())+": "+str(e)
		jep.pause()
		i = i + 1
	

def set(jep):
	"""
	Uses CQL sets
	"""
	print "starting set test for "+jep.host
	canonical = tbname('set_app')
	createtb = 'CREATE TABLE %s (id int PRIMARY KEY, s set<varchar>)' % canonical
	session = prep_session(jep, canonical, createtb)

	sel = SimpleStatement(
		"SELECT s FROM %s WHERE id=0 LIMIT 1" % canonical,
			consistency_level=ConsistencyLevel.ALL
		)
	session.execute("INSERT into %s (id, s) VALUES (0,{})" % canonical)
	ipt = iptables.Iptables(jep.host, jep)
	jep.history.set_checker(getattr(jep.mod, 'same'))

	i = 0
	while i < jep.props['count']:
		blocked = ipt.split_unsplit_all(i, jep)
		try:
			value = jep.host+" "+str(i)
			idx = jep.history.add(jep.host, 0, value, ipt.isblocked())
			updstr = "UPDATE %s SET s = s + {'%s'}  WHERE id=0" % (canonical, value)
			print jep.host+": "+updstr
			upd = SimpleStatement(updstr, consistency_level=ConsistencyLevel.ANY)
			session.execute(upd)
			jep.history.update(idx, {'end': nanotime.now()})

			rows = session.execute(sel)
			for row in rows:
				if value in row.s: 
					jep.history.update(idx, {'found': value, 'rawtime': nanotime.now()})
			jep.history.printEvt(idx)
			
		except Exception as e:
			print jep.host+" "+str(nanotime.now())+": "+str(e)
		jep.pause()
		i = i + 1

def isolation(jep):
	"""
	This app tests whether or not it is possible to consistently update multiple
	cells in a row, such that either *both* writes are visible together, or
	*neither* is.

	Each client picks a random int identifier to distinguish itself from the
	other clients. It tries to write this identifier to cell A, and -identifier
	to cell B. The write is considered successful if A=-B. It is unsuccessful if
	A is *not* equal to -B; e.g. our updates were not isolated.

	'concurrency defines the number of writes made to each row. 
 	"""
	print "starting test isolation for "+jep.host
	canonical = tbname('iso_app')
	createtb = 'CREATE TABLE %s (id varchar PRIMARY KEY, a int, b int)' % canonical
	session = prep_session(jep, canonical, createtb)

	# math operators don't work and you need to manually add an index to use a = 1 etc.
	sel = SimpleStatement(
		"SELECT id,a,b FROM %s"  % canonical,
			consistency_level=ConsistencyLevel.ALL
		)
	ipt = iptables.Iptables(jep.host, jep)
	jep.history.set_checker(getattr(jep.mod, 'isempty'))

	i = 0
	value = jep.hosts.index(jep.host) + 1
	writes = {}
	while i < jep.props['count']:
		blocked = ipt.split_unsplit_all(i, jep)
		try:
			key = str(i)
			writes[key] = True
			time.sleep(random.randint(0,200)/1e3)
			idx = jep.history.add(jep.host, key, (key,value,-value), ipt.isblocked())
			"""
                       ; If you force timestamp collisions instead of letting
                       ; them happen naturally, you can reliably cause
                       ; conflicts in 99% of rows! :D
                       ; (using :timestamp 1)
 			"""
			# insstr = "INSERT INTO %s (id,a,b) VALUES ('%s',%d,%d) USING TIMESTAMP 1 " % (canonical,key,value,-value)
			insstr = "INSERT INTO %s (id,a,b) VALUES ('%s',%d,%d) " % (canonical,key,value,-value)
			print jep.host+": "+insstr
			ins = SimpleStatement(insstr)
			# create some entropy
			for c in range(2):
				session.execute(ins)
				jep.history.update(idx, {'end': nanotime.now(), 'notes': "%scount %d"% (ipt.isblocked(), c+1)})

			# we want to find anything left over from this select as that is an error
			# if everything went well then the result set should be empty
			rows = session.execute(sel)
			found = []
			for row in rows:
				if row.a != -row.b:
					found.append(row)
			jep.history.update(idx, {'found': found, 'rawtime': nanotime.now()})
			jep.history.printEvt(idx)
			
		except Exception as e:
			print jep.host+" "+str(nanotime.now())+": "+"ERROR: "+str(e)
		jep.pause()
		i = i + 1

def transaction(jep):
	"""
	Uses paxos CAS
	"""
	host = jep.host
	count = jep.props['count']
	hosts = jep.hosts
	history = jep.history
	print "start transaction test on "+host
	canonical = tbname('transaction')

	session = prep_session(jep, canonical)

	sel = SimpleStatement(
		"SELECT stuff FROM %s WHERE id=0 LIMIT 1" % canonical,
			consistency_level=ConsistencyLevel.ALL
		)
	ipt = iptables.Iptables(host, jep)
	jep.history.set_checker(getattr(jep.mod, 'sequence'))

	value = 'start'
	instr = "INSERT INTO %s (id, stuff) VALUES (0, '%s')" % (canonical, value)
	print jep.host+": "+instr
	ins = SimpleStatement( instr, consistency_level=ConsistencyLevel.QUORUM)
	session.execute(ins)

	i = 0
	while i < count:
		blocked = ipt.split_unsplit_all(i, jep)
		value = value+" "+str(i)
		idx = history.add(host, 0, value, ipt.isblocked())
		try:
			tries = 0
			while tries < 100:
				try:
					row = session.execute(sel)[0]
					updstr = "UPDATE %s SET stuff='%s' WHERE id=0 IF stuff='%s'" % (canonical,value,row.stuff)
					print jep.host+" "+str(nanotime.now())+": "+updstr
					upd = SimpleStatement(updstr, consistency_level=ConsistencyLevel.QUORUM)
					res = session.execute(upd)
					break
				except Timeout as t:
					time.sleep(random.randint(0,100)/1e3)
				tries = tries + 1

			history.update(idx, {'end': nanotime.now()})
			rows = session.execute(sel)
			history.update(idx, {'found': rows, 'rawtime': nanotime.now()})
			history.printEvt(idx)
		except Exception as e:
			print jep.host+" "+str(nanotime.now())+": "+str(e)
		jep.pause()
		i = i + 1


def transaction_dup(jep):
	"""
	Uses paxos CAS
	Tests that transactions may only be consumed once
	"""
	host = jep.host
	count = jep.props['count']
	wait = jep.props['wait']
	hosts = jep.hosts
	history = jep.history
	print "start transaction duplicate detection (transaction_dup) test on "+host
	canonical = tbname('transaction_dup')
	createtb = "CREATE TABLE %s (id varchar PRIMARY KEY, consumed boolean)" % canonical
	session = prep_session(jep, canonical, createtb)
	ipt = iptables.Iptables(host, jep)
	jep.history.set_checker(getattr(jep.mod, 'isempty'))

	sel = SimpleStatement(
		"SELECT id, consumed FROM %s" % canonical,
			consistency_level=ConsistencyLevel.ALL
		)

	dupes = {}
	i = 0
	while i < count:
		blocked = ipt.split_unsplit_all(i, jep)
		idx = history.add(host, i, True, ipt.isblocked())
		try:
			key = jep.host+" "+str(i)
			try:
				instr = "INSERT INTO %s (id, consumed) VALUES ('%s', false)" % (canonical, key)
				print jep.host+": "+instr
				ins = SimpleStatement( instr, consistency_level=ConsistencyLevel.QUORUM)
				session.execute(ins)
				updstr = "UPDATE %s SET consumed=true WHERE id='%s' IF consumed=false" % (canonical, key)
				print jep.host+": "+updstr
				upd = SimpleStatement(updstr, consistency_level=ConsistencyLevel.QUORUM)
				res1 = session.execute(upd)[0]
				res2 = session.execute(upd)[0]
				# two transactions back to back should not work
				if res1.applied and res2.applied: dupes[key] = True
			except Timeout as t:
				pass

			history.update(idx, {'end': nanotime.now()})
			rows = session.execute(sel)
			found = []
			for row in rows:
				if row.id in dupes:
					found.append(row.id)
			history.update(idx, {'found': found, 'rawtime': nanotime.now()})
			history.printEvt(idx)

		except Exception as e:
			print jep.host+" "+str(nanotime.now())+": "+str(e)
		jep.pause()
		i = i + 1


