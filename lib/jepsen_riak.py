import riak
import nanotime
import time
import re
import iptables
import results

tests = [ "basic", "lww_all", "lww_quorum", "lww_sloppy_quorum", "crdt" ]
testprops = {
	"basic": {'n_val': 3, 'r': 1, 'pr': 1, 'w': 1, 'dw': 1, 'pw': 1, 'allow_mult': True},
	"lww_all": {'n_val': 3, 'r': 'all', 'pr': 'all', 'w': 'all', 'dw': 'all', 'pw': 'all', 'allow_mult': False},
	"lww_quorum": {'n_val': 3, 'r': 'quorum', 'pr': 'quorum', 'w': 'quorum', 'dw': 'quorum', 'pw': 'quorum', 'allow_mult': False},
	"lww_sloppy_quorum": {'n_val': 3, 'r': 'quorum', 'pr': 1, 'w': 'quorum', 'dw': 1, 'pw': 1, 'allow_mult': False},
	"crdt": {'n_val': 3, 'r': 1, 'pr': 0, 'w': 1, 'dw': 0, 'pw': 0, 'allow_mult': True},
}

initwait = 10
# keys actually have to be strings
_id = "jepsen"

def resolver(fetched):
	# this doesn't seem to return a single value:
	# print max(fetched.siblings, lambda x: x.last_modified)
	global _id
	latest = None
	# print "resolver found siblings ",fetched.siblings
	for s in fetched.siblings:
		if latest == None or latest.last_modified < s.last_modified:
			latest = s
	fetched.siblings = [latest]
	return fetched.siblings

def setup(jep):
	pass

def get_port(jep):
	if 'port' in jep.props:
		port = jep.props['port']
	else: 
		port = 8087
	return port

def prep_conn(jep):
	"""
	create a connection for doing actual tests
	"""
	global testprops
	port = get_port(jep)
	print "connecting to "+jep.host+" on port "+str(port)
	client = riak.RiakClient(host=jep.host, pb_port=port, protocol='pbc')
	client.resolver = resolver

	# properties for a bucket should be set once
	bname = 'jepsen_'+jep.test
	bucket = client.bucket(bname)

	if jep.test not in testprops:
		props = testprops['basic']
		print "setup: using basic properties "+props
	else: 
		props = testprops[jep.test]
		print "setup: using custom properties for "+str(jep.test)+": "+str(props)

	bucket.set_properties(props)

	bucket.delete(_id)
	key = bucket.new(_id,data=-1)
	key.store()
	# print "waiting "+str(initwait)+"s "
	# time.sleep(initwait)
	return bucket

def connect_all(jep):
	"""
	connect to all hosts so we can monitor what is on them during the test
	"""
	port = get_port(jep)
	bname = 'jepsen_'+jep.test
	clients = []
	buckets = []
	for h in jep.hosts:
		clients.append(riak.RiakClient(host=jep.host, pb_port=port, protocol='pbc'))
	for c in clients:
		buckets.append(c.bucket(bname))
	return buckets
		
def cleanup(jep):
	pass

def noop(jep):
	print "running noop test for host "+jep.host

# for checking history events
def same(event):
	if event.value == event.found:
		event.resultmsg = "event ("+str(event.idx)+") same."
		event.result = True
	else:
		event.resultmsg = "event ("+str(event.idx)+") different."
		event.result = False

def basic(jep):
	"""
	properties
	n_val: total number of hosts in quorum group
	r: number to read from
	pr: number of primaries to read from
	w: number to write to
	dw: number to durably write to
	pw: number of primaries to write to
	allow_mult: keep multiple values of things and use vector clock or some other way to select value
	"""
	print "running riak test for host "+jep.host
	global _id
	bucket = prep_conn(jep)
	fetched = bucket.get(_id)
	ipt = iptables.Iptables(jep.host, jep)
	jep.history.set_checker(getattr(jep.mod, 'same'))

	comp = []
	i = 0
	while i < jep.props['count']:
		blocked = ipt.split_unsplit_all(i, jep, jep.props['partition_wait'])
		try:
			value = jep.host+" "+str(i)
			idx = jep.history.add(jep.host, _id, value, ipt.isblocked())
			# temporary just write to n1 and see if other hosts get data
			fetched.data = value
			fetched.store()
			jep.history.update(idx, {'end': nanotime.now()})
			fetched = bucket.get(_id)
			jep.history.update(idx, {'found': fetched.data, 'rawtime': nanotime.now()})
			jep.history.printEvt(idx)
		except Exception as e:
			print jep.host+" ("+str(i)+"): "+str(e)
		i = i + 1
		jep.pause()

def lww_all(jep):
	basic(jep)

def lww_quorum(jep):
	basic(jep)

def lww_sloppy_quorum(jep):
	basic(jep)

def crdt(jep):
	basic(jep)
