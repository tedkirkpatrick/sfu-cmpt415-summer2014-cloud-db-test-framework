import riak
import nanotime
import time

testprops = {
	"basic": {'n_val': 3, 'r': 1, 'pr': 1, 'w': 1, 'dw': 1, 'pw': 1, 'allow_mult': True},
	"lww_all": {'n_val': 3, 'r': 'all', 'pr': 'all', 'w': 'all', 'dw': 'all', 'pw': 'all', 'allow_mult': False},
	"lww_quorum": {'n_val': 3, 'r': 'quorum', 'pr': 'quorum', 'w': 'quorum', 'dw': 'quorum', 'pw': 'quorum', 'allow_mult': False},
	"lww_sloppy_quorum": {'n_val': 3, 'r': 'quorum', 'pr': 1, 'w': 'quorum', 'dw': 1, 'pw': 1, 'allow_mult': False},
	"crdt": {'n_val': 3, 'r': 1, 'pr': 0, 'w': 1, 'dw': 0, 'pw': 0, 'allow_mult': True},
}

initwait = 10
bucket = None
client = None

def resolver(fetched):
	# this doesn't seem to return a single value:
	# print max(fetched.siblings, lambda x: x.last_modified)
	latest = None
	for s in fetched.siblings:
		if latest == None or latest.last_modified < s.last_modified:
			latest = s
	fetched.siblings = [latest]
	return fetched.siblings

def setup(st):
	global testprops, bucket, client, initwait
	if 'port' in st.props:
		port = st.props['port']
	else: 
		port = 8087
	print "connecting to "+st.props['host']+" on port "+str(port)
	client = riak.RiakClient(host=st.props['host'], pb_port=port, protocol='pbc')
	client.resolver = resolver

	# properties for a bucket should be set once
	bucket = client.bucket('speedtest')

	if st.test == None or st.test not in testprops:
		props = testprops['basic']
		print "setup: using basic properties "+str(props)
	else: 
		props = testprops[st.test]
		print "setup: using custom properties for "+str(st.test)+": "+str(props)

	bucket.set_properties(props)

	# print "waiting "+str(initwait)+"s "
	# time.sleep(initwait)

def cleanup(st):
	pass

def read(props):
	global bucket
	# you can make secondary indexes but how do you get a range of keys?
	# possible to make secondary index on keys?
	if 'range' in props:
		fetched = [bucket.get(str(k)) for k in range(props['range'][0], props['range'][1])]
	else:
		fetched = bucket.get(props['key'])
	return fetched

def write(props):
	global bucket
	fetched = bucket.get(props['key'])
	if fetched == None:
		return bucket.new(props['key'],props['value'])
	else:
		fetched.data = props['value']
		return fetched.store()

