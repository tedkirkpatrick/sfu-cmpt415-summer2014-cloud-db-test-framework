from cassandra import ConsistencyLevel
from cassandra import Timeout
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import time

def tbname(table):
	return keyspace+'.'+table

keyspace = 'speedtest'
initsleep = 5
session = None
canonical = tbname('basic')
host = None

def setup(st):
	"""
	called from Speed class
	"""
	prep_session(st)

def cleanup(st):
	"""
	called from Speed class
	this should be optional as we may want to look at the db after the test 
	try:
		if st.props['destroy'] == True:
			global session, keyspace, initwait
			session.execute("DROP KEYSPACE "+keyspace)
			time.sleep(initwait)
	except Exception as e:
		print host+": "+str(e)
	"""
	pass

def prep_session(st, createtb=None, createks=None):
	"""
	create keyspace and table
	"""
	global initsleep, keyspace, session, canonical, host

	host = st.props['host']
	if 'port' in st.props:
		cluster = Cluster([host+':'+str(st.props['port'])])
	else: 
		cluster = Cluster([host])
	session = cluster.connect()
	"""
	# did this in cqlsh as dropping and rebuilding the table was VERY unreliable

	if createks == None: 
		# from cql3 as reported on http://stackoverflow.com/questions/9656371/cql-how-to-check-if-keyspace-exists
		createks = "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class':'SimpleStrategy', 'replication_factor':3}" % keyspace
	if createtb == None:
		createtb = 'CREATE TABLE %s (id varchar PRIMARY KEY, stuff varchar)' % canonical

	# only allow one host to do this setup step? doesn't seem to work well with cassandra
	print "%s setting up keyspace.table %s" % (host, canonical)
	session.execute(createks)
	# make sure keyspace exists on this host
	tries = 0
	while tries < 10:
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
		print host+": no table %s" % canonical
	try:
		# this can fail if the table already exists
		session.execute(createtb)
	except Exception as e:
		print host+": "+str(e)	

	# make sure table exists on this host
	tries = 0
	while tries < 10:
		time.sleep(initsleep)
		rows = session.execute("SELECT keyspace_name,columnfamily_name FROM system.schema_columnfamilies")
		for row in rows:
			if row.keyspace_name+'.'+row.columnfamily_name == canonical:
				tries = 11
				break
		tries = tries + 1
	"""


def read(props): 
	"""
	just do a basic key read
	"""
	global canonical, session
	if 'range' in props:
		rng = ','.join(map(lambda x: "'"+str(x)+"'", range(props['range'][0], props['range'][1])))
		sel = SimpleStatement(
			"SELECT stuff FROM %s WHERE id in (%s)" % (canonical, rng),
				consistency_level=ConsistencyLevel.ALL
		)
	else: 
		sel = SimpleStatement(
			"SELECT stuff FROM %s WHERE id='%s' LIMIT 1" % (canonical, props['key']),
				consistency_level=ConsistencyLevel.ALL
		)
	return session.execute(sel)

def write(props): 
	"""
	just do a basic key value update to a table
	"""
	global canonical, session
	instr = "INSERT INTO %s (id, stuff) VALUES ('%s', '%s')" \
			% (canonical, props['key'], props['value'])
	ins = SimpleStatement( instr, consistency_level=ConsistencyLevel.QUORUM)
	return session.execute(ins)

