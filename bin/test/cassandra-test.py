#!/usr/bin/env python
import time
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

cluster = Cluster(['n1','n2','n3','n4','n5'])
# cluster = Cluster(['n1'])
session = cluster.connect()
try:
	session.execute("CREATE KEYSPACE cal_test WITH replication = {'class':'SimpleStrategy', 'replication_factor':3}")
except:
	print "already have keyspace cal_test"

session.execute('USE cal_test')
try:
	session.execute('DROP TABLE cal_test.simple')
except:
	print "no table simple"
try:
	session.execute('CREATE TABLE cal_test.simple (id int PRIMARY KEY, stuff varchar)')
except:
	print "already have table simple?"

def stats(ary, elapsed):
	key = str(int(1000*elapsed))
	if key in ary:
		ary[key] = ary[key] + 1
	else:
		ary[key] = 1
	
now = int(time.time())
IDs = [] 
inserts = {}
selects = {}
eselects = {}
pselects = {}
for i in range(1000):
	ID = i + now
	start = time.time()
	session.execute("INSERT INTO cal_test.simple (id, stuff) values (%s, %s)",
		(ID, "EXECUTED: "+str(time.asctime())))
	end = time.time()
	IDs.append(ID)
	elapsed = end - start
	stats(inserts, elapsed)
	print "inserted "+str(ID)+" time "+str(elapsed)

clevel = ConsistencyLevel.ALL
qstr = 'SELECT * from cal_test.simple where id='
query = SimpleStatement(qstr+'%s', consistency_level=clevel)
pquery = session.prepare(qstr+'?')
pquery.consistency_level = clevel

for ID in IDs:
	start = time.time()
	rows = session.execute(qstr+'%s',(ID,))
	end = time.time()
	elapsed = end - start
	stats(eselects, elapsed)
	for row in rows:
		print str(row)+" time "+str(elapsed)

	start = time.time()
	rows = session.execute(query,(ID,))
	end = time.time()
	elapsed = end - start
	stats(selects, elapsed)
	for row in rows:
		print str(row)+" time "+str(elapsed)

	start = time.time()
	rows = session.execute(pquery,(ID,))
	end = time.time()
	elapsed = end - start
	stats(pselects, elapsed)
	for row in rows:
		print str(row)+" time "+str(elapsed)
print inserts
print eselects
print selects
print pselects
