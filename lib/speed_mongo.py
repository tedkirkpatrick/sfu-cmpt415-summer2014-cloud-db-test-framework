#!/usr/bin/env python
from pymongo import MongoClient
import datetime
import nanotime
import iptables
import results
import datetime

session = None
db = None
client = None
ts = datetime.datetime.utcnow()

def setup(st):
	global db, session, client
	if 'port' in st.props:
		port = st.props['port']
	else: 
		port = 27017
	# have to use cloudsmall2 for our setup as that is the primary
	# seem to need to add replicaSet to get r/w to work without causing exceptions
	client = MongoClient('cloudsmall2', port, replicaSet='cs0')
	db = client.speedtest
	session = db.speedtest
	
def cleanup(st):
	global client
	if client != None: client.disconnect()

def mk_rec(key, value):
	global ts
	rec = { 
		"author": "speedtest",
		"key": str(key),
		"text": str(value),
		"tags": ["speedtest"],
		"ts": ts
	}
	return rec

def read(props):
	found = False
	if 'range' in props:
		# forcing it to iterate has a major effect on performance - however, it never seems to find anything ???
		for doc in  session.find({"$and": [{"key": {"$gte": props['range'][0]}},{"key": {"$lte": props['range'][1]}}]}):
			found = doc
	else:
		doc =  session.find_one({'key': props['key']})
		found = doc
	return found

def write(props):
	rec = mk_rec(props['key'],props['value'])
	_id = session.insert(rec)
	return _id

