#!/usr/bin/env python
from pymongo import MongoClient
import datetime
import nanotime
import iptables
import results

tests = [ "basic", "journal", "fsync" ]

def setup(jep):
	pass

def prep_session(jep, writeconcern=None):
	global db, session
	if 'port' in jep.props:
		port = jep.props['port']
	else: 
		port = 27017

	# try sending all traffic to the same host
	# the only host that actually accepts writes is assumed to be the first 
	# host = jep.host
	host = jep.hosts[0]

	if writeconcern != None:
		client = MongoClient(host=host, port=port, **writeconcern)
	else:
		client = MongoClient(host, port)
	db = client.jepsen
	session = db.jepsen
	return session
	
def cleanup(jep):
	global db, session
	if session != None: session.disconnect()

def noop(jep):
	print "running mongo noop test for host "+jep.host

def mk_rec(jep, i, tag, writeconcern):
	ts = datetime.datetime.utcnow()
	rec = { "author": jep.host,
		"text": jep.host+": "+str(i)+", ts: "+str(ts)+", writeconcern: "+str(writeconcern),
		"tags": ["jepsen",tag],
		"date": ts
	}
	return rec

# for checking history events
def same(event):
	if event.found == None:
		event.resultmsg = "event ("+str(event.idx)+") empty."
		event.result = True
	else:
		for (k,v) in event.value.items():
			if k == 'date': continue
			if k not in event.found or v != event.found[k]:
				event.resultmsg = "event ("+str(event.idx)+") different at "+str(k)
				event.result = False
				return
		event.resultmsg = "event ("+str(event.idx)+") same."
		event.result = True

def basic(jep, writeconcern={'slaveOk': False}):
	"""
	just add a record to our db
	"""
	print "running mongo test for host "+jep.host+" writeconcern "+str(writeconcern)
	session = prep_session(jep, writeconcern)
	ipt = iptables.Iptables(jep.host, jep)
	jep.history.set_checker(getattr(jep.mod, 'same'))

	rec = mk_rec(jep, -1, "insert", writeconcern)

	_id = None
	try:
		_id = session.insert(rec)
	except Exception as e:
		print jep.host+": "+str(e)

	i = 0
	while i < jep.props['count']:
		blocked = ipt.split_unsplit_all(i, jep)
		try:
			rec = mk_rec(jep, i, "update", writeconcern)
			idx = jep.history.add(jep.host, "jepsen", rec, ipt.isblocked())
			if _id != None: session.update({'_id':_id}, rec)
			jep.history.update(idx, {'end': nanotime.now()})
			found =  session.find_one({'_id':_id})
			jep.history.update(idx, {'found': found, 'rawtime': nanotime.now()})
			jep.history.printEvt(idx)

		except Exception as e:
			print jep.host+": "+str(e)
		i = i + 1
		jep.pause()

def journal(jep):
	basic(jep, {'w': 5, 'wtimeout': 100, 'j': True, 'slaveOk': False}) 

def fsync(jep):
	basic(jep, {'w': 5, 'wtimeout': 100, 'fsync': True, 'slaveOk': False})

