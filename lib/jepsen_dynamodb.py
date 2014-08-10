# amazon dynamodb
# test if it is as consistent as they say it is
from boto import dynamodb2
from boto.dynamodb2.table import Table
from boto.dynamodb2.fields import HashKey
import nanotime
import results
import os
import time

region = 'us-west-2'
tb_name = "sfu-cmpt415-test-table"
schema = [ HashKey('key') ]
throughput  = { 'read': 5, 'write': 2 }
table = None
key = 'jepsen'
initwait = 5

tests = [ "basic" ]

def setup(jep):
	global table, schema, throughput, region, initwait
	connection=dynamodb2.connect_to_region(
		region,
		aws_access_key_id=os.environ['aws_access_key_id'],
		aws_secret_access_key=os.environ['aws_secret_access_key']
	)
	table = Table(tb_name,schema=schema,throughput=throughput,connection=connection)
	time.sleep(initwait)

def cleanup(jep):
	pass

def noop(jep):
	print "running noop for host "+jep.host

# log checker callback
def same(event):
	if event.found == None:
		event.resultmsg = "event "+str(event.idx)+" empty."
		event.result = False
	elif event.value == event.found:
		event.resultmsg = "event "+str(event.idx)+" ok."
		event.result = True
	else:
		event.resultmsg = "event "+str(event.idx)+" different."
		event.result = False

def basic(jep):
	global table, tb_name, key
	# how do we handle conflicts?
	if 'overwrite' in jep.props:
		overwrite = jep.props['overwrite']
	else:
		overwrite = False
	print "running basic test for dynamodb on table "+tb_name
	try:
		item = table.get_item(key=key)
	except Exception as e:
		print "failed to get item: "+str(e)
		item = table.put_item(data={'key':key,'stuff':'new'})
		item = table.get_item(key=key)
		print "made new item"
		print str(item)

	jep.history.set_checker(getattr(jep.mod, 'same'))
	i = 0
	while i < jep.props['count']:
		i = i + 1
		try:
			value = jep.host+" "+str(nanotime.now())
			idx = jep.history.add(jep.host,0,value)
			print "making stuff "+value
			item['stuff'] = value
			item.save(overwrite=overwrite)
			jep.history.update(idx, { "end": nanotime.now() })
			item = table.get_item(key=key)
			jep.history.update(idx, { "rawtime": nanotime.now(), "found": item['stuff'] })
			jep.history.printEvt(idx)
			jep.pause()

		except Exception as e:
			print jep.host+" ("+str(i)+"): "+str(e)

