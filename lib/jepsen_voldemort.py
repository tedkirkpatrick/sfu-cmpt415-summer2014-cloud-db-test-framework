#!/usr/bin/env python
import voldemort
import iptables
import results
import nanotime
import time

tests = [ "basic" ]
portmap = [('n1',6666),('n2',6668),('n3',6670),('n4',6672),('n5',6674)]
ports = {'n1': 6666,'n2': 6668,'n3': 6670,'n4': 6672,'n5': 6674}
initwait = 5

def setup(jep):
	pass

def cleanup(jep):
	pass

def noop(jep):
	print "running noop for host "+jep.host

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
	basic test: write same key and read after
	"""
	global portmap, ports
	client = voldemort.StoreClient("test", [(jep.host, ports[jep.host])])
	print "running basic test for host "+jep.host+" port "+str(ports[jep.host])
	key = "jepsen_basic"
	ipt = iptables.Iptables(jep.host, jep)
	jep.history.set_checker(getattr(jep.mod, 'same'))

	i = 0
	while i < jep.props['count']:
		blocked = ipt.split_unsplit_all(i, jep)
		try:
			value = jep.host+" "+str(i)
			idx = jep.history.add(jep.host, key, value, ipt.isblocked())
			print "put "+key+" = "+value
			client.put(key, value)
			jep.history.update(idx, {'end': nanotime.now()})
			found = client.get(key)
			# second element in found is the vectorclock object
			jep.history.update(idx, {'found': found[0][0], 'rawtime': nanotime.now()})
			jep.history.printEvt(idx)
			
		except Exception as e:
			# print jep.host+" ("+str(i)+"): "+str(e)
			pass

		jep.pause()
		i = i + 1
