#!/usr/bin/env python
import voldemort
import time

host = None
client = None
keys = {}

def setup(st):
	global client
	tries = 100
	while tries > 0 and client == None:
		tries = tries - 1
		try:
			client = voldemort.StoreClient("speedtest", [('cloudsmall1',6666),('cloudsmall2',6666),('cloudsmall3',6666),('cloudsmall4',6666),('cloudsmall5',6666)])
		except Exception as e:
			print "voldemort startup: "+str(e)
			client = None
			time.sleep(1)

# always running this as we can use all the hosts to connect
setup(None)

def cleanup(st):
	global client, keys
	for k in keys:
		client.delete(k)
	client.close()

def read(props):
	global client
	# don't think there is a way to get a range of values in voldemort
	if 'range' in props:
		found = client.get_all(map(lambda x: str(x), range(props['range'][0],props['range'][1])))
	else:
		found = client.get(props['key'])
	return found

def write(props):
	global client, keys
	keys[props['key']] = True
	return client.put(props['key'], str(props['value']))

