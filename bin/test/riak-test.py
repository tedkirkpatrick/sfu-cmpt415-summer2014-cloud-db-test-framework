#!/usr/bin/env python
import riak
client = riak.RiakClient(host='192.168.122.91', pb_port=8087, protocol='pbc')
bucket = client.bucket('cal-test')

data = {'one':1,'two':2,'three':3,'four':4,'five':5,'six':6,'seven':7,'eight':8,'nine':9,'ten':10}
for t,i in data.items():
	fetched = bucket.get(t)
	# unlike cassandra new will endlessly create new stores with the same key value
	# so we would want to check if the item exists
	if fetched.siblings == []: 
		key = bucket.new(t,data=i)
		key.store()
	fetched = bucket.get(t)
	for s in fetched.siblings:
		print s.data
		print str(s.last_modified)

