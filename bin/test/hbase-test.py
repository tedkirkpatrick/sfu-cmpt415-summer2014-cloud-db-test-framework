#!/usr/bin/env python
# this does work but requires rest interface to be turned on
# bin/hbase rest start -p 8070
# import cProfile
from starbase import Connection
import time
c = Connection(host='cloudsmall1',port=8070)
print str(c.tables())
t = c.table('speedtest:test0')
print str(t.columns())
keys = [ 'key test %d' % i for i in range(0, 5000) ]
values = [ "value-%(id)d %(ts)f" % { 'id': i, 'ts': time.time() } for i in range(0, 5000) ]
def build():
	b = t.batch()
	for i in range(0, 50):
		key = keys[i]
		value = values[i]
		print key+" => "+value
		b.insert(key, { 'f1': { 'x': value }, 'f2': { 'y': value }, 'f3': { 'z': value } })
	b.commit()

def read():
	for i in range(0, 50):
		key = keys[i]
		row = t.fetch(key)
		print key+" is "+str(row)

startt = time.time()
build()
wstartt = time.time()
writet = time.time() - startt
read()
endt = time.time()
readt = endt - wstartt
allt = endt - startt
print "write "+str(writet)+" read "+str(readt)+" all "+str(allt)

"""
this doesn't work
import happybase
connection = happybase.Connection(host='hostname',port=9000,compat='0.96')
try:
	print "creating table"
	table = connection.create_table('table-name',{'cf1': dict(),'cf2': dict(),'cf3': dict()})
	print str(table.families())
except:
	print "failed"
	table = connection.table('table_test')
	print str(table.families())

"""
