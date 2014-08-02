#!/usr/bin/env python
import voldemort
# test is a store name in the stores.xml file for the voldemort cluster
print "connecting"
client = voldemort.StoreClient("test", [('n1',6666),('n2',6668),('n3',6670),('n4',6672),('n5',6674)])
# that worked but anything below will get an "Unknown store 'test'." message
# connecting and using store "test" does work from the java shell (bin/voldemort-shell.sh) in the source tree
print str(client)
client.put("foop","hello")
resp = client.get("foop")
print str(resp)
resp = client.get("foo")
print str(resp)
client.put("foop","bye")
resp = client.get("foop")
print str(resp)
