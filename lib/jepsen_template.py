# example template for test group
# make one of these for each storage technology to be tested

tests = [ "basic" ]

def setup(jep):
	pass

def cleanup(jep):
	pass

def noop(jep):
	print "running noop for host "+jep.host

def basic(jep):
	noop(jep)
