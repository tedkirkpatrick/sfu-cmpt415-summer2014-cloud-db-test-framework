import MySQLdb as mdb
import os

conn = None
cur = None

def setup(st):
	global conn, cur
	host = os.environ['MYHOST']
	user = os.environ['MYUSER']
	password = os.environ['MYPASSWD']
	conn = mdb.connect(host, user, password, 'speedtest')
	if not conn: raise Exception("Can't start up mysql!")
	cur = conn.cursor()

def cleanup(st):
	global cur
	cur.execute("delete from speedtest")
	if conn: conn.close()

def read(props):
	global cur
	if 'range' in props:
		cur.execute("select stuff from speedtest where id between '%s' and '%s'" % (props['range'][0],props['range'][1]))
	else:
		cur.execute("select stuff from speedtest where id='%s'" % props['key'])
	row = cur.fetchone()
	return row

def write(props):
	global cur
	return cur.execute("replace into speedtest set stuff='%s',id='%s'" % (props['value'], props['key'])) 

