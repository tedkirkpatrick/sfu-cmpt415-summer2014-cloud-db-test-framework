# this does work but requires rest interface to be turned on
# bin/hbase rest start -p 8070
from starbase import Connection
conn = None
table = None
host = None
port = 8070

def setup(st):
	global conn, table, host, port

	host = st.props['host']

	if 'port' in st.props:
		port = st.props['port'] 

	conn = Connection(host=host, port=port) 
	table = conn.table('speedtest:basic')
	# if not table.exists(): table.create('stuff')

def cleanup(st):
	global table
	table.drop()

def write(props):
	return table.insert(str(props['key']), {'stuff': {'value': props['value']}}) 

def read(props):
	if 'range' in props:
		return [table.fetch(str(k)) for k in range(props['range'][0],props['range'][1])]
	return table.fetch(props['key'])


