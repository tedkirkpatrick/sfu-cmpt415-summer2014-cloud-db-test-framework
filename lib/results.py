import nanotime
import sys
from multiprocessing import Lock

class Results:
	"""
	basic container for test results
	"""
	events = None
	start = None
	lock = None
	checker = None
	count = 0

	def __init__(self, start=None):
		if start == None: self.start = nanotime.now()
		else: self.start = start
		self.events = []
		self.lock = Lock()

	# add callback for checking results
	def set_checker(self, checker):
		self.checker = checker

	def add(self, host, key, value, notes="", start=None):
		self.lock.acquire()
		event = JepsenEvent(host, key, value, notes, start)
		if self.checker != None: event.set_checker(self.checker)
		event.idx = self.count
		self.count = self.count + 1
		self.events.append(event)
		self.lock.release()
		return event.idx

	def update(self, idx, props):
		try:
			self.lock.acquire()
			self.events[int(idx)].update(props)
		except Exception as e:
			print "Results.update(): error event "+str(idx)+": "+str(e)
		finally:
			self.lock.release()
		

	def addEvent(self, event):
		self.lock.acquire()
		self.events.append(event)
		self.lock.release()
		return event

	def get(self, idx):
		try:
			return self.events[idx]
		except: 
			print "Results.get(): event "+str(idx)+" not found"
			return None

	def printEvt(self, idx):
		print self.events[idx]

	def histograms(self, bucketsz=1000000):
		self.bucketsz = bucketsz
		reads = {}
		writes = {}
		unknown = {'reads': 0, 'writes': 0}
		self.histograms = {}
		for i, event in enumerate(self.events):
			e = int(event.elapsed())
			re = int(event.rawelapsed())
			he = e/bucketsz
			hre = re/bucketsz
			# ignoring 0 values for e,re to make this work with the speed tests
			if e > 0:
				if he in writes:
					writes[he] = writes[he] + 1
				else:
					writes[he] = 1
			elif e < 0:
				unknown['writes'] = unknown['writes'] + 1

			if re > 0:
				if hre in reads:
					reads[hre] = reads[hre] + 1
				else:
					reads[hre] = 1
			elif re < 0:
				unknown['reads'] = unknown['reads'] + 1

		self.histograms['reads'] = reads
		self.histograms['writes'] = writes
		self.histograms['unknown'] = unknown
		return self.histograms

	def ms(self, val):
		if val == None: return 0.0
		return float(val)/float(self.bucketsz)

	def stats(self,bucketsz=1000000):
		self.bucketsz = bucketsz
		minread = None
		minwrite = None
		maxread = None
		maxwrite = None
		wtotal = 0.0
		rtotal = 0.0
		wcount = 0.0
		rcount = 0.0
		for i, event in enumerate(self.events):
			e = int(event.elapsed())
			re = int(event.rawelapsed())
			if re > 0 and re < minread: minread = re
			if e > 0: 
				e = e
				if minwrite == None or e < minwrite: minwrite = e
				if maxwrite == None or maxwrite < e: maxwrite = e
				wtotal = wtotal + self.ms(e)
				wcount = wcount + 1.0
			if re > 0:
				re = re
				if minread == None or re < minread: minread = re
				if maxread == None or maxread < re: maxread = re
				rtotal = rtotal + self.ms(re)
				rcount = rcount + 1.0
		minread = self.ms(minread)
		maxread = self.ms(maxread)
		minwrite = self.ms(minwrite)
		maxwrite = self.ms(maxwrite)
		if wcount == 0: wcount = 1
		if rcount == 0: rcount = 1
		waverage = float(wtotal)/float(wcount)	
		raverage = float(rtotal)/float(rcount)	
		self.stats = {'reads': { 'min': minread, 'max': maxread, 'avg': raverage, 'count': rcount, 'total': rtotal }, 
				'writes': { 'min': minwrite, 'max': maxwrite, 'avg': waverage, 'count': wcount, 'total': wtotal }}
		return self.stats
			
			
	def dump(self, fname=None):
		try:
			savedout = sys.stdout
			if fname != None:
				sys.stdout = open(fname,'a')
			self.lock.acquire()
			for i, event in enumerate(self.events):
				print ("%05d" % i)+" "+str(event)

			try:
				print str(self.stats())
			except Exception as se:
				print "stats failed in results.dump: "+str(se)
			try:
				print str(self.histograms())
			except Exception as he:
				print "histograms failed in results.dump: "+str(he)

			self.lock.release()
			sys.stdout = savedout

		except Exception as e:
			print "Results.dump(): "+str(e)


class JepsenEvent:
	"""
	define an event
	"""
	# times
	start = 0
	end = 0
	# time after read after write
	rawtime = 0
	key = None 
	value = None # written value
	found = None # read after write value
	host = None
	notes = ""
	idx = -1
	# callback 
	checker = None
	result = False
	resultmsg = ""

	def __init__(self, host, key, value, notes="", start=None):
		if start == None: self.start = nanotime.now()
		else: self.start = start
		self.end = nanotime.nanotime(0)
		self.rawtime = nanotime.nanotime(0)
		self.host = host
		self.key = key
		self.value = value
		self.notes = notes
		self.checker = None

	# add a callback to check results
	def set_checker(self, checker):
		self.checker = checker

	def check(self):
		if self.checker != None:
			try:
				(self.checker)(self)
			except Exception as e:
				print "history event checker: "+str(e)
	
	def elapsed(self):
		if self.end <= 0: return -1
		return int(self.end - self.start)

	def rawelapsed(self):
		if self.end <= 0: return -1
		if self.rawtime <= 0: return -1
		return int(self.rawtime - self.end)

	def success(self):
		if self.start > 0 and self.end >= self.start: return True
		return False

	def successStr(self):
		if self.success(): return "Success"
		return "Failed"

	def update(self, props):
		for prop, value in props.iteritems():
			if prop == "host": self.host = value
			elif prop == "notes": self.notes = value
			elif prop == "start": self.start = value
			elif prop == "end": self.end = value
			elif prop == "key": self.key = value
			elif prop == "value": self.value = value
			elif prop == "found": self.found = value
			elif prop == "rawtime": self.rawtime = value
			elif prop == "resultmsg": self.resultmsg = value
			elif prop == "result": self.result = value

	def consistent(self):
		return self.value == self.found

	def __str__(self):
		self.check()
		s = str(self.host)+" "+str(nanotime.nanotime(self.start))+": "+\
			str(self.key)+" = '"+str(self.value)+"' ('"+str(self.found)+"') "+\
			str(self.elapsed())+"ns ("+str(self.rawelapsed())+"ns) "+\
			self.successStr()+" "+str(self.notes)+" "+self.resultmsg
		return s
		

