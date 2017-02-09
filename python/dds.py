#
# Distributed data streams
#

import simpy
import random

sim = None

class Simulation(simpy.Environment):
	def __init__(self):
		super().__init__()

		self.chan = Channel(self)
		self.coord = Coordinator(self.chan)
		self.site = LocalSite(self.coord, self.chan)
		self.stream = LocalStream(self.site)


		self.process(self.coord.process())
		self.process(data_source(self.stream))


class Channel(simpy.Store):
	def __init__(self, sim):
		super().__init__(sim)
	
	send = simpy.Store.put
	recv = simpy.Store.get


class Coordinator:
	def __init__(self, chan):
		self.chan = chan
		self.n = 0

	def process(self):
		while True:
			msg = yield self.chan.recv(self.chan)
			self.n += 1


class LocalSite:
	def __init__(self, coord, chan):
		self.coord = coord
		self.chan = chan

	def process(self, rec):
		if sim.now % 100000 == 0:
			print(rec, " received at t=", sim.now)
		self.chan.send(self.chan, rec)



class LocalStream:
	def __init__(self, site):
		self.local_site = site

	def emit(self, i):
		self.local_site.process(i)



def data_source(stream):
	for i in range(1370000):
		yield sim.timeout(1)
		stream.emit(i)



if __name__=='__main__':
	sim = Simulation()
	sim.run()

	print("Total messages=",sim.coord.n)
