#
# Simulator for the geometric method and its variants
#

from sim.components import *
from gm.basic import foo

class Foo:
	def __await__(self):
		for i in range(5):
			yield i

async def print_them():
	print(await Foo())

import asyncio


if __name__=='__main__':
	print("GmSim v0.1")

	loop = asyncio.get_event_loop()
	loop.run_until_complete(print_them())
	loop.close()

	nw = CoordNetwork(10)
	foo()
	print(nw)
