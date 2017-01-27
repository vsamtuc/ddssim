#
# Simulator for the geometric method and its variants
#

from sim.components import *
from gm.basic import foo

import types

@types.coroutine
def bar(i):
	for j in range(i):
		yield j
	return i

async def foo(i):
	g = await bar(i)
	return i+g

async def foos():
	for i in range(10):
		print("async value of foo", await foo(i))



if __name__=='__main__':

	print(bar(2).send(None))

	for i in foos().__await__():
		print("ret by __await__",i)

	print("GmSim v0.1")

	nw = CoordNetwork(10)
	print(nw)
