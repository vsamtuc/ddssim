
from math import *
import numpy as np

#
# We are interested in the closest point from (p,q) to the curve
# y(x) = sqrt(x^2 + 1),  where p>0.  
#
#  There are two methods, one that of binary search and the other is relaxation
#
#  Both methods are based on the observation that the solution xi (and psi = sqrt(xi^2+1))
#  has the property  
#   p/xi + q/psi = 2
#
#  Thereofore, if we define g(x) = 2-p/x-q/y(x), it will be g(xi) = 0.
#  This is the only root of g  (in (0,+inf)), and also
#  g(0) = -inf,  g(+inf) = 2.
#

def y(x): return sqrt(x*x+1.)

def g(x, p, q): return 2. - p/x - q/y(x)

def Dg(x, p, q):
	y3 = 2.*y(x)**3
	return p/(x*x)+q*x/y3

def relerr(a,b):
	return fabs((a-b)/b)

def binsearch(p,q, eps=1.e-12):
	"""
	We find an overestimate of xi as max(p,q), and do binary search.
	"""

	if p==0.0:
		if q<=2.0:
			return (0., 1.)
		else:
			return (sqrt((q/2.)**2 - 1), q/2.)

	elif q==0:
		return ( p/2., y(p/2.))

	else:
		loops = 0
		# get an overestimate
		xi = copysign(max(abs(p),q), p) 
		assert g(xi,p,q)>0

		xiprev = xi
		gx = g(xiprev, p, q)
		while gx>0:
			xi = xiprev
			xiprev /= 2.
			#print(xiprev,p,q)
			gx = g(xiprev,p,q)
			loops+=1

		if gx==0.0:
			return (xiprev, y(xiprev))

		if not g(xiprev,p,q)<0: # we have 0!
			return (xiprev)

		# ok , now we have the root in [xiprev,xi].
		# shrink the space as needed
		while fabs((xi-xiprev)/xiprev)>eps:
			loops +=1 
			if loops>150:
				break
			xm = (xi+xiprev)/2.
			gx = g(xm,p,q)
			if gx>0:
				xi = xm
			elif gx<0:
				xiprev=xm
			else:
				break
		print("bisection loops=",loops,"              x=",xiprev)
		return (xiprev,y(xiprev)) 

def sgn(x):
	if x<0:
		return -1.
	elif x>0:
		return 1.
	else:
		return 0.


def relaxation(p,q, eps=1.e-12):
	x1 = copysign(max(abs(p),q), p)
	gamma = 0.5
	loops = 0
	u = 0.6
	rho = 0.0
	while True:
		loops += 1
		x0=x1
		y0 = y(x0)
		w = q*(1 - 0.5*(x0/y0)**2)/y0

		Fderiv = 1 + gamma*( w - 2)
		#print("F'=",Fderiv,"x0=", x0, "gamma=",gamma)

		# if Fderiv > 0.1: 
		# 	gamma = 0.9/(2-w)
		gamma =  u/(2.-w) 
		u = rho+u*(1.-rho)
		# elif Fderiv<0.:
		# 	print("FAILURE")
		# 	break
		print("g(x0)=",g(x0,p,q))
		x1 = x0*(1. - gamma*g(x0,p,q))
		if fabs((x1-x0)/x0)<=eps:
			break
		if loops>120:
			print("FAILURE")
			break
	print("relaxation loops=",loops)
	return (x1, y(x1))


def dist(p,q, func):
	(xi,psi) = func(p,q)
	d = sqrt( (xi-p)**2 + (psi-q)**2 )
	if q<y(p):
		return -d
	else:
		return d

def newton(p,q,eps):

	if p==0.0:
		if q<=2.0:
			return (0., 1.)
		else:
			return (sqrt((q/2.)**2 - 1), q/2.)

	elif q==0:
		return ( p/2., y(p/2.))

	else:
		loops = 0
		# get an overestimate
		xi = copysign(max(abs(p),q), p)
		assert g(xi,p,q)>0

		xiprev = xi
		gx = g(xiprev, p, q)
		while gx>0:
			xi = xiprev
			xiprev /= 2.
			gx = g(xiprev,p,q)
			loops+=1

		if gx==0.0:
			return (xiprev, y(xiprev))

		if not g(xiprev,p,q)<0: # we have 0!
			return xiprev, y(xiprev)

		# ok , now we have the root in (xiprev,xi).
		# shrink the space as needed
		x0 = (xiprev+xi)/2.

		# Do Newton iteration
		iter = 120
		while iter:
			loops +=1 
			x1 = x0 - g(x0,p,q)/Dg(x0,p,q)
			if fabs((x1-x0)/x0)<eps:
				break
			else:
				x0 = x1
			iter -= 1
		print("newton loops=",loops)
		return x1, y(x1)



def test(p,q, eps=1E-12):
	xr,yr = relaxation(p,q, eps)
	xb,yb = binsearch(p,q,eps)
	xn, y1 = newton(p,q,eps)
	print("Max. difference relax= ",100.*fabs(xr-xb)/xb ,"%", " Newton=",100.*fabs(xn-xb)/xb ,"%")





	
