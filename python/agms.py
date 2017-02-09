from math import sqrt
import numpy as np


class hash_family:
	def __init__(self, depth):
		self.depth = depth
		self.F = np.random.randint(0, 1<<31-1, size=(6,depth), dtype=np.int64)

	@staticmethod
	def hash31(a,b,x):
		r = a*x+b
		return ((r>>31)+r) & 2147483647

	def hash(self, x):
		F = self.F
		return self.hash31(F[0], F[1], x)

	def fourwise(self, x):
		F = self.F
		return 2*(((
			self.hash31(
				self.hash31(
					self.hash31(x,F[2],F[3]),
					x,F[4]),
				x,F[5])

			) & 32768)>>15)-1


	_cache = {}

	@staticmethod
	def get_cached(d):
		hf = hash_family._cache.get(d)
		if hf is None:
			hf = hash_family(d)
			hash_family._cache[d] = hf
		return hf


class projection:
	def __init__(self, width, depth=None, hf=None):
		if hf is None:
			assert depth is not None
			self.depth = depth
			self.hf = hash_family.get_cached(depth)
		else:
			self.hf = hf
			self.depth = hf.depth
		self.width = width

	def __eq__(self, other):
		return self.hf is other.hf and self.width==other.width
	def __ne__(self, other):
		return not (self.__eq__(other))
	def __hash__(self):
		return hash(self.hf)^self.width

	def epsilon(self):
		return 4./sqrt(self.width)
	def prob_failure(self):
		return 0.5**(self.depth/2)



class sketch:
	def __init__(self, proj):
		self.proj = proj
		self.vec = np.zeros((proj.depth, proj.width))
		self.pos = None
		self.delta = None

	def update(self, key, freq = 1):
		self.pos = self.proj.hf.hash(key) % self.proj.width
		self.delta = self.proj.hf.fourwise(key)*freq
		print(self.pos)
		print(self.delta)
		self.vec[range(self.proj.depth), self.pos] += self.delta

def row_dot(vec1, vec2):
	return np.einsum('ij,ij->i',vec1, vec2)

def sk_inner(sk1, sk2):
	return np.median(row_dot(sk1.vec, sk2.vec))

def test_sk_inner():
	proj = projection(500,11)
	sk1 = sketch(proj)
	sk2 = sketch(proj)
	sk1.update(243521,1)
	sk2.update(243521,1)
	assert sk_inner(sk1,sk2)==1

def sparse_inner(s1, s2):
	return sum(s1[k]*s2[k] for k in s1)

def test_sk_est():
	proj = projection(1500,21)
	print("sketch accuracy = ",proj.epsilon())
	sk1 = sketch(proj)
	sk2 = sketch(proj)

	S1 = np.random.randint(10000, size=50000)
	#S2 = np.random.randint(10000, size=50000)
	S2=S1

	for x in S1:
		sk1.update(x)
	for x in S2:
		sk2.update(x)

	from collections import Counter
	c1 = Counter(S1)
	c2 = Counter(S2)

	exc = sparse_inner(c1,c2)

	cossim = exc/sqrt(sparse_inner(c1,c1)*sparse_inner(c2,c2))

	print("similarity=", cossim)
	print("row_dot=",row_dot(sk1.vec, sk2.vec))

	est = sk_inner(sk1, sk2)
	err= abs(exc-est)/(exc /cossim)

	print("error=",err," exc=",exc," est=",est)
	#return v1, sk1
	assert err < proj.epsilon(), "bad accuracy %f"%err


test_sk_est()
