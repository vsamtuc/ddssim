import os,json
from itertools import product
from collections import ChainMap
from string import Formatter

#
# Classes to construct experiments
#

class ExperimentFactory(Formatter):
	"""
	Encapsulate the idiosyncracies of PBS installations and automate
	the generation of experiment code
	"""
	def __init__(self, name, objl, **kwargs):
		"""
		Construct an experiment factory.
		`name` is the experiment name
		`objlist` is an iterable returning json-encodable objects
		
		The encoding is done by an encoder capable of expanding callables.
		If a item callable is to be expanded into json, the value returned by
		self.json_encode(self, item).
		"""
		assert isinstance(name,str)
		self.joblist = objl
		self.job_cfg = { "jobid": None, "obj": None }
		self.jobset_cfg = dict(**kwargs)
		self.jobset_cfg = { "exp_name": name }
		self.CFG = ChainMap(self.job_cfg, self.jobset_cfg, self.pbs_cfg, os.environ)

	def json_encode(self, obj):
		# if this is a callable object, call it with self as argument
		if callable(obj):
			obj = obj(self)
		return obj

	def get_value(self, key, args, kwargs):
		# Called when self is used as a Formatter
		if isinstance(args, int):
			return args[key]
		elif key in kwargs:
				return kwargs[key]
		else:
			return self.json_encode(self.CFG[key])

	def __getitem__(self, key):
		"""
		Convenience function
		"""
		return self.CFG[key]

	def json_dump(self, obj, jsfile=None):
		if jsfile is not None:
			return json.dump(obj, jsfile, indent="\t", default=self.json_encode)
		else:
			return json.dumps(obj, indent="\t", default=self.json_encode)


	def generate(self, batch):
		"""
		Generate a set of files for pbs execution.
		For each job there is a .pbs file and a .json file.
		Jobs are numbered from 1 up to the number of elements in objl.

		exp_name is the experiment name
		objl     is the list of job objects
		"""

		# populate the jobset_cfg
		self.jobset_cfg.update(kwargs)
		self.jobset_cfg['exp_name'] = exp_name
		self.jobset_cfg.setdefault("jobdir",".")

		jobid = 0
		for obj in objl:
			jobid += 1
			self.job_cfg['jobid'] = jobid
			self.job_cfg['obj'] = obj

			# write the pbs file
			pbs_filename = self.format("{exp_name}{jobid:05d}.pbs")
			with open(pbs_filename,'w') as pbsfile:
	  			pbsfile.write(self.format(self.pbs_tmpl))

	  		# write the json file
			json_filename = self.format("{exp_name}{jobid:05d}.json")
			with open(json_filename, 'w') as jsfile:
				self.json_dump(obj, jsfile)


		self.jobset_cfg['exp_name'] = None
		self.job_cfg['jobid'] = None
		self.job_cfg['obj'] = None






# 
# Callables to inject config info into the objects during json encoding
#
def fmt(tmpl):
	"""
	Return a callable that takes as input the config object and returns a string
	"""
	return lambda fmt: fmt.format(tmpl)


#
# Classes and functions to construct json object lists
#

class objlist:
	"""
	Encapsulation for a list of dicts. Instances of this class are
	used in constructing complex json-serializable objects.
	"""
	def __init__(self, L=None):
		"""
		Initialize an objlist by passing it an iterable of objects.

		If L is already a list, it is acquired by the object.

		If L is an iterable (such as another objlist), its contents become
		the contents of the new objlist.

		If L is a dict, it becomes the only member of this object.

		If L is None, the object is initialized to the empty list.
		"""
		if L is None:
			L = []
		elif isinstance(L, dict):
			L = [L]
		else:
			L = list(L)
		for i in L:
			if not isinstance(i, dict):
				raise ValueError("Expected list of dict, got "+repr(i)+" in "+repr(L))
		self.L = L

	def copy(self):
		"""
		Return a deep copy of the objlist
		"""
		return objlist(d.copy() for d in self.L)

	def __eq__(self, other):
		"""
		Compare two objlists
		"""
		other = self.__objlistify(other)
		return self.L == other.L

	def __objlistify(self, other):
		if not isinstance(other, (objlist, dict)):
			raise ValueError("cannot add objlist to "+repr(other))
		if isinstance(other, dict):
			other = objlist([other])
		return other

	def __add__(self, other):
		"""
		Concatenate two objlists
		"""
		other = self.__objlistify(other)
		return objlist(self.L+other.L)
	def __radd__(self,other):
		other = self.__objlistify(other)
		return objlist(other.L+self.L)

	def __bool__(self):
		"""
		Return True if length of the objlist is not 0
		"""
		return bool(self.L)

	def __len__(self):
		return len(self.L)

	@staticmethod
	def __tensor(L1, L2):
		for d1 in L1:
			for d2 in L2:
				m = d1.copy()
				m.update(d2)
				yield m

	def __mul__(self, other):
		"""
		Create the cross (tensor) product of two lists, by combining
		each item of one with each item of the other
		"""
		other = self.__objlistify(other)
		return objlist(self.__tensor(self.L, other.L))
	def __rmul__(self, other):
		other = self.__objlistify(other)
		return objlist(self.__tensor(other.L, self.L))

	@staticmethod
	def __zip(L1,L2):
		if len(L1) != len(L2):
			raise ValueError("cannot zip lists of different length")
		for (d1,d2) in zip(L1, L2):
			m = d1.copy()
			m.update(d2)
			yield m


	def __or__(self,other):
		"""
		Zip two objlists of equal length (in the broadcasting sense, i.e. one
		could be a singleton, in which case this is equivalent to __mul__)
		"""
		other = self.__objlistify(other)
		if len(self.L)==1 or len(other.L)==1:
			return objlist(self.__tensor(self.L, other.L))
		else:
			return objlist(self.__zip(self.L, other.L))

	def __ror__(self, other):
		return self.__or__(other)

	def select(self, pred):
		"""
		Return an objlist containing only those elements of self.L satisfying
		pred. 

		Note that the new objlist shares the elements with the original list,
		therefore any modifying operation on the returned list will modify the
		objects of the original list.
		"""
		return objlist(x for x in self.L if pred(x))

	def where(self, key, pred):
		"""
		Return an objlist containing only those elements whose key satisfies
		pred. 

		Note that the new objlist shares the elements with the original list,
		therefore any modifying operation on the returned list will modify the
		objects of the original list.
		"""
		return objlist(x for x in self.L if key in x and pred(x[key]))

	def with_key(self, key):
		"""
		Return an objlist containing only those elements containing key.

		Note that the new objlist shares the elements with the original list,
		therefore any modifying operation on the returned list will modify the
		objects of the original list.
		"""
		return self.select(lambda obj: key in obj)

	def without_key(self, key):
		"""
		Return an objlist containing only those elements missing key. 

		Note that the new objlist shares the elements with the original list,
		therefore any modifying operation on the returned list will modify the
		objects of the original list.
		"""
		return self.select(lambda obj: key in obj)

	def __setitem__(self, key, val):		
		"""
		Set d[key]=val for each object d in the objlist
		"""
		if val is ...:
			self.pop(key)
		else:
			for d in self.L:
				d[key] = val

	def __getitem__(self, key):
		"""
		Return a list of values for the given key. 
		
		The length of the returned list is equal to the 
		For those objects which do not contain key, an Ellipsis is returned.
		"""
		return (d.get(key,...) for d in self.L)

	def __delitem__(self, key):
		self.pop(key)

	def pop(self, key):
		"""
		Delete the key from each object and return a list of the deleted elements
		"""
		return [x.pop(key,...) for x in self.L]

	def update(self, dv):
		"""
		Update each object of the list with the given object dv
		"""
		for d in self.L:
			d.update(dv)

	def project(self, func):
		"""
		Update each object d of the list with the object returned from func(d).
		"""
		for d in self.L:
			d.update(func(d))

	def delete(self, pred):
		self.L = [x for x in self.L if not pred(x)]

	def nest(self, key):
		"""
		Return an objlist with singleton objects of the form {key: d} for each
		d in this objlist
		"""
		return objlist({key: d} for d in self.L)

	def nil(self):
		"""
		Return a new objlist with len(self) copies of an empty object
		"""
		return objlist({} for i in range(len(self)))

	def __iter__(self):
		return iter(self.L)

	def __repr__(self):
		return "objlist(%s)" % repr(self.L)


def param(name, vals):
	"""
	Make an objlist of singletons {name: val} for each value in iterable vals
	"""
	return objlist(({name: val} if val is not ... else {}) for val in vals)

def zipper(name, *objlists):
	"""
	Return an objlist of singletons of the form {name: [elems]}
	where elems is the tuple constructed from zipping the given objlists
	"""
	return objlist({name: list(tup)} for tup in zip(*(ol.L for ol in objlists)))

def join(name, *objlists):
	"""
	Return an objlist of singletons of the form {name: [elems]}
	where elems is the tuple constructed from taking the cartesian product of 
	the given objlists
	"""
	return objlist({name: list(tup)} for tup in product(*(ol.L for ol in objlists)))



if __name__=='__main__':
	import unittest

	class TestObjList(unittest.TestCase):
		def test_param(self):
			self.assertEqual(param("k",[1]), objlist({"k":1}) )
			self.assertEqual(param("k",[1,2,...]), objlist([{"k":1},{"k":2},{}]) )
			self.assertEqual(param("k",[...]), objlist({}) )
			self.assertEqual(param("k",[...,...]), objlist([{},{}]) )
			self.assertEqual(param("k",[...,1,...]), objlist([{},{"k":1},{}]))

		def test_zipper(self):
			c1 = zipper("point", param("x", [1,2,3]), param("y",[4,5,6]))
			c2 = objlist([
				{"point": [{"x":1}, {"y":4}]},
				{"point": [{"x":2}, {"y":5}]},
				{"point": [{"x":3}, {"y":6}]}
				])
			self.assertEqual(c1, c2)

		def test_zipper_empty(self):
			self.assertEqual(zipper("foo"), objlist())
			self.assertEqual(zipper("foo", param("k",[...])), {"foo":[{}]})

		def test_merging(self):
			c = objlist([{"x":1,"y":3}, {"x":2, "y":4}])
			self.assertEqual(param("x",[1,2])|param("y",[3,4]), c)
			self.assertEqual(param("y",[3,4])|param("x",[1,2]), c)

		def test_merge_empty(self):
			self.assertEqual(param("x",[1,2])|objlist({}), param("x",[1,2]))
			self.assertEqual(param("x",[1,2])|{}, param("x",[1,2]))
			self.assertEqual({}|param("x",[1,2]), param("x",[1,2]))

		def test_add(self):
			self.assertEqual(param("x",[1])+param("x",[2]), param("x",[1,2]))
			self.assertEqual( {"x": 1}+param("x",[2]), param("x",[1,2]))
			self.assertEqual( param("x",[1])+{"x": 2} , param("x",[1,2]))

		def test_copy(self):
			c = param("x", [1,2,3])
			self.assertEqual(c, c.copy())
			c = objlist()
			self.assertEqual(c, c.copy())		
			c = objlist({})
			self.assertEqual(c, c.copy())		

		def test_iter(self):
			c = param("x", [1,2,3])
			self.assertEqual(c, objlist(c))
			c = objlist()
			self.assertEqual(c, objlist(c))		
			c = objlist({})
			self.assertEqual(c, objlist(c))		

		def test_select(self):
			self.assertEqual(param("x",[1,2,3,4,5]).select(lambda obj: obj["x"]>4),
				{"x": 5})
			self.assertEqual(param("x",[1,2,3]).select(lambda obj: True), param("x",[1,2,3]))
			self.assertEqual(param("x",[1,2,3]).select(lambda obj: False), param("x",[]))

		def test_bool(self):
			self.assertTrue(objlist({}))
			self.assertFalse(objlist())

		def test_len(self):
			self.assertEqual(len(objlist()), 0)
			self.assertEqual(len(objlist({})), 1)
			self.assertEqual(len(param("x", range(100))), 100)

		def test_getitem(self):
			L  = param("x", [1,2,3]) | param("y", [4,5,6])
			self.assertEqual( list(L['x']), [1,2,3] )
			self.assertEqual( list(L['y']), [4,5,6] )
			self.assertEqual( list(L['z']), [...,...,...])

		def test_setitem(self):
			L  = param("x", [1,2,3]) | param("y", [4,5,6])
			L["x"] = 0
			self.assertEqual( list(L['x']), [0,0,0] )
			self.assertEqual( list(L['y']), [4,5,6] )
			self.assertEqual( list(L['z']), [...,...,...])

		def test_setitem_ellipsis(self):
			L  = param("x", [1,2,3]) | param("y", [4,5,6])
			L["x"] = ...
			self.assertEqual( list(L['x']), [...,...,...] )
			self.assertEqual( list(L['y']), [4,5,6] )
			self.assertEqual( list(L['z']), [...,...,...])

		def test_pop(self):
			L  = param("x", [1,2,3]) | param("y", [4,5,6])
			self.assertEqual( L.pop('x'), [1,2,3] )
			self.assertEqual( list(L['x']), [...,...,...] )

		def test_update(self):
			L  = param("x", [1,2,3]) | param("y", [4,5,6])
			L.update({"x":1})
			self.assertEqual(L, {"x":1}*param("y", [4,5,6]) )

		def test_project(self):
			L  = param("x", [1,2,3]) | param("y", [4,5,6])
			L.project(lambda obj: {"x": obj["x"]+3})
			self.assertEqual(L, param("x",[4,5,6])|param("y", [4,5,6]) )

	unittest.main()


