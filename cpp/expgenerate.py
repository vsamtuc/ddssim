import os,json
from itertools import product
from collections import ChainMap
from string import Formatter

#
# Class to construct experiments
#

class ExperimentFactory(Formatter):
	"""
	Encapsulate the idiosyncracies of PBS installations and automate
	the generation of experiment code
	"""
	def __init__(self, name, joblist, **kwargs):
		"""
		Construct an experiment factory.
		`name` is the experiment name
		`joblist` is an iterable returning json-encodable objects
		
		JSON encoding is done by an encoder capable of understanding callables.
		If a callable item is to be encoded into JSON, the value returned by
		self.json_encode(self, item). For example:

		from datetime import datetime

		{
			"creation_time": lambda fct: str(datetime.now())
			"no_of_components": lambda fct: len(fct['components'])
		}

		will result in an object whose creation time field will be generated
		as a string at JSON encoding time. Notice the wrapping into a lambda,
		which accepts (without using) this object.

		The items that can be used in the encoding include (from higher to lower
		priority):
		- the per-job keys:
		  - 'jobid'  an integer starting at 1, containing the current job serial no
		  - 'obj'    the object defining the current job

		- the per-jobset keys:
		  - all kwargs passed to method 'generate()'
		  - 'exp_name' (experiment name) a string
		  - 'job_list' the list of job objects to generate

		- the environment (os.environ)
		"""
		assert isinstance(name,str)
		self.exp_name = name
		self.joblist = list(joblist)
		self.job_cfg = { "jobid": None, "obj": None }
		self.jobset_cfg = dict(**kwargs)
		self.jobset_cfg["exp_name"] = name
		self.jobset_cfg["job_list"] = self.joblist

		self.jobset_cfg.setdefault("jobdir",".")
		self.CFG = ChainMap(self.job_cfg, self.jobset_cfg, os.environ)

	def json_encode(self, obj):
		# if this is a callable object, call it with self as argument
		if callable(obj):
			obj = obj(self)
		return obj

	def get_value(self, key, args, kwargs):
		"""
		Overloads the method inherited from Formatter, to look into self.CFG.
		This allows to use all job-related data in templates.
		"""
		if isinstance(args, int):
			val = args[key]
		elif key in kwargs:
				val = kwargs[key]
		else:
			val = self.CFG[key]
		return self.json_encode(val)

	def __getitem__(self, key):
		"""
		Convenience function to access self.CFG
		"""
		return self.CFG[key]

	def json_dump(self, obj, jsfile=None):
		if jsfile is not None:
			return json.dump(obj, jsfile, indent="\t", default=self.json_encode)
		else:
			return json.dumps(obj, indent="\t", default=self.json_encode)


	def generate(self, batch, **kwargs):
		"""
		Generate a set of files for batch execution.
		For each job there is a batch submission (e.g. .pbs, .slurm, ...) 
		file and a .json file. Jobs are numbered from 1 up to the number 
		of elements in objl.

		exp_name is the experiment name
		objl     is the list of job objects
		"""

		# populate the jobset_cfg
		saved_jobset_cfg = self.jobset_cfg.copy()
		self.jobset_cfg.update(kwargs)

		# generate the jobs
		jobid = 0
		for obj in self.joblist:
			# Update self.CFG (via self.job_cfg) so that json encoding can work correctly
			jobid += 1
			self.job_cfg['jobid'] = jobid
			self.job_cfg['obj'] = obj

			# Crete the metadata object
			job_mdata = {
				"job_name":None				
			}

	  		# write the json file
			json_filename = self.format("{jobdir}/{exp_name}{jobid:05d}.json")
			with open(json_filename, 'w') as jsfile:
				self.json_dump(obj, jsfile)

			# add the job
			batch.add_job(self, obj)

		# restore job_cfg
		self.job_cfg['jobid'] = None
		self.job_cfg['obj'] = None

		# restore jobset_cfg
		self.jobset_cfg.clear()
		self.jobset_cfg.update(saved_jobset_cfg)

# 
# Callables to inject config info into the objects during json encoding
#

def fmt(tmpl):
	"""
	Return a callable that takes as input the config object and returns a string.
	The string may contain encoded fields from this object.

	For example:
	{
		"homedir" : fmt("{HOME}")
	}
	"""
	return lambda fmt: fmt.format(tmpl)

def current_datetime(fmt):
	"""
	usage { "curdate": current_datetime }
	"""
	import datetime
	return str(datetime.now())

def auto_uuid(fmt):
	"""
	usage { "id": auto_uuid }

	Note: this may give surprising results if used unwisely!
	"""
	import uuid
	return str(uuid.uuid1())




#
# Generator template for batch system job submission files
#

class BatchQueue:
	"""
	Represents a typical batch queue as a template for job submission files,
	together with arguments appearing in the template. The template may also
	refer to arguments taken from the CFG of an experiment factory
	"""
	def __init__(self, suffix, tmpl, **kwargs):
		"""
		A template and arguments appearing in the template.

		The template may use arguments like jobid and exp_name that
		are defined by an experiment factory. Also, the template may
		be a callable
		"""
		self.suffix = suffix
		self.tmpl = tmpl
		self.kwargs = dict(**kwargs)

	def add_job(self, exp_fct, obj):
		"""
		Add a new job submission file using the template
		{jobdir}/{exp_name}{jobid:05d}.pbs
		"""
		pbs_filename = exp_fct.format("{jobdir}/{exp_name}{jobid:05d}.{suffix}", suffix=self.suffix, **self.kwargs)
		with open(pbs_filename,'w') as pbsfile:
  			pbsfile.write(exp_fct.format(self.tmpl, **self.kwargs))


class PbsQueue(BatchQueue):
	def __init__(self, tmpl, **kwargs):
		super().__init__("pbs", tmpl, **kwargs)

class SlurmQueue(BatchQueue):
	def __init__(self, tmpl, **kwargs):
		super().__init__("sbat", tmpl, **kwargs)




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

		def test_encoding(self):			
			def my_subobject(fct):    # a function to use in an object
				return {"a": "success"}

			# We shall only use the json_encode method of this object
			fct = ExperimentFactory("test_encoding", objlist())
			import json
			# encode object with embedded function
			jsn = json.dumps({ "foo": my_subobject }, default=fct.json_encode)
			# encode 'ground' object normally
			jsn2 = json.dumps({ "foo": {"a": "success"}})
			# they must be equal
			self.assertEqual(jsn,jsn2)

	unittest.main()


