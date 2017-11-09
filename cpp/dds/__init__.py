#
# The Distributed Data Stream library
#
# This library wraps the C++ library of the same name
#


##############################################
#
#  Imports and pythonization of warts
#
##############################################


import _dds

# from dds.hh
from _dds import ds_metadata, dds_record, qtype,\
  query, self_join, join, twoway_join, id_set, named

def __describe(mdata):
	return {
		'size': mdata.size,
		'mintime': mdata.mintime,
		'maxtime': mdata.maxtime,
		'minkey': mdata.minkey,
		'maxkey': mdata.maxkey,
		'stream_ids': set(mdata.stream_ids),
		'source_ids': set(mdata.source_ids)
	}

ds_metadata.describe = __describe
del __describe

# from data_source.hh

from _dds import data_source, time_window_source, \
	wcup_ds, crawdad_ds, time_window,\
	uniform_data_source, \
	buffered_dataset, buffered_data_source, \
	materialized_data_source, hdf5_ds


# from output.hh

from _dds import basic_column, output_binding, \
	output_table, result_table, time_series,\
	output_file, open_mode, output_c_file, output_stdout,\
	output_stderr, output_pyfile, output_hdf5

def _output_table_getattr(self, attr):
	return self[attr].value
def _output_table_setattr(self, attr, val):
	self[attr].value = val  # this may throw (e.g. for computed)
output_table.__getattr__ = _output_table_getattr
output_table.__setattr__ = _output_table_setattr

from _dds import (column_str, column_bool, column_float, column_double,
 column_short, column_int, column_long, column_llong,
 column_ushort, column_uint, column_ulong, column_ullong,
 computed_short, computed_int, computed_long, computed_llong,
 computed_ushort, computed_uint, computed_ulong, computed_ullong,
 computed_bool, computed_float, computed_double
 )

# eca.hh

from _dds import Event, INIT, DONE, START_STREAM, END_STREAM,\
  START_RECORD, END_RECORD, VALIDATE, REPORT, RESULTS,\
  eca_rule, basic_control

 # method.hh

from _dds import context, reactive, dataset, reporter,\
   progress_reporter, CTX

# accurate.hh

from _dds import data_source_statistics, selfjoin_exact_method,\
	twoway_join_exact_method, selfjoin_agms_method, \
	twoway_join_agms_method

# results.hh

from _dds import local_stream_stats, network_comm_results, \
	network_host_traffic, network_interfaces

# safezone.hh

from _dds import selfjoin_query, selfjoin_agms_safezone

# mathlib.hh

from _dds import Vec

# dsarch.hh

from _dds import (channel, host, basic_network, 
	rpc_obj, rpc_method, rpc_interface, rpc_protocol,
	process, local_site,
	RPCC_BITS_PER_IFC, unknown_addr,
	RPCC_ENDP_MASK, RPCC_METH_MASK, RPCC_RESP_MASK
	)

# 
import dds.agms

# cfgfile.hh
from _dds import execute

##########################################
#
#  Python Utilities
#
#  This is the pythonic part of the standard
#  dds library.
#
##########################################

def loadcfg(fname):
	"""
	This method loads a json jobfile into python objects.

	Note that these files may contain comments, which the json
	parser in python cannot swallow, thus we first filter the
	file with a  simple regular expression.

	Unfortunately, this r.e. may fail, if a '//' appears inside
	a string of the file. Use with caution! 
	"""
	import json, re
	jstxt = open(fname).read()
	jstxt = re.sub(r'//.*\n', '\n', jstxt)
	return json.loads(jstxt)


#############################
#
# Execution patterns
# 
#############################





#############################
#
# Execution data
#
#############################





#############################
#
# Result analysis
#
#############################





