#
# General-purpose library for distributed stream simulations
#

import sys, os
from dds import *
import pandas as pd
import numpy as np


##############################################
#
#  Standard execution utilities
#
##############################################

#
#  Input data/streams
#

dataset_path = os.getenv("HOME")+"/src/datasets/"
def wc_day44():
	return wcup_ds(dataset_path+"wc_day44")
def wc_day45():
	return wcup_ds(dataset_path+"wc_day44")
def wc_day46():
	return wcup_ds(dataset_path+"wc_day44")
def crawdad():
	return p_ds(dataset_path+"wifi_crawdad_sorted")


def feed_data(dset, max_length=None, streams=None, sources=None,time_window=None):
	D = dataset()
	D.load(dset)
	if max_length is not None:
		D.set_max_length(max_length)
	if streams is not None:
		D.hash_streams(streams)
	if sources is not None:
		D.hash_sources(sources)
	if time_window is not None:
		D.set_time_window(time_window)
	D.create()


#
#
#  Output config
#
#

def setup_output(tseries = 1000, txtout=False):
	from datetime import datetime
	import h5py as h5
	
	std_tables = [CTX.timeseries,
		network_comm_results,
		network_host_traffic,
		network_interfaces
	]

	# make a run id
	runid = "run_%d" % (round(datetime.now().timestamp())%100000)
	print("runid=",runid)

	R = reporter()

	h5file = h5.File(runid+".h5")
	h5of = output_hdf5(h5file.id.id, open_mode.truncate)

	R.sample(CTX.timeseries, tseries)
	for table in std_tables:
		R.watch(table)
		table.bind(h5of)
	pbar = progress_reporter(40)

	CTX.C += [h5of, R, pbar]


def all_runs():
	l =  [x for x in os.listdir() if x.startswith('run_') and x.endswith('.h5')]
	l.sort()
	return l

def cleanup_runs():
	allfiles = all_runs()
	if len(allfiles)>1:
		for f in allfiles[:-1]:
			os.unlink(f)

def latest_run():
	allfiles = all_runs()
	if allfiles:
		return allfiles[-1]


#
#
#  Execution
#
#

def execute():

	feed_data(wc_day46(), streams=1, time_window=4*3600)

	# components
	proj = agms.projection(7, 500)
	proj.epsilon = 0.05
	CTX.C = [
		tods.network(proj, 0.1),
		gm2.network(0, proj, 0.1),
		selfjoin_exact_method(0),
		selfjoin_agms_method(0, proj)
	]

	setup_output(tseries=10000)
	CTX.run()
	del CTX.C



#
#
#  Output analytics
#
#

import pandas as pd

def latest_store():
	return pd.HDFStore(latest_run())



def traffic_by_message_type(bytes_per_message=20.):
	"""
	For each network draw a pie chart showing the distribution
	of traffic among message types.
	"""
	store = latest_store()
	nht = store['network_host_traffic']
	ni = store['network_interfaces']

	# join with ni to replace rpcc with text

	# first add the RSP codes to ni, getting nin
	nio = pd.DataFrame(ni[ni.oneway==0])
	nio['rpcc'] +=1
	nin = pd.concat([ni, nio])

	# join nht with nin
	ntraf = pd.merge(nht, nin, 
		left_on=('netname','endp'), right_on=('netname','rpcc')
		)[['netname','method','src','dst','msgs','bytes']]

	# group by method and sum
	ntraf = ntraf.groupby(('netname','method')).sum()
	# throw away the (meaningless) src and dst sums
	ntraf = ntraf[['msgs','bytes']]

	for netname in ni['netname'].unique():
		# get the data for GM2
		ntraf_net = ntraf.T[netname].T
		ntraf_tot = bytes_per_message*ntraf_net['msgs'] + ntraf_net['bytes']
	
		# plot together
		ntraf_tot.plot.pie(subplots=True)



def query_approximation_error():
	"""
	For each network, show the time-series of the relative error
	in query estimates.
	"""
	store = latest_store()
	gm2 = store.timeseries.gm2_
	tods = store.timeseries.tods_
	agms = store.timeseries.agms_selfjoin_0
	err_gm2 = abs((gm2-agms)/agms)
	err_tods = abs((tods-agms)/agms)
	df = pd.DataFrame({'err_gm2':err_gm2, 'err_tods':err_tods})
	df.plot(logy=True)


class plot_wrapper:
	def __init__(self,plotfn):
		self.plotfn = plotfn
	def __repr__(self):
		return self.plotfn()

show_traffic_by_message_type = plot_wrapper(traffic_by_message_type)
show_query_approximation_error = plot_wrapper(query_approximation_error)





if __name__=='__main__':
	#execute_exp()
	pass

