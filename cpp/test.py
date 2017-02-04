import sys
from dds import *

def execute():

	wcup = wcup_ds("/home/vsam/src/datasets/wc_day44")

	D = dataset()
	D.load(wcup)
	D.set_max_length(100000)
	D.hash_sources(4)
	D.set_time_window(3600)
	D.create()

	r = progress_reporter(40)
	dss = data_source_statistics()

	components = []
	sids = list(CTX.metadata().stream_ids) # need to order them!
	for i in range(len(sids)):
		components.append(selfjoin_exact_method(sids[i]))
		for j in range(i):
			components.append(twoway_join_exact_method(sids[j], sids[i]))

	wcout = CTX.open("wc_tseries.dat",open_mode.truncate)
	#wcout = output_pyfile(sys.stdout)
	CTX.timeseries.bind(wcout)
	print("timeseries=", CTX.timeseries.size())

	repter = reporter(50000)

	CTX.run()

	CTX.close_result_files()

def test():
	f = output_pyfile(sys.stdout)
	f.bind(CTX.timeseries)
	CTX.timeseries.prolog()	

if __name__=='__main__':
	execute()

