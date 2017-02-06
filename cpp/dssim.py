
import sys
from dds import *

def prepare_data():
	wcup = wcup_ds("/home/vsam/src/datasets/wc_day44")

	D = dataset()
	D.load(wcup)
	D.set_time_window(3600)
	D.create()

def prepare_components():
	components = []
	sids = list(CTX.metadata().stream_ids) # need to order them!
	for i in range(len(sids)):
		components.append(selfjoin_exact_method(sids[i]))
		for j in range(i):
			components.append(twoway_join_exact_method(sids[j], sids[i]))

	r = progress_reporter(40)
	dss = data_source_statistics()
	components += [r,dss]
	return components


def execute():
	prepare_data()
	components = prepare_components()

	# prepare output
	wcout = CTX.open("wc_tseries.dat",open_mode.truncate)
	sout = output_pyfile(sys.stdout)

	CTX.timeseries.bind(wcout)
	print("timeseries=", CTX.timeseries.size())

	repter = reporter(25000)

	# run
	CTX.run()

	# cleanup
	CTX.close_result_files()


def execute_generated():
	CTX.data_feed(uniform_data_source(5, 25, 1000000, 100000))
	components = prepare_components()

	wcout = CTX.open("uni_tseries.dat",open_mode.truncate)
	sout = output_pyfile(sys.stdout)

	CTX.timeseries.bind(wcout)
	print("timeseries=", CTX.timeseries.size())

	repter = reporter(1000)

	# run
	CTX.run()

	# cleanup
	CTX.close_result_files()


if __name__=='__main__':
	#execute()
	#execute_generated()
	pass

