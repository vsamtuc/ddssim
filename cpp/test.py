import sys
from dds import *

def prepare_data():
	#wcup = wcup_ds("/home/vsam/src/datasets/wc_day44")
	wcup = wcup_ds("/storage/tuclocal/vsam/src/datasets/wc_day44")

	D = dataset()
	D.load(wcup)
	D.set_max_length(1000)
	D.hash_sources(4)
	D.hash_streams(2)
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

def printcb(msg):
	return lambda:print(msg)

msg1 = printcb("Reacted to init")
msg2 = printcb("Should not happen!")
msg3 = printcb("Done is called !!!!!!!")

class my_component(reactive):
	def __init__(self):
		super().__init__()
		self.on(INIT, msg1)
		self.on(START_STREAM, self.start)
		self.rend = self.on(END_STREAM,msg2)
		self.on(DONE, msg3)
		self.on(END_STREAM, self.end)
	def start(self):
		self.cancel(self.rend)
		del self.rend
	def end(self):
		print("Cancelling all (and myself!)")
		self.cancel_all()
	def __del__(self):
		print("Destroyed")


def execute():
	prepare_data()
	components = prepare_components()

	# prepare output
	wcout = CTX.open("wc_tseries.dat",open_mode.truncate)
	sout = output_pyfile(sys.stdout)
	CTX.timeseries.bind(sout)
	CTX.timeseries.bind(wcout)
	print("timeseries=", CTX.timeseries.size())

	repter = reporter(50000)

	action = lambda: print("Hello!")
	my_component()

	rule = CTX.on(INIT, lambda: False, action)
	print("Rule added:", int(rule.event))

	# run
	CTX.run()
	# cleanup
	CTX.close_result_files()
	CTX.cancel_rule(rule)

def test():
	f = output_pyfile(sys.stdout)
	f.bind(CTX.timeseries)
	CTX.timeseries.prolog()	

if __name__=='__main__':
	execute()
	pass
