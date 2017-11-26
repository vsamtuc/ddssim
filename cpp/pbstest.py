from genpbs import *


if __name__=='__main__':
	# set up a big job

	pr = {"depth": 7}*param("width", [500,1000,1500,2000])
	pr0 = pr|{"epsilon":0.0}

	proj = pr.nest("projection")
	proj0 = pr0.nest("projection")

	beta = param("beta", [0.02,0.04,0.06,0.08,0.1])

	stream = {"query": "SELFJOIN", "stream":0}

	gmcommon = proj0*beta*stream

	fgm = objlist({ "name": "fgm", "type": "FGM" })*gmcommon
	fgm_nocm = objlist({ "name": "fgm_nocm", "type": "FGM", "use_cost_model":False })*gmcommon
	fgm_noeik = objlist({ "name": "fgm_noeik", "type": "FGM", "eikonal":False })*gmcommon
	sgm = objlist({ "name": "sgm", "type": "SGM"})*gmcommon

	agms = objlist({"name":"amssj0", "type":"agms_query"})*stream*proj*beta.nil()
	exact = (objlist({ "name": "sj0", "type": "exact_query"})|stream)*gmcommon.nil()

	components = zipper("components", fgm, fgm_nocm, fgm_noeik, sgm, agms, exact)

	k = param("hash_sources",[2,4,6,8,12,16,20,...])
	window = param("time_window", [3600*i for i in range(1,5)])
	window.project(lambda obj: {"warmup_time": obj["time_window"]})

	dataset = objlist({"driver": "hdf5",
			"file": "/git/ddssim/cpp/wc_day46.h5",
			"hash_streams": 1,
			"set_max_length": 10000000
		}) * k * window
	dataset = dataset.nest("dataset")

	files = {
	    "files" : {
			"sto" : "stdout:",
			"gm_comm" : fmt("file:{exp_name}{jobid:05d}.dat?open_mode=truncate"),
			"comm" : fmt("file:{exp_name}{jobid:05d}_comm.dat?open_mode=truncate"),
			"h5f" : fmt("hdf5:{exp_name}{jobid:05d}_results.h5"),
			"wcout" : fmt("file:{exp_name}{jobid:05d}_tseries.dat?open_mode=truncate,format=csvtab")
	    },

	    "bind" : {
			"timeseries" : ["wcout", "h5f"],
			"gm_comm_results": ["sto", "gm_comm"],
			"network_comm_results": ["comm", "h5f"],
			"network_host_traffic": ["h5f"],
			"network_interfaces" : ["h5f"]
	    },

	    "sample" : {
			"timeseries": 1000
	    }	
	}

	jobset = components * dataset * files
	print("Generating",len(jobset),"jobs")

	FAKI.generate("test_pbs", jobset, jobdir='/home/vsam/git/ddssim/cpp')
