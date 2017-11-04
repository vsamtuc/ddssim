
import json
from itertools import product

pbs_template = """\
#PBS -N agmrun%05d
#PBS -l nodes=1
#PBS -q quzo
#PBS -o .
#PBS -j oe
#PBS -d /home/vsam/exp1
###PBS -m a -M vsam@softnet.tuc.gr
#PBS -k oe

/home/vsam/git/ddssim/cpp/agmexp /home/vsam/exp1/agm%05d.json
"""


def write_pbs(jobid):
    with open("agmrun%05d.pbs" % jobid,'w') as pbsfile:
        pbsfile.write(pbs_template % (jobid,jobid))


def component(proto, beta, l, d):
    proj = {"depth":d, "width":l, "epsilon":0.0}
    return {
        "type": proto,
        "stream":0,
        "projection": proj,
        "beta": beta
    }

def dataset(k, wtime):
    dset = {
        "driver": "hdf5",
        "file" : "/git/ddssim/cpp/wc_day46.h5",
        "hash_streams": 1
        }
    if k is not None:
        dset["hash_sources"] = k
    if wtime is not None:
        dset["time_window"] = wtime
        dset["warmup_time"] = wtime
        dset["cooldown"] = True
    return dset
        
def write_json(jobid, k, wtime, proto, abeta, al, ad):
    OBJ = {}

    OBJ["components"] = [component(p,b,l,d)
                         for p,b,l,d in product(proto, abeta, al, ad)
    ]

    OBJ["dataset"] = dataset(k, wtime)

    OBJ["files"] = {
        "gm_comm" : "file:agmcomm%05d.dat?open_mode=append" % jobid,
    }

    OBJ["bind"] = {
        "gm_comm_results": ["gm_comm"]
    }

    OBJ["sample"] = {
        "timeseries": 1000
    }
            
    with open("agm%05d.json" % jobid, 'w') as jsfile:
        json.dump(OBJ, jsfile, indent=1)


def exp1():
    ak = [None,2,4,6,8,12,16,20]
    awtime = [3600,7200,10800,14400,18000]

    abeta = [0.02, 0.04, 0.06, 0.08, 0.1]
    al = [500,1000,1500,2000,2500]
    ad = [7]
    proto = ["agm","gm"]

    jobid = 1
    
    for k in ak:
        for wtime in awtime:
            write_json(jobid, k, wtime, proto, abeta, al, ad)
            write_pbs(jobid)
            jobid+=1

if __name__=='__main__':
    exp1()
