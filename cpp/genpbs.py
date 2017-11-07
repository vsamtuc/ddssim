import os,json
from itertools import product

#
# The PBS file
#

pbs_template = """\
#PBS -N agmrun{jobid:05d}
#PBS -l nodes=1
#PBS -q {queue}
#PBS -o {jobdir}
#PBS -e {jobdir}
#PBS -d {jobdir}
#PBS -j oe
###PBS -m a -M vsam@softnet.tuc.gr
#PBS -k oe

{executable} {jobdir}/agm{jobid:05d}.json
"""

HOME = os.getenv('HOME')

cfg = {
    "queue": "batch",
    "jobdir": HOME+"/exp1",
    "executable": HOME+"/git/ddssim/cpp/dssim"
}

def write_pbs(jobid):
    with open("agmrun%05d.pbs" % jobid,'w') as pbsfile:
        pbsfile.write(pbs_template.format(jobid=jobid, **cfg))

#
# The json file
#

def component(name, proto, beta, l, d):
    proj = {"depth":d, "width":l, "epsilon":0.0}
    return {
        "name": name,
        "type": proto,
        "stream":0,
        "projection": proj,
        "beta": beta
    }

def components(proto, abeta, al, ad)
    return [component("{p}{n}".format(p=p,n=n),p,b,l,d)
        for p,(n,(b,l,d)) in product(proto, enumerate(product(abeta, al, ad),1))
    ]

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
        dset["flush_window"] = False
    return dset
        
def write_json(jobid, k, wtime, proto, abeta, al, ad):
    OBJ = {
        "components": components(proto, abeta, al, ad),
        "dataset": dataset(k, wtime),
        "files": {
            "gm_comm" : "file:agm%05d.dat?open_mode=truncate" % jobid,
        }
        "bind": {
            "gm_comm_results": ["gm_comm"]
        }
        "sample": {
            "timeseries": 1000
        } 
    }            
    with open("agm%05d.json" % jobid, 'w') as jsfile:
        json.dump(OBJ, jsfile, indent=1)


def exp1():
    #ak = [None,2,4,6,8,12,16,20]
    #awtime = [3600,7200,10800,14400,18000]
    #abeta = [0.02, 0.04, 0.06, 0.08, 0.1]
    #al = [500,1000,1500,2000,2500]
    #ad = [7]
    #proto = ["agm","gm"]

    ak=[None]
    awtime=[None]
    abeta=[0.1]
    al=[500]
    ad=[7]
    proto=["agm","gm"]

    jobid = 1
    
    for k in ak:
        for wtime in awtime:
            write_json(jobid, k, wtime, proto, abeta, al, ad)
            write_pbs(jobid)
            jobid+=1

if __name__=='__main__':
    exp1()
