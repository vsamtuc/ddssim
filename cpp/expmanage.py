#
# Jupyter code for managing experiments
#

#
# Note: currently, this is PBS-specific
#

import json, re, io, os, subprocess
from enum import Enum

from IPython.display import display
#from __future__ import print_function
from ipywidgets import interact, interactive, interactive_output, fixed, interact_manual
import ipywidgets as widgets


class PbsJob:
    """
    A simple object initialized from the XML output of 'qstat -x'
    """
    def __init__(self, xml):
        # simply copy the xml tags recursively
        for child in xml:
            if child.text is not None:
                setattr(self, child.tag, child.text)
            else:
                setattr(self, child.tag, PbsJob(child))


            
class Status(Enum):
    """
    This enum describes the status of a job.

    CREATED the job simply exists, it has not been run
    QUEUED  the job is in the batch queue but it is not running
    RUNNING the job is currently executing
    FAILED  execution has ended with an error, output is invalid/partial
    DONE    execution has ended successfully, output is valid
    """
    def __init__(self, label, color):
        self.label = label
        self.color = color
        
    CREATED = ('created', 'yellow')
    QUEUED  = ('in queue', 'green')
    RUNNING = ('running', 'blue')
    FAILED  = ('failed', 'red')
    DONE    = ('done', 'magenta')


class Job:
    """
    A job object contains current data about the state of a job
    """
    def __init__(self, exp, jobid):
        self.exp = exp
        self.jobid = jobid
        self.pbs_job = None
        self.output_size = None

    def job_name(self):
        """
        The job name
        """
        return "{exp_name}{jobid:05d}".format(exp_name=self.exp.exp_name,jobid=self.jobid)

    def status(self):
        """
        The job status
        """
        if self.pbs_job is not None:
            if self.pbs_job.job_state=='Q':
                return Status.QUEUED
            elif self.pbs_job.job_state=='R':
                return Status.RUNNING
            else:
                raise ValueError("unknown PBS job state:"+self.pbs_job.job_state)
        elif self.output_size is not None:
            if self.output_size >0:
                return Status.DONE
            else:
                return Status.FAILED
        else:
            return Status.CREATED

    def get_pbs_attr(self, attr, default=None):
        """
        Get a PBS path expression, described by a string.
        For example,  job.get_pbs_attr(".foo.bar", None) will try to access
        attribute pbsjob.foo.bar, and return None if the access 
        failed.
        """
        # Use eval to return an object path, or default if there is an error
        try:
            return eval("self.pbs_job"+attr, {}, {"self": self})
        except:
            return default

    def run(self, overwrite=False):
        # let the experiment actually do it
        self.exp.run(self, overwrite)
        

class Metadata:
    """
    This class wraps metadata about an experiment.
    It can be instantiated without an argument, in which case it will look for
    a file called "metadata.json" in the current directory
    """
    
    @staticmethod
    def get_metadata():
        """
        Actually open and parse file metadata.json
        """
        with open("metadata.json",'r') as mdfile:
            md = json.load(mdfile)
            return md
        
    def __init__(self, md=None):
        if md is None:
            md = Metadata.get_metadata()
        self.exp_name = md['exp_name']
        self.njobs = md['njobs']

    def _repr_html_(self):
        return """<div>Experiment <b>{exp_name}</b> ({njobs} jobs)</div>""".format(exp_name=self.exp_name, 
                                                                                   njobs=self.njobs)
    def __repr__(self):
        return "Metadata({exp_name})".format(exp_name=self.exp_name)
        
        
class QStat:
    """
    Execute and parse the output of 'qstat -x', creating an indexed array of
    PbsJob objects, only for the current experiment.
    """
    
    @staticmethod
    def __qstat_all():
        return subprocess.run(['/storage/exp_soft/tuc/torque/bin/qstat','-x'],
                              stdout=subprocess.PIPE)

    def __qstat_job(pbs_job):
        return subprocess.run(['/storage/exp_soft/tuc/torque/bin/qstat','-x', pbs_job],
                              stdout=subprocess.PIPE)
            
    def __init__(self, pbs_id=None):
        self.meta = Metadata()
        # Execute qstat and obtain the output
        if pbs_id is None:
            proc = self.__qstat_all()
        else:
            proc = self.__qstat_job(pbs_id)

        if proc.returncode != 0:
            self.errmsg = "Cannot execute qstat to acquire state"
            self.jobs = None
        else:
            self.jobs = self.process_xml(proc.stdout)
            self.jobs_by_name = {x.Job_Name: x for x in self.jobs}
            self.errmsg = None

    def get_job(self, jobid):
        job_name = "{exp_name}{jobid:05d}".format(exp_name=self.meta.exp_name, jobid=jobid)
        return self.jobs_by_name.get(job_name, None)
        
    
    def process_xml(self, xml):
        import xml.etree.ElementTree as ET
        root = ET.fromstring(xml)
        jobs = []
        jobname_re = re.compile(self.meta.exp_name+"[0-9]{5}")
        for job in root:
            if job.tag!='Job': 
                continue
            job_name = [job[pos].text for pos in range(len(job)) if job[pos].tag=='Job_Name'][0]
            if not jobname_re.fullmatch(job_name):
                continue
            jobs.append(PbsJob(xml=job))
        return jobs
            
    def _repr_html_(self):
        if self.errmsg is None:
            stream = io.StringIO()
            stream.write("""<div>Queue State for jobs of experiment <b>{exp_name}</b></div>"""
                         .format(exp_name=self.meta.exp_name))
            if len(self.jobs)==0:
                stream.write("<p>No jobs</p>")
            else:
                stream.write("""<table>
                <tr><th>job name</th><th>batch job id</th><th>job state</th>
                """)
                for job in self.jobs:
                    stream.write("""
                    <tr><td>{job_name}</td><td>{queue_id}</td><td>{job_status}</td></tr>
                    """.format(job_name=job.Job_Name, queue_id=job.Job_Id, job_status=job.job_state)
                                )
                stream.write("""</table>""")
            return stream.getvalue()
            
        else:
            return """Could not get state: {errmsg}""".format(errmsg=self.errmsg)


class JobList:
    """
    A list of jobs, with display logic
    """
    def __init__(self, exp_name, jobs):
        self.exp_name = exp_name
        self.jobs = jobs    
        
    def _repr_html_(self):        
        stream = io.StringIO()
        stream.write("<div>")
        stream.write("Jobs of experiment <b>{exp_name}</b>: {num}    "
                     .format(exp_name=self.exp_name, num=len(self.jobs)))

        # Compute the counts for each state
        from collections import Counter
        status_count = Counter(job.status() for job in self.jobs)
        counter_line= ", ".join("{count} {status}".format(count=status_count[s], status=s.label)
                                for s in status_count if status_count[s]>0)

        if counter_line:
            stream.write("<em>(")
            stream.write(counter_line)
            stream.write(")</em>")
        
        stream.write("</div>")
        stream.write("""<table>
            <tr>
            <th>Job name</th>
            <th>Batch job id</th>
            <th>Status</th>
            <th>Output</th>
            <th>Run time</th>
            </tr>
            """)
        if len(self.jobs)==0:
            stream.write("""<tr><td colspan="5"><em>No jobs</em></td><tr>""")
        for job in self.jobs:
            stream.write("""
            <tr>
            <td>{job_name}</td>
            <td>{queue_id}</td>
            <td style="background-color: {status_color}" >{job_status}</td>
            <td>{output_file}</td>
            <td>{run_time}</td>
            </tr>
            """.format(job_name=job.job_name(), 
                       queue_id= job.get_pbs_attr(".Job_Id", "---"),
                       job_status=job.status().label, status_color=job.status().color,
                       output_file = (job.output_size if job.output_size is not None else "---"),
                       run_time = job.get_pbs_attr(".resources_used.walltime", "---")
            ))
        stream.write("""</table>""")
        return stream.getvalue()

    def select(self, pred):
        return JobList(self.exp_name, [job for job in self.jobs if pred(job)])
    
    def by_status(self, status):
        return self.select(lambda job: job.status()==status)

    def __iter__(self):
        return iter(self.jobs)
    
class Experiment(JobList):
    """
    Represents the data known about a whole experiment
    """
    def __init__(self, md=None):
        self.meta = Metadata() if md is None else md
        self.exp_name = self.meta.exp_name
        self.jobs = [Job(self,jobid) for jobid in range(1,self.meta.njobs+1)]
        self.gather_state()

    def __gather_state_for_job(self, job, qstat):
        # update job status from given qstat
        job.pbs_job = qstat.get_job(job.jobid)
        
        # update output file status
        outfile = "{job_name}.dat".format(job_name=job.job_name())
        job.output_file = outfile
        job.output_size = os.stat(outfile).st_size if os.access(outfile, os.F_OK) else None
                    
    def gather_state(self):
        qstat = QStat()
        for job in self.jobs:
            self.__gather_state_for_job(job, qstat)


    def run(self, job, overwrite=False):
        """
        Run (or re-run) a job
        """        
        if job.pbs_job is not None:
            raise RuntimeError("A job which is already running cannot be run!")
        if job.status() is Status.DONE and not overwrite:
            raise RuntimeError("To run a DONE job again, you must specify overwrite")

        # ok, we do it
        job_subfile = "{job_name}.pbs".format(job_name=job.job_name())
        proc = subprocess.run(["qsub", job_subfile], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # Raise if unsuccessful
        proc.check_returncode()

        # else, qsub stdout contains the job 
        pbs_jobid = proc.stdout.strip()

        # Get QStat for the new job
        qstat = QStat(pbs_jobid)
        self.__gather_state_for_job(job, qstat)
        return pbs_jobid
        

class ExperimentBrowser:
    """
    A view of an experiment object, adorned with widgets.
    """
    def __init__(self, exp=None):
        if exp is None:
            exp = Experiment()
        self.exp = exp
        
        #chooser = interactive(self.show_jobs, 
        #                choice=widgets.Dropdown(options=list(opts.keys())) )
        self.chooser = widgets.Dropdown(options=list(self.opts.keys()))
        chooser_label = widgets.Label(value="Jobs by status:")
        refresher = widgets.Button(description="Refresh data")
        refresher.on_click(self.refresh_state)

        self.out = interactive_output(self.show_jobs, { "choice": self.chooser })

        display(widgets.HBox([chooser_label, self.chooser, refresher]))
        display(self.out)
        

    opts = {
        "all": lambda job: True,
        "created": lambda job: job.status()==Status.CREATED,
        "failed": lambda job: job.status()==Status.FAILED,
        "queued": lambda job: job.status()==Status.QUEUED,
        "done": lambda job: job.status()==Status.DONE,
        "running": lambda job: job.status()==Status.RUNNING,
        "queued or running": lambda job: job.status() in (Status.RUNNING,Status.QUEUED)
    }

    def refresh_state(self, button):
        self.exp.gather_state()
        self.out.clear_output()
        with self.out:
            self.show_jobs(self.chooser.value)
    
    def show_jobs(self, choice):
        display(self.exp.select(self.opts[choice]))


