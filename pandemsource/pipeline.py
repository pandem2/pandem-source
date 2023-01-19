import pykka
import os
import time
import threading
import datetime
import json
import hashlib
from collections import defaultdict
from . import worker
from . import util
from abc import ABC, abstractmethod, ABCMeta
from .util import printMem
import pickle
import logging

l = logging.getLogger("pandem.pipeline")

class Pipeline(worker.Worker):
    __metaclass__ = ABCMeta  
    def __init__(self, name, orchestrator_ref, settings, retry_failed = False, restart_job = -1, retry_active = True): 
        super().__init__(name = name, orchestrator_ref = orchestrator_ref, settings = settings)
        self.retry_failed = retry_failed
        self.retry_active = retry_active
        self.restart_job = restart_job
        self._jobs_to_keep = settings["pandem"]["source"]["pipeline"]["jobs-to-keep"]

    def on_start(self):
        super().on_start()
        self._storage_proxy = self._orchestrator_proxy.get_actor('storage').get().proxy()
        self._frxml_proxy = self._orchestrator_proxy.get_actor('ftreader_xml').get().proxy()
        self._frcsv_proxy = self._orchestrator_proxy.get_actor('ftreader_csv').get().proxy()
        self._frxls_proxy = self._orchestrator_proxy.get_actor('ftreader_xls').get().proxy()
        self._frjson_proxy = self._orchestrator_proxy.get_actor('ftreader_json').get().proxy()
        self._unarchive_proxy = self._orchestrator_proxy.get_actor('unarchiver').get().proxy()
        self._dfreader_proxy = self._orchestrator_proxy.get_actor('dfreader').get().proxy()
        self._standardizer_proxy = self._orchestrator_proxy.get_actor('standardizer').get().proxy()
        self._variables_proxy = self._orchestrator_proxy.get_actor('variables').get().proxy()
        self._nlp_proxy = self._orchestrator_proxy.get_actor('nlp_annotator').get().proxy()
        self._aggregate_proxy = self._orchestrator_proxy.get_actor('aggregator').get().proxy()
        self._evaluator_proxy = self._orchestrator_proxy.get_actor('evaluator').get().proxy()

        self.job_steps = defaultdict(dict)
        self.decompressed_files = defaultdict(list)
        self.job_precalstamp= {}
        self.job_precalstep = {}
        self.job_df = defaultdict(dict)
        self.job_tuples = defaultdict(dict)
        self.job_stdtuples = defaultdict(dict)
        self.job_aggrtuples = defaultdict(dict)
        self.job_precalinds = {}
        self.job_indicators = {}
        self.job_issues = defaultdict(list)
        self.pending_count = {}
        self.pending_total = {}

        

        self.job_dicos = [
          self.decompressed_files, self.job_df, self.job_tuples, self.job_stdtuples, self.job_aggrtuples, 
          self.job_precalinds, self.job_indicators, self.pending_count, self.pending_total, self.job_issues, self.job_precalstamp, self.job_precalstep
        ]

        self.last_step = "ended"
        jobs = self._storage_proxy.read_db('job',
                                          filter= lambda x: 
                                             ( 
                                                 ((x['status'] == 'in progress' and self.retry_active) or (x['status'] == 'failed' and self.retry_failed)) 
                                                   and x['step'] != 'submitted_started' 
                                                   and x['step'] != self.last_step
                                                   and self.restart_job == -1
                                             ) or x["id"] == self.restart_job
                                         ).get()
        if jobs is not None :
            jobs = jobs.to_dict(orient = 'records')

            new_dls = dict(
              (dls["scope"]["source"],dls) 
              for dls in  (
                self._storage_proxy.read_file(f["path"]).get() for f in self._storage_proxy.list_files('source-definitions').get() if f['path'].endswith(".json"))
              )
            for j in jobs:
              j["step"]="submitted_ended"
              j["status"]="in progress"
              j["dls_json"] = new_dls[j["source"]]
              # deleting issues on jobs we are restarting
              self._storage_proxy.delete_db('issue', filter = lambda i: int(i['job_id']) == int(j["id"])) 

            self.job_steps['submitted_ended'] = dict([(j["id"], j) for j in jobs])  
        process_repeat = worker.Repeat(datetime.timedelta(seconds=1))
        self.register_action(process_repeat, self.process_jobs) 
    

    def loop_actions(self):
        self._self_proxy.process_jobs()

    def process_jobs(self):
        # This function will process active jobs asynchronoulsy.
        # Based on current status an action will be performed and the status will be updated
        # Jobs after submit that has just been submitted 
        # they will go to decompress or to format read
        jobs = self.job_steps['submitted_ended'].copy().values()
        ipath = 0
        done = False
        while not done:
          done = True
          for job in jobs:
            paths = job['source_files']
            if ipath == 0:
              if "decompress" not in job['dls_json']['acquisition'].keys():
                self.update_job_step(job, 'read_format_started', 0.1)
                self.pending_count[job["id"]] = len(paths)
              else: 
                filter_paths = job['dls_json']["acquisition"]["decompress"]["path"]
                self.pending_count[job["id"]] = len(paths) * len(filter_paths) 
                self.update_job_step(job, 'unarchive_started', 0.1)
            if ipath < len(paths):
              done = False
              if "decompress" not in job['dls_json']['acquisition'].keys():
                self.send_to_readformat(paths[ipath], job)
              else:
                self.send_to_unarchive(paths[ipath], filter_paths, job)
          ipath = ipath + 1

        # Jobs after decompression ends
        jobs = self.job_steps['unarchive_ended'].copy().values()
        ipath = 0
        done = False
        while not done: 
          done = True
          for job in jobs:
            paths = self.decompressed_files[job['id']]
            if ipath == 0:
              self.pending_count[job["id"]] = len(paths)
              self.update_job_step(job, 'read_format_started', 0.2)
            if ipath < len(paths):
              done = False
              self.send_to_readformat(paths[ipath], job)
          ipath = ipath + 1

        # Jobs after read format ends
        jobs = self.job_steps['read_format_ended'].copy().values()
        ipath = 0
        done = False
        while not done: 
          done = True
          for job in jobs:
            dfs = self.job_df[job["id"]]
            paths = list(dfs.keys())
            paths.sort()
            if ipath == 0:
              self.pending_count[job["id"]] = len(paths)
              self.update_job_step(job, 'read_df_started', 0.3)
            if ipath < len(paths):
              done = False
              path = paths[ipath]
              self.send_to_read_df(dfs[path], path, job)
          ipath = ipath + 1
        
        # Jobs after read df ends
        jobs = self.job_steps['read_df_ended'].copy().values()
        ipath = 0
        done = False
        while not done: 
          done = True
          for job in jobs:
            tuples = self.job_tuples[job["id"]]
            paths = list(tuples.keys())
            paths.sort()
            if ipath == 0:
              self.pending_count[job["id"]] = len(paths)
              self.update_job_step(job, 'standardize_started', 0.4)
            if ipath < len(paths):
              done = False
              path = paths[ipath]
              self.send_to_standardize(tuples[path], path, job)
          ipath = ipath + 1
        
        # Jobs after standardize ends
        jobs = self.job_steps['standardize_ended'].copy().values()
        ipath = 0
        done = False
        while not done: 
          done = True
          for job in jobs:
            tuples = self.job_stdtuples[job["id"]]
            paths = list(tuples.keys())
            paths.sort()
            if ipath == 0:
              self.pending_count[job["id"]] = len(paths)
              # non standardized tuples can be deleted
              self.job_tuples.pop(job["id"])
              self.job_df.pop(job["id"])
              if self.job_to_annotate(job):
                self.update_job_step(job, 'annotate_started', 0.5)
              elif ipath == 0:
                self.update_job_step(job, 'aggregate_started', 0.6)
            
            if ipath < len(paths):
              done = False
              path = paths[ipath]
              if self.job_to_annotate(job):
                self.send_to_annotate(tuples[path], path, job)
              elif ipath == 0:
                self.send_to_aggregate(tuples, job)
          ipath = ipath + 1

        # Jobs after annnotate text ends
        for job in self.job_steps['annotate_ended'].copy().values():
            tuples = self.job_stdtuples[job["id"]]
            self.update_job_step(job, 'aggregate_started', 0.6)
            self.send_to_aggregate(tuples, job)
        
        # Jobs after aggregate ends
        for job in self.job_steps['aggregate_ended'].copy().values():
            # standardized tuples can be deleted
            self.job_stdtuples.pop(job["id"])
            tuples = self.job_aggrtuples[job["id"]]
            self.update_job_step(job, 'precalculate_started', 0.7)
            self.send_to_precalculate(tuples, job)
        
        # Jobs after precalculate ends
        jobs = self.job_steps['precalculate_ended'].copy().values()
        ipart = 0
        done = False
        while not done: 
          done = True
          for job in jobs:
            part_list = [*self.job_aggrtuples[job["id"]].keys()]
            if ipart == 0:
              self.pending_count[job["id"]] = len(part_list)
              self.pending_total[job["id"]] = len(part_list)
              self.update_job_step(job, 'publish_started', 0.9)
            if ipart < len(part_list):
              done = False
              tuples = self.job_aggrtuples[job["id"]][part_list[ipart]]
              self.send_to_publish(tuples, job)
            if ipart == len(part_list):
              # aggregated tuples are not necessary anymore
              self.job_aggrtuples.pop(job["id"])
          ipart = ipart + 1


        # Jobs after calculate ends
        jobs = self.job_steps['calculate_ended'].copy().values()
        ipart = 0
        done = False
        while not done:
          done = True
          for job in jobs:
            part_list = [*self.job_indicators[job["id"]].keys()]
            if ipart == 0:
              self.pending_count[job["id"]] = len(part_list)
              self.pending_total[job["id"]] = len(part_list)
              self.update_job_step(job, 'publish_started', 0.9)
            if ipart < len(part_list):
              done = False
              indicators_tuples = self.job_indicators[job["id"]][part_list[ipart]]
              self.send_to_publish(indicators_tuples, job)
            if ipart == len(part_list):
              pass
          ipart = ipart + 1

        # Jobs after publish indicator ended
        for job in self.job_steps['publish_ended'].copy().values():
            calc_step = self.job_precalstep[job['id']] + 1
            self.job_precalstep[job['id']] = calc_step
            # recreating the indicators to calculate for the current step
            ind_in_step = [(k,v) for k,v in self.job_precalinds[job['id']] if v["step"] == calc_step]
            if len(ind_in_step) > 0:
              self.update_job_step(job, 'calculate_started', 0.8)
              self.send_to_calculate(ind_in_step, job)
            else:       
              self.update_job_step(job, 'ended', 1.0)
              self.remove_job(job)
     
    def submit_files(self, dls, paths):
        job_record = {'source': dls['scope']['source'],
                      'file_sizes': [os.path.getsize(path)/1024 for path in paths],
                      'progress': 0.0,
                      'start_on': datetime.datetime.now(),
                      'end_on': None,
                      'step': 'submitted_started',
                      'status': 'in progress',
                      'dls_json' : dls
                     }
        job_id = self._storage_proxy.write_db(job_record, 'job').get()
        dest_files = [str(job_id)+'_'+'_'.join(path.split('files/')[1].split('/')) for path in paths]
        
        paths_in_staging = [self.staging_path(job_id, 'in', file) for file in dest_files]
        self._storage_proxy.copy_files(paths, paths_in_staging)
        job_id = self._storage_proxy.write_db({'id': job_id,
                                              'source_files': paths_in_staging,
                                              'step': 'submitted_ended'}, 
                                             'job').get()
        job = self._storage_proxy.read_db('job', filter= lambda x: x['id']==job_id).get().to_dict(orient='records')[0]

        self.update_job_step(job, 'submitted_ended', 0.05)

    def send_to_readformat(self, path, job):
       dls = job["dls_json"]
       format_name = dls["acquisition"]["format"]["name"].lower() if "format" in dls["acquisition"] and "name" in dls["acquisition"]["format"] else None

       if format_name == "csv" or path.endswith('.csv'):
           self._frcsv_proxy.read_format_start(job, path)
       elif format_name == "xml" or path.endswith('.rdf') or path.endswith('.xml'):
           self._frxml_proxy.read_format_start(job, path)
       elif format_name == "xls" or path.endswith(".xls") or path.endswith('.xlsx'):
           self._frxls_proxy.read_format_start(job, path)
       elif format_name == "json" or path.endswith('.json') or path.endswith('.json.gz'):
           self._frjson_proxy.read_format_start(job, path)
       else:
           raise RuntimeError(f'Unsupported format in {path}')

    def read_format_end(self, job, path, df):
        if job["status"] == "failed":
          return
        self.pending_count[job["id"]] = self.pending_count[job["id"]] - 1 
        
        self.job_df[job["id"]][path] = df 
        if self.pending_count[job["id"]] == 0:
            self.update_job_step(job, 'read_format_ended', 0.25)

    def send_to_unarchive(self, path, filter_paths, job):
        for filter_path in filter_paths:
          self._unarchive_proxy.unarchive(path, filter_path, job)

    
    def unarchive_end(self, archive_path, filter_path, bytes, job):
        if job["status"] == "failed":
          return
        self.pending_count[job["id"]] = self.pending_count[job["id"]] - 1
        dest_file = hashlib.sha1(f"{archive_path}/{filter_path}".encode()).hexdigest()+'_'+os.path.basename(filter_path)
        dir_in_staging = self.staging_path(job['id'], 'unarch')
        os.makedirs(dir_in_staging, exist_ok = True)
        path_in_staging = self.staging_path(job['id'], 'unarch', dest_file)
        self.decompressed_files[job['id']].append(path_in_staging)
        self._storage_proxy.write_file(path_in_staging, bytes, 'wb+').get()
        if self.pending_count[job["id"]] == 0:
            self.update_job_step(job, 'unarchive_ended', 0.15)

    def send_to_read_df(self, df, path, job):
        self._dfreader_proxy.df2tuple(df, path, job, job["dls_json"])

    def read_df_end(self, tuples, n_tuples, issues, path, job): 
        if job["status"] == "failed":
          return
        self.pending_count[job["id"]] = self.pending_count[job["id"]] - 1
        self.job_tuples[job["id"]][path] = tuples
        self.job_issues[job["id"]].extend(issues)
       
        if (self.pending_count[job["id"]] + len(self.job_tuples[job["id"]])) == 0:
          progress = 0.0
        else: 
          progress =  (1.0/10) * len(self.job_tuples[job["id"]]) / (self.pending_count[job["id"]] + len(self.job_tuples[job["id"]]))
        
        if self.pending_count[job["id"]] == 0:
            self.write_issues(self.job_issues[job["id"]]) 
            errors_number = sum(issue['issue_severity']=='error' for issue in self.job_issues[job["id"]])
            warning_number = sum(issue['issue_severity']=='warning' for issue in self.job_issues[job["id"]])
            if errors_number == 0 and (n_tuples > 0 or warning_number == 0):
                self.update_job_step(job, 'read_df_ended', 0.3 + progress)
            else:
                self.fail_job(job)
        else:
            self.update_job_step(job, job["step"], 0.3 + progress)

    def send_to_standardize(self, tuples, path, job):
        if job["status"] == "failed":
          return
        self._standardizer_proxy.standardize(tuples, path, job, job["dls_json"])

    def standardize_end(self, tuples, n_tuples, issues, path, job): 
        if job["status"] == "failed":
          return
        self.pending_count[job["id"]] = self.pending_count[job["id"]] - 1
        self.job_stdtuples[job["id"]][path] = tuples 
        # deleting existing ref-not-found issue types since they come from a previous execution
        self.job_issues[job["id"]] = [i for i in self.job_issues[job['id']] if i['issue_type'] != "ref-not-found"]
        self._storage_proxy.delete_db("issue", lambda i: int(i["job_id"]) == int(job['id']) and i['issue_type'] == "ref-not-found").get()
        
        self.job_issues[job["id"]].extend(issues)
        if self.pending_count[job["id"]] + len(self.job_stdtuples[job["id"]]) == 0:
          progress = 0.0
        else:
          progress =  (1.0/10) * len(self.job_stdtuples[job["id"]]) / (self.pending_count[job["id"]] + len(self.job_stdtuples[job["id"]]))
        
        if self.pending_count[job["id"]] == 0:
            self.write_issues(self.job_issues[job["id"]]) 
            errors_number = sum(issue['issue_severity']=='error' for issue in self.job_issues[job["id"]])
            warning_number = sum(issue['issue_severity']=='warning' for issue in self.job_issues[job["id"]])
            if all(t is None for t in self.job_stdtuples[job['id']].values()):
                # If all tuples returnes are None means that some referential were missing and that this source needs to be delayed
                l.debug(f"Source {job['dls_json']['scope']['source']} has been reversed to read format end since some referential failed completely, probably some depending source is missing")
                self.update_job_step(job, 'read_df_ended',  0.4)
            elif errors_number == 0 and (n_tuples > 0 or warning_number == 0):
                self.update_job_step(job, 'standardize_ended',  0.4 + progress)
            else:
                self.fail_job(job)
        else:
            self.update_job_step(job, job["step"], 0.4 + progress)

    def send_to_annotate(self, tuples, path, job):
        self._nlp_proxy.annotate(tuples, path, job)

    def annotate_end(self, tuples, path, job):
        if job["status"] == "failed":
          return
        self.pending_count[job["id"]] = self.pending_count[job["id"]] - 1
        self.job_stdtuples[job["id"]][path] = tuples
        if self.pending_count[job["id"]] == 0:
            self.update_job_step(job, 'annotate_ended', 0.55)

    def send_to_aggregate(self, tuples, job):
        self._aggregate_proxy.aggregate(tuples, job)

    def aggregate_end(self, tuples, var, progress, job): 
        if job["status"] == "failed":
          return
        
        self.job_aggrtuples[job["id"]][var] = tuples
        
        if progress == 1:
          self.update_job_step(job, 'aggregate_ended', 0.7)
        else:
          self.update_job_step(job, job["step"], 0.6 + progress / 10)
    
    def send_to_precalculate(self, tuples, job):
        self._evaluator_proxy.plan_calculate(tuples, job)

    def precalculate_end(self, indicators_to_calculate, job): 
        if job["status"] == "failed":
          return
        self.job_precalinds[job["id"]] = indicators_to_calculate
        self.job_precalstep[job["id"]] = 0
        self.job_precalstamp[job["id"]] = time.time()
        self.update_job_step(job, 'precalculate_ended', 0.75)

    def send_to_calculate(self, indicators_to_calculate, job): 
        self._evaluator_proxy.calculate(indicators_to_calculate, job)

    def calculate_end(self, ind_tuples, job): 
        if job["status"] == "failed":
          return
        self.job_indicators[job["id"]] = ind_tuples
        self.update_job_step(job, 'calculate_ended', 0.85)

    def send_to_publish(self, tuples, job):
        calc_step = self.job_precalstep[job['id']]
        calc_stamp = self.job_precalstamp[job['id']] 
        self._variables_proxy.write_variable(tuples, calc_step, calc_stamp, job)
    
    def publish_end(self, job): 
        if job["status"] == "failed":
          return
        self.pending_count[job["id"]] = self.pending_count[job["id"]] - 1
        if self.pending_total[job["id"]] == 0:
          progress == 0
        else:
          progress =  ((self.pending_total[job["id"]] - self.pending_count[job["id"]]) / self.pending_total[job["id"]])/10
        if self.pending_count[job["id"]] == 0:
          self.update_job_step(job, 'publish_ended', 1)
        else:
          self.update_job_step(job, job["step"], 0.9 + progress)
    
    # this function returns a future that can be waited to ensure that file job is written to disk
    def update_job_step(self, job, step, progress):
        l.info(f"Changing to step {step} for job {int(job['id'])} source {job['dls_json']['scope']['source']} {round(progress*100, 1)}%")
        #printMem()
        # removing job from current step dict
        if job["step"] in self.job_steps and job["id"] is not None:
          if job["id"] in self.job_steps[job["step"]]:
            self.job_steps[job["step"]].pop(job["id"])
          else:
            l.warning(f"Warning: could not find id {int(job['id'])} in job_steps {job['step']}") 
        # changing the job step 
        job["step"] = step
        job["progress"] = progress

        # addong the job to te new dict unless is on the last step
        if step == self.last_step:
          job["status"] = "success"
          job["end_on"] = datetime.datetime.now()
          job["progress"] = 1
          self._storage_proxy.delete_job_cache(job["id"]).get()
        else:
          self.job_steps[step][job["id"]] = job

        ret  = self._storage_proxy.write_db(job, 'job')
        return ret
    
    # this function returns a future that can be waited to ensure that file job is written to disk
    def fail_job(self, job, delete_job = False, issue = None):
        if type(job) == int :
          for stepjobs in self.job_steps.values():
            if job in stepjobs:
              job = stepjobs[job]
              break
        if type(job) == int:
          if delete_job:
            self.clean_job_ids([job])
          else:
            raise ValueError(f"Could not find job {job}")
        else:
          l.warning(f"Changing status of job {job['id']} to 'failed' for {job['dls_json']['scope']['source']}")
          # removing job from current step dict
          if job["step"] in self.job_steps and job["id"]:
            if job["id"] in self.job_steps[job["step"]]:
              self.job_steps[job["step"]].pop(job["id"])
            else:
              l.warning(f"Warning: could not find id {job['id']} in job_steps {job['step']}") 
          # changing the job status 
          job["status"] = "failed"

          if delete_job:
            self.remove_job(job)
          else:
            self._storage_proxy.write_db(job, 'job')
          # writing and issue if required
          if issue is not None:
            self._storage_proxy.write_db(record=issue, db_class='issue').get() 
          self._storage_proxy.delete_job_cache(job["id"]).get()

    def write_issues(self, issues):
        type_count = {issue['issue_type']:0 for issue in issues}
        for issue in issues:
            if type_count[issue['issue_type']] < 50:
              type_count[issue['issue_type']] =  type_count[issue['issue_type']] + 1
              self._storage_proxy.write_db(record=issue, db_class='issue').get() 

    def job_to_annotate(self, job):
      if "columns" in job['dls_json']:
        for col in job['dls_json']['columns']:
          if "variable" in col and col["variable"] == "article_text":
            return True
      return False

    def remove_job(self, job):    
        for job_dico in self.job_dicos:
            if job["id"] in job_dico:
              job_dico.pop(job["id"])
        self.clean_old_jobs(job["source"])

    def clean_old_jobs(self, source):
        # getting 10 oldest jobs
        jobs = self._storage_proxy.read_db("job", lambda j: j["source"] == source).get()
        if len(jobs) > self._jobs_to_keep:
          n_del = len(jobs) - self._jobs_to_keep
          to_del = jobs.sort_values("id", axis = 0, ascending = True).head(n_del)["id"].tolist()
          self.clean_job_ids(to_del) 
    
    def clean_job_ids(self, ids):
      to_del = {int(j) for j in ids}
      # deleting issues
      self._storage_proxy.delete_db("issue", lambda i: int(i["job_id"]) in to_del ).get()
      # deleting jobs
      self._storage_proxy.delete_db("job", lambda j: int(j["id"]) in to_del).get()
      # deleting staging
      for job_id in to_del:
        self._storage_proxy.delete_dir(self.staging_path(int(job_id)))

