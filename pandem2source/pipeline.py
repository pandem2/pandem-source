import pykka
import os
import time
import threading
import datetime
import json
from collections import defaultdict
from . import worker
from . import util
from abc import ABC, abstractmethod, ABCMeta

class Pipeline(worker.Worker):
    __metaclass__ = ABCMeta  
    def __init__(self, name, orchestrator_ref, settings, retry_failed = False): 
        super().__init__(name = name, orchestrator_ref = orchestrator_ref, settings = settings)
        self.retry_failed = retry_failed

    def on_start(self):
        super().on_start()
        self._storage_proxy = self._orchestrator_proxy.get_actor('storage').get().proxy()
        self._frxml_proxy = self._orchestrator_proxy.get_actor('ftreader_xml').get().proxy()
        self._frcsv_proxy = self._orchestrator_proxy.get_actor('ftreader_csv').get().proxy()
        self._frxls_proxy = self._orchestrator_proxy.get_actor('ftreader_xls').get().proxy()
        self._unarchive_proxy = self._orchestrator_proxy.get_actor('unarchiver').get().proxy()
        self._dfreader_proxy = self._orchestrator_proxy.get_actor('dfreader').get().proxy()
        self._standardizer_proxy = self._orchestrator_proxy.get_actor('standardizer').get().proxy()
        self._variables_proxy = self._orchestrator_proxy.get_actor('variables').get().proxy()
        self.job_steps = defaultdict(dict)
        self.decompressed_files = defaultdict(list)
        self.job_df = defaultdict(dict)
        self.job_tuples = defaultdict(dict)
        self.job_stdtuples = defaultdict(dict)
        self.job_issues = defaultdict(list)
        self.pending_count = {}

        self.job_dicos = [self.decompressed_files, self.job_df, self.job_tuples, self.job_stdtuples, self.pending_count, self.job_issues]

        self.last_step = "publish_ended" #TODO: update this as last step evolves
        jobs = self._storage_proxy.read_db('job',
                                          filter= lambda x: 
                                             ( x['status'] == 'in progress' or (x['status'] == 'failed' and self.retry_failed)) 
                                             and x['step'] != 'submitted_started' 
                                             and x['step'] != self.last_step 
                                         ).get()
        if jobs is not None :
            jobs = jobs.to_dict(orient = 'records')

            new_dls = dict((dls["scope"]["source"],dls) for dls in  (self._storage_proxy.read_file(f["path"]).get() for f in self._storage_proxy.list_files('source-definitions').get()))
            for j in jobs:
              j["step"]="submitted_ended"
              j["status"]="in progress"
              j["dls_json"] = new_dls[j["source"]]
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
        for job in self.job_steps['submitted_ended'].copy().values():
            submitted_files = job['source_files']
            if "decompress" not in job['dls_json']['acquisition'].keys():
                self.update_job_step(job, 'read_format_started')
                self.send_to_readformat(submitted_files, job)
            else:
                self.update_job_step(job, 'unarchive_started')
                self.send_to_unarchive(submitted_files, job)
        
        # Jobs after decompression ends
        for job in self.job_steps['unarchive_ended'].copy().values():
            decompressed_files = self.decompressed_files[job['id']]
            self.update_job_step(job, 'read_format_started')
            self.send_to_readformat(decompressed_files, job)

        # Jobs after read format ends
        for job in self.job_steps['read_format_ended'].copy().values():
            dfs = self.job_df[job["id"]]
            self.update_job_step(job, 'read_df_started')
            self.send_to_read_df(dfs, job)
        
        # Jobs after read df ends
        for job in self.job_steps['read_df_ended'].copy().values():
            tuples = self.job_tuples[job["id"]]
            self.update_job_step(job, 'standardize_started')
            self.send_to_standardize(tuples, job)

        # Jobs after standardize ends
        for job in self.job_steps['standardize_ended'].copy().values():
            tuples = self.job_stdtuples[job["id"]]
            self.update_job_step(job, 'publish_started')
            self.send_to_publish(tuples, job)
        
        # Jobs after publish ends
        for job in self.job_steps['publish_ended'].copy().values():
            # cleaning job dicos
            for job_dico in self.job_dicos:
              if job["id"] in job_dico:
                job_dico.pop(job["id"])
            # TODO: delete jobs other than last 10 jobs per source
    

    def submit_files(self, dls, paths):
        job_record = {'source': dls['scope']['source'],
                      'file_sizes': [os.path.getsize(path)/1024 for path in paths],
                      'progress': '0%',
                      'start_on': datetime.datetime.now(),
                      'end_on': '',
                      'step': 'submitted_started',
                      'status': 'in progress',
                      'dls_json' : dls
                     }
        job_id = self._storage_proxy.write_db(job_record, 'job').get()
        dest_files = [str(job_id)+'_'+'_'.join(path.split('files/')[1].split('/')) for path in paths]
        paths_in_staging = [self.staging_path(dls, file) for file in dest_files]
        self._storage_proxy.copy_files(paths, paths_in_staging)
        job_id = self._storage_proxy.write_db({'id': job_id,
                                              'source_files': paths_in_staging,
                                              'step': 'submitted_ended'}, 
                                             'job').get()
        job = self._storage_proxy.read_db('job', filter= lambda x: x['id']==job_id).get().to_dict(orient='records')[0]

        self.update_job_step(job, 'submitted_ended')

    def send_to_readformat(self, paths, job):
       self.pending_count[job["id"]] = len(paths)
       for file_path in paths:
          file_ext = os.path.splitext(file_path)[1]
          if file_ext == '.csv':
              self._frcsv_proxy.read_format_start(job, file_path)
          elif file_ext == '.rdf':
              self._frxml_proxy.read_format_start(job, file_path)
          elif file_ext == '.xml':
              self._frxml_proxy.read_format_start(job, file_path)
          elif file_ext in ('.xls', '.xlsx'):
              self._frxls_proxy.read_format_start(job, file_path)
        
          else:
              raise RuntimeError('unsupported format')

    def read_format_end(self, job, path, df):
        #print(f'df head for {job["id"]} is: {df.head()}')
        #print(f'pending_count is: {self.pending_count}')
        self.pending_count[job["id"]] = self.pending_count[job["id"]] - 1

        #print(f'df head for file: {path} is : {df.head(10)}')
        self.job_df[job["id"]][path] = df
        if self.pending_count[job["id"]] == 0:
            self.update_job_step(job, 'read_format_ended')

    def send_to_unarchive(self, paths, job):
        filter_paths = job['dls_json']["acquisition"]["decompress"]["path"]
        self.pending_count[job["id"]] = len(paths) * len(filter_paths)
        
        for archive_path in paths:
          for filter_path in filter_paths:
            self._unarchive_proxy.unarchive(archive_path, filter_path, job)

    
    def unarchive_end(self, archive_path, filter_path, bytes, job):
        self.pending_count[job["id"]] = self.pending_count[job["id"]] - 1
        
        dest_file = os.path.basename(archive_path)+'_'+('_'.join(filter_path.split('/'))) 
        path_in_staging = self.staging_path(job['dls_json'], dest_file)
        self.decompressed_files[job['id']].append(path_in_staging)
        self._storage_proxy.write_file(path_in_staging, bytes, 'wb+').get()
        if self.pending_count[job["id"]] == 0:
            self.update_job_step(job, 'unarchive_ended')

    def send_to_read_df(self, dfs, job):
        self.pending_count[job["id"]] = len(dfs)
        for path, df in dfs.items():
            self._dfreader_proxy.df2tuple(df, path, job, job["dls_json"])

    def read_df_end(self, tuples, issues, path, job): 
        self.pending_count[job["id"]] = self.pending_count[job["id"]] - 1
        self.job_tuples[job["id"]][path] = tuples
        self.job_issues[job["id"]].extend(issues)
        if self.pending_count[job["id"]] == 0:
            self.write_issues(self.job_issues[job["id"]]) 
            errors_number = sum(issue['issue_severity']=='error' for issue in self.job_issues[job["id"]])
            if errors_number == 0:
                self.update_job_step(job, 'read_df_ended')
            else:
                self.fail_job(job)

    def send_to_standardize(self, tuples, job):
        self.pending_count[job["id"]] = len(tuples)
        for path, ttuples in tuples.items():
            self._standardizer_proxy.standardize(ttuples, path, job, job["dls_json"])

    def standardize_end(self, tuples, issues, path, job): 
        self.pending_count[job["id"]] = self.pending_count[job["id"]] - 1
        self.job_stdtuples[job["id"]][path] = tuples
        self.job_issues[job["id"]].extend(issues)
        if self.pending_count[job["id"]] == 0:
            self.write_issues(self.job_issues[job["id"]]) 
            errors_number = sum(issue['issue_severity']=='error' for issue in self.job_issues[job["id"]])
            if errors_number == 0:
                self.update_job_step(job, 'standardize_ended')
            else:
                self.fail_job(job)

    def send_to_publish(self, tuples, job):
        self.pending_count[job["id"]] = len(tuples)
        for path, ttuples in tuples.items():
            #print(f"publishing ${ttuples}")
            self._variables_proxy.write_variable(ttuples, path, job)

    def publish_end(self, path, job): 
        self.pending_count[job["id"]] = self.pending_count[job["id"]] - 1
        if self.pending_count[job["id"]] == 0:
            self.update_job_step(job, 'publish_ended')
    
    # this function returns a future that can be waited to ensure that file job is written to disk
    def update_job_step(self, job, step):
        print(f"Changing to step {step} for job {job['id']} source {job['dls_json']['scope']['source']}")
        # removing job from current step dict
        if job["step"] in self.job_steps and job["id"]:
          if job["id"] in self.job_steps[job["step"]]:
            self.job_steps[job["step"]].pop(job["id"])
          else:
            print(f"Warning: could not find id {job['id']} in job_steps {job['step']}") 
        # changing the job step 
        job["step"] = step

        # addong the job to te new dict unless is on the last step
        if step == self.last_step:
          job["status"] = "Success"
        else:
          self.job_steps[step][job["id"]] = job

        return self._storage_proxy.write_db(job, 'job')
    
    # this function returns a future that can be waited to ensure that file job is written to disk
    def fail_job(self, job):
        print(f"Changing status of job {job['id']} to 'failed' for {job['dls_json']['scope']['source']}")
        print(util.pretty(self.job_issues[job["id"]]))
        # removing job from current step dict
        if job["step"] in self.job_steps and job["id"]:
          if job["id"] in self.job_steps[job["step"]]:
            self.job_steps[job["step"]].pop(job["id"])
          else:
            print(f"Warning: could not find id {job['id']} in job_steps {job['step']}") 
        # changing the job status 
        job["status"] = "failed"

        return self._storage_proxy.write_db(job, 'job')

    def write_issues(self, issues):
        issues_codes = set([issue['issue_type'] for issue in issues])
        for code in issues_codes:
            issues_to_write = [issue for issue in issues if issue['issue_type']==code]
            if len(issues_to_write)>50:
                issues_to_write = issues_to_write[:49]
            for issue in issues_to_write:
                self._storage_proxy.write_db(record=issue, db_class='issue').get() 

  

    def staging_path(self, dls, *args):
        return self.pandem_path(f'files/staging', dls['scope']['source'], *args)

