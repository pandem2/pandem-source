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
import logging as l

class Pipeline(worker.Worker):
    __metaclass__ = ABCMeta  
    def __init__(self, name, orchestrator_ref, settings, retry_failed = False, restart_job = 0): 
        super().__init__(name = name, orchestrator_ref = orchestrator_ref, settings = settings)
        self.retry_failed = retry_failed
        self.restart_job = restart_job

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
        self.job_steps = defaultdict(dict)
        self.job_df = defaultdict(dict)
        self.job_tuples = defaultdict(dict)
        self.job_stdtuples = defaultdict(dict)
        self.job_aggrtuples = defaultdict(dict)
        self.job_precaltuples = defaultdict(dict)
        self.job_precalinds = defaultdict(dict)
        self.job_indicators = defaultdict(dict)
        self.job_issues = defaultdict(list)
        self.pending_count = {}

        

        self.job_dicos = [
          self.decompressed_files, self.job_df, self.job_tuples, self.job_stdtuples, self.job_aggrtuples, self.job_precaltuples, 
          self.job_precalinds, self.job_indicators, self.pending_count, self.job_issues
        ]

        self.last_step = "publish_indicators_ended" #TODO: update this as last step evolves
        jobs = self._storage_proxy.read_db('job',
                                          filter= lambda x: 
                                             (  ( (x['status'] == 'in progress' or x['status'] == 'failed') and self.retry_failed) 
                                                 and x['step'] != 'submitted_started' 
                                                 and x['step'] != self.last_step
                                              ) or x["id"] == self.restart_job
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
            if self.job_to_annotate(job):
                self.update_job_step(job, 'annotate_started')
                self.send_to_annotate(tuples, job)
            else:
                self.update_job_step(job, 'aggregate_started')
                self.send_to_aggregate(tuples, job)

        # Jobs after annnotate text ends
        for job in self.job_steps['annotate_ended'].copy().values():
            tuples = self.job_stdtuples[job["id"]]
            self.update_job_step(job, 'aggregate_started')
            self.send_to_aggregate(tuples, job)
        
        # Jobs after aggregate ends
        for job in self.job_steps['aggregate_ended'].copy().values():
            tuples = self.job_aggrtuples[job["id"]]
            self.update_job_step(job, 'precalculate_started')
            self.send_to_precalculate(tuples, job)
        
        # Jobs after precalculate ends
        for job in self.job_steps['precalculate_ended'].copy().values():
            tuples = self.job_precaltuples[job["id"]]  
            self.update_job_step(job, 'publish_facts_started')
            self.send_to_publish_facts(tuples, job)

        # Jobs after publish facts ends
        for job in self.job_steps['publish_facts_ended'].copy().values():
            indicators_to_calculate = self.job_precalinds[job["id"]]    
            self.update_job_step(job, 'calculate_started')
            # Limit indicators to calculate for those on 
            # step = self.job_precalstep[job['id']]
            # indicators_to_calculate = self.job_precalinds[job['id']]['steps'][step]
            self.send_to_calculate(indicators_to_calculate, job)
                   

         # Jobs after calculate ends
        for job in self.job_steps['calculate_ended'].copy().values():
            indicators_tuples = self.job_indicators[job["id"]]
            self.update_job_step(job, 'publish_indicators_started')
            self.send_to_publish_indicators(indicators_tuples, job)

        # Jobs after publish ends
        for job in self.job_steps['publish_indicators_ended'].copy().values():
            # step = self.job_precalstep[job['id']] + 1
            # total_steps  = len(self.job_precalinds[job['id']]['steps'])
            # if step < total_steps:
            #   self.job_precalstep[job['id']] = step
            #   indicators_to_calculate = self.job_precalinds[job['id']]['steps'][step]
            #   self.send_to_calculate(indicators_to_calculate, job)
            # else :
            #   cleaning job dicos
            for job_dico in self.job_dicos:
              if job["id"] in job_dico:
                job_dico.pop(job["id"])
     
    

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
        
        # if 'datahub' in dls['scope']['source']:
        #     paths = [path for path in paths if '18322.csv' in path]
        #     print(f' Paths list is {paths} ')
        # dest_files = [str(job_id)+'_'+'_'.join(path.split('files/')[1].split('/')) for path in paths]
        # paths_in_staging = [self.staging_path(dls, dest_files[0]) ]
        
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
       dls = job["dls_json"]
       format_name = dls["acquisition"]["format"]["name"].lower() if "format" in dls["acquisition"] and "name" in dls["acquisition"]["format"] else None

       for file_path in paths:
          if format_name == "csv" or file_path.endswith('.csv'):
              self._frcsv_proxy.read_format_start(job, file_path)
          elif format_name == "xml" or file_path.endswith('.rdf') or file_path.endswith('.xml'):
              self._frxml_proxy.read_format_start(job, file_path)
          elif format_name == "xls" or file_path.endswith(".xls") or file_path.endswith('.xlsx'):
              self._frxls_proxy.read_format_start(job, file_path)
          elif format_name == "json" or file_path.endswith('.json') or file_path.endswith('.json.gz'):
              self._frjson_proxy.read_format_start(job, file_path)
          else:
              raise RuntimeError(f'Unsupported format in {file_path}')

    def read_format_end(self, job, path, df):
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
            warning_number = sum(issue['issue_severity']=='warning' for issue in self.job_issues[job["id"]])
            if errors_number == 0 and (len(tuples["tuples"]) > 0 or warning_number == 0):
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
            warning_number = sum(issue['issue_severity']=='warning' for issue in self.job_issues[job["id"]])
            if errors_number == 0 and (len(tuples["tuples"]) > 0 or warning_number == 0):
                self.update_job_step(job, 'standardize_ended')
            else:
                self.fail_job(job)
    
    def send_to_annotate(self, tuples, job):
        self.pending_count[job["id"]] = len(tuples)
        for path, ttuples in tuples.items():
            self._nlp_proxy.annotate(ttuples, path, job)

    def annotate_end(self, tuples, path, job):
        self.pending_count[job["id"]] = self.pending_count[job["id"]] - 1
        self.job_stdtuples[job["id"]][path] = tuples
        if self.pending_count[job["id"]] == 0:
            self.update_job_step(job, 'annotate_ended')

    def send_to_aggregate(self, tuples, job):
        self.pending_count[job["id"]] = len(tuples)
        for path, ttuples in tuples.items():
            self._aggregate_proxy.aggregate(ttuples, path, job)

    def aggregate_end(self, tuples, path, job): 
        self.pending_count[job["id"]] = self.pending_count[job["id"]] - 1
        self.job_aggrtuples[job["id"]][path] = tuples
        if self.pending_count[job["id"]] == 0:
            self.update_job_step(job, 'aggregate_ended')
    
    def send_to_precalculate(self, tuples, job):
        self.pending_count[job["id"]] = len(tuples)
        for path, ttuples in tuples.items():
            self._evaluator_proxy.pre_calculate(ttuples, path, job)

    def precalculate_end(self, indicators_to_calculate, tuples, path, job): 
        self.pending_count[job["id"]] = self.pending_count[job["id"]] - 1
        self.job_precaltuples[job["id"]][path] = tuples
        self.job_precalinds[job["id"]][path] = indicators_to_calculate
        if self.pending_count[job["id"]] == 0:
            self.update_job_step(job, 'precalculate_ended')

    def send_to_publish_facts(self, tuples, job):
        self.pending_count[job["id"]] = len(tuples)
        for path, ttuples in tuples.items():
            self._variables_proxy.write_variable(ttuples, "raw", path, job)
    
    def publish_facts_end(self, path, job): 
        self.pending_count[job["id"]] = self.pending_count[job["id"]] - 1
        if self.pending_count[job["id"]] == 0:
            self.update_job_step(job, 'publish_facts_ended')


    def send_to_calculate(self, indicators_to_calculate, job): 
        self.pending_count[job["id"]] = len(indicators_to_calculate)
        first_path = list(indicators_to_calculate)[0]
        first_path_dict = indicators_to_calculate[first_path]
        self._evaluator_proxy.calculate(first_path_dict, first_path, job)
        if len(indicators_to_calculate.keys()) > 0:
            for path in list(indicators_to_calculate)[1:]:
                indicators_to_cal = indicators_to_calculate[path]
                for ind in indicators_to_cal.copy():
                    if ind != 'update_scope':
                        ind_map = indicators_to_cal[ind]
                        if ind in first_path_dict:
                            if all (comb_values in first_path_dict[ind]['comb_values'] for comb_values in indicators_to_cal[ind]['comb_values']):
                                indicators_to_cal.pop(ind)
                            else:
                                attr_comb = ind_map['comb_values'][:]
                                for i, comb_values in enumerate(attr_comb):
                                    if comb_values in first_path_dict[ind]['comb_values']:
                                        ind_map['comb_values'].pop(i)
                                    else:
                                        first_path_dict[ind]['comb_values'].append(comb_values)
                        if not ind_map['comb_values']:
                            indicators_to_cal.pop(ind)
                self._evaluator_proxy.calculate(indicators_to_cal, path, job)

    

    def calculate_end(self, ind_tuples, path, job): 
        self.pending_count[job["id"]] = self.pending_count[job["id"]] - 1
        print(f' The pending count is : {self.pending_count[job["id"]]}')
        self.job_indicators[job["id"]][path] = ind_tuples
        if self.pending_count[job["id"]] == 0:
            self.update_job_step(job, 'calculate_ended')

    def send_to_publish_indicators(self, indicators_tuples, job):
        self.pending_count[job["id"]] = len(indicators_tuples)
        for path, ind_tuples in indicators_tuples.items():
            self._variables_proxy.write_variable(ind_tuples, "ind", path, job)

    def publish_indicators_end(self, path, job):
        self.pending_count[job["id"]] = self.pending_count[job["id"]] - 1
        if self.pending_count[job["id"]] == 0:
            self.update_job_step(job, 'publish_indicators_ended')
    
    # this function returns a future that can be waited to ensure that file job is written to disk
    def update_job_step(self, job, step):
        l.debug(f"Changing to step {step} for job {job['id']} source {job['dls_json']['scope']['source']}")
        # removing job from current step dict
        if job["step"] in self.job_steps and job["id"]:
          if job["id"] in self.job_steps[job["step"]]:
            self.job_steps[job["step"]].pop(job["id"])
          else:
            l.warning(f"Warning: could not find id {job['id']} in job_steps {job['step']}") 
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
        l.warning(f"Changing status of job {job['id']} to 'failed' for {job['dls_json']['scope']['source']}")
        l.info(util.pretty(self.job_issues[job["id"]]))
        # removing job from current step dict
        if job["step"] in self.job_steps and job["id"]:
          if job["id"] in self.job_steps[job["step"]]:
            self.job_steps[job["step"]].pop(job["id"])
          else:
            l.warning(f"Warning: could not find id {job['id']} in job_steps {job['step']}") 
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

    def job_to_annotate(self, job):
      if "columns" in job['dls_json']:
        for col in job['dls_json']['columns']:
          if "variable" in col and col["variable"] == "article_text":
            return True
      return False
      
  

    def staging_path(self, dls, *args):
        return self.pandem_path(f'files/staging', dls['scope']['source'], *args)

