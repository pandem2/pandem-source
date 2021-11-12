import pykka
import os
import time
import threading
import datetime
import json
from collections import defaultdict
from . import worker
from abc import ABC, abstractmethod, ABCMeta

class Pipeline(worker.Worker):
    __metaclass__ = ABCMeta  
    def __init__(self, name, orchestrator_ref, settings): 
        super().__init__(name = name, orchestrator_ref = orchestrator_ref, settings = settings)

    def on_start(self):
        super().on_start()
        self._storage_proxy = self._orchestrator_proxy.get_actor('storage').get().proxy()
        self._frxml_proxy = self._orchestrator_proxy.get_actor('ftreader_xml').get().proxy()
        self._frcsv_proxy = self._orchestrator_proxy.get_actor('ftreader_csv').get().proxy()
        self._unarchive_proxy = self._orchestrator_proxy.get_actor('unarchiver').get().proxy()
        self.job_steps = dict()
        self.job_steps['read_format_started'] = []
        self.job_steps['read_format_ended'] = []
        self.job_steps['submitted_ended'] = []
        self.job_steps['decompress_ended'] = []
        self.job_df = defaultdict(dict)
        jobs = self._storage_proxy.read_db('job',
                                          filter=lambda x : x['status']=='in progress' and x['step']!='submitted_started').get()
        if jobs is not None:
            self.job_steps['submitted_ended'] = jobs.to_dict(orient='records')   

    def staging_path(self, dls, *args):
        return self.pandem_path(f'files/staging', dls['scope']['source'], *args)
    
    def loop_actions(self):
        self._self_proxy.process_jobs()

    def process_jobs(self):
        if len(self.job_steps['submitted_ended'])>0:
            for job in self.job_steps['submitted_ended']:
                submitted_files = job['source_files']
                #here send only csv files
                for file_path in submitted_files:
                    file_ext = os.path.splitext(file_path)[1]
                    if file_ext == '.csv':
                        self._frcsv_proxy.read_format_start(job, file_path)
                    if file_ext == '.xml':
                        self._frxml_proxy.read_format_start(job, file_path)
                job['step'] = 'read_format_started'
                #print(f'job is :{job}')
                job_id = self._storage_proxy.write_db(job, 'job').get()
                self.job_steps['read_format_started'].append(job) 
            self.job_steps['submitted_ended'] = []    
        if len(self.job_steps['decompress_ended'])>0:
            for job in self.job_steps['decompress_ended']:
                submitted_files = job['source_files']
                for file_path in submitted_files:
                    file_ext = os.path.splitext(file_path)[1]
                    if file_ext == '.csv':
                        self._frcsv_proxy.read_format_start(job, file_path)
                    if file_ext == '.xml':
                        self._frxml_proxy.read_format_start(job, file_path)
                job['step'] = 'read_format_started'
                job_id = self._storage_proxy.write_db(job, 'job').get()
                self.job_steps['read_format_started'].append(job)
            self.job_steps['decompress_ended'] = []
         
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
        #print(f'in pipeline sublit_file, job_record: {job_record}')
        job_id = self._storage_proxy.write_db(job_record, 'job').get()
        #dest_files = [str(job_id)+'_'+'_'.join(path.split('/')) for path in paths]
        dest_files = [str(job_id)+'_'+'_'.join(path.split('files/')[1].split('/')) for path in paths]
        print(f'dest file before copy: {dest_files}')
        #print(f'destination files: {dest_files}')
        paths_in_staging = [self.staging_path(dls, file) for file in dest_files]
        self._storage_proxy.copy_files(paths, paths_in_staging)
        job_id = self._storage_proxy.write_db({'id': job_id,
                                              'source_files': paths_in_staging,
                                              'step': 'submitted_ended'}, 
                                             'job').get()
        job_submitted = self._storage_proxy.read_db('job', filter= lambda x: x['id']==job_id).get().to_dict(orient='records')[0]
        self.job_steps['submitted_ended'].append(job_submitted)
        if "decompress" in dls["acquisition"].keys():
            self.job_steps['submitted_ended'].remove(job_submitted)
            self._unarchive_proxy.unarchive(job_submitted).get()
            job_decompressed = self._storage_proxy.read_db('job', filter= lambda x: x['id']==job_id).get().to_dict(orient='records')[0]
            self.job_steps['decompress_ended'].append(job_decompressed)
    
    def decompress_end(self, job, bytes): 
        dest_files = [str(job['id'])+'_'+'_'.join(file.split('/')) for file in job['dls_json']["acquisition"]["decompress"]["path"]]
        paths_in_staging = [self.staging_path(job['dls_json'], file) for file in dest_files]
        for path, byte in zip(paths_in_staging, bytes):
            self._storage_proxy.write_file(path, byte, 'wb+')
        job_id = self._storage_proxy.write_db({'id': job['id'],
                                              'step': 'decompress_ended'},
                                             'job').get()

    def read_format_end(self, job, path, df): 

        #job = [element for element in self.job_steps['read_format_started'] if path in element['source_files']][0]
        job_id = int(job['id'])
        print(f'df head for file: {path} is : {df.head()}')
        self.job_df[job_id][path] = df
        job['step'] = 'read_format_end'
        job_id = self._storage_proxy.write_db(job, 'job').get()
        self.job_steps['read_format_started'].remove(job)
        self.job_steps['read_format_ended'].append(job)
        # for key, value in self.job_df[job_id].items():
        #     print(f'a file path for the job id: {}: {key}')
        


  


 


