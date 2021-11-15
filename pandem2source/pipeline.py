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
        self._dfreader_proxy = self._orchestrator_proxy.get_actor('dfreader').get().proxy()
        self._standardizer_proxy = self._orchestrator_proxy.get_actor('standardizer').get().proxy()
        self.job_steps = dict()
        self.job_steps['read_format_started'] = []
        self.job_steps['read_format_ended'] = []
        self.job_steps['submitted_ended'] = []
        self.job_steps['decompress_ended'] = []
        self.job_df = defaultdict(dict)
        self.decompressed_files = defaultdict(list)
        last_step = "read_format_end" #TODO: update this as last step evolves
        jobs = self._storage_proxy.read_db('job',
                                          filter= lambda x: x['status']=='in progress' and x['step']!='submitted_started' and x['step']!=last_step 
                                         ).get()
        if jobs is not None:
            self.job_steps['submitted_ended'] = jobs.to_dict(orient='records')   


    def staging_path(self, dls, *args):
        return self.pandem_path(f'files/staging', dls['scope']['source'], *args)
    

    def loop_actions(self):
        self._self_proxy.process_jobs()


    def send_to_readformat(self, file_path, job):
        file_ext = os.path.splitext(file_path)[1]
        print(f'file extenstion is: {file_ext}')
        if file_ext == '.csv':
            self._frcsv_proxy.read_format_start(job, file_path)
        elif file_ext == '.rdf':
            self._frxml_proxy.read_format_start(job, file_path)
        elif file_ext == '.xml':
            self._frxml_proxy.read_format_start(job, file_path)
        else:
            raise RuntimeError('unsupported format')


    def process_jobs(self):
        if len(self.job_steps['submitted_ended'])>0:
            for job in self.job_steps['submitted_ended']:
                submitted_files = job['source_files']
                if "decompress" not in job['dls_json']['acquisition'].keys():
                    for file_path in submitted_files:
                        self.send_to_readformat(file_path, job)
                    job['step'] = 'read_format_started'
                    job_id = self._storage_proxy.write_db(job, 'job').get()
                    self.job_steps['read_format_started'].append(job) 
                else:
                    self._unarchive_proxy.unarchive(job)
            self.job_steps['submitted_ended'] = []    
        if len(self.job_steps['decompress_ended'])>0:
            for job in self.job_steps['decompress_ended']:
                submitted_files = self.decompressed_files[job['id']]
                print(f'decompressed files: {submitted_files}')
                for file_path in submitted_files:
                    self.send_to_readformat(file_path, job)
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
        job_id = self._storage_proxy.write_db(job_record, 'job').get()
        dest_files = [str(job_id)+'_'+'_'.join(path.split('files/')[1].split('/')) for path in paths]
        paths_in_staging = [self.staging_path(dls, file) for file in dest_files]
        self._storage_proxy.copy_files(paths, paths_in_staging)
        job_id = self._storage_proxy.write_db({'id': job_id,
                                              'source_files': paths_in_staging,
                                              'step': 'submitted_ended'}, 
                                             'job').get()
        job_submitted = self._storage_proxy.read_db('job', filter= lambda x: x['id']==job_id).get().to_dict(orient='records')[0]

        self.job_steps['submitted_ended'].append(job_submitted)
        
    
    def decompress_end(self, job, zip_path, file, bytes):
        dest_file = os.path.basename(zip_path)+'_'+('_'.join(file.split('/'))) 
        path_in_staging = self.staging_path(job['dls_json'], dest_file)
        self.decompressed_files[job['id']].append(path_in_staging)
        self._storage_proxy.write_file(path_in_staging, bytes, 'wb+').get()
        if len(self.decompressed_files[job['id']]) == len(job['source_files'])*len(job['dls_json']["acquisition"]["decompress"]["path"]):
            job_id = self._storage_proxy.write_db({'id': job['id'],
                                              'step': 'decompress_ended'},
                                             'job').get()
            job_decompressed = self._storage_proxy.read_db('job', filter= lambda x: x['id']==job_id).get().to_dict(orient='records')[0]
            self.job_steps['decompress_ended'].append(job_decompressed)


    def read_format_end(self, job, path, df): 
        job_id = int(job['id'])
        print(f'df head for file: {path} is : {df.head(10)}')
        self.job_df[job_id][path] = df
        if len(self.job_df[job['id']]) == len(job['source_files']):
            job['step'] = 'read_format_end'
            job_id = self._storage_proxy.write_db(job, 'job').get()
            self.job_steps['read_format_started'].remove(job)
            self.job_steps['read_format_ended'].append(job)
        


  




