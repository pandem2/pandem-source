import pykka
import os
import time
import threading
import datetime
import json
from collections import defaultdict

class Pipeline(pykka.ThreadingActor):
    

    def __init__(self, name, orchestrator_ref, storage_ref, ftreader_ref, dfreader_ref, settings): #ftreader_ref, dfreader_ref,
        super(Pipeline, self).__init__()
        self.name = name
        self.orchestrator_proxy = orchestrator_ref.proxy()
        self.storage_proxy = storage_ref.proxy()
        self.ftreader_proxy = ftreader_ref.proxy()
        self.dfreader_proxy = dfreader_ref.proxy()
        self.settings = settings
        print(f'here in {self.name} __init__')
      
       
    def on_start(self):
        self.job_steps = dict()
        self.job_steps['read_format_started'] = []
        self.job_steps['read_format_end'] = []
        self.job_steps['submitted_ended'] = []
        self.job_df = defaultdict(dict)
        jobs = self.storage_proxy.read_db('job',
                                          filter= lambda x: x['status']=='in progress' and x['step']!='submitted_started'
                                         ).get()
        if jobs is not None:
            self.job_steps['submitted_ended'] = jobs.to_dict(orient='records')   
        threading.Thread(target=self.actor_loop).start()


    def submit_files(self, dls, paths):
        job_record = {'source': dls['scope']['source'],
                      'file_sizes': [os.path.getsize(os.path.join(os.getenv('PANDEM_HOME'),
                                                                  'files',
                                                                  path)
                                                    )/1024 for path in paths],
                      'progress': '0%',
                      'start_on': datetime.datetime.now(),
                      'end_on': '',
                      'step': 'submitted_started',
                      'status': 'in progress',
                      'dls_json' : dls#json.dumps(dls)
                     }
        #print(f'in pipeline sublit_file, job_record: {job_record}')
        job_id = self.storage_proxy.write_db(job_record, 'job').get()
        dest_files = [str(job_id)+'_'+'_'.join(path.split('/')) for path in paths]
        #print(f'destination files: {dest_files}')
        dest_paths = [os.path.join('staging', dls['scope']['source'], dest_file) for dest_file in dest_files]
        paths_in_staging = self.storage_proxy.copy_files(paths, dest_paths)
        job_id = self.storage_proxy.write_db({'id': job_id,
                                              'source_files': dest_paths,
                                              'step': 'submitted_ended'}, 
                                             'job').get()
        job_record = self.storage_proxy.read_db('job', filter= lambda x: x['id']==job_id).get().to_dict(orient='records')[0]
        #print(f'job record for this source: {source_name} that will be added to job_steps[submitted_ended] is: {job_record}')
        self.job_steps['submitted_ended'].append(job_record)
        return job_id


    def read_format_end(self, path, df): #job_id, dls, 
        job = [element for element in self.job_steps['read_format_started'] if path in element['source_files']][0]
        self.job_df[int(job['id'])][path] = df
        if len(self.job_df[job['id']]) == len(job['source_files']):
            job['step'] = 'read_format_end'
            job_id = self.storage_proxy.write_db(job, 'job').get()
            self.job_steps['read_format_started'].remove(job)
            self.job_steps['read_format_end'].append(job)
            for key, value in self.job_df[job['id']].items():
                print(f'a file path for the job id: {job["id"]}: {key}')
        return job['id']


    def process_jobs(self):
        if len(self.job_steps['submitted_ended'])>0:
            for job in self.job_steps['submitted_ended']:
                submitted_files = job['source_files']
                #here send only csv files
                for file_path in submitted_files:
                    self.ftreader_proxy.read_format_start(file_path, job['dls_json'])
                job['step'] = 'read_format_started'
                #print(f'job is :{job}')
                job_id = self.storage_proxy.write_db(job, 'job').get()
            self.job_steps['read_format_started'] = self.job_steps['submitted_ended'] 
            self.job_steps['submitted_ended'] = []


    def send_heartbeat(self):
        self.orchestrator_proxy.get_heartbeat(self.name)
    
    
    def actor_loop(self):
        my_proxy = self.actor_ref.proxy()
        while True:
            self.send_heartbeat()
            my_proxy.process_jobs().get()
            time.sleep(20)

 


