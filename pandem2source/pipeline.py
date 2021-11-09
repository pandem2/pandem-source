import os
import datetime
import json
from collections import defaultdict
from . import worker

class Pipeline(worker.Worker):
    def __init__(self, name, orchestrator_ref, settings):
        super().__init__(name = name, orchestrator_ref = orchestrator_ref, settings = settings) 
       
    def loop_actions(self):
        self._self_proxy.process_jobs().get()

    def on_start(self):
        super().on_start()
        self._storage_proxy = self._orchestrator_proxy.get_actor('storage').get().proxy()
        self._ftreader_proxy = self._orchestrator_proxy.get_actor('ftreader').get().proxy()
        self._dfreader_proxy = self._orchestrator_proxy.get_actor('dfreader').get().proxy()
        #self._standardizer_proxy = self._orchestrator_proxy.get_actor('standardizer').get().proxy()
        
        self.job_steps = dict()
        self.job_steps['read_format_started'] = []
        self.job_steps['read_format_end'] = []
        self.job_steps['submitted_ended'] = []
        self.job_df = defaultdict(dict)
        last_step = "read_format_end" #TODO: update this as last step evolves
        jobs = self._storage_proxy.read_db('job',
                                          filter= lambda x: x['status']=='in progress' and x['step']!='submitted_started' and x['step']!=last_step 
                                         ).get()
        if jobs is not None:
            self.job_steps['submitted_ended'] = jobs.to_dict(orient='records')   

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
        job_id = self._storage_proxy.write_db(job_record, 'job').get()
        dest_files = [str(job_id)+'_'+'_'.join(path.split('/')) for path in paths]
        #print(f'destination files: {dest_files}')
        dest_paths = [os.path.join('staging', dls['scope']['source'], dest_file) for dest_file in dest_files]
        paths_in_staging = self._storage_proxy.copy_files(paths, dest_paths)
        job_id = self._storage_proxy.write_db({'id': job_id,
                                              'source_files': dest_paths,
                                              'step': 'submitted_ended'}, 
                                             'job').get()
        job_record = self._storage_proxy.read_db('job', filter= lambda x: x['id']==job_id).get().to_dict(orient='records')[0]
        #print(f'job record for this source: {source_name} that will be added to job_steps[submitted_ended] is: {job_record}')
        self.job_steps['submitted_ended'].append(job_record)
        return job_id


    def read_format_end(self, path, df): #job_id, dls, 
        job = [element for element in self.job_steps['read_format_started'] if path in element['source_files']][0]
        self.job_df[int(job['id'])][path] = df
        if len(self.job_df[job['id']]) == len(job['source_files']):
            job['step'] = 'read_format_end'
            job_id = self._storage_proxy.write_db(job, 'job').get()
            self.job_steps['read_format_started'].remove(job)
            self.job_steps['read_format_end'].append(job)
            #for key, value in self.job_df[job['id']].items():
                #print(f'a file path for the job id: {job["id"]}: {key}')
        return job['id']


    def process_jobs(self):
        if len(self.job_steps['submitted_ended'])>0:
            for job in self.job_steps['submitted_ended']:
                submitted_files = job['source_files']
                #here send only csv files
                for file_path in submitted_files:
                    self._ftreader_proxy.read_format_start(file_path, job['dls_json'])
                job['step'] = 'read_format_started'
                #print(f'job is :{job}')
                job_id = self._storage_proxy.write_db(job, 'job').get()
            self.job_steps['read_format_started'] = self.job_steps['submitted_ended'] 
            self.job_steps['submitted_ended'] = []
