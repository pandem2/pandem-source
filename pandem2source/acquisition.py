import pykka
import subprocess
import os
import time
import threading
from . import pipeline
import requests


class Acquisition(pykka.ThreadingActor):
    
    def __init__(self, name, orchestrator_ref, storage_ref, settings): 
        super(Acquisition, self).__init__()
        self.name = name
        self.orchestrator_proxy = orchestrator_ref.proxy()
        self.storage_proxy = storage_ref.proxy()
        self.settings = settings
        print(f'here in {self.name} __init__')
         
    # def on_start(self):
    #     #self.current_sources = dict()
    #     pipeline_ref = self.orchestrator_proxy.get_actor('pipeline').get()
    #     print(f'pipeline ref within acquisition is:: {pipeline_ref}')
    #     self.pipeline_proxy = pipeline_ref.proxy()
    #     print(f'pipeline proxy within acquisition is: {self.pipeline_proxy}')
    #     threading.Thread(target=self.actor_loop).start()

    # def add_datasource(self, dls):
    #     if self.name.split('_')[-1] == 'git': 
    #         source_path = os.path.join(os.getenv('PANDEM_HOME'), 'files/git', dls['scope']['source'])
    #         if not os.path.exists(source_path):
    #             os.makedirs(source_path)
    #         repo_name = dls['acquisition']['channel']['paths'][0].split('/')[0]
    #         #clone the git source if it doen't exist within 'files/git': en accédant directement au 'files/git'
    #         if not os.path.exists(os.path.join(source_path, repo_name)):
    #             subprocess.run(['git', 'clone',  dls['acquisition']['channel']['url']],
    #                            cwd=source_path
    #                           ) 
    #             #send cloned files in target subdirectories to the pipeline actor
    #             files_to_pipeline = []
    #             for subdir in dls['acquisition']['channel']['paths']:
    #                 files_paths = self.storage_proxy.list_files(os.path.join('git',
    #                                                                           dls['scope']['source'],
    #                                                                           subdir)).get()
    #                 files_to_pipeline.extend([file_path['path'] for file_path in files_paths ])
    #             #print(f'files to pipeline after commit : {files_to_pipeline}')
    #             job_id = self.pipeline_proxy.submit_files(dls, files_to_pipeline).get()
    #             print(f'job id for the clonining is {job_id}')
    #             #print(f"cloned files from source {dls['scope']['source']} sent to the pipeline are: {files_to_pipeline}") 
    #             dist_branch =  dls['acquisition']['channel']['branch']
    #             last_commit = subprocess.run(['git', 'rev-parse', 'origin/'+dist_branch],
    #                                           capture_output=True,
    #                                           text=True,
    #                                           cwd=os.path.join(source_path, repo_name))
    #             #register the {'id', 'name', 'git_last_commit'}  to the database through the storage actor write_db
    #             id_source = self.storage_proxy.write_db({'name': dls['scope']['source'],
    #                                                      'repo': repo_name,
    #                                                      'git_last_commit': last_commit.stdout.rstrip()
    #                                                     },
    #                                                    'source').get()
    #             print(f'id_source after cloning: {id_source}')
    #             self.current_sources[id_source] = dls
    #         else:
    #             id_source = self.storage_proxy.read_db('source', lambda x: x['name']==dls['scope']['source']).get()['id'].values[0]
    #             print(f'id source without cloning is:{id_source}')
    #             self.current_sources[id_source] = dls              

    # def monitor_source(self): 
    #     if self.name.split('_')[-1] == 'git':
    #         for source_id, dls in self.current_sources.items():
    #             #print(f'dealing with source id: {source_id}')
    #             dist_branch = dls['acquisition']['channel']['branch']
    #             repo_name = dls['acquisition']['channel']['paths'][0].split('/')[0]
    #             last_saved_commit = self.storage_proxy.read_db('source', lambda x: x['id']==source_id).get()['git_last_commit'].values[0]
    #             #print(f'last saved commit is: {last_saved_commit}')
    #             subprocess.run(['git', 'pull',  'origin', dist_branch], 
    #                            cwd=os.path.join(os.getenv('PANDEM_HOME'), 'files/git', dls['scope']['source'], repo_name) 
    #                           )
    #             #get updated files
    #             files_to_pipeline = []
    #             for subdir in dls['acquisition']['channel']['paths']:
    #                 subdir = os.path.join(*subdir.split('/')[1:])
    #                 #print(f'target subdirectory is: {subdir}')
    #                 new_files_subdir = subprocess.run(['git', 'diff', '--name-only', last_saved_commit, 'HEAD', subdir], 
    #                                             capture_output=True,
    #                                             text=True,
    #                                             cwd=os.path.join(os.getenv('PANDEM_HOME'),
    #                                                             'files/git',
    #                                                             dls['scope']['source'],
    #                                                             repo_name) 
    #                                            )
    #                 if new_files_subdir.stdout != '':
    #                     #print(f'new files: {new_files.stdout}')
    #                     new_files = new_files_subdir.stdout.rstrip().split('\n')
    #                     files_paths =  [os.path.join('git', dls['scope']['source'], repo_name, new_file) for new_file in new_files]
    #                     files_to_pipeline.extend(files_paths)
    #             #print(f'files to pipeline : {files_to_pipeline}')
    #             if len(files_to_pipeline)>0:
    #                 job_id = self.pipeline_proxy.submit_files(dls, files_to_pipeline).get()
    #                 #print(f'job id for the last pull is {job_id}')
    #             last_commit = subprocess.run(['git', 'rev-parse', 'origin/'+dist_branch], 
    #                                                 capture_output=True,
    #                                                 text=True,
    #                                                 cwd=os.path.join(os.getenv('PANDEM_HOME'), 'files/git', dls['scope']['source'], repo_name)                                             )                 
    #             id_source = self.storage_proxy.write_db({'name': dls['scope']['source'],
    #                                                              'repo': repo_name,
    #                                                             'git_last_commit': last_commit.stdout.rstrip(),
    #                                                             'id': source_id
    #                                                             }, 
    #                                                             'source').get()   

    # def send_heartbeat(self):
    #     self.orchestrator_proxy.get_heartbeat(self.name)

    # def actor_loop(self):
    #     my_proxy = self.actor_ref.proxy()
    #     while True:
    #         my_proxy.monitor_source()
    #         self.send_heartbeat()
    #         time.sleep(20)
    
    

############################################################################################################################
############################################################################################################################


class AcquisitionURL(Acquisition):

    def __init__(self, name, orchestrator_ref, storage_ref, settings): 
        super().__init__(name, orchestrator_ref, storage_ref, settings)
        

    def on_start(self):
        self.current_sources = dict()
        pipeline_ref = self.orchestrator_proxy.get_actor('pipeline').get()
        print(f'pipeline ref within acquisition is:: {pipeline_ref}')
        self.pipeline_proxy = pipeline_ref.proxy()
        print(f'pipeline proxy within acquisition is: {self.pipeline_proxy}')
        threading.Thread(target=self.actor_loop).start()

    def add_datasource(self, dls):
        
        source_path = os.path.join(os.getenv('PANDEM_HOME'), 'files/url', dls['scope']['source'])
        if not os.path.exists(source_path):
            os.makedirs(source_path)
        #repo_name = dls['acquisition']['channel']['paths'][0].split('/')[0]
        #upload the url source if it doen't exist within 'files/url': en accédant directement au 'files/git'
        #download content if doesn't exist
        url = dls['acquisition']['channel']['url']
        r = requests.get(url)#allow_redirects=True
        file_path = os.path.join(source_path, '_'.join(url.split('//')[1].split('/')))
        print(f'file path is: {file_path}')
        if not os.path.exists(file_path):
            with open (file_path,'wb') as cont:
                cont.write(r.content)
            #send downloaded file to the pipeline actor
          
            job_id = self.pipeline_proxy.submit_files(dls, [file_path]).get()
            print(f'job id for the dowloaded content is {job_id}')
           
            last_etag = r.headers.get('ETag')
            #register the {'id', 'name', 'last'}  to the database through the storage actor write_db
            id_source = self.storage_proxy.write_db({'name': dls['scope']['source'],
                                                     'url_last_etag': last_etag},
                                                    'source').get()
            print(f'id_source after downloading: {id_source}')
            self.current_sources[id_source] = dls
        else:
            id_source = self.storage_proxy.read_db('source', lambda x: x['name']==dls['scope']['source']).get()['id'].values[0]
            self.current_sources[id_source] = dls  


    def monitor_source(self): 
        
        for source_id, dls in self.current_sources.items():
            #print(f'dealing with source id: {source_id}')
            #repo_name = dls['acquisition']['channel']['paths'][0].split('/')[0]
            last_saved_etag = self.storage_proxy.read_db('source', lambda x: x['id']==source_id).get()['url_last_etag'].values[0]
            url = dls['acquisition']['channel']['url']
            source_path = os.path.join(os.getenv('PANDEM_HOME'), 'files/url', dls['scope']['source'])
            #downloaded content will be within the same file name
            file_path = os.path.join(source_path, '_'.join(url.split('//')[1].split('/')))
            r = requests.get(url)
            current_etag = r.headers.get('ETag')
            if current_etag != last_saved_etag:
                with open (file_path,'wb') as cont:
                    cont.write(r.content)
                job_id = self.pipeline_proxy.submit_files(dls, [file_path]).get()
                id_source = self.storage_proxy.write_db({'name': dls['scope']['source'],
                                                            'url_last_etag': current_etag,
                                                            'id': source_id}, 
                                                           'source').get()


    def send_heartbeat(self):
        self.orchestrator_proxy.get_heartbeat(self.name)

    def actor_loop(self):
        my_proxy = self.actor_ref.proxy()
        while True:
            my_proxy.monitor_source()
            self.send_heartbeat()
            time.sleep(20)
#####################################################################################################################################
#####################################################################################################################################


class AcquisitionGIT(Acquisition):

    def __init__(self, name, orchestrator_ref, storage_ref, settings): 
        super().__init__(name, orchestrator_ref, storage_ref, settings)
    

    def on_start(self):
        self.current_sources = dict()
        pipeline_ref = self.orchestrator_proxy.get_actor('pipeline').get()
        print(f'pipeline ref within acquisition is:: {pipeline_ref}')
        self.pipeline_proxy = pipeline_ref.proxy()
        print(f'pipeline proxy within acquisition is: {self.pipeline_proxy}')
        threading.Thread(target=self.actor_loop).start()
    


    def add_datasource(self, dls):
        if self.name.split('_')[-1] == 'git': 
            source_path = os.path.join(os.getenv('PANDEM_HOME'), 'files/git', dls['scope']['source'])
            if not os.path.exists(source_path):
                os.makedirs(source_path)
            repo_name = dls['acquisition']['channel']['paths'][0].split('/')[0]
            #clone the git source if it doen't exist within 'files/git': en accédant directement au 'files/git'
            if not os.path.exists(os.path.join(source_path, repo_name)):
                subprocess.run(['git', 'clone',  dls['acquisition']['channel']['url']],
                               cwd=source_path
                              ) 
                #send cloned files in target subdirectories to the pipeline actor
                files_to_pipeline = []
                for subdir in dls['acquisition']['channel']['paths']:
                    files_paths = self.storage_proxy.list_files(os.path.join('git',
                                                                              dls['scope']['source'],
                                                                              subdir)).get()
                    files_to_pipeline.extend([file_path['path'] for file_path in files_paths ])
                #print(f'files to pipeline after commit : {files_to_pipeline}')
                job_id = self.pipeline_proxy.submit_files(dls, files_to_pipeline).get()
                print(f'job id for the clonining is {job_id}')
                #print(f"cloned files from source {dls['scope']['source']} sent to the pipeline are: {files_to_pipeline}") 
                dist_branch =  dls['acquisition']['channel']['branch']
                last_commit = subprocess.run(['git', 'rev-parse', 'origin/'+dist_branch],
                                              capture_output=True,
                                              text=True,
                                              cwd=os.path.join(source_path, repo_name))
                #register the {'id', 'name', 'git_last_commit'}  to the database through the storage actor write_db
                id_source = self.storage_proxy.write_db({'name': dls['scope']['source'],
                                                         'repo': repo_name,
                                                         'git_last_commit': last_commit.stdout.rstrip()
                                                        },
                                                       'source').get()
                print(f'id_source after cloning: {id_source}')
                self.current_sources[id_source] = dls
            else:
                id_source = self.storage_proxy.read_db('source', lambda x: x['name']==dls['scope']['source']).get()['id'].values[0]
                print(f'id source without cloning is:{id_source}')
                self.current_sources[id_source] = dls              

    def monitor_source(self): 
        if self.name.split('_')[-1] == 'git':
            for source_id, dls in self.current_sources.items():
                #print(f'dealing with source id: {source_id}')
                dist_branch = dls['acquisition']['channel']['branch']
                repo_name = dls['acquisition']['channel']['paths'][0].split('/')[0]
                last_saved_commit = self.storage_proxy.read_db('source', lambda x: x['id']==source_id).get()['git_last_commit'].values[0]
                #print(f'last saved commit is: {last_saved_commit}')
                subprocess.run(['git', 'pull',  'origin', dist_branch], 
                               cwd=os.path.join(os.getenv('PANDEM_HOME'), 'files/git', dls['scope']['source'], repo_name) 
                              )
                #get updated files
                files_to_pipeline = []
                for subdir in dls['acquisition']['channel']['paths']:
                    subdir = os.path.join(*subdir.split('/')[1:])
                    #print(f'target subdirectory is: {subdir}')
                    new_files_subdir = subprocess.run(['git', 'diff', '--name-only', last_saved_commit, 'HEAD', subdir], 
                                                capture_output=True,
                                                text=True,
                                                cwd=os.path.join(os.getenv('PANDEM_HOME'),
                                                                'files/git',
                                                                dls['scope']['source'],
                                                                repo_name) 
                                               )
                    if new_files_subdir.stdout != '':
                        #print(f'new files: {new_files.stdout}')
                        new_files = new_files_subdir.stdout.rstrip().split('\n')
                        files_paths =  [os.path.join('git', dls['scope']['source'], repo_name, new_file) for new_file in new_files]
                        files_to_pipeline.extend(files_paths)
                #print(f'files to pipeline : {files_to_pipeline}')
                if len(files_to_pipeline)>0:
                    job_id = self.pipeline_proxy.submit_files(dls, files_to_pipeline).get()
                    #print(f'job id for the last pull is {job_id}')
                last_commit = subprocess.run(['git', 'rev-parse', 'origin/'+dist_branch], 
                                                    capture_output=True,
                                                    text=True,
                                                    cwd=os.path.join(os.getenv('PANDEM_HOME'), 'files/git', dls['scope']['source'], repo_name)                                             )                 
                id_source = self.storage_proxy.write_db({'name': dls['scope']['source'],
                                                                 'repo': repo_name,
                                                                'git_last_commit': last_commit.stdout.rstrip(),
                                                                'id': source_id
                                                                }, 
                                                                'source').get()   

    # def send_heartbeat(self):
    #     self.orchestrator_proxy.get_heartbeat(self.name)
    
    # def actor_loop(self):
    #     my_proxy = self.actor_ref.proxy()
    #     while True:
    #         my_proxy.monitor_source()
    #         self.send_heartbeat()
    #         time.sleep(20)