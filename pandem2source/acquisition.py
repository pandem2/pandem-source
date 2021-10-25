import pykka
import subprocess
import os
import time
import threading


class Acquisition(pykka.ThreadingActor):
    

    def __init__(self, name, orchestrator_ref, storage_ref, settings): 
        super(Acquisition, self).__init__()
        self.name = name
        self.orchestrator_proxy = orchestrator_ref.proxy()
        self.storage_proxy = storage_ref.proxy()
        self.settings = settings
        print(f'here in {self.name} __init__')
      
       
    def on_start(self):
        self.current_sources = dict()
        threading.Thread(target=self.actor_loop).start()
        print(f'here in {self.name} on-start')


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
                for subdir in dls['acquisition']['channel']['paths']:
                    files_to_pipeline = self.storage_proxy.list_files(os.path.join('git',
                                                                                   dls['scope']['source'],
                                                                                   subdir)).get()
                    
                    print(f"cloned files from source {dls['scope']['source']} sent to the pipeline are: {files_to_pipeline}") 
                dist_branch =  dls['acquisition']['channel']['branch']
                last_commit = subprocess.run(['git', 'rev-parse', 'origin/'+dist_branch],
                                              capture_output=True,
                                              text=True,
                                              cwd=os.path.join(source_path, repo_name)
                                             )
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
                print(f'dealing with source id: {source_id}')

                dist_branch = dls['acquisition']['channel']['branch']
                repo_name = dls['acquisition']['channel']['paths'][0].split('/')[0]
                last_saved_commit = self.storage_proxy.read_db('source', lambda x: x['id']==source_id).get()['git_last_commit'].values[0]
                print(f'last saved commit is: {last_saved_commit}')
                subprocess.run(['git', 'pull',  'origin', dist_branch], 
                               cwd=os.path.join(os.getenv('PANDEM_HOME'), 'files/git', dls['scope']['source'], repo_name) 
                              )
                
                #get updated files
                updated_files = dict()
                for subdir in dls['acquisition']['channel']['paths']:
                    subdir = os.path.join(*subdir.split('/')[1:])
                    print(f'target subdirectory is: {subdir}')
                    new_files = subprocess.run(['git', 'diff', '--name-only', last_saved_commit, 'HEAD', subdir], 
                                                capture_output=True,
                                                text=True,
                                                cwd=os.path.join(os.getenv('PANDEM_HOME'),
                                                                'files/git',
                                                                dls['scope']['source'],
                                                                repo_name)
                                               )
                    if new_files.stdout != '':
                        print(f'new files: {new_files.stdout}') 
                        
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
                    else:
                        print(f'no new files from this source : {source_id}/{subdir}')



    def send_heartbeat(self):
        self.orchestrator_proxy.get_heartbeat(self.name)
    
    
    def actor_loop(self):
        while True:
            self.actor_ref.proxy().monitor_source()
            self.send_heartbeat()
            time.sleep(20)


