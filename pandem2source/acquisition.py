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
        self.current_sources[dls['scope']['source']] = dls
        if dls['acquisition']['channel']['name'] == 'git': 
            
            source_path = os.path.join(os.getenv('PANDEM_HOME'), 'files/git', dls['scope']['source'])
            if not os.path.exists(source_path):
                os.makedirs(source_path)

            repo_name = dls['acquisition']['channel']['paths'][0].split('/')[0]
            # make a git subdirection for this source
            
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
                    #pipeline.start()
                    print(f"cloned files from source {dls['scope']['source']} sent to the pipeline are: {files_to_pipeline}")   
                last_commit = subprocess.run('git rev-parse origin/main', #master
                                              shell=True,
                                              capture_output=True,
                                              text=True,
                                              cwd=os.path.join(source_path, repo_name)
                                             )
                #register the {'id', 'name', 'git_last_commit'}  to the database through the storage actor write_db
                id_source = self.storage_proxy.write_db({'name': dls['scope']['source'],
                                                         'repo': repo_name,
                                                         #'subdirs': dls['acquisition']['channel']['paths'],
                                                         'git_last_commit': last_commit.stdout.rstrip()
                                                        },
                                                       'source').get()
        print(f"here in {self.name} on add_source with repo: {repo_name}")


    def monitor_source(self): #as a background task
        if self.name.split('_')[-1] == 'git':
            #source_path = os.path.join(os.getenv('PANDEM_HOME'), 'files/git', dls['scope']['source'])
            sources_df = self.storage_proxy.read_db('source').get()# à modifier le nom de la variable
            for id, row in sources_df.iterrows():
                print(f'id :  {id}')
                print(f'row is: {row}')
                #dls = source['dls']
                #pull the source:
                subprocess.run(['git', 'pull',  'origin', 'main'], #or main
                               cwd=os.path.join(os.getenv('PANDEM_HOME'), 'files/git', row['name'], row['repo']) #can clone on the current repo?
                              ) 
                #get updated files
                updated_files = dict()
                for subdir in self.current_sources[row['name']]['acquisition']['channel']['paths']:
                    print('we are here')
                    subdir = os.path.join(*subdir.split('/')[1:])
                    print(f'subdir : {subdir}')
                    new_files = subprocess.run(['git', 'diff', '--name-only', row['git_last_commit'], 'HEAD', subdir], 
                                                capture_output=True,
                                                text=True,
                                                cwd=os.path.join(os.getenv('PANDEM_HOME'),
                                                                'files/git',
                                                                row['name'],
                                                                row['repo'])
                                               )
                    if new_files.stdout != '':
                        print(f'new files: {new_files.stdout}') #sent to the pipeline actor
                        last_commit = subprocess.run('git rev-parse origin/main', #master
                                                    shell=True,
                                                    capture_output=True,
                                                    text=True,
                                                    cwd=os.path.join(os.getenv('PANDEM_HOME'), 'files/git', row['name'], row['repo'])                                             )                 
                        id_source = self.storage_proxy.write_db({'name': row['name'],
                                                                 'repo': row['repo'],
                                                                'git_last_commit': last_commit.stdout.rstrip(),
                                                                #'subdirs': row['subdirs'],
                                                                'id': id
                                                                }, 
                                                                'source').get()   
                    else:
                        print('no new files from this source subdir')


    def send_heartbeat(self):
        self.orchestrator_proxy.get_heartbeat(self.name)
    
    
    def actor_loop(self):
        while True:
            self.actor_ref.proxy().monitor_source()
            self.send_heartbeat()
            time.sleep(20)


