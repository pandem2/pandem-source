import os, re
import pykka
import pandas as pd
import orchestrator
import threading
import time


class Storage(pykka.ThreadingActor):

    def __init__(self, name, orchestrator_ref, settings): 

        super().__init__()
        self.name = name
        self.orchestrator_ref = orchestrator_ref.proxy()
        self.settings = settings
     

    def on_start(self):
        
        self.db_tables = dict()
        if os.path.exists('/home/database/jobs.pickle'):
            self.db_tables['job'] = pd.read_pickle('/home/database/jobs.pickle')
        else:
            self.db_tables['job'] = pd.DataFrame({'id': pd.Series(dtype='int'),
                                             'source': pd.Series(dtype='str'), 
                                             'source_files': pd.Series(dtype=object), #list of string
                                             'file_sizes': pd.Series(dtype=object),#list of integers
                                             'progress': pd.Series(dtype='int'),
                                             'start_on': pd.Series(dtype=object), #parse to datetime
                                             'end_on': pd.Series(dtype=object), #parse to datetime
                                             'source': pd.Series(dtype='str'), 
                                             'status': pd.Series(dtype='str')})
        if os.path.exists('/home/database/issues.pickle'):
            self.db_tables['issue'] = pd.read_pickle('/home/database/issues.pickle')
        else:
            self.db_tables['issue'] = pd.DataFrame({'id': pd.Series(dtype='int'),
                                             'step': pd.Series(dtype='str'), 
                                             'line': pd.Series(dtype='int'), 
                                             'source': pd.Series(dtype='str'),
                                             'file': pd.Series(dtype='str'),
                                             'message': pd.Series(dtype='str'),
                                             'raised_on': pd.Series(dtype=object), #parse to datetime
                                             'job_id': pd.Series(dtype='int'), 
                                             'issue_type': pd.Series(dtype='str')})
                                             
                      
        #send heartbeat to orchestrator that runs in background
        
        threading.Thread(target=self.send_heartbeat).start()
        
        
        print('here in storage on-start')

    def send_heartbeat(self):
        while True:
            time.sleep(3)
            self.orchestrator_ref.get_heartbeat('storage')
        
    def write_file(self, path, name, bytes, mode): 
        ''' mode: 
            wb+  create file if it doesn't exist and open it in overwrite mode.
                It overwrites the file if it already exists
            ab+  create file if it doesn't exist and open it in append mode
        '''
        with open(os.path.join(self.settings['home_dir'], path, name), mode) as f:
            f.write(bytes)
        return 'Done'
    
    def read_files(self, path, name):
        with open(os.path.join(settings['home_dir'], path, name), 'rb') as f:
                bytes = f.read()
        return bytes 
    
    def list_files(self, path, match):
        if os.path.exists(path):
            files = os.listdir(os.path.join(self.settings['home_dir'], path))
            matched_files = [{'path': path, 'name':file} for file in files if re.mach(match,file)]
            return matched_files
        else:
            raise FileNotFoundError("folder {0} does not exist!".format(path))
        
    def delete_files(self, path, match):
        if os.path.exists(path):
            files = os.listdir(os.path.join(self.home_dir, path))
            matched_files = [file for file in files if re.mach(match,file)]
            if len(matched_files) > 0:
                for file in matched_files:
                    os.remove(os.path.join(settings['home_dir'], path, file))
                return 'Ok'
            else:
                return 'no files founded' #est-ce qu'on va traiter ce cas là?

        else:
            raise FileNotFoundError("folder {0} does not exist!".format(path))
        
    def write_db(self, record, db_class): 
        df = self.db_tables[db_class]
        if not 'id' in  record:
            record['id'] = df.index.max()+1
        df.loc[record['id']] = record
        self.db_tables[db] = df
        df.to_pickle(os.path.join(settings['home_dir'], '/database', db_class+'s'+'.pickle'))
        return record['id'] #df[dict[“id”]]?

    def read_db(self, db_class, filter):
        df = self.db_tables[db_class]
        df = df.loc[df.apply(filter, axis = 1)]
        return df

    def delete_db(self, db_class, filter):
        df = self.db_tables[db_class]
        df = df.drop(index = df.index[df.apply(filter, axis = 1)])
        df.to_pickle(os.path.join(settings['home_dir'], '/database', db_class+'s'+'.pickle'))
        return df





 

