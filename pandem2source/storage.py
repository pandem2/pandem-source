import os, re
import pykka
import pandas as pd
import json
from . import orchestrator
import threading
import time



class Storage(pykka.ThreadingActor):


    def __init__(self, name, orchestrator_ref, settings): 
        super(Storage, self).__init__()
        self.name = name
        self.orchestrator_proxy = orchestrator_ref.proxy()
        self.settings = settings
        
     
    def on_start(self):
        #create empty dataframes in self.db_tables if pickle files doesn't exist
        self.db_tables = dict()
        if os.path.exists(os.path.join(os.getenv('PANDEM_HOME'), 'database/jobs.pickle')):
            self.db_tables['job'] = pd.read_pickle(os.path.join(os.getenv('PANDEM_HOME'), 'database/jobs.pickle'))
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
        if os.path.exists(os.path.join(os.getenv('PANDEM_HOME'), 'database/issues.pickle')):
            self.db_tables['issue'] = pd.read_pickle(os.path.join(os.getenv('PANDEM_HOME'), 'database/issues.pickle'))
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
        if os.path.exists(os.path.join(os.getenv('PANDEM_HOME'), 'database/sources.pickle')):
            self.db_tables['source'] = pd.read_pickle(os.path.join(os.getenv('PANDEM_HOME'), 'database/sources.pickle'))
        else:                                    
            self.db_tables['source'] = pd.DataFrame({'id': pd.Series(dtype='int'),
                                                     'name': pd.Series(dtype='str'), 
                                                     'repo': pd.Series(dtype='str'),
                                                     #'subdirs': pd.Series(dtype=object), 
                                                     'git_last_commit': pd.Series(dtype='str')})        
        #send heartbeat to orchestrator that runs in background
        threading.Thread(target=self.send_heartbeat).start()


    def send_heartbeat(self):
        while True:
            time.sleep(10)
            self.orchestrator_proxy.get_heartbeat(self.name)
        

    def write_file(self, path, name, bytes, mode): 
        ''' mode: 
            wb+  create file if it doesn't exist and open it in overwrite mode.
                It overwrites the file if it already exists
            ab+  create file if it doesn't exist and open it in append mode
        '''
        with open(os.path.join(self.settings['home_dir'], path, name), mode) as f:
            f.write(bytes)
        return 'Done'
    

    def read_files(self, path):
        if path.split('.')[-1] == "json":
            with open(os.path.join(os.getenv('PANDEM_HOME'), 'files', path), 'r') as f:
                data_dict = json.load(f)
            return data_dict


    def list_files(self, path, match=None, recursive=False, exclude=['.git']):
        files_paths = []
        if recursive:
            for (dirpath, dirnames, filenames) in os.walk(os.path.join(os.getenv('PANDEM_HOME'), 'files', path)):
                for dir in exclude:
                    if dir in dirnames:
                        dirnames.remove(dir)
                files_paths += [os.path.join(dirpath, file) for file in filenames]
        else:
            files_paths= os.listdir(os.path.join(os.getenv('PANDEM_HOME'), 'files', path))
        files_names = [os.path.basename(file_path) for file_path in files_paths]
        if match is not None:
            matched_files = [{'path': path+'/'+file_name, 'name':file_name} for file_name in files_names if re.mach(match,file_name)] 
            return matched_files
        else:
            return [{'path': path+'/'+file_name, 'name':file_name} for file_name in files_names]
       
        
    def delete_files(self, path, match):
        if os.path.exists(path):
            files = os.listdir(os.path.join(os.getenv('PANDEM_HOME'), path))
            matched_files = [file for file in files if re.mach(match,file)]
            if len(matched_files) > 0:
                for file in matched_files:
                    os.remove(os.path.join(self.settings['home_dir'], path, file))
                return 'Ok'
            else:
                return 'no files founded' 
        else:
            raise FileNotFoundError("folder {0} does not exist!".format(path))
        

    def write_db(self, record, db_class): 
        df = self.db_tables[db_class]
        if not 'id' in  record:
            if df.shape[0] > 0:
                record['id'] = df.index.max()+1
            else:
                record['id'] = 1
        df.loc[record['id']] = record
        self.db_tables[db_class] = df
        df.to_pickle(os.path.join(os.getenv('PANDEM_HOME'), 'database', db_class+'s'+'.pickle'))
        return record['id']


    def read_db(self, db_class, filter=None):
        df = self.db_tables[db_class]
        if filter != None:
            df = df.loc[df.apply(filter, axis = 1)]
        return df


    def delete_db(self, db_class, filter=None):
        df = self.db_tables[db_class]
        if filter != None:
            df = df.drop(index = df.index[df.apply(filter, axis = 1)])
        self.db_tables[db_class] = df
        df.to_pickle(os.path.join(os.getenv('PANDEM_HOME'), 'database', db_class+'s'+'.pickle'))
        return df





 

