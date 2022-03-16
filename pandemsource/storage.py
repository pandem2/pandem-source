import os, re
import pandas as pd
import json
import shutil
from io import BytesIO
from . import worker
from . import util
import shelve

class Storage(worker.Worker):
   
    def __init__(self, name, orchestrator_ref, settings): 
        super().__init__(name, orchestrator_ref, settings)
     
    def loop_actions(self):
        pass

    def on_start(self):
        super().on_start()
        # create base directories
        if not os.path.exists(self.pandem_path("database")):
           os.makedirs(self.pandem_path("database"))
        if not os.path.exists(self.pandem_path("files")):
           os.makedirs(self.pandem_path("files"))
        if not os.path.exists(self.pandem_path("files", "staging")):
           os.makedirs(self.pandem_path("files", "staging"))
        # create dict of job cached objects
        # self.job_cache = {}
        # create empty dataframes in self.db_tables if pickle files doesn't exist
        self.db_tables = dict()
        test_bool = os.path.exists(os.path.join(os.getenv('PANDEM_HOME'), 'database/jobs.pickle'))
        if os.path.exists(os.path.join(os.getenv('PANDEM_HOME'), 'database/jobs.pickle')):
            self.db_tables['job'] = pd.read_pickle(os.path.join(os.getenv('PANDEM_HOME'), 'database/jobs.pickle'))
        else:
            self.db_tables['job'] = pd.DataFrame({'id': pd.Series(dtype='int'),
                                             'source': pd.Series(dtype='str'), 
                                             'source_files': pd.Series(dtype=object),
                                             'file_sizes': pd.Series(dtype=object),
                                             'progress': pd.Series(dtype=float),
                                             'start_on': pd.Series(dtype=object),
                                             'end_on': pd.Series(dtype=object),
                                             'step': pd.Series(dtype='str'), 
                                             'status': pd.Series(dtype='str'),
                                             'dls_json': pd.Series(dtype=object)
                                             })
        if os.path.exists(os.path.join(os.getenv('PANDEM_HOME'), 'database/issues.pickle')):
            self.db_tables['issue'] = pd.read_pickle(os.path.join(os.getenv('PANDEM_HOME'), 'database/issues.pickle'))
        else:
            self.db_tables['issue'] = pd.DataFrame({'id': pd.Series(dtype='int'),
                                             'step': pd.Series(dtype='str'), 
                                             'line': pd.Series(dtype='int'), 
                                             'source': pd.Series(dtype='str'),
                                             'file': pd.Series(dtype='str'),
                                             'message': pd.Series(dtype='str'),
                                             'raised_on': pd.Series(dtype=object),
                                             'job_id': pd.Series(dtype='int'), 
                                             'issue_type': pd.Series(dtype='str'),
                                             'issue_severity': pd.Series(dtype='str')
                                             })
        if os.path.exists(os.path.join(os.getenv('PANDEM_HOME'), 'database/sources.pickle')):
            self.db_tables['source'] = pd.read_pickle(os.path.join(os.getenv('PANDEM_HOME'), 'database/sources.pickle'))
        else:                                    
            self.db_tables['source'] = pd.DataFrame({'id': pd.Series(dtype='int'),
                                                     'name': pd.Series(dtype='str'), 
                                                     'last_hash': pd.Series(dtype='str'),
                                                     'last_exec':pd.Series(dtype=object),
                                                     'next_exec': pd.Series(dtype=object)
                                                    })   

             
    def write_file(self, path, bytes, mode): #absolute path here
        with open(path, mode) as f:
            f.write(bytes) 
    

    def read_file(self, path): #absolute path here
        if path.startswith('PANDEM_HOME'):
          file_path = path
        else :
          file_path = os.path.join(os.getenv('PANDEM_HOME'), 'files', path)
        
        if file_path.split('.')[-1] == "json":
            with open(file_path, 'r') as f:
                data_dict = json.load(f)
            return data_dict
        else :
            with open(file_path, 'rb') as f:
                bytes_data = BytesIO(f.read())
            return bytes_data


    def copy_files(self, src_paths, dest_paths): #absolute paths here
        if not os.path.isdir(os.path.dirname(dest_paths[0])):
            os.makedirs(os.path.dirname(dest_paths[0]))
        for (src_path, dest_path) in zip(src_paths, dest_paths):
            shutil.copyfile(src_path, dest_path)  
        
            
    def list_files(self, path, match=None, recursive=True, exclude=['.git'], include_dirs = False, include_files = True): #path under files
        files_paths = []
        if recursive:
            for (dirpath, dirnames, filenames) in os.walk(os.path.join(os.getenv('PANDEM_HOME'), 'files', path)):
                for dir in exclude:
                    if dir in dirnames:
                        dirnames.remove(dir)
                files_paths += [os.path.join(dirpath, file) for file in filenames]
        else:
            paths= os.listdir(os.path.join(os.getenv('PANDEM_HOME'), 'files', path))
            files_paths = [file_path for file_path in paths 
              if include_files and os.path.isfile(os.path.join(os.getenv('PANDEM_HOME'), 'files', path, file_path)) or
                 include_dirs and os.path.isdir(os.path.join(os.getenv('PANDEM_HOME'), 'files', path, file_path)) 

            ]
        if match is not None:
            matched_files = [{'path': file_path, 'name':os.path.basename(file_path)} for file_path in files_paths if re.mach(match, os.path.basename(file_path))] 
            return matched_files
        else:
            return [{'path': file_path, 'name':os.path.basename(file_path)} for file_path in files_paths]

    def list_files_abs(self, path, match=None, recursive=True, exclude=['.git']): #absolute path for target directory
        files_paths = []
        if recursive:
            for (dirpath, dirnames, filenames) in os.walk(path):
                for dir in exclude:
                    if dir in dirnames:
                        dirnames.remove(dir)
                files_paths += [os.path.join(dirpath, file) for file in filenames]
        else:
            paths= os.listdir(path)
            files_paths = [file_path for file_path in paths if os.path.isfile(os.path.join(path, file_path))]
        if match is not None:
            matched_files = [{'path': file_path, 'name':os.path.basename(file_path)} for file_path in files_paths if re.mach(match, os.path.basename(file_path))] 
            return matched_files
        else:
            return [{'path': file_path, 'name':os.path.basename(file_path)} for file_path in files_paths]
       
        
    def delete_file(self, path, match):
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
    
    def exists(self, path):
        return os.path.exists(path)
    
    def delete_dir(self, path):
        if os.path.exists(path):
            shutil.rmtree(path)
        
    def write_db(self, record, db_class):
        df = self.db_tables[db_class]
        if not 'id' in  record:
            if df.shape[0] > 0:
                record['id'] = df.index.max()+1
            else:
                record['id'] = 1
            df.at[int(record['id'])] = record
        else:
            for key, value in record.items():
                df.at[int(record['id']), key] = value
        self.db_tables[db_class] = df
        dest = os.path.join(os.getenv('PANDEM_HOME'), 'database', db_class+'s'+'.pickle') 
        tmp = f"{dest}.tmp"
        df.to_pickle(tmp)
        shutil.move(tmp, dest)
        return record['id']


    def read_db(self, db_class, filter=None):
        df = self.db_tables[db_class]
        if df.shape[0] > 0:
            if filter != None:
                df = df.loc[df.apply(filter, axis = 1)]
            return df
        else:
            return None


    def delete_db(self, db_class, filter=None):
        df = self.db_tables[db_class]
        if filter != None and len(df) > 0:
            to_del = df.loc[df.apply(filter, axis = 1)]
            if len(to_del)>0:
              df = df.drop(index = to_del.index)
        self.db_tables[db_class] = df
        df.to_pickle(os.path.join(os.getenv('PANDEM_HOME'), 'database', db_class+'s'+'.pickle'))
        return df

    def get_job_cache(self, job_id):
      job_id = int(job_id)
      shelve_path = util.pandem_path("files", "staging", str(job_id), "cache")
      #if job_id in self.job_cache:
      #  self.job_cache[job_id] =  shelve.open(shelve_path,  writeback=False)
      #else:
      if not os.path.exists(util.pandem_path("files", "staging", str(job_id))):
          os.makedirs(util.pandem_path("files", "staging", str(job_id)))
      #  self.job_cache[job_id] =  
      return shelve.open(shelve_path,  writeback=False)
      #return self.job_cache[job_id]

    def to_job_cache(self, job_id, key, data):
      job_id = int(job_id)
      key = str(key)
      with self.get_job_cache(job_id) as cache:
        cache[key] = data
      return CacheValue(job_id, key, self._self_proxy)

    def from_job_cache(self, job_id, key):
      job_id = int(job_id)
      key = str(key)
      with self.get_job_cache(job_id) as cache:
        return cache[key]

    def delete_job_cache(self, job_id):
      job_id = int(job_id)
      shelve_path = util.pandem_path("files", "staging", str(job_id), "cache")
      if os.path.exists(shelve_path):
        os.remove(shelve_path)

   
 
class CacheValue:
  def __init__(self, job_id, key, storage_proxy):
    self.job_id = job_id
    self.key = key
    self._storage_proxy = storage_proxy

  def value(self):
    return self._storage_proxy.from_job_cache(self.job_id, self.key).get()


