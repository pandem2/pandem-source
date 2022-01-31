import time
from . import worker
from . import util
from abc import ABC, abstractmethod, ABCMeta

class FormatReader(worker.Worker):
    __metaclass__ = ABCMeta  
    def __init__(self, name, orchestrator_ref, settings): 
        super().__init__(name = name, orchestrator_ref = orchestrator_ref, settings = settings)
         
    def on_start(self):
        super().on_start()
        self._storage_proxy = self._orchestrator_proxy.get_actor('storage').get().proxy()
        self._pipeline_proxy = self._orchestrator_proxy.get_actor('pipeline').get().proxy()

    def loop_actions(self):
        pass

    @abstractmethod
    def read_df(self, file_path, dls):
        pass
    
    def read_format_start(self, job, file_path):
        #print(f'I m in read_format_start : {file_path} with job : {job}')
        df = self.read_df(file_path, job['dls_json'])
        dls = job['dls_json']

        # adding column line number if it does not exists
        if 'line_number' not in df.columns :
            df['line_number'] = range(1, len(df)+1) 

        # adding file_path column
        if 'file' not in df.columns:
          df['file'] = file_path

        # calling custom df transformer if defined
        custom_trans = util.get_custom(["sources", dls["scope"]["source"].replace("-", "_").replace(" ", "_")],"df_transform")
        if custom_trans is not None:
          df = custom_trans(df)

        # Applying filters if any
        if "filter" in dls:
          for f in dls["filter"]:
            if "type" in f and f["type"] == "in" and "column" in f and "values" in f and f["column"] in df:
              df = df[df[f["column"]].isin(f["values"])]

        # reseting index
        df.reset_index(drop=True, inplace=True)
        self._pipeline_proxy.read_format_end(job, file_path, df).get()

        

