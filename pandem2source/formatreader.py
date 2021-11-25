import time
from . import worker
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
        self._pipeline_proxy.read_format_end(job, file_path, df).get()

        

