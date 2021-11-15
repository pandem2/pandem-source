import os
from . import worker
from abc import ABC, abstractmethod, ABCMeta
import time
from zipfile import ZipFile
from io import BytesIO

class Unarchive(worker.Worker):
    __metaclass__ = ABCMeta  
    def __init__(self, name, orchestrator_ref, settings): 
        super().__init__(name = name, orchestrator_ref = orchestrator_ref, settings = settings)
         
    def on_start(self):
        super().on_start()
        self._pipeline_proxy = self._orchestrator_proxy.get_actor('pipeline').get().proxy()

    def loop_actions(self):
        pass
    
    def unarchive(self, job): 
        #print('here in unarchive')
        # print(f'files_to_filter: {files_to_filter}')
        for zip_path in job['source_files']:
            for file in job['dls_json']["acquisition"]["decompress"]["path"]:
                print(f'file to filter {file}')
                with ZipFile(zip_path) as zip_archive:
                    self._pipeline_proxy.decompress_end(job, zip_path, file, BytesIO(zip_archive.read(file)).read())#.get()


       


