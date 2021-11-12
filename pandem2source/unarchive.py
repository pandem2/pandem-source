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
        files_to_filter = job['dls_json']["acquisition"]["decompress"]["path"]
       # print(f'files_to_filter: {files_to_filter}')
        zip_path = job['source_files'][0]
       # print(f'zip bpath is: {zip_path}')
        filtered_bytes = []
        for file in files_to_filter:
            #print(f'file before filter {file}')
            with ZipFile(zip_path) as zip_archive:
                filtered_bytes.append(BytesIO(zip_archive.read(file)).read())
        self._pipeline_proxy.decompress_end(job, filtered_bytes)#.get()


       


