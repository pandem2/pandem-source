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
    
    def unarchive(self, archive_path, filter_path, job): 
        #print('here in unarchive')
        # print(f'files_to_filter: {files_to_filter}')
        with ZipFile(archive_path) as zip_archive:
            self._pipeline_proxy.unarchive_end(archive_path, filter_path, BytesIO(zip_archive.read(filter_path)).read(), job)


       


