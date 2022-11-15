from . import worker
from abc import ABCMeta
from gzip import GzipFile
from zipfile import ZipFile
from io import BytesIO
import subprocess

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
        process = subprocess.run(["file", "-b", "--mime-type", archive_path], capture_output=True)
        archive_type = process.stdout.decode("utf8").strip()
        if archive_type == "application/zip":
            with ZipFile(archive_path) as zip_archive:
                self._pipeline_proxy.unarchive_end(archive_path, filter_path, BytesIO(zip_archive.read(filter_path)).read(), job)
        elif archive_type == "application/gzip":
            with GzipFile(archive_path) as gzip_archive:
                self._pipeline_proxy.unarchive_end(archive_path, filter_path, gzip_archive.read(), job)

       


