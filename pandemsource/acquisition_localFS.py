import os
import hashlib
from . import acquisition


class AcquisitionLocalFS(acquisition.Acquisition):
    def __init__(self, name, orchestrator_ref, settings): 
        super().__init__(name = name, orchestrator_ref = orchestrator_ref, settings = settings, channel = "input-local")
        
    def new_files(self, dls, last_hash):
        file_path = self.source_path(dls, dls['acquisition']['channel']['xls_file'])
        if os.path.exists(file_path):
            current_hash = hashlib.md5(open(file_path,'rb').read()).hexdigest()
            files_to_pipeline = []
            if not os.path.exists(file_path) or last_hash == "":
                files_to_pipeline.extend([file_path])
            # the file already exists and we know the last etag 
            elif current_hash != last_hash:
                files_to_pipeline.extend([file_path])   
            return {"hash":current_hash, "files":files_to_pipeline}  
        else:
            return {"hash":'', "files":[]} 