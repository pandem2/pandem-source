import os
import requests
from . import acquisition


class AcquisitionURL(acquisition.Acquisition):
    def __init__(self, name, orchestrator_ref, settings): 
        super().__init__(name = name, orchestrator_ref = orchestrator_ref, settings = settings, channel = "url")
        
    def new_files(self, dls, last_hash):
        url = dls['acquisition']['channel']['url']
        source_dir = self.source_path(dls)
        file_path = self.source_path(dls, '_'.join(url.split('//')[1].split('/')))
        r = requests.get(url)#allow_redirects=True
        current_etag = r.headers.get('ETag')
        
        # If the file does not exist or if no commit is provided all files will be sent to the pipeline
        files_to_pipeline = []
        if not os.path.exists(file_path) or last_hash == "":
            print(f"Downloading file {file_path}")
            with open (file_path,'wb') as cont:
                cont.write(r.content)
            files_to_pipeline.extend([file_path])
        # the file already exists and we know the last etag 
        elif current_etag != last_hash:
            print(f'current etag: {current_etag}')
            print(f'last_ahash: {last_hash}')
            with open (file_path,'wb') as cont:
                cont.write(r.content)
            files_to_pipeline.extend([file_path])
            
        return {"hash":current_etag, "files":files_to_pipeline}           

        
