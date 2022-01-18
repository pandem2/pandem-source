import os
import requests
from . import acquisition


class AcquisitionURL(acquisition.Acquisition):
    def __init__(self, name, orchestrator_ref, settings): 
        super().__init__(name = name, orchestrator_ref = orchestrator_ref, settings = settings, channel = "url")
        
    def new_files(self, dls, last_hash):
        url = dls['acquisition']['channel']['url']
        source_dir = self.source_path(dls)
        file_path = self.source_path(dls, '_'.join(url.replace(":","_").split('//')[1:]).replace('/', "_"))
        r = requests.get(url)
        current_etag = r.headers.get('ETag')
        files_to_pipeline = []
        if not os.path.exists(file_path) or last_hash == "":
            with open (file_path,'wb') as cont:
                cont.write(r.content)
            files_to_pipeline.extend([file_path])
        # the file already exists and we know the last etag 
        elif current_etag != last_hash:
            with open (file_path,'wb') as cont:
                cont.write(r.content)
            files_to_pipeline.extend([file_path])   
        return {"hash":current_etag, "files":files_to_pipeline}           


def download_if_new(url, dest):
  dest_hash = os.path.join(dest, ".hash")
  
  if os.path.exists(dest_hash): 
    with open(dest_hash, 'r') as file:
      last_hash = file.read()
  else:
    last_hash = None
  
  r = requests.get(url)
  new_hash = r.headers.get('ETag')

  if not os.path.exists(dest) or las_hash is None or last_hash != new_hash:
    with open (file_path,'wb') as cont:
      cont.write(r.content)
    if new_hash is not None:
      with open (file_path,'w') as h:
        h.write(new_hash)
    
