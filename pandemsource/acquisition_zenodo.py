import os
import gzip
import shutil
import requests
from urllib.parse import urlparse
from . import acquisition

class AcquisitionZenodo(acquisition.Acquisition):
    def __init__(self, name, orchestrator_ref, settings):
        super().__init__(name = name, orchestrator_ref = orchestrator_ref, settings = settings, channel = "zenodo")
    
    def new_files(self, dls, last_hash):
        print("he ho")
        source = dls['acquisition']['channel']['search']
        match = dls['acquisition']['channel']['match']
        url = f'https://zenodo.org/api/records/?q=conceptrecid:{source}&access_token='
        r = requests.get(url)
        if r.status_code != 200:
            raise ValueError(f'{url} returns ERROR CODE {r.status_code}')
        j = r.json()
        if len(j['hits']['hits']) != 1:
            raise ValueError(f'Cannot identify a unique source with the provided string')
        hashes = last_hash.split(';')
        pairs = [(f['links']['self'], f['checksum']) for f in j['hits']['hits'][0]['files'] if f['checksum'] not in hashes]
        print(len(pairs))
        current_hash = []
        files_to_pipeline = []
        print("Debug:")
        print(f"hashes: {hashes}")
        print(f"current_hash: {current_hash}")
        for url, file_hash in pairs:
            parts = urlparse(url)
            dest_name = parts.path.replace('/', '_')
        
            file_path = self.source_path(dls, dest_name)
            r = requests.get(url)
            current_hash.append(file_hash)
            print(f"current_hash: {current_hash}")
            if not os.path.exists(file_path[:-3]) or last_hash == "":
                with open(file_path, 'wb') as cont:
                    cont.write(r.content)
                with gzip.open(file_path, 'rb') as f_in:
                    with open(file_path[:-3], 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                os.remove(file_path)
                files_to_pipeline.extend([file_path[:-3]])
        print('DL FILES COMPLETED')
        return {"hash":";".join(current_hash), "files":files_to_pipeline}