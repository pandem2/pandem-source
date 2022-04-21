import os
import gzip
import shutil
import logging
import requests
from urllib.parse import urlparse
from . import acquisition

l = logging.getLogger("pandem.zenodo")

class AcquisitionZenodo(acquisition.Acquisition):
    def __init__(self, name, orchestrator_ref, settings):
        super().__init__(name = name, orchestrator_ref = orchestrator_ref, settings = settings, channel = "zenodo")
    
    def new_files(self, dls, last_hash):
        source = dls['acquisition']['channel']['search']
        match = dls['acquisition']['channel']['match']
        url = f'https://zenodo.org/api/records/?q=conceptrecid:{source}&access_token='
        r = requests.get(url)
        response_status(r.status_code, url=url)

        j = r.json()
        if len(j['hits']['hits']) != 1:
            raise ValueError(f'Cannot identify a unique source with the provided string')
        hashes = last_hash.split(';')
        pairs = [(f['links']['self'], f['checksum']) for f in j['hits']['hits'][0]['files'] if f['checksum'] not in hashes]
        current_hash = []
        files_to_pipeline = []
        for url, file_hash in pairs:
            parts = urlparse(url)
            dest_name = parts.path.replace('/', '_')
            file_path = self.source_path(dls, dest_name)
            
            r = requests.get(url)
            response_status(r.status_code, url=url)
            
            current_hash.append(file_hash)
            if not os.path.exists(file_path[:-3]) or last_hash == "":
                with open(file_path, 'wb') as cont:
                    cont.write(r.content)
                try:
                    with gzip.open(file_path, 'rb') as f_in:
                        with open(file_path[:-3], 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)
                    os.remove(file_path)
                    fpath_no_ext = os.path.splitext(file_path)[0]
                    files_to_pipeline.extend([fpath_no_ext])
                except gzip.BadGzipFile:
                    l.debug(f'{file_path} is corrupted. Skipping it')
        return {"hash":";".join(current_hash), "files":files_to_pipeline}


def response_status(rstatus: int, url: str = 'URL') -> None:
    if 200 <= rstatus and rstatus < 300:
        l.debug(f'{url} was accessed successfully')
    elif 500 <= rstatus and rstatus < 600:
        raise ValueError(f'Zenodo API is currently unavailable: {rstatus}')
    else:
        raise ValueError(f'{url} returns ERROR CODE {rstatus}')