import os
import gzip
import shutil
import logging
import requests
import re
from urllib.parse import urlparse
import urllib.request 
from . import acquisition
from . import util

l = logging.getLogger("pandem.zenodo")

class AcquisitionZenodo(acquisition.Acquisition):
    def __init__(self, name, orchestrator_ref, settings):
        super().__init__(name = name, orchestrator_ref = orchestrator_ref, settings = settings, channel = "zenodo")
    
    def new_files(self, dls, last_hash):
        source = dls['acquisition']['channel']['search']
        match = dls['acquisition']['channel']['match'] if 'match' in dls['acquisition']['channel'] else None 
        ignore_unchanged = dls['acquisition']['channel']['ignore_unchanged'] if 'ignore_unchanged' in dls['acquisition']['channel'] else True

        if match is not None:
          match = re.compile(match)
        url = f'https://zenodo.org/api/records/?q=conceptrecid:{source}&access_token='
        r = requests.get(url)
        response_status(r.status_code, url=url)
        j = r.json()
        
        if len(j['hits']['hits']) != 1:
            raise ValueError(f'Cannot identify a unique source with the provided string')
        hashes = last_hash.split(';')
        pairs = sorted(((f['links']['self'], f['checksum']) for f in j['hits']['hits'][0]['files']), key= lambda p:p[0])
        current_hash = []
        files_to_pipeline = []
        
        for url, file_hash in pairs:
            parts = urlparse(url)
            dest_name = parts.path.replace('/', '_')
            file_path = self.source_path(dls, dest_name)
            file_changed = not os.path.exists(file_path) or f"md5:{util.md5(file_path)}" != file_hash
            if match is None or match.search(file_path) is not None:
              # only downloading the file if it has changed (or new)
              if file_changed:
                l.debug(f"change detected in {url} downloading into {file_path}")
                urllib.request.urlretrieve(url, file_path)
              else:
                l.debug(f"Ignoring download of unchanged file {file_path}")
              
              # sending the file to process if it has changed or if ignore unchanged is set to False
              if file_changed or ignore_unchanged == False:
                l.debug(f"Sending file {file_path} to process")
                files_to_pipeline.append(file_path)
              else:
                l.debug(f"Ignoring processing of unchanged file {file_path}")

              # adding the file hash to total hash in any case
              current_hash.append(file_hash)
        # if all files are unchanged then sending no files to pipeline even thought ignore_unchanged is set to False
        new_hash = ";".join(current_hash)
        if ignore_unchanged == False and len(files_to_pipeline) > 0 and new_hash == last_hash:
          l.debug("Ignoring files since no hash has changed")
          files_to_pipeline = []
        return {"hash":new_hash, "files":files_to_pipeline}


def response_status(rstatus: int, url: str = 'URL') -> None:
    if 200 <= rstatus and rstatus < 300:
        l.debug(f'{url} was accessed successfully')
    elif 500 <= rstatus and rstatus < 600:
        raise ValueError(f'Zenodo API is currently unavailable: {rstatus}')
    else:
        raise ValueError(f'{url} returns ERROR CODE {rstatus}')
