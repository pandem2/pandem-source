from . import acquisition
import requests

class AcquisitionZenodo(acquisition.Acquisition):
    def __init__(self, name, orchestrator_ref, settings):
        super().__init__(name=name, orchestrator_ref=orchestrator_ref, settings=settings)
    
    def new_files(self, dls, last_hash):
        source = dls['acquisition']['channel']['search']
        match = dls['acquisition']['channel']['match']
        url = f'https://zenodo.org/api/records/?q={source}&access_token='
        r = requests.get(url)
        if r.status_code != 200:
            raise ValueError(f'{url} returns ERROR CODE {r.status_code}')
        