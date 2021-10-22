import pykka
from . import storage
from . import acquisition
import datetime
import os


class Orchestration(pykka.ThreadingActor):
    
    def __init__(self, settings):
        super(Orchestration, self).__init__()
        self.settings = settings
        self.current_actors = dict()
        
   
    def on_start(self):      
        storage_ref = storage.Storage.start('storage', self.actor_ref, self.settings)
        self.current_actors['storage'] = {'ref': storage_ref} 
        
        # list source definition files within 'source-definitions' through storage actor
        source_files = storage_ref.proxy().list_files('source-definitions').get()
        
        # read json dls files into dicts
        dls_dicts = [storage_ref.proxy().read_files(file_name['path']).get() for file_name in source_files]
        
        sources_labels = set([dls['acquisition']['channel']['name'] for dls in dls_dicts])
        for label in sources_labels:
            acquisition_ref = acquisition.Acquisition.start('acquisition_'+label, self.actor_ref, storage_ref, self.settings)
            acquisition_proxy = acquisition_ref.proxy()
            dls_label = [dls for dls in dls_dicts if dls['acquisition']['channel']['name'] == label]
            for dls in dls_label:
                acquisition_proxy.add_datasource(dls)
                
            self.current_actors['acquisition_'+label] = {'ref': acquisition_ref, 'sources': dls_label}
        
        
    def get_heartbeat(self, actor_name):
        now = datetime.datetime.now()
        self.current_actors[actor_name]['heartbeat'] = now
        print(f'heartbeat from {actor_name} at: {now}')
        

        
    



    
