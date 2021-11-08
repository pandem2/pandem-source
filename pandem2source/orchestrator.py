import pykka
from . import storage
from . import acquisition_url
from . import pipeline
from . import formatreader
from . import dfreader
import datetime
import os


class Orchestration(pykka.ThreadingActor):
    
    def __init__(self, settings):
        super(Orchestration, self).__init__()
        self.settings = settings
        self.current_actors = dict()
        
   
    def on_start(self):      
        #lauch storage_actor
        storage_ref = storage.Storage.start('storage', self.actor_ref, self.settings)
        self.current_actors['storage'] = {'ref': storage_ref}
        # list source definition files within 'source-definitions' through storage actor
        source_files = storage_ref.proxy().list_files('source-definitions').get()
        # read json dls files into dicts
        dls_dicts = [storage_ref.proxy().read_files(file_name['path']).get() for file_name in source_files]
        #launch dataframe reader acor
        dfreader_ref = dfreader.DataframeReader.start('dfreader', self.actor_ref, storage_ref, self.settings)
        self.current_actors['dfreader'] = {'ref': dfreader_ref}
        #launch format reader actor
        ftreader_ref = formatreader.FormatReaderXML.start('ftreader', self.actor_ref, storage_ref, self.settings)
        self.current_actors['ftreader'] = {'ref': ftreader_ref}
        #launch pipeline actor
        pipeline_ref = pipeline.Pipeline.start('pipeline', self.actor_ref, self.settings)
        pipeline_proxy = pipeline_ref.proxy()
        self.current_actors['pipeline'] = {'ref': pipeline_ref}
        #launch acquisition actor(s)
        sources_labels = set([dls['acquisition']['channel']['name'] for dls in dls_dicts])
        for label in sources_labels:
            #launch only url acquisition
            if label == "url":
                acquisition_ref = acquisition_url.AcquisitionURL.start(name = 'acquisition_'+label, orchestrator_ref = self.actor_ref, settings = self.settings)
                acquisition_proxy = acquisition_ref.proxy()
                dls_label = [dls for dls in dls_dicts if dls['acquisition']['channel']['name'] == label]
                for dls in dls_label:
                    acquisition_proxy.add_datasource(dls)
                self.current_actors['acquisition_'+label] = {'ref': acquisition_ref, 'sources': dls_label} 
        print('in orchestrator on-start')
        
        
    def get_heartbeat(self, actor_name):
        now = datetime.datetime.now()
        self.current_actors[actor_name]['heartbeat'] = now
        print(f'heartbeat from {actor_name} at: {now}')
        
    def get_actor(self, actor_name):
        return self.current_actors[actor_name]['ref']


        
    



    
