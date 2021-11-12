import pykka
from . import storage
from . import acquisition_url
from . import acquisition_git
from . import pipeline
from . import formatreader_xml
from . import formatreader_csv
from . import unarchive
import datetime
import os


class Orchestration(pykka.ThreadingActor):
    
    def __init__(self, settings):
        super().__init__()
        self.settings = settings
        self.current_actors = dict()
        
    def on_start(self):      
        #lauch storage_actor
        storage_ref = storage.Storage.start('storage', self.actor_ref, self.settings)
        storage_proxy = storage_ref.proxy()
        self.current_actors['storage'] = {'ref': storage_ref}
        #launch pipeline actor
        pipeline_ref = pipeline.Pipeline.start('pipeline', self.actor_ref, self.settings)
        self.current_actors['pipeline'] = {'ref': pipeline_ref}
        #launch xml format reader actor
        xmlreader_ref = formatreader_xml.FormatReaderXML.start('ftreader_xml', self.actor_ref, self.settings)
        self.current_actors['ftreader_xml'] = {'ref': xmlreader_ref}
        #launch csv format reader actor
        csvreader_ref = formatreader_csv.FormatReaderCSV.start('ftreader_csv', self.actor_ref, self.settings)
        self.current_actors['ftreader_csv'] = {'ref': csvreader_ref}
        #launch unarchiver actor
        unarchive_ref = unarchive.Unarchive.start('unarchiver', self.actor_ref, self.settings)
        self.current_actors['unarchiver'] = {'ref': unarchive_ref}
        # list source definition files within 'source-definitions' through storage actor
        source_files = storage_proxy.list_files('source-definitions').get()
        # read json dls files into dicts
        dls_dicts = [storage_proxy.read_file(file_name['path']).get() for file_name in source_files]
        #launch acquisition actor(s)
        sources_labels = set([dls['acquisition']['channel']['name'] for dls in dls_dicts])
        acquisition_actors = {"url": acquisition_url.AcquisitionURL,
                              "git": acquisition_git.AcquisitionGIT}
        for label in sources_labels:
            acquisition_ref = acquisition_actors[label].start(name = 'acquisition_'+label, orchestrator_ref = self.actor_ref, settings = self.settings)
            acquisition_proxy = acquisition_ref.proxy()
            dls_label = [dls for dls in dls_dicts if dls['acquisition']['channel']['name'] == label]
            for dls in dls_label:
                acquisition_proxy.add_datasource(dls)
            self.current_actors['acquisition_'+label] = {'ref': acquisition_ref, 'sources': dls_label} 
        print('in orchestrator on-start')
        
    def get_heartbeat(self, actor_name):
        now = datetime.datetime.now()
        self.current_actors[actor_name]['heartbeat'] = now
        #print(f'heartbeat from {actor_name} at: {now}')
        
    def get_actor(self, actor_name):
        return self.current_actors[actor_name]['ref']


        
    



    
