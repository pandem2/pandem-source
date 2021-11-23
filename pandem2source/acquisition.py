import os
from . import worker
from abc import ABC, abstractmethod, ABCMeta

class Acquisition(worker.Worker):
    __metaclass__ = ABCMeta  
    def __init__(self, name, orchestrator_ref, settings, channel): 
        self.channel = channel
        super().__init__(name = name, orchestrator_ref = orchestrator_ref, settings = settings)
         
    def on_start(self):
        super().on_start()
        self.current_sources = dict()
        self._storage_proxy = self._orchestrator_proxy.get_actor('storage').get().proxy()
        self._pipeline_proxy = self._orchestrator_proxy.get_actor('pipeline').get().proxy()

    def loop_actions(self):
        self._self_proxy.monitor_source()
    
    @abstractmethod
    def new_files(self, dls, last_hash):
        pass

    def source_path(self, dls, *args):
        return self.pandem_path(f'files/{self.channel}', dls['scope']['source'], *args)

    def add_datasource(self, dls):
       source_dir = self.source_path(dls)
       if not os.path.exists(source_dir):
           os.makedirs(source_dir)
       # registering new source if not already on the source table
       find_source = self._storage_proxy.read_db('source', lambda x: x['name']==dls['scope']['source']).get()
       if find_source is None  or len(find_source["id"]) == 0:
            id_source = self._storage_proxy.write_db(
                 {
                  'name': dls['scope']['source'],
                  'last_hash':""
                 },
                'source'
            ).get()
       else:
            id_source = find_source["id"].values[0]
       # updating the internal variable current sources
       self.current_sources[id_source] = dls              

    def monitor_source(self): 
        # Iterating over all registered sources looking for new files
        for source_id, dls in self.current_sources.items():
            last_hash = self._storage_proxy.read_db('source', lambda x: x['id']==source_id).get()['last_hash'].values[0]
            #Getting new files if any
            print(f'last hash is: {last_hash}')
            nf = self.new_files(dls, last_hash)
            files_to_pipeline = nf["files"]
            #print(f'files to pipeline: {files_to_pipeline}')
            new_hash = nf["hash"]
            # If new files are found they will be send to the pipeline 
            if len(files_to_pipeline)>0:
                #TODO: remove!!!!!!!!!!!!!!!!!
                #files_to_pipeline = files_to_pipeline[0:1]
                # Sending files to the pipeline
                self._pipeline_proxy.submit_files(dls, files_to_pipeline).get()
                # Storing the new hash into the db
                self._storage_proxy.write_db(
                    {'name': dls['scope']['source'],
                     'last_hash': new_hash,
                     'id': source_id
                    }, 
                    'source'
                ).get()   

