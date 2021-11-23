import os
from . import worker
from abc import ABC, abstractmethod, ABCMeta
from datetime import datetime, timedelta
import time

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

    # def loop_actions(self):
    #     self._self_proxy.monitor_source()
    
    @abstractmethod
    def new_files(self, dls, last_hash):
        pass

    def source_path(self, dls, *args):
        return self.pandem_path(f'files/{self.channel}', dls['scope']['source'], *args)

    def monitor_source(self, source_id, dls): 
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


    def actor_loop(self):
        while True:
            time.sleep(0.1)
            last_executions = [action['last_exec'] if action['last_exec'] is not None else datetime.now()-timedelta(days=2) \
                               for action in self._actions]
            next_executions = [action['repeat'].next_execution(last_exec) for action, last_exec in zip(self._actions, last_executions)]
            next_deltas = [(next_time - datetime.now()).total_seconds() for next_time in next_executions]
            next_action_index = next_deltas.index(min(next_deltas)) #return the index of the first min value
            if datetime.now() > next_executions[next_action_index]:
                next_action = self._actions[next_action_index]
                id_source = next_action['id_source']
                if id_source is not None:  #not send_heartbeat action
                    dls = self.current_sources[id_source]
                    next_action["func"](id_source, dls)
                    self._storage_proxy.write_db(
                                                {'id': id_source,
                                                 'last_exec': datetime.now(),
                                                 'next_exec': next_action['repeat'].next_execution(datetime.now())
                                                }, 
                                                'source'
                                            ).get()   
                else:
                    next_action["func"]()
                next_action['last_exec'] = datetime.now()
                self._actions[next_action_index] = next_action
            

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
        loop_frequency_str = dls['scope']['frequency'].strip()
        if loop_frequency_str=='':
            pass           #what should be done here?
        elif loop_frequency_str=='daily': #could we have frequecy less than daily?
            self.monitor_repeat = Acquisition.Repeat(timedelta(days=1))
        elif loop_frequency_str.split(' ')[-1]=='seconds':
            loop_frequency = int(loop_frequency_str.split(' ')[-2]) #every n seconds
            self.monitor_repeat = Acquisition.Repeat(timedelta(seconds=loop_frequency))
        elif loop_frequency_str.split(' ')[-1]=='minutes':
            loop_frequency = int(loop_frequency_str.split(' ')[-2]) #every n minutes
            self.monitor_repeat = Acquisition.Repeat(timedelta(minutes=loop_frequency))
        elif loop_frequency_str.split(' ')[-1]=='hours':
            loop_frequency = int(loop_frequency_str.split(' ')[-2]) #every n hours
            self.monitor_repeat = Acquisition.Repeat(timedelta(hours=loop_frequency))
        self.register_action(repeat=self.heartbeat_repeat, action=lambda: self.monitor_source(id_source, dls), id_source=id_source)        

<<<<<<< HEAD
    # def monitor_source(self): 
    #     # Iterating over all registered sources looking for new files
    #     for source_id, dls in self.current_sources.items():
    #         last_hash = self._storage_proxy.read_db('source', lambda x: x['id']==source_id).get()['last_hash'].values[0]
    #         #Getting new files if any
    #         print(f'last hash is: {last_hash}')
    #         nf = self.new_files(dls, last_hash)
    #         files_to_pipeline = nf["files"]
    #         #print(f'files to pipeline: {files_to_pipeline}')
    #         new_hash = nf["hash"]
    #         # If new files are found they will be send to the pipeline 
    #         if len(files_to_pipeline)>0:
    #             #TODO: remove!!!!!!!!!!!!!!!!!
    #             files_to_pipeline = files_to_pipeline[0:1]
    #             # Sending files to the pipeline
    #             self._pipeline_proxy.submit_files(dls, files_to_pipeline).get()
    #             # Storing the new hash into the db
    #             self._storage_proxy.write_db(
    #                 {'name': dls['scope']['source'],
    #                  'last_hash': new_hash,
    #                  'id': source_id
    #                 }, 
    #                 'source'
    #             ).get()   
=======
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
>>>>>>> main

