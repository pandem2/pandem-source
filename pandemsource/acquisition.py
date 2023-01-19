import os
from . import worker
from abc import ABC, abstractmethod, ABCMeta
from datetime import datetime, timedelta
import time
from . import util
import logging
import json

l = logging.getLogger("pandem.acquisition")

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

    
    @abstractmethod
    def new_files(self, dls, last_hash):
        pass

    def source_path(self, dls, *args):
        if dls['acquisition']['channel']['name']=='input-local':
            return self.pandem_path(f'files/{self.channel}', *args)
        else:
            return self.pandem_path(f'files/{self.channel}', dls['scope']['source'], *args)

    def monitor_source(self, source_id, dls, freq): 
        #print(f'here acquisition monitor_source loop: {source_id} {freq}')
        #if post processing is present on the dls then each step will have its own hash
        s = self._storage_proxy.read_db('source', lambda x: x['id']==source_id).get()
        last_hash = s['last_hash'].values[0]
        sname = s['name'].values[0]

        last_hashes = []
        nf = []
        source_dir = self.source_path(dls)
        if "post_processing" in dls["acquisition"]["channel"] and last_hash is not None and last_hash != '':
          try:
            last_hashes = json.loads(last_hash)
            last_hash = last_hashes[0]
          except:
            pass
        #Getting new files if the source is not currently saturated
        source_steps = self._storage_proxy.read_db('job', lambda j:j.status == 'in progress' and j.source == sname).get()
        if source_steps is not None:
          saturated = len(source_steps["step"]) > len(source_steps["step"].unique())
          isat = 2
          while saturated:
            source_steps = self._storage_proxy.read_db('job', lambda j:j.status == 'in progress' and j.source == sname).get()
            saturated = len(source_steps["step"]) > len(source_steps["step"].unique())
            l.debug(f"source {sname} is saturated. Waiting {isat*isat} seconds")
            time.sleep(isat*isat)
            isat = isat + 1
        
        nf.append(self.new_files(dls, last_hash))
        if "post_processing" in dls["acquisition"]["channel"]:
          for i, function in enumerate(dls["acquisition"]["channel"]["post_processing"]):
            # we expect the function to be present on the default sorce script
            f = util.get_custom(["sources", dls["scope"]["source"].replace("-", "_").replace(" ", "_")], function)
            if f is not None:
              os.chdir(source_dir)
              l.debug(f"Calling custom script transformation {function}")
              nf.append(
                f(
                  files_hash = nf[-1], 
                  last_hash = last_hashes[i + 1] if i + 1 < len(last_hashes) else last_hash, 
                  dls = dls,
                  orchestrator = self._orchestrator_proxy,
                  logger = l,
                )
              )
            else: 
              raise ValueError(f"Custom post processing action {function} not found custom scripts for {dls['scope']['source']}")


        files_to_pipeline = nf[-1]["files"]
        #print(f'files to pipeline: {files_to_pipeline}')
        new_hash = [nfi["hash"] for nfi in nf]
        new_hash = new_hash[0] if len(new_hash) == 1 else json.dumps(new_hash)
        # If new files are found they will be send to the pipeline 
        if len(files_to_pipeline)>0:
            l.info(f'New {len(files_to_pipeline)} files to process, hash went from {last_hash} to {new_hash}')
            # Sending files to the pipeline
            self._pipeline_proxy.submit_files(dls, files_to_pipeline).get()
            # executing after_submit if set
            if "after_submit" in dls["acquisition"]["channel"]:
              function = dls["acquisition"]["channel"]["after_submit"]
              f = util.get_custom(["sources", dls["scope"]["source"].replace("-", "_").replace(" ", "_")], function)
              if f is not None:
                os.chdir(source_dir)
                l.debug(f"Calling custom script for after submit {function}")
                f(
                  files_hash = nf[-1], 
                  dls = dls,
                  logger = l
                )
              else: 
                raise ValueError(f"Custom after submit action {function} not found custom scripts for {dls['scope']['source']}")
            # Storing the new hash into the db
            self._storage_proxy.write_db(
                {'name': dls['scope']['source'],
                    'last_hash': new_hash,
                    'id': source_id
                }, 
                'source'
            ).get()
        # updating next execution
        self._storage_proxy.write_db(
           {'id': source_id,
               'last_exec': freq.last_exec,
               'next_exec': freq.next_exec()
           }, 
           'source'
        ).get()  

        # if something was returned and the source is marked as repeat_until_empty a new acquisition is launched
        if len(files_to_pipeline) > 0 and "schedule" in dls["acquisition"] and dls["acquisition"]["schedule"].get("repeat_until_empty"):
          self._self_proxy.monitor_source(source_id, dls, freq)

    def add_datasource(self, dls, force_acquire, ignore_last_exec):
        source_dir = self.source_path(dls)
        if not os.path.exists(source_dir):
            os.makedirs(source_dir)
        # registering new source if not already on the source table
        find_source = self._storage_proxy.read_db('source', lambda x: x['name']==dls['scope']['source']).get()
        if find_source is None  or len(find_source["id"]) == 0:
            last_exec = None
            id_source = self._storage_proxy.write_db(
                 {
                  'name': dls['scope']['source'],
                  'last_hash':""
                 },
                'source'
            ).get()
        else:
            id_source = find_source["id"].values[0]
            # ignoring last exec if force acquire or ignore_last_exec are requested 
            if force_acquire or ignore_last_exec:
               last_exec = None
            else:
               last_exec = find_source["last_exec"].values[0]
            
            # reseting the last_hash if force acquire is requested 
            if force_acquire:
               for s in find_source.to_dict(orient='records'):
                  s["last_hash"] = ""
                  s["last_exec"] = None
                  s["next_exec"] = None
                  self._storage_proxy.write_db(s, "source").get()

         # updating the internal variable current sources
        self.current_sources[id_source] = dls 
        
        loop_frequency_str = dls['scope']['frequency'].strip()
        if 'frequency_start_hour' in dls['scope']:
          start_hour = int(dls['scope']['frequency_start_hour'])
        else :
          start_hour = None
        if 'frequency_end_hour' in dls['scope']:
          end_hour = int(dls['scope']['frequency_end_hour'])
        else :
          end_hour = None
        if loop_frequency_str=='':
            monitor_repeat = worker.Repeat(timedelta(days=1), start_hour = start_hour, end_hour = end_hour, last_exec = last_exec)         
        elif loop_frequency_str=='daily':
            monitor_repeat = worker.Repeat(timedelta(days=1), start_hour = start_hour, end_hour = end_hour, last_exec = last_exec)
        elif loop_frequency_str.split(' ')[-1]=='seconds':
            loop_frequency = int(loop_frequency_str.split(' ')[-2]) #every n seconds
            monitor_repeat = worker.Repeat(timedelta(seconds=loop_frequency), start_hour = start_hour, end_hour = end_hour, last_exec = last_exec)
        elif loop_frequency_str.split(' ')[-1]=='minutes':
            loop_frequency = int(loop_frequency_str.split(' ')[-2]) #every n minutes
            monitor_repeat = worker.Repeat(timedelta(minutes=loop_frequency), start_hour = start_hour, end_hour = end_hour, last_exec = last_exec)
        elif loop_frequency_str.split(' ')[-1]=='hours':
            loop_frequency = int(loop_frequency_str.split(' ')[-2]) #every n hours
            monitor_repeat = worker.Repeat(timedelta(hours=loop_frequency), start_hour = start_hour, end_hour = end_hour, last_exec = last_exec)

        self.register_action(repeat=monitor_repeat, action=lambda: self.monitor_source(id_source, dls, monitor_repeat))        



