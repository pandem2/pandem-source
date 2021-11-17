import subprocess
import os
import time
import threading
from . import worker


class Standardizer(worker.Worker):
    def __init__(self, name, orchestrator_ref, settings): 
        super().__init__(name = name, orchestrator_ref = orchestrator_ref, settings = settings)
        self._orchestrator_proxy = orchestrator_ref.proxy()
        print(f'here in {self.name} __init__')

    def on_start(self):
        threading.Thread(target=self.actor_loop).start()
        self._storage_proxy=self._orchestrator_proxy.get_actor('storage').get().proxy()
        print(f'here in {self.name} on-start')

    def read_df_start(self, dls):
        pass
        
    def send_heartbeat(self):
        self._orchestrator_proxy.get_heartbeat(self.name)    

    def actor_loop(self):
        while True:
            self.send_heartbeat()
            time.sleep(20)

    def get_variables(self): 
        var_list=self._storage_proxy.read_files('variables/variables.json').get()
        for var in var_list: 
            dic_variables[var['variable']]=var
            if 'aliases' in dic_variables :
                for alias in dic_variables['aliases']:
                    alias_var=var.clone()
                    alias_var['variable']=alias['alias']
                    alias_var['modifiers']=alias['modifiers']
                    dic_variables[alias['alias']]=alias_var
        return dic_variables

    def get_referential(self, variables_name):
        pass

    def standardize(self, tuples_to_validate, file_name, job_id):
        pass

