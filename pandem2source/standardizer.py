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
        self._storage_proxy=self._orchestrator_proxy.get_actor('storage').get().proxy()
        print(f'here in {self.name} on-start')
          
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
        #variable_name = geo or diseases or country_code
        var_geo=self._storage_proxy.read_files('variables/geo/geo_sample.json').get()
        var_diseases=self._storage_proxy.read_files('variables/diseases/diseases_sample.json').get()
        var_country_code=self._storage_proxy.read_files('variables/country_code/country_code_sample.json').get()
        dic_ref=var_diseases
        return dic_ref

    def standardize(self, tuples_to_validate, file_name, job_id):
        #for var in tuples_to_validate :
        #    if var[''] in self.get_referential('geo').values()
        #    if var[''] in self.get_referential('diseases').values()
        #    if var[''] in self.get_referential('country_code').values()
        pass
    