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

    def get_referential(self,variable_name):
        #variable_name = nom du dossier = geo or diseases or country_code
        list_files=[]
        referentiel=[]
        path=os.path.join(os.getenv('PANDEM_HOME'), 'files/variables/', variable_name)
        if os.path.isdir(path):
            list_files=self._storage_proxy.list_files(path).get()
            for file in list_files:
                var_list=self._storage_proxy.read_files(file['path']).get()
                for var in var_list['tuples']:
                    referentiel.append(var)
        else: 
            return None

        return referentiel

    #def standardize(self, tuples_to_validate, file_name, job_id):
    def standardize(self):
        #tuples_to_validate = tuples
        tuples=self._storage_proxy.read_files('variables/covid19-datahub-sample-tuples.json').get()
        for one_tuple in tuples['tuples']: 
            print(one_tuple['attrs'])
            for attr in one_tuple['attrs']:
                print(attr)

        #for var in tuples_to_validate :
        #    if var[''] in self.get_referential('geo').values()
        #    if var[''] in self.get_referential('diseases').values()
        #    if var[''] in self.get_referential('country_code').values()
        
        return None