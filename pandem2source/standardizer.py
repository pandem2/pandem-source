import subprocess
import os
import time
import threading
from . import worker
from datetime import datetime


class Standardizer(worker.Worker):
    def __init__(self, name, orchestrator_ref, settings): 
        super().__init__(name = name, orchestrator_ref = orchestrator_ref, settings = settings)
        self._orchestrator_proxy = orchestrator_ref.proxy()
        print(f'here in {self.name} __init__')

    def on_start(self):
        self._storage_proxy=self._orchestrator_proxy.get_actor('storage').get().proxy()
        print(f'here in {self.name} on-start')
        self._varaibles_proxy=self._orchestrator_proxy.get_actor('variables').get().proxy()
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

    #def standardize(self, tuples_to_validate, var_name, job):
    def standardize(self,var_name):  
        job={'job_id':123, 'step':'step-job'}
        tuples=self._storage_proxy.read_files('variables/covid19-datahub-sample-tuples.json').get()
        referential=self._varaibles_proxy.get_referential(var_name).get()
        list_issues=[]
        list_ref=[]
        list_ref=set([x['attr'] for x in referential])

        for var in tuples['tuples']:
            one_tuple=var['attrs'][var_name]
            if one_tuple in list_ref: 
                None
            else:
                message=(f"Code {one_tuple} does not exist in referential '{var_name}'. Line {var['attrs']['line_number']} in file {tuples['scope']['file_name']} (source: '{tuples['scope']['source']}').")
                issue={ job['step'], 
                        var['attrs']['line_number'], 
                        tuples['scope']['source'], 
                        tuples['scope']['file_name'], 
                        message, 
                        datetime.now(), 
                        job['job_id'], 
                        "ref-not-found"}
                list_issues.append(issue)
                #si aucune erreur on retourne la liste des tuples
        return list_issues

