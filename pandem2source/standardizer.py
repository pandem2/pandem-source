import subprocess
import os
import time
import threading
from . import worker
from datetime import datetime
import copy


class Standardizer(worker.Worker):
    def __init__(self, name, orchestrator_ref, settings): 
        super().__init__(name = name, orchestrator_ref = orchestrator_ref, settings = settings)
        self._orchestrator_proxy = orchestrator_ref.proxy()
        print(f'here in {self.name} __init__')

    def on_start(self):
        self._storage_proxy=self._orchestrator_proxy.get_actor('storage').get().proxy()
        print(f'here in {self.name} on-start')
        self._variables_proxy=self._orchestrator_proxy.get_actor('variables').get().proxy()
        print(f'here in {self.name} on-start')

    #def standardize(self, tuples_to_validate, job):
    def standardize(self):  
        job={'job_id':123, 'step':'step-job'}
        line_number='12'
        tuples=self._storage_proxy.read_files('variables/covid19-datahub-sample-tuples.json').get()
        std_tuples=copy.deepcopy(tuples)
        variables=self._variables_proxy.get_variables().get()
        list_issues=[]
        list_ref=[]
        refsalias=[]
        refs_alias={}
        refs_values={}

        type_translate=['referential_alias']
        type_validate=['referential', 'referential_alias']
        
        for var in tuples['tuples']:
            std_var=copy.deepcopy(var)
            for var_name in var['attrs'].keys():
                if var_name not in refs_values and variables[var_name]['type'] in type_validate:
                    referential=self._variables_proxy.get_referential(var_name).get()
                    refs_values[var_name]=set([x['attr'] for x in referential])
                if var_name not in refs_alias and variables[var_name]['type'] in type_translate: 
                    alias=self._variables_proxy.get_referential(var_name).get() 
                    code=variables[var_name]['linked_attributes'][0]
                    refs_alias[var_name] = dict([(x['attr'],x['attrs'][code]) for x in alias])
                var_value=var['attrs'][var_name]

                if var_value in refs_values[var_name]: 
                    if var_name in refs_alias : 
                        std_var['attrs'].pop(var_name)
                        std_var['attrs'][code]=refs_alias[var_name][var_value]
                        
                        std_tuples['tuples'].append(std_var)
                        del std_tuples['tuples'][std_tuples['tuples'].index(var)]
                else:
                    message=(f"Code {var_value} does not exist in referential '{var_name}'. Line {line_number} in file {tuples['scope']['file_name']} (source: '{tuples['scope']['source']}').")
                    issue={ job['step'], 
                            line_number, 
                            tuples['scope']['source'], 
                            tuples['scope']['file_name'], 
                            message, 
                            datetime.now(), 
                            job['job_id'], 
                            "ref-not-found"}
                    list_issues.append(issue)
            else:
                None
        for var in std_tuples['tuples']:
            var['attrs']['source']=tuples['scope']['source']
            var['attrs']['file_name']=tuples['scope']['file_name']
            var['attrs']['sent_on']=tuples['scope']['sent_on']
            var['attrs']['sent_by']=tuples['scope']['sent_by']
            for global_var in std_tuples['scope']['globals']:
                pass
                #var['attrs']['global_var['variables']']=global_var
                #print (global_var)

        if not(list_issues): 
            return std_tuples
        else: 
            return list_issues
