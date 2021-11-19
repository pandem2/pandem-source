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
        """
            IN:         tuples_to_validate and object job
            ACTIONS:    check the code and updates with a code if the values are in a ref
            OUT:        if issues not null list of issue
                        else list of tuples standardize
        """
        #Initialisation
        job={'job_id':123, 'step':'step-job'}
        line_number='12'
        tuples=self._storage_proxy.read_files('variables/covid19-datahub-sample-tuples.json').get()
        std_tuples={'scope':{}, 'tuples':[]}
        std_var={}
        variables=self._variables_proxy.get_variables().get()
        list_issues=[]
        list_ref=[]
        global_tuple={}
        update_tuple={}
        refs_alias={}
        refs_values={}

        type_translate=['referential_alias']
        type_validate=['referential', 'referential_alias']
        
        for i in range(-2, len(tuples['tuples'])):
            #retrieves the globals variable
            if i == -2: 
                std_var['attrs']=dict([(x['variable'],x['value']) for x in tuples['scope']['globals']])
            #retrieves the update variable
            elif i == -1: 
                std_var['attrs']=dict([(x['variable'],x['value']) for x in tuples['scope']['update_scope']])
            #retrieves the tupple
            else:
                std_var=copy.deepcopy(tuples['tuples'][i])


            for var_name in std_var['attrs'].copy().keys():
                #retrieves the referentiel
                if var_name not in refs_values and variables[var_name]['type'] in type_validate:
                    referential=self._variables_proxy.get_referential(var_name).get()
                    refs_values[var_name]=set([x['attr'] for x in referential])
                if var_name not in refs_alias and variables[var_name]['type'] in type_translate: 
                    alias=self._variables_proxy.get_referential(var_name).get() 
                    code=variables[var_name]['linked_attributes'][0]
                    refs_alias[var_name] = dict([(x['attr'],x['attrs'][code]) for x in alias])
                var_value=std_var['attrs'][var_name]

                #variable type is referentiel_translate
                if variables[var_name]['type'] in type_translate:
                    if var_value in refs_values[var_name]:
                        if var_name in refs_alias : 
                            std_var['attrs'].pop(var_name)
                            std_var['attrs'][code]=refs_alias[var_name][var_value]

                else:
                    #Create a issue
                    message=(f"Code {var_value} does not exist in referential '{var_name}'. Line {line_number} in file {tuples['scope']['file_name']} (source: '{tuples['scope']['source']}').")
                    issue={ job['step'], 
                            line_number, 
                            tuples['scope']['source'], 
                            tuples['scope']['file_name'], 
                            message, 
                            datetime.now().isoformat(timespec='minutes'), 
                            job['job_id'], 
                            "ref-not-found"}
                    list_issues.append(issue)

            if i == -2: 
                global_tuple=std_var
            elif i == -1: 
                update_tuple=std_var
            else:
                for cle, value in global_tuple['attrs'].items():
                    std_var['attrs'][cle]=value
                std_tuples['tuples'].append(std_var)

        std_tuples['scope']=tuples['scope']['update_scope']

        if not(list_issues): 
            return std_tuples
        else: 
            return list_issues
