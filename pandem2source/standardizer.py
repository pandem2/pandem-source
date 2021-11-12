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
    def standardize(self,file_name, job_id):
        #tuples_to_validate = tuples
        #file_name = nom du référentiel à recupérer
        #job_id = nom de l'attribu à tester

        tuples=self._storage_proxy.read_files('variables/covid19-datahub-sample-tuples.json').get()
        referential=self.get_referential(file_name)
        list_ref=[]

        for ref in referential: 
            if ref['attr'] not in list_ref:
                list_ref.append(ref['attr'])
            else:
                pass

        for var in tuples['tuples']: 
            one_tuple=var['attrs'][job_id]
            if one_tuple in list_ref: 
                print(one_tuple,'existe')
            else:
                #Création d'un objet issue
                print('la maladie ', one_tuple, 'nexiste pas')
        
        return None