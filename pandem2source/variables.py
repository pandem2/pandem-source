from . import worker
import os


class Variables(worker.Worker):
    def __init__(self, name, orchestrator_ref, settings): 
        super().__init__(name = name, orchestrator_ref = orchestrator_ref, settings = settings)
        self._orchestrator_proxy = orchestrator_ref.proxy()

    def on_start(self):
        super().on_start()
        self._storage_proxy=self._orchestrator_proxy.get_actor('storage').get().proxy()

    def get_variables(self): 
        dic_variables = dict()
        var_list=self._storage_proxy.read_files('variables/variables.json').get()
        for var in var_list: 
            dic_variables[var['variable']]=var
            if 'aliases' in var :
                for alias in var['aliases']:
                    alias_var=var.copy()
                    if "alias" in alias:
                      alias_var['variable']=alias['alias']
                    if "modifiers" in alias:
                      alias_var['modifiers']=alias['modifiers']
                    if "alias" in alias:
                      dic_variables[alias['alias']]=alias_var
        return dic_variables

    def get_referential(self,variable_name):
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


