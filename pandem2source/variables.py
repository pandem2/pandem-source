from . import worker
import os
from . import util 
import itertools
import json
from collections import defaultdict

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


    def read_variable(self,variable_name, filter):
        dir_path = util.pandem_path('files/variables/', variable_name)
        if os.path.isdir(dir_path):
            requested_list = []
            list_files = self._storage_proxy.list_files_abs(dir_path).get()
            for file in list_files:
                var_tuples = self._storage_proxy.read_file(file['path']).get()['tuples']
                requested_vars = [var_tuple for var_tuple in var_tuples \
                                     if all(att in var_tuple['attrs'] and any(val in var_tuple['attrs'][att] for val in filter[att]) \
                                            for att in filter)]
            requested_list.extend(requested_vars)
            return requested_list
        else: 
            return None

    
    def get_partition(self, tuple, partition):
        return '_'.join([key + '-' + val for key, val in tuple['attrs'].items() if key in partition]) + '.json'


        
    def write_variable(self, input_tuples, partition):
        partition_dict = defaultdict(list)
        for tuple in input_tuples['tuples']:
            file_name = self.get_partition(tuple, partition)
            partition_dict[file_name].append(tuple)
        update_filter = []
        for filter in input_tuples['scope']['update_scope']:
            if not isinstance(filter['value'], list):
                update_filter.append({'variable':filter['variable'], 'value':[filter['value']]})
            else:
                update_filter.append(filter)
        for file, tuples in partition_dict.items():
            #var_dir: same partition file in two variables directories?
            file_path = util.pandem_path('files/variables', file)
            if not os.path.exists(file_path):
                tuples_to_dump = {'tuples': tuples}
                with open(file_path, 'w+') as f:
                    json.dump(tuples_to_dump, f)
            else:
                with open(file_path, 'r') as f:
                    last_tuples = json.load(f)
                for tup in last_tuples['tuples']:
                    cond_count = len(update_filter)
                    for filt in update_filter: 
                        if filt['variable'] in tup['attrs'].keys() and tup['attrs'][filt['variable']] in filt['value']:
                            cond_count = cond_count - 1
                    if cond_count > 0:
                        print(tup)
                        tuples.append(tup)            
                tuples_to_dump = {'tuples': tuples}
                with open(file_path, 'w') as f:
                    json.dump(tuples_to_dump, f)

                    













