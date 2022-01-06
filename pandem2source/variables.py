from . import worker
import os
from . import util 
import itertools
import json
import datetime
import numpy
from collections import defaultdict

class Variables(worker.Worker):
    def __init__(self, name, orchestrator_ref, settings): 
        super().__init__(name = name, orchestrator_ref = orchestrator_ref, settings = settings)

    def on_start(self):
        super().on_start()
        self._storage_proxy=self._orchestrator_proxy.get_actor('storage').get().proxy()
        self._pipeline_proxy=self._orchestrator_proxy.get_actor('pipeline').get().proxy()

    # def get_variables(self): 
    #     dic_variables = dict()
    #     var_list=self._storage_proxy.read_file('variables/variables.json').get()
    #     for var in var_list: 
    #         dic_variables[var['variable']]=var.copy()
    #         if 'aliases' in var :
    #             for alias in var['aliases']:
    #                 alias_var=var.copy()
    #                 #if "alias" in alias:
    #                 #  alias_var['variable']=alias['alias']
    #                 if "modifiers" in alias:
    #                   alias_var['modifiers']=alias['modifiers']
    #                 if "alias" in alias:
    #                   dic_variables[alias['alias']]=alias_var
    #     return dic_variables
    
    def get_variables(self): 
        dic_variables = dict()
        var_list=self._storage_proxy.read_file('variables/variables.json').get()
        for var in var_list: 
            if not var["base_variable"]:            
                base_dict = var.copy()
                base_dict["aliases"] = []
                base_dict.pop("modifiers")
                base_dict.pop("base_variable")
                aliases = [{"alias":v['variable'], 
                            "variable": v['base_variable'],
                            "modifiers": v['modifiers']}
                            for v in var_list if v['base_variable']==var['variable']]
                
                base_dict["aliases"] = aliases
                dic_variables[var['variable']] = base_dict
                if aliases:
                    for alias in aliases:
                        alias_dict = base_dict.copy()
                        if "modifiers" in alias:
                            alias_dict['modifiers'] = alias['modifiers']
                        dic_variables[alias['alias']] = alias_dict
        return dic_variables

    def get_referential(self,variable_name):
        #print(f"getting {variable_name}")
        list_files=[]
        referentiel=[]
        path=os.path.join(os.getenv('PANDEM_HOME'), 'files/variables/', variable_name)
        if os.path.isdir(path):
            list_files=self._storage_proxy.list_files(path).get()
            for file in list_files:
                var_list=self._storage_proxy.read_file(file['path']).get()
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
        if partition is None:
          return 'default.json'
        else:
          return '_'.join([key + '-' + str(val) for key, val in tuple['attrs'].items() if key in partition]) + '.json'


    def write_variable(self, input_tuples, path, job):
        class JsonEncoder(json.JSONEncoder):
          def default(self, z):
            if isinstance(z, datetime.datetime) or isinstance(z, numpy.int64) or isinstance(z, datetime.date):
              return (str(z))
            else:
              return super().default(z)
        variables = self.get_variables()
        partition_dict = defaultdict(list)
        for tuple in input_tuples['tuples']:
            var_name = None
            for key, var in tuple.items():
                if key != "attrs" and len(tuple[key].keys()) > 0:
                    var_name = list(tuple[key].keys())[0]
            if var_name is not None:
              partition_dict[var_name].append(tuple) 
        partition_dict_final = defaultdict(lambda: defaultdict(list))
        for var, tuple_list in partition_dict.items():
            for tuple in tuple_list:
                file_name = self.get_partition(tuple, variables[var]["partition"])
                partition_dict_final[var][file_name].append(tuple)
        update_filter = []
        for filter in input_tuples['scope']['update_scope']:
            if not isinstance(filter['value'], list):
                update_filter.append({'variable':filter['variable'], 'value':[str(filter['value'])]})
            else:
                update_filter.append({'variable':filter["variable"], 'value':list([str(f) for f in filter["value"]])})
        for var, tuples_dict in partition_dict_final.items():
            var_dir = util.pandem_path('files/variables', var)
            if not os.path.exists(var_dir):
                os.makedirs(var_dir)
            for file_name, tuples_list in tuples_dict.items():
                if 'attr' in tuples_list[0]:
                    unique_values = {}
                    for index, tuple in enumerate(tuples_list):
                        if tuple['attr'][var] not in unique_values.values():
                            unique_values[index] = tuple['attr'][var]
                    tuples_list = [tuples_list[key] for key in unique_values.keys()]

               
                file_path = util.pandem_path(var_dir, file_name)
                if not os.path.exists(file_path):
                    tuples_to_dump = {'tuples': tuples_list}
                    
                    with open(file_path, 'w+') as f:
                        json.dump(tuples_to_dump, f, cls=JsonEncoder, indent = 4)
                else:
                    with open(file_path, 'r') as f:
                        last_tuples = json.load(f)
                    tuple_list = []
                    tuples_to_dump = {'tuples': tuples_list}
                    for tup in last_tuples['tuples']:
                        cond_count = len(update_filter)
                        for filt in update_filter: 
                            if filt['variable'] in tup['attrs'].keys() and tup['attrs'][filt['variable']] in filt['value']:
                                cond_count = cond_count - 1
                        if cond_count > 0:
                            tuples_list.append(tup)            
                    with open(file_path, 'w') as f:
                        json.dump(tuples_to_dump, f, cls=JsonEncoder, indent = 4)

        self._pipeline_proxy.publish_end( path = path, job = job)
                    













