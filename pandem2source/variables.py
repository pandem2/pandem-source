from . import worker
import os
from . import util 
import itertools
import json
import datetime
import numpy
from collections import defaultdict
from .util import JsonEncoder
import logging as l

class Variables(worker.Worker):
    def __init__(self, name, orchestrator_ref, settings): 
        super().__init__(name = name, orchestrator_ref = orchestrator_ref, settings = settings)

    def on_start(self):
        super().on_start()
        self._storage_proxy=self._orchestrator_proxy.get_actor('storage').get().proxy()
        self._pipeline_proxy=self._orchestrator_proxy.get_actor('pipeline').get().proxy()
        self._variables = None

    def get_variables(self):
        if self._variables is None:
          dic_variables = dict()
          var_list=self._storage_proxy.read_file('variables/variables.json').get()
          for var in var_list: 
              if not var["base_variable"]:            
                  base_dict = var.copy()
                  base_dict["aliases"] = []
                  base_dict.pop("base_variable")
                  aliases = [{"alias":v['variable'], 
                              "variable": v['base_variable'],
                              "formula": v['formula'],
                              "modifiers": v['modifiers']}
                              for v in var_list if v['base_variable']==var['variable']]
                  
                  base_dict["aliases"] = aliases
                  dic_variables[var['variable']] = base_dict
                  if aliases:
                      for alias in aliases:
                          alias_dict = base_dict.copy()
                          alias_dict['formula'] = alias['formula'] if alias['formula'] is not None else None
                          alias_dict['modifiers'] = alias['modifiers']
                          dic_variables[alias['alias']] = alias_dict
          self._variables = dic_variables
        return self._variables

    def get_referential(self,variable_name):
        #print(f"getting {variable_name}")
        list_files=[]
        referentiel=[]
        path=os.path.join(os.getenv('PANDEM_HOME'), 'files/variables/', variable_name)
        if os.path.isdir(path):
            list_files=self._storage_proxy.list_files(path).get()
            for file in list_files:
                var_list=self._storage_proxy.read_file(file['path']).get()
                if type(var_list) == dict:
                    for var in var_list['tuples']:
                        referentiel.append(var)
        else: 
            return None
        return referentiel


    def read_variable(self,variable_name, filter = {}):
        # l.debug(f"requesting {variable_name} with filter = {filter}" )       
        dir_path = util.pandem_path('files/variables/', variable_name)
        variables = self.get_variables()
        if os.path.isdir(dir_path):
            for mod in variables[variable_name]["modifiers"]:
              if mod['variable'] in filter:
                filter = filter.copy()
                filter[mod['variable']] = {mod['value']}
            requested_list = []
            list_files = self._storage_proxy.list_files_abs(dir_path).get()
            target_files = []
            for file in list_files:
                file_name = file['name']
                if file_name == 'default.json':
                    target_files.append(file)
                else:
                    can_ignore = False
                    for var_val in file_name.split('.')[:-1]:
                        var = var_val.split('=')[0]
                        val = var_val.split('=')[1]
                        if var in filter and type(filter[var]) in [list, set] and val not in filter[var]:
                            can_ignore = True
                            break
                        elif var in filter and type(filter[var]) not in [list, set] and filter[var] != val:
                            can_ignore = True
                            break
                    if not can_ignore:
                        target_files.append(file)
            for file in target_files:
                var_tuples = self._storage_proxy.read_file(file['path']).get()['tuples']
                requested_vars = []
                for var_tuple in var_tuples:
                    if all(att in var_tuple['attrs'].keys() for att in filter.keys()):
                        if all((val is None or var_tuple['attrs'][att]==val or var_tuple['attrs'][att] in val) for att, val in filter.items()) :
                            requested_vars.append(var_tuple)
                requested_list.extend(requested_vars)
            return requested_list
        else: 
            return None

    

    
    def get_partition(self, tuple, partition):
        if partition is None:
          return 'default.json'
        else:
          return '.'.join([key + '=' + str(val) for key, val in tuple['attrs'].items() if key in partition]) + '.json'

    def remove_private(self, tuples, variables):
        private_attrs = {k for k, v in variables.items() if v["type"] == "private"}
        for t in tuples:
          if "attrs" in t:
            if t["attrs"].keys().isdisjoint(private_attrs):
              yield t
            else:
              tt = t.copy()
              tt["attrs"] = {k:v for k,v in t["attrs"].items() if k not in private_attrs}
              yield tt
 
    def write_variable(self, input_tuples, step, job):
        variables = self.get_variables()
        partition_dict = defaultdict(list)
        if step ==0:
            self.write_variable(self.tag_source_var(job["dls_json"]), None, job)

        if input_tuples['tuples']:
            for tuple in self.remove_private(input_tuples['tuples'], variables):
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
                        # this is for avoiding having duplicates on referentials (attr instead of obs)
                        # TODO: Improve this using the variable type
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
                        #tuple_list = []
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

        if step is not None:
            self._pipeline_proxy.publish_end(job = job)

        
                    
    def tag_source_var(self, dls):
       tags = dls['scope']['tags'] if 'tags' in dls['scope'] else []
       source = dls['scope']['source']
       return {
         'scope':{
           'update_scope':[
             {'variable':'source', 'value':source}
           ]
         },
         'tuples':[*({'attr':{'tag_source': tag+'_'+source}, 'attrs':{'tag':tag, 'source': source}} for tag in tags)]
       }

    def lookup(self, variables, combinations, source, filter = None, include_source = True, include_tag = False, types=None):
      dico_vars  = self.get_variables()
      modifiers = {v:{m['variable']:m['value'] for m in vardef['modifiers']} for v, vardef in dico_vars.items() if len(dico_vars[v]["modifiers"]) > 0}
      tag_source = self.read_variable("tag_source")
      tags = {t['attrs']['tag'] for t in tag_source if t['attrs']['source'] == source}
      others = {t['attrs']['source'] for t in tag_source if t['attrs']['tag'] in tags and t['attrs']['source'] != source}

      attrs = set()
      filt = {}
      if "attrs" in combinations:
        indexed_comb = {}
        for comb in combinations:
          if "attrs" in comb:
            keys = sorted(comb["attrs"].keys())
            attrs.update(keys)
            key = tuple((k, comb[k]) for k in keys if types is None or dico_vars[k]["type"] in types)
            for k, v in key.items():
              if k not in filt:
                filt[k] = {}
              filt[k].add(v)
            indexed_comb[key] = comb["obs"] if "obs" in comb else {}
      else:
        indexed_comb = combinations
        for key in indexed_comb:
          for k, v in key:
            if k not in filt:
              filt[k] = set()
            filt[k].add(v)
          if types is None:
            attrs.update(k for k, v in key)

      if types is None:
        types = {dico_vars[t]["type"] for t in attrs}
      if filter is not None:
        filt.update(filter)
      res = {}
      
      for var in variables:
        tuples = []
        if include_source:
          f = {k:v for k, v in filt.items() if v is not None}
          f.update({"source":source})
          # TODO make read variable work with alias variables so it can return active cases and confirmed cases on the same query
          tuples = self.read_variable(var, filter = f)
        if include_tag and (tuples is None or len(tuples) == 0):
          f = {k:v for k, v in filt.items() if v is not None}
          f.update({"source":others})
          tuples = self.read_variable(var, filter = f)
        # if currnet variable has modifiers we have to find the matching index for the tuple
        key_map = {tuple((k, (v if var not in modifiers or not k in modifiers[var] else modifiers[var][k])) for k, v in comb):comb for comb in combinations}
        if tuples is not None:
          for t in tuples:
            keys = sorted(attr for attr in t["attrs"].keys() if dico_vars[attr]["type"] in types)
            key = key_map[tuple((k, t["attrs"][k]) for k in keys)]
            filter_value = {k:v for k, v in t["attrs"].items() if k in filter}  
            if key in indexed_comb:
              if not key in res:
                res[key] = {}
              if "obs" in t:
                obs_name =  next(iter(t["obs"].keys()))
                if not obs_name in res[key]:
                  res[key][obs_name] = []
                res[key][obs_name].append({"value":t["obs"][obs_name], "attrs":filter_value})
      return res
      











