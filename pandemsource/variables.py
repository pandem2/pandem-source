from . import worker
import os
from . import util 
from .storage import CacheValue
from . import admin
import itertools
import json
import datetime
import numpy
from collections import defaultdict
from . import util
from .util import JsonEncoder
from .util import printMem
import logging
import pickle
import copy

l = logging.getLogger("pandem.variable")

class Variables(worker.Worker):
    def __init__(self, name, orchestrator_ref, settings): 
        super().__init__(name = name, orchestrator_ref = orchestrator_ref, settings = settings)

    def on_start(self):
        super().on_start()
        self._storage_proxy=self._orchestrator_proxy.get_actor('storage').get().proxy()
        self._pipeline_proxy=self._orchestrator_proxy.get_actor('pipeline').get().proxy()
        self._variables = None
        self._timeseries = None
        self._timeseries_outdated = False
        self.timeseries_hash = ''

    def get_variables(self):
        if self._variables is None:
          dic_variables = dict()
          var_list=admin.read_variables_definitions()
          for var in var_list: 
              if not var["base_variable"]:            
                  base_dict = var.copy()
                  base_dict["aliases"] = []
                  base_dict.pop("base_variable")
                  aliases = [{"alias":v['variable'], 
                              "variable": v['base_variable'],
                              "formula": v['formula'],
                              "modifiers": v['modifiers'],
                              "description": v['description'],
                              "no_report":v['no_report'],
                              "synthetic_tag":v['synthetic_tag'],
                              "synthetic_blocker":v['synthetic_blocker'],
                              "type": v['type']
                              }
                              for v in var_list if v['base_variable']==var['variable']]
                  
                  base_dict["aliases"] = aliases
                  dic_variables[var['variable']] = base_dict
                  if aliases:
                      for alias in aliases:
                          alias_dict = base_dict.copy()
                          alias_dict['formula'] = alias['formula'] if alias['formula'] is not None else None #base_dict['formula']
                          alias_dict['modifiers'] = alias['modifiers']
                          alias_dict['no_report'] = alias['no_report']
                          alias_dict['synthetic_tag'] = alias['synthetic_tag']
                          alias_dict['synthetic_blocker'] = alias['synthetic_blocker']
                          alias_dict['description'] = alias['description'] if alias['description'] is not None else alias_dict['description']
                          alias_dict['type'] = alias['type'] if alias['type'] is not None else alias_dict['type']
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
        #l.debug(f"requesting {variable_name} with filters in = {filter.keys()}" )       
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
                elif file_name.endswith(".json"):
                    can_ignore = False
                    for var_val in file_name.split('.')[:-1]:
                        var = var_val.split('=')[0]
                        val = var_val.split('=')[1]
                        if val == "@None@" and var in filter and filter[var] is not None:
                            can_ignore = True
                            break
                        elif var in filter and type(filter[var]) in [list, set] and val not in filter[var]:
                            can_ignore = True
                            break
                        elif var in filter and type(filter[var]) not in [list, set] and filter[var] != val:
                            can_ignore = True
                            break
                    if not can_ignore:
                        target_files.append(file)
            for file in target_files:
                try:
                  var_tuples = self._storage_proxy.read_file(file['path']).get()['tuples']
                  #l.debug(f"Opening {file['path']}")
                except Exception as e:
                  l.error(f"Error found while reading file {file['path']}")
                  raise e
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
          #return '.'.join([key + '=' + str(val) for key, val in tuple['attrs'].items() if key in partition]) + '.json'
          return '.'.join([key + '=' + str(tuple['attrs'][key] if key in tuple['attrs'] and tuple['attrs'][key] is not None else '@None@') for key in partition]) + '.json'

    def remove_attrs(self, t, private_attrs):
        if "attrs" in t:
          if t["attrs"].keys().isdisjoint(private_attrs):
            return t
          else:
            tt = t.copy()
            tt["attrs"] = {k:v for k,v in t["attrs"].items() if k not in private_attrs}
            return tt

    def apply_aliases(self, t, aliases):
        applied = False
        if "obs" not in t or t["obs"].keys().isdisjoint(aliases.keys()):
          return t
        else:
          for var, value in t["obs"].items():
              tt = copy.deepcopy(t)
              if "attrs" not in tt:
                 tt["attrs"] = {}
              # renaming the observation to its base variable
              if var != aliases[var]["variable"]:
                tt["obs"][aliases[var]["variable"]] = tt["obs"].pop(var)
              
              # applyting modifiers
              for modifier in aliases[var]["modifiers"]:
                tt["attrs"][modifier['variable']] = modifier['value']
          return tt

    def write_variable(self, input_tuples, step, delete_stamp, job):
        # if input_tuples is in cache get its value
        if type(input_tuples) == CacheValue:
          input_tuples = input_tuples.value()

        variables = self.get_variables()
        partition_dict = defaultdict(list)
        if step ==0:
            self.write_variable(self.tag_source_var(job["dls_json"]), None, None, job)

        if input_tuples['tuples']:
            # building a dictionary with destination files for tuples depending on variable name at their partitioning schema
            private_attrs = {k for k, v in variables.items() if v["type"] == "private"}
            aliases = {v:vardic for v, vardic in variables.items() if "modifiers" in vardic and len(vardic["modifiers"]) > 0}
            for t in input_tuples['tuples']:
                # removing private attributes
                t = self.remove_attrs(t, private_attrs)
                # applying modifiers if tuple pbservation contains modifiers
                t = self.apply_aliases(t, aliases)
                # associating this tuple tuple to its destination variable
                var_name = None
                for key, var in t.items():
                    if key != "attrs" and len(t[key].keys()) > 0:
                        var_name = list(t[key].keys())[0]
                if var_name is not None:
                  partition_dict[var_name].append(t)

            # building the dict tuples per destination files
            partition_dict_final = defaultdict(lambda: defaultdict(list))
            for var, tuple_list in partition_dict.items():
                for t in tuple_list:
                    file_name = self.get_partition(t, variables[var]["partition"])
                    partition_dict_final[var][file_name].append(t)
            
            # identifying the scope of data that will be replaced by current tuples
            update_filter = []
            for filter in input_tuples['scope']['update_scope']:
                if not isinstance(filter['value'], list) and not isinstance(filter['value'], set):
                    update_filter.append({'variable':filter['variable'], 'value':[json.dumps(filter["value"], cls=JsonEncoder).replace("\"", "")]})
                else:
                    update_filter.append({'variable':filter["variable"], 'value':list([json.dumps(f, cls=JsonEncoder).replace("\"", "") for f in filter["value"]])})
                        
            # Iterating on each variable to write
            for var, tuples_dict in partition_dict_final.items():
                var_dir = util.pandem_path('files/variables', var)
                if not os.path.exists(var_dir):
                    os.makedirs(var_dir)
                # Iterating on each file to write
                for file_name, tuples_list in tuples_dict.items():
                    if 'attr' in tuples_list[0]:
                        # this is for avoiding having duplicates on referentials (attr instead of obs)
                        # TODO: Improve this using the variable type
                        unique_values = {}
                        for index, t in enumerate(tuples_list):
                            if t['attr'][var] not in unique_values.values():
                                unique_values[index] = t['attr'][var]
                        tuples_list = [tuples_list[key] for key in unique_values.keys()]

                   
                    file_path = util.pandem_path(var_dir, file_name)
                    # If file does not exists then we can just dump the tuples (no need to delete) 
                    tuples_to_dump = {'tuples': tuples_list}
                    if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
                        util.save_json(tuples_to_dump, file_path, indent = 4)
                    # If step = 0 and file exists already then we need to remove lines on the replacement scope
                    # We do that by appending to tuples_list any existing row not in the replacement scope and then overriding the file 
                    else:
                        with open(file_path, 'r') as f:
                            try:
                              last_tuples = json.load(f)
                            except:
                              raise ValueError(f"Error while interpreting file {f} as JSON")
                        delete_old = delete_stamp is None or os.path.getmtime(file_path) < delete_stamp
                        for tup in last_tuples['tuples']:
                            cond_count = len(update_filter)
                            if delete_old:
                              for filt in update_filter: 
                                if filt['variable'] in tup['attrs'].keys() and tup['attrs'][filt['variable']] in filt['value']:
                                    cond_count = cond_count - 1
                              # addding the tuple if one of the conditions failed
                              if cond_count > 0:
                                tuples_list.append(tup)
                            else:
                                tuples_list.append(tup)
                        # replacing the file
                        util.save_json(tuples_to_dump, file_path, indent = 4)
                   
                    # TODO: get deleted files and remove from time series before adding new ones
                    # Now that we have updated the file we can update the time series cache
                    self.get_timeseries(tuples=tuples_to_dump['tuples'], tuples_path=file_path, save_changes = False)

        # saving changes done on the time series cache
        self.get_timeseries(save_changes = True)
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

    def lookup(self, variables, combinations, source, filter = None, include_source = True, include_tag = False, types=['referential', 'geo_referential', 'characteristic']):
      # doing recursive calls if the combination includes different granularities
      granularities = {tuple(k for k, v in t) for t in combinations}
      if len(granularities) > 1:
        ret = {}
        for gr in granularities:
          combs = [t for t in combinations if tuple(k for k, v in t) == gr]
          ret.update(self.lookup(variables, combs, source, filter, include_source, include_tag, types))
        return ret
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
        vfilt = filt.copy()
        tuples = []
        base_var = dico_vars[var]["variable"]
        # making this query work with aliases variables
        if base_var in dico_vars:
          if var in modifiers:
            vfilt.update(modifiers[var])
        

        tries = 2
        found = False
        while tries > 0 and not found:
            if tries == 2 and include_source:
              f = {k:v for k, v in vfilt.items() if v is not None}
              f.update({"source":source})
              tuples = self.read_variable(base_var, filter = f)
            if tries == 1 and include_tag:
              f = {k:v for k, v in vfilt.items() if v is not None}
              f.update({"source":others})
              tuples = self.read_variable(base_var, filter = f)
            # key_map will contain the non modified attributes for each combination as keys and the original combination as value
            key_map = {}
            for comb in indexed_comb:
              freekey = tuple((k, v) for k, v in comb if var not in modifiers or k not in modifiers[var])
              if freekey not in key_map:
                key_map[freekey] = set([comb])
              else:
                key_map[freekey].add(comb)

            if tuples is not None:
              for t in tuples:
                # file_keys are the non null attrs from in the tuples to be considered as time series keys
                file_keys = sorted(
                  attr for attr in t["attrs"].keys() 
                    if attr in dico_vars and 
                      dico_vars[attr]["type"] in types 
                      and (t["attrs"][attr] is not None)
                      and dico_vars[attr]["linked_attributes"] is None
                  ) # or (var in modifiers and attr in modifiers[var])
                # free_keys are the keys in the file which are not modified
                free_keys = list(attr for attr in file_keys if not var in modifiers or attr not in modifiers[var])
                free_key = tuple((k, t["attrs"][k]) for k in free_keys)
                        
                if free_key in key_map:
                  for key in key_map[free_key]: 
                    filter_value = {k:v for k, v in t["attrs"].items() if k in (filter if filter is not None else [])}  
                    if key in indexed_comb:
                      found = True
                      if key not in res:
                        res[key] = {}
                      if "obs" in t:
                        obs_name =  next(iter(t["obs"].keys()))
                        if not obs_name in res[key]:
                          res[key][obs_name] = []
                        res[key][obs_name].append({"value":t["obs"][obs_name], "attrs":filter_value})
            tries = tries - 1
            tuples = None
      return res
      

    def get_timeseries(self, tuples=None, tuples_path=None, save_changes = True):
      var_dic = self.get_variables()
      storage_proxy  = self._storage_proxy

      if tuples is not None and tuples_path is None:
        raise ValueError("If tuples is None, tuple path is needed in order to register the path new tuples")
      if tuples is None and tuples_path is not None:
        tuples = storage_proxy.read_file(util.pandem_path(tuples_path)).get()['tuples']
      
      cache_path = util.pandem_path('files', 'variables', 'time_series.pi')
      
      # restoring current time series from memory or disk
      if self._timeseries is not None:
        cache = self._timeseries
      elif storage_proxy.exists(cache_path).get():
        with open(cache_path, 'rb') as cf:
          cache = pickle.load(cf)
        self._timeseries = cache
      else:
        # If there is no currently any cahe we have to build it
        # by reading all published variables 
        cache = {}
        self._timeseries_outdated = True
        self._timeseries = cache
        for var, varinfo in var_dic.items(): 
          if varinfo["type"] in ["observation", "indicator", "resource"] and varinfo["variable"] == var:
            var_path = util.pandem_path('files', 'variables', var)
            if storage_proxy.exists(var_path).get():
               l.debug(f'Producing timeseries cache for {var}')
               for finfo in storage_proxy.list_files(util.pandem_path('files', 'variables', var_path)).get():
                 t = storage_proxy.read_file(finfo["path"]).get()
                 if type(t) == dict:
                   self.get_timeseries(tuples = t['tuples'], tuples_path = finfo['path'], save_changes = False)
      
      # If no new tuples no analyze return cache 
      if tuples is not None:
          for t in tuples:
            if "obs" in t and len(t["obs"]) > 0 and "attrs" in t:
              var_name = next(iter(t["obs"]))
              if var_name in var_dic and "aliases" in var_dic[var_name]:
                obs_name = var_name
                
                # Fitting an alias if possible
                biggest_alias = 0
                for alias in var_dic[var_name]["aliases"]:
                  if len(alias['modifiers']) > biggest_alias and not alias['no_report']:
                    if all(m['value'] is None or (m['variable'] in t['attrs'] and t['attrs'][m['variable']] == m['value']) for m in alias['modifiers']):
                      obs_name = alias['alias']
                      biggest_alias = len(alias['modifiers'])
                # getting tuple key
                key = []
                key.append(("indicator", obs_name))
                to_ignore = ['not_characteristic', 'date']
                key.extend([(k, v) for k,v in t["attrs"].items() if v is not None and k in var_dic and var_dic[k]["type"] not in to_ignore and var_dic[k]["linked_attributes"] is None or k == "source"])
                key.sort(key = lambda p:p[0])
                key = tuple(key)
          
                # getting dates
                dates = [str(v) for k,v in t["attrs"].items() if k in var_dic and var_dic[k]["type"] == "date"]
                if len(dates)>0:
                  min_date = min(dates)
                  max_date = max(dates)
                else:
                  min_date = None
                  max_date = None
                
                tuples_relpath = os.path.relpath(tuples_path, util.pandem_path())
                # storing the series key on the cache with its statistics (min date, max date and files)
                if key not in cache:
                  self._timeseries_outdated = True
                  cache[key] = {"paths":{tuples_relpath}, "min_date":min_date, "max_date":max_date}
                else:
                  if tuples_relpath not in cache[key]["paths"]:
                    cache[key]["paths"] = cache[key]["paths"].union({tuples_relpath})
                    self._timeseries_outdated = True
                  if min_date is not None and min_date != cache[key]["min_date"]:
                    cache[key]["min_date"] = min(min_date, cache[key]["min_date"]) if cache[key]["min_date"] is not None else min_date
                    self._timeseries_outdated = True
                  if max_date is not None and max_date != cache[key]["max_date"]:
                    cache[key]["max_date"] = max(max_date, cache[key]["max_date"]) if cache[key]["max_date"] is not None else max_date
                    self._timeseries_outdated = True

      # Saving changes to picke if any change found and save_changes is requested
      if self._timeseries_outdated and save_changes:
        util.save_pickle(cache, cache_path)
        
        self._timeseries_outdated = False
        self.timeseries_hash = str(datetime.datetime.now())
      return cache  
