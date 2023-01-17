import time
from . import worker
from abc import ABC, abstractmethod, ABCMeta
import numpy as np
import copy
import json
from .util import JsonEncoder
from . import util 
import logging
import os
import pickle
import base64

l = logging.getLogger("pandem.aggregator")

class Aggregator(worker.Worker):
  __metaclass__ = ABCMeta  
  def __init__(self, name, orchestrator_ref, settings): 
      super().__init__(name = name, orchestrator_ref = orchestrator_ref, settings = settings)    

  def on_start(self):
      super().on_start()
      self._storage_proxy = self._orchestrator_proxy.get_actor('storage').get().proxy()
      self._pipeline_proxy = self._orchestrator_proxy.get_actor('pipeline').get().proxy() 
      self._variables_proxy = self._orchestrator_proxy.get_actor('variables').get().proxy()

  def aggregate(self, list_of_tuples, job):
    list_of_tuples = self.distribute_tuples_by_partition(list_of_tuples, job["id"])

    variables = self._variables_proxy.get_variables().get()
    geos = {var["variable"] for var in variables.values() if var["type"] == "geo_referential"}
    geo_parents = {var["linked_attributes"][0]:var["variable"] for var in variables.values() if var["type"] == "referential_parent" and var["linked_attributes"] is not None and var["linked_attributes"][0] in geos}

    tuples = self._variables_proxy.get_referential("geo_code").get()

    var_asc = {code:self.rel_ascendants(self.descendants(code, parent)) for code, parent in geo_parents.items()}
    nparts = len(list_of_tuples)
    ndone = 0

    # Iterating over groups of tupeles stored in cache
    for var, var_tuples in list_of_tuples.items():
      # getting vartuples value from cache
      var_tuples = var_tuples.value()

      # Analyzing existing tuples of codes and dates and creating necessary data structures 
      existing_codes = {attr:set() for attr in var_asc.keys()}
      existing_dates = set()
      ts_partition = {}
      
      for t in var_tuples['tuples']:
        if "attrs" in t:
          date = None
          # getting existing aggregating codes and dates
          for attr, val in t['attrs'].items():
            if attr in var_asc:
              existing_codes[attr].add(val)
            if attr in variables and variables[attr]["type"]=="date":
              date = val
              existing_dates.add(date)
          # getting time serie keys (for adding missing values)
          if date is not None:
            key = self.tup_key(t, with_obs = True, with_date = False, variables = variables)
            if key not in ts_partition:
              ts_partition[key] = dict()
            ts_partition[key][date]=t
      existing_dates = sorted(existing_dates)
      
      missing_tuples = []
      # Adding missing dates for each time serie and adding closest possible value
      for ts in ts_partition.values():
        for i, date in enumerate(existing_dates):
          if date not in ts:
            # we have here a missing value on a time serie
            # let's find the closest value
            replacement = None
            for j in range(1, max(i, len(existing_dates)-i)):
              if replacement is None:
                k = i - j
                if k >= 0:
                  if existing_dates[k] in ts:
                    replacement = ts[existing_dates[k]]
                k = i + j
                if k < len(existing_dates):
                  if existing_dates[k] in ts:
                    replacement = ts[existing_dates[k]]
            # if replacement is not None we have found a replacement, we need to create a copy
            if replacement is not None:
              c = copy.deepcopy(replacement)
              date_attr = [attr for attr in c["attrs"].keys() if attr in variables and variables[attr]["type"] == "date"][0]
              c["attrs"][date_attr] = date
              missing_tuples.append(c)
      # adding missing tuples 
      var_tuples['tuples'].extend(missing_tuples)
      
      # new codes to be added to the update scope if necessary
      new_codes = {attr:set() for attr in var_asc.keys()}
      
      # Aggregating loop
      # variables for aggregating
      cumm = {}
      untouched = []
      # Iterating over each tuple tuples and adding parent keys
      for t in [t for t in var_tuples['tuples'] if "attr" in t or "obs" in t]:
        aggr_func =  self.tuple_aggregate_function(t, variables)
        if aggr_func is None:
          untouched.append(t)
        else:
          # iterating over keys parents and itself and applying aggregation functio 
          for aggr_key, tt, parent_attr, parent_value in self.pairs_to_aggregate(t, var_asc, variables, existing_codes):
            if not aggr_key in cumm:
              cumm[aggr_key] = tt
            else:
              var_name = list(t["obs"].keys())[0]
              cumm[aggr_key]["obs"][var_name] = aggr_func([cumm[aggr_key]["obs"][var_name], tt["obs"][var_name]])
            # registering new codes that so we can update the update scope
            if parent_attr is not None:
              new_codes[parent_attr].add(parent_value)

      result = var_tuples.copy()
      result['tuples'] = untouched + [*cumm.values()]
      # increasing the update scope if aggregating vaiable is in update scope and new codes were added by aggregation
      for u in result['scope']['update_scope']:
        if u["variable"] in new_codes:
          codes = {*u["value"]}
          codes.update(new_codes[u["variable"]])
          u["value"] = [*codes]
          
      # storing in cache
      ret = self._storage_proxy.to_job_cache(job["id"], f"agg_{var}", result).get()
      ndone = ndone + 1

      # sending the current variables back to the pipeline
      self._pipeline_proxy.aggregate_end(tuples = ret, var = var, progress = ndone/nparts, job = job)

  def descendants(self, code, parent):
    codes = self._variables_proxy.get_referential(code).get()
    if codes is None:
      return None
    rel = {t['attr'][code]:t['attrs'][parent] if parent in t['attrs'] else None for t in codes}
    return self.rel_descendants(rel) 

  def rel_descendants(self, rel, desc = {}):
    if len(desc) == 0:
      desc = {c:{c} for c, p in rel.items()}
    added = True
    while added:
      added = False
      for e, dd in desc.items():
        for c, p in rel.items():
          if p in dd and c not in desc[e]:
            desc[e].add(c)
            added = True
    return desc

  def rel_ascendants(self, descendants):
    if descendants is None:
      return None
    asc = {}
    for code, descs in descendants.items():
      for desc in descs:
        if not desc in asc:
          asc[desc] = {code}
        else:
          asc[desc].add(code)
    return asc

  def aggregate_function(self, unit):
    if unit is None:
      return lambda values: None 

    unit = unit.lower().replace(" ", "")
    if unit in ['people', 'number', 'qty', 'days']:
        return np.sum
    elif unit in ['comma_list']:
      return ','.join
    else:
      return lambda values: None 
   

  def tuple_aggregate_function(self, t, variables):
    if ("obs" in t 
      and len(t["obs"].keys()) > 0 
      and list(t["obs"].keys())[0] in variables 
      and "unit" in variables[list(t["obs"].keys())[0]] ):
        return self.aggregate_function(variables[list(t["obs"].keys())[0]]["unit"])
    else:
        return None

  def tup_key(self, t, with_obs, with_date, variables):
      return tuple(
      ([("variable", i) for i in [*t["obs"].keys()][0:1]] if with_obs and "obs" in t and len(t["obs"].keys())>0 else []) +
      [(k, t["attrs"][k]) for k in ( 
      sorted([
        attr for attr in t["attrs"].keys() 
        if variables[attr]["type"] in ["characteristic", "referential", "geo_referential", "referential_parent"] or
           (with_date and  variables[attr]["type"] == "date")
      ]))])

  def pairs_to_aggregate(self, t, var_asc, variables, existing_codes):
    if not "attrs" in t or len(t["attrs"].keys()) == 0:
      yield ('', t, None, None)
    else:
      key = self.tup_key(t, with_obs = True, with_date = True, variables = variables)
      # adding identity aggregation 
      yield (key, t, None, None)

      # adding ascendants aggregation
      for code_var, ascendants in var_asc.items():
        if code_var in t["attrs"] and t["attrs"][code_var] in ascendants:
          code =  t["attrs"][code_var]
          for asc in ascendants[code]:
            if asc in existing_codes[code_var]:
              continue
            c = copy.deepcopy(t)
            c["attrs"][code_var] = asc
            key = self.tup_key(c, with_obs = True, with_date = True, variables = variables)
            if(code != asc): # we have to remove the idenntity since it was already added
              yield (key, c, code_var, asc)

  def get_dist_key(self, t, variables ):
    var = None
    for group in t.keys():
      if group !="attrs":
        var = next(iter(t[group].keys()))
    if var is None:
      return None
    
    split_attrs = [attr for attr in sorted(variables[var]["partition"] or []) if attr != "geo_code"]
    return  tuple((("ind", var))) + tuple(((attr, (t["attrs"][attr] if "attrs" in t and attr in t["attrs"] else None)) for attr in split_attrs)) 

  def distribute_tuples_by_partition(self, tuples, job_id):
    variables = self._variables_proxy.get_variables().get()
    ret = {}
    cache_path = util.pandem_path("files", "staging", str(int(job_id)), "cache-files")
    if not os.path.exists(cache_path):
      os.makedirs(cache_path)
    uscope = {
        "scope":{"update_scope":[]}
    }
    cache_files = {}
    cache_paths = {}
    
    i = 0
    for p, tt in tuples.items():
      if tt is not None:
        # getting tuples from cache
        tt = tt.value()
        keys_in_path = set()
        for t in tt['tuples']:
          # key identification
          key = self.get_dist_key(t, variables)
          if key is not None and key not in keys_in_path:
            # adding key to avoind entering again for same key
            keys_in_path.add(key)
            # trying to get tuples from job cache
            if key not in cache_files:
              cache_file =  os.path.join(cache_path, str(i))
              i = i + 1
              cache_paths[key] = cache_file
              if os.path.exists(cache_file):
                os.remove(cache_file)
              cache_files[key] = open(cache_file, "at")
            cache_files[key].writelines((base64.b64encode(pickle.dumps(ttt)).decode('utf-8')+"\n" for ttt in tt['tuples'] if self.get_dist_key(ttt, variables)== key))
            
            # updating the update scope 
            for u in tt['scope']['update_scope']:
              v = u["value"] if type(u["value"]) in [list, set] else [u["value"]]
              found = False
              for uu in uscope['scope']['update_scope']:
                if u['variable'] == uu["variable"] :
                 uu["value"] = {*uu["value"], *v}
                 found = True
              if not found:
                 uscope['scope']['update_scope'].append({"variable":u["variable"], "value":v})
       
    #storing results distributed by variables on cache
    for key, f in cache_files.items():
      f.close()
      rebuild = {"scope":{"update_scope":uscope['scope']['update_scope']}}
      with open(cache_paths[key], "r") as rf:
        rebuild["tuples"] = [pickle.loads(base64.b64decode(l)) for l in rf.readlines()]
      ret[key] = self._storage_proxy.to_job_cache(job_id, f"agg_{key}", rebuild).get()
    l.debug("Tuples redistributed by variable partitioning model (excluding geo)")
    return ret
