import os

from numpy import int64
from . import worker
from abc import ABC, abstractmethod, ABCMeta
from datetime import datetime, timedelta
import time
import re
from collections import defaultdict
import itertools as it
import tempfile
import json
import subprocess
from copy import deepcopy
from itertools import chain
import logging
import traceback
from . import util
from .storage import CacheValue

l = logging.getLogger("pandem.evaluator")


class Evaluator(worker.Worker):
    __metaclass__ = ABCMeta  
    def __init__(self, name, orchestrator_ref, settings): 
        super().__init__(name = name, orchestrator_ref = orchestrator_ref, settings = settings)    
         
    def on_start(self):
        super().on_start()
        self._storage_proxy = self._orchestrator_proxy.get_actor('storage').get().proxy()
        self._pipeline_proxy = self._orchestrator_proxy.get_actor('pipeline').get().proxy() 
        self._variables_proxy = self._orchestrator_proxy.get_actor('variables').get().proxy() 
        self._dict_of_variables = self._variables_proxy.get_variables().get()
        self._indicators, self._modifiers, self._parameters, self._scripts, self._params_set = self.get_indicators(self._dict_of_variables)

    def get_indicators(self, var_dic):
        # Read the formulas of all indicators 
        formulas = {var:var_dic['formula'] for var, var_dic in var_dic.items() if var_dic['formula']} 
        # Parse parameters from the functions/formula text and store them in parameters dict: 
        # for each indicator, returning a list for each parameters. e.g. parameters[“incidence”]=[“reporting_period”, “number_of_cases”, “population”]
        parameters = {ind:re.findall (r'([^(, )]+)(?!.*\()', formula) for ind, formula in formulas.items()}
        modifiers = {ind:({m["variable"]:m["value"] for m in var_dic[ind]["modifiers"]}) if "modifiers" in var_dic[ind] else {} for ind in var_dic}
        # Build indicators dict that returns for each variable used on an indicator a list of indicators where this variable is used. eg. indicators[“population”]=[“Incidence”, “prevalence, …”]
        scripts = {ind:re.search(r'^.*?(?=\()', formula).group() for ind, formula in formulas.items()}
        indicators = dict()
        params_set = {params[i] for ind, params in parameters.items() for i in range(len(params))}
        for param in params_set:
            ind_list = [ind for ind, params in parameters.items() if param in params]
            indicators[param] = ind_list

        return indicators, modifiers, parameters, scripts, params_set

    #def get_obs_name(self, obs, attrs):
    #    if self._dict_of_variables[obs]["aliases"] is None or len(self._dict_of_variables[obs]["aliases"]) == 0 or self._dict_of_variable[obs]["variable"] != obs:
    #      return obs
    #    else:
    #      for alias in self._dict_of_variables[obs]["aliases"]:
    #        if all(modifier["variable"] in attrs and attrs[modifier["variable"]] == modifier["value"] for modifier in alias["modifiers"]):
    #          return alias["alias"]
    #    return obs
    #          

    def modifiers_in_key(self, var_name, tuple_key):
      var_dic = self._dict_of_variables
      if "modifiers" in var_dic[var_name]:
        for mod in modifiers:
          mod_found = False
          for var, value in tuple_key.items():
            if var == mod["variable"]:
              mod_found = True
              if value != mod["value"]:
                return False
              break
          if not mod_found:
            return False
      return True

    def plan_calculate(self, dic_of_tuples, job):
        # defined variables 
        var_dic = self._dict_of_variables
        # modifiers for each variable
        modifiers = self._modifiers
        # all variables being used as parameters in a set
        params_set = self._params_set
        # parameters contails the needed parameters for calculating each variable containing a function definition
        parameters = self._parameters
        # obs_keys will contain all of the variables found on the tuples with its date period and combinations
        obs_keys = {}
        
        obs_aliases = {}
        next_keys = {}
        
        # iterating over all tuples in the pipeline to calculate 
        for ttuples in dic_of_tuples.values():
          # getting tuples from cache
          list_of_tuples = (ttuples.value() if type(ttuples) == CacheValue else ttuples)['tuples']
          for t in list_of_tuples:
            if "obs" in t and "attrs" in t: 
              # getting the base variale of the tuple
              base_name = next(iter(t["obs"].keys()))
              # var_names are the variables that could be matched using this base variables
              var_names = [base_name]
              # evaluating if current tuple is an alias of the base variable to allow applying formulas on derivated variables
              if 'aliases' in  var_dic[base_name]:
                for alias in var_dic[base_name]['aliases']:
                  if all(m['variable'] in t['attrs'] and t['attrs'][m['variable']] == m['value'] for m in alias['modifiers']):
                    var_names.append(alias['alias'])
              # at this point var_names will contain all matching variables that matches the current tuple
              # for each matched variables we will add its combinations in obs_keys
              for var_name in var_names:
                if var_name in params_set or var_name in parameters.keys():
                  if var_name not in obs_keys:
                    obs_keys[var_name] = {
                      "comb":set(),
                      "dates":set()
                    }
                  date_attrs = set(vn for vn in t["attrs"].keys() if var_dic[vn]['type'] == 'date' and t["attrs"][vn] is not None)
                  independent_attrs = [t for t in t["attrs"].keys() if t not in modifiers[var_name] or  modifiers[var_name][t] is not None]
                  sorted_attrs = list(sorted(
                    vn for vn in independent_attrs 
                    if var_dic[vn]['type'] not in ['not_characteristic', 'date', 'private'] 
                      and t["attrs"][vn] is not None
                      and var_dic[vn]["linked_attributes"] is None
                  ))
                  if len(sorted_attrs) > 0 and len(date_attrs)>0:
                    obs_keys[var_name]["comb"].add(tuple((vn, t["attrs"][vn]) for vn in sorted_attrs))
                    obs_keys[var_name]["dates"].add(tuple((vn, t["attrs"][vn]) for vn in date_attrs))
        var_obs = {}
        indicators_to_cal = []
        step = 0
        stop = False
        # we are going to iterate over all available indicator trying to evaluate them based on the data on the tuples and published
        # the loop will keep going as long as new indicator - combinations are produced
        while not stop:
            stop = True
            for ind, params in parameters.items():
                synthetic_tag = var_dic[ind]['synthetic_tag'] 
                synthetic_blocker = set(var_dic[ind]['synthetic_blocker']) if var_dic[ind]['synthetic_blocker'] is not None else {}
                dls = job['dls_json']
                is_synthetic = synthetic_tag is not None
                # test to skip syhthetic formula when not applicable
                if is_synthetic:
                  if 'synthetize' not in dls:
                    continue
                  # if DLS information is available, we look at the 'active' status
                  elif 'active' in dls['synthetize'] and not dls['synthetize']['active']:
                    continue
                  # if tags are available in DLS, we check the tag of the synthetic formula is in the DLS tags
                  if len(set(synthetic_tag).intersection(set(dls['synthetize']['tags']))) == 0:
                    continue
                # preparing variables to evaluate if the current indicator can be calculated
                mod = modifiers[ind]
                date_par = next(p for p in params if var_dic[p]['type'] == 'date')
                base_date = var_dic[date_par]['variable']
                no_date_pars = [p for p in params if p != date_par]
                attr_pars =  [p for p in no_date_pars if var_dic[p]['type'] not in {'observation', 'indicator', 'resource'}]
                obs_pars =  list([p for p in params if var_dic[p]['type'] in {'observation', 'indicator', 'resource'}])
                base_pars =  list([var_dic[p]["variable"] for p in obs_pars])
                main_obs = obs_pars[0] if len(obs_pars)>0 else None
                main_base = base_pars[0] if len(base_pars)>0 else None
                
                # identifying the combinations available for the first parameter so we can identify if new combinations can be calculated for this indicator
                comb = set()
                # getting all existing combinations for the main obs 
                for ptest in obs_keys:
                  if main_base == var_dic[ptest]['variable']:
                    for c in obs_keys[ptest]["comb"]:
                      sc = dict(c)
                      # The combination will be included if all not None modifiers of the main observation parameter are found on the combination
                      # and if none of the blocker attributes are present 
                      if (all(k in sc and sc[k] == v for k, v in modifiers[main_obs].items() if v is not None) and 
                            sc.keys().isdisjoint(synthetic_blocker)):
                        comb.add(c)
                
                # in order to test this indicator we need to find at least the first observation on the current values
                if len(comb) > 0:
                    scomb = comb
                    comb = list(comb) 
                    # testing the tuples than satisfy the provided parameters
                    #l.debug(f"ind {ind} -> obs_pars {obs_pars} base_pars {base_pars}")
                    if len(obs_pars) > 0 and (base_pars[0] in obs_keys or obs_pars[0] in obs_keys):
                      #l.debug("go")
                      # We need to identify all tuples present on all obs_pars that respect the attr pars  
                      dates = obs_keys[main_obs]["dates"] if main_obs in obs_keys else obs_keys[main_base]["dates"]
                      date_filter = {base_date: {str(v) for date_comb in dates for k, v in date_comb if k == base_date}}
                      #date_filter.update(mofifiers[date_par])
                      # we can proceed the date_par has been found
                      if len(date_filter[base_date]) > 0:
                        # Iterating over all obs_par in formula
                        for i in range(0, len(obs_pars)):
                            obs_to_test = obs_pars[i]
                            base_to_test = base_pars[i]
                            if i == 0:
                              pcomb = comb
                            else:
                              pcomb = set()
                              for ptest in obs_keys:
                                if base_to_test == var_dic[ptest]['variable']:
                                  for c in obs_keys[ptest]["comb"]:
                                    sc = dict(c)
                                    if all(k in sc and sc[k] == v for k, v in modifiers[obs_to_test].items() if v is not None):
                                      pcomb.add(c)
                              # if the current obs is missing combination we will see if there is published data for it
                              missing_combs = set()
                              for cc in comb:
                                applied = dict(cc)
                                applied.update(modifiers[obs_to_test])
                                applied = (tuple(sorted(((k, v) for k,v in applied.items() if v is not None), key = lambda p: p[0])))
                                if not applied in pcomb:  
                                  missing_combs.add(applied)
                              if len(missing_combs) > 0 :
                                # if the current obs has null modifiers we have to delete those from lookup key
                                pub_comb = self._variables_proxy.lookup([base_to_test], missing_combs, job['dls_json']['scope']['source'], date_filter, include_source = False, include_tag = True).get()
                                if len(pub_comb) > 0:
                                  obs_keys[obs_to_test] = {
                                    "comb":set(pub_comb.keys()).union(pcomb),
                                    "dates":dates
                                  }
                            # iterating over current possible combinations
                            j = 0
                            while j < len(comb):
                                # the current combination should exists on obs_keys for the current parameter base observation
                                # tuple must contain the attrs_pars unless the current observation overrides it with a modifier
                                attr_pars_ok = True
                                key = comb[j]
                                # testing independent keys which is attributes not modified by the variable
                                for attr_par in attr_pars:
                                  ignore_attr = attr_par in modifiers[obs_to_test]
                                  if not ignore_attr and not any(k == attr_par for k, v in key):
                                    attr_pars_ok = False
                                    break

                                if attr_pars_ok:
                                  # checking that tuple contains the modifiers of the current observation unless is null
                                  attrs_obs_ok = True
                                  for obs_mod_var, obs_mod_value in modifiers[obs_to_test].items():
                                    if obs_mod_value is not None and obs_mod_var not in modifiers[ind]:
                                      if not any(k == obs_mod_var and v == obs_mod_value for k, v in key):
                                        attrs_obs_ok = False
                                        break
                                  if attrs_obs_ok:
                                    # checking if the current combination exists for the expected base variable
                                    # applying modifiers
                                    applied_key = tuple(sorted([(k,v) for k,v in ({**dict(key), **modifiers[obs_to_test]}.items()) if v is not None], key = lambda p: p[0]))
                                    if( i == 0 or 
                                      any(not {key, applied_key}.isdisjoint(obs_keys[o]["comb"]) for o in obs_keys if base_to_test == var_dic[o]['variable'])): 
                                      j = j + 1
                                    else:
                                      comb.pop(j)
                                  else:
                                    comb.pop(j)
                                else:
                                  comb.pop(j)
                        # The remaining combination have passed all validations to calculate the candidate indicator
                        if len(comb) > 0:
                          # applying modifiers of indicators to the remaining tuples
                          origcombs = dict()
                          if ind in modifiers:
                            for j in range(0, len(comb)):
                              dcomb = dict(comb[j])
                              dcomb.update(modifiers[ind])
                              new_comb = tuple(sorted(((k, v) for k,v in dcomb.items() if v is not None), key = lambda p: p[0]))
                              origcombs[new_comb] = comb[j]
                              comb[j] = new_comb
                                    
                          new_combs = set(comb) - (obs_keys[ind]["comb"] if ind in obs_keys else set())
                          next_keys[ind] = {
                            "comb": new_combs,
                            "dates":dates
                          }
                          # adding the found indicator to the list to calculate
                          if len(new_combs) > 0:
                            #l.debug(f"added! {ind}->{len(new_combs)}")
                            indicators_to_cal.append((ind, {
                              "step":step + 1,
                              "comb":[origcombs[c] for c in new_combs],
                              "dates":dates,
                              "date_par":date_par
                            }))
                            # we have found something new so we will try to performa a new step
                            stop = False
            step = step + 1
            # updating obs_keys using next_keys (indicators that will be calculated on this step)
            for k, v in next_keys.items():
              if k not in obs_keys:
                obs_keys[k] = v
              else:
                obs_keys[k]["comb"] = obs_keys[k]["comb"].union(v["comb"])
                obs_keys[k]["dates"] = obs_keys[k]["dates"].union(v["dates"])
            next_keys.clear()


        self._pipeline_proxy.precalculate_end(indicators_to_cal, job=job)

       
    def prepare_scripts(self, ind, job):
        scripts = self._scripts
        params = self._parameters
        vars_dic =  self._dict_of_variables
        
        # read the functin code from indcators directory
        function_path = self.pandem_path(f'files', 'indicators', scripts[ind], 'function.R')
        if not os.path.exists(function_path):
            raise FileNotFoundError(f"Cannot found R scrip for calculate indicator {ind} it should be located at {function_path}")
        with open(function_path) as f:
            function_code = f.readlines()
        staging_dir = self.staging_path(job['id'], f'ind/{ind}')
        if not os.path.exists(staging_dir):
            os.makedirs(staging_dir)
        exec_file_path = os.path.join(staging_dir, 'exec.R')
        result_path = os.path.join(staging_dir, 'result.json')
        if os.path.exists(result_path):
          os.remove(result_path)
        # write the R script within the exec.R file
        execf = open(exec_file_path, 'w+')
        for param in params[ind]:
            param_file_path = os.path.join(staging_dir, param+'.json')
            execf.write(f'`{param}_mat` <- jsonlite::fromJSON("{param_file_path}")'+'\n')
            if os.path.exists(param_file_path):
              os.remove(param_file_path)
        param0 = params[ind][0]
        execf.write('res <- sapply(1:ncol('+param0+'_mat), function(col) {'+'\n')
        for param in params[ind]:
          execf.write(f'`{param}` = {param}_mat[,col]'+'\n')
        for code_line in function_code:
          execf.write(code_line)
        execf.write('})\n')
        execf.write(f'jsonlite::write_json(res, "{result_path}", matrix = "columnmajor")'+'\n')
        execf.close()

    def calculate(self, indicators_to_cal, job, ignore_pipeline = False):
        try:
            vars_dic =  self._dict_of_variables
            params = self._parameters
            source =  job['dls_json']['scope']['source']
            ret = {}

            part = 0
            # mixing indicators to calculate on all paths
            if indicators_to_cal and len(indicators_to_cal) > 0:
                # looping trough all indicators
                for ind, ind_map in indicators_to_cal:
                    combis = ind_map["comb"]
                    l.debug(f"calculating {ind} in {source} for {len(combis)} combinations")
                    # creating scripts for indicator
                    self.prepare_scripts(ind, job)
                    # getting all matching combinations for the indicator calculation from the database
                    obs = {p:vars_dic[p]["variable"] for p in params[ind] if vars_dic[p]["type"] in ["observation", "indicator", "resource"]}
                    var_date, base_date = next(iter((p, vars_dic[p]['variable']) for p in params[ind] if vars_dic[p]["type"] == "date"))
                    main_par, main_base = next(iter((p, vars_dic[p]['variable']) for p in params[ind] if vars_dic[p]["type"] in ["observation", "indicator", "resource"] ))
                    attrs = {p:vars_dic[p]["variable"] for p in params[ind] if p not in set(obs.keys())}
                    # iterating though each combination and launching the scripts to calculate the results
                    ii = 0
                    if len(combis) > 0: 
                      for cslice in util.slices((c for c in combis), 200):
                        ii = ii + len(cslice)
                        part = part + 1
                        key = f"{ind}_{part}" 
                        result = {"tuples": []}
                        if(ii < len(combis)):
                          l.debug(f"Calculating combinations until {ii}")
                        data = self._variables_proxy.lookup(list(obs.keys()), cslice, source, {base_date:None} , include_source = True, include_tag = True).get()
                        # getting sorted dates
                        dates = sorted({v["attrs"][base_date] for row in data.values() for v in row[main_base] if main_base in row})
                        # writing parameters matrices 
                        staging_dir = self.staging_path(job['id'], f'ind/{ind}')
                        can_run = True
                        # on file per parameter 
                        for p in params[ind]:
                          param_file_path = os.path.join(staging_dir, p+'.json')
                          with open(param_file_path, 'w') as jsonf:
                            jsonf.write("[")
                            sep = ""
                            # one row per date
                            all_none = True
                            for date in dates:
                              # one element per combination
                              param_values = [self._get_param_value(p, comb, date, data, obs, attrs, main_par, base_date) for comb in cslice]
                              jsonf.write(sep)
                              sep = ","
                              for v in param_values:
                                if v is not None:
                                  all_none = False
                              # writing the values on the parameter file 
                              jsonf.write(json.dumps([(v if v != float('inf') else None) for v in param_values]))
                            jsonf.write("]")
                            if all_none:
                              can_run = False

                        # executing the script
                        exec_file_path = os.path.join(staging_dir, 'exec.R')
                        result_path = os.path.join(staging_dir, 'result.json')
                        if can_run:
                          subprocess.run (f'Rscript --vanilla {exec_file_path}', shell=True, cwd=staging_dir)
                          if os.path.exists(result_path):
                              modifiers = {t["variable"]:t["value"] for t in vars_dic[ind]["modifiers"] } if "modifiers" in vars_dic[ind] else {}
                              with open(self.pandem_path(result_path)) as f:
                                  r = json.load(f)
                              assert(len(r) == len(cslice))
                              for combi_res, comb in zip(r, cslice):
                                for date, value in zip(dates, combi_res):
                                  ind_date_tuple = {'obs': {ind:value if value != "NA" else None},
                                                  'attrs':{**{base_date:date, "source":source},
                                                           **{k:v for k,v in comb},
                                                           ** modifiers
                                                           }
                                                  }
                                  result['tuples'].append(ind_date_tuple)
                              result['scope'] = {}            
                              result['scope']['update_scope'] = [{'variable':'source', 'value':[source]}]            
                              ret[key] = self._storage_proxy.to_job_cache(job["id"], f"calc_{key}", result).get()
                          else:
                              l.warning(f'result file {result_path} not found')
             

            if ignore_pipeline:
              return ret
            else: 
              self._pipeline_proxy.calculate_end(ret, job = job).get()
        except Exception as e: 
            l.warning(f"Error during calculation, job {job['id']} will fail")
            for line in traceback.format_exc().split("\n"):
              l.debug(line)
            issue = { "step":job['step'], 
                   "line":0, 
                   "source":job['source'], 
                   "file":'', 
                   "message":f"{str(e)}\n{traceback.format_exc()}", 
                   "raised_on":datetime.now(), 
                   "job_id":job['id'], 
                   "issue_type":"error",
                   'issue_severity':"error"
            }
            if not ignore_pipeline:
              self._pipeline_proxy.fail_job(job, issue = issue).get()

    def _get_param_value(self, param, comb, date, data, obs, attrs, main_par, base_date):
        if comb in data:
          row = data[comb]
          # if the parameter is an observation we will look into the associated row attribute getting the date from attrs 
          if param in obs and obs[param] in row:
            for v in row[obs[param]]:
              if v["attrs"][base_date] == date:
                return v["value"]
          # if not an observation then the result is expected to be on attrs or in combination
          elif not param in obs:
            for v in row[obs[main_par]]:
              if v["attrs"][base_date] == date:
                if attrs[param] in v["attrs"]:
                  return v["attrs"][attrs[param]]
                else:
                  comb_map = {k:v for k, v in comb if k == attrs[param]}
                  return comb_map[attrs[param]]
        return None

