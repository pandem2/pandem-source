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
        # Parse parameters from the functions/formula text and store them in parameters dict: for each indicator, returning a list for each parameters. e.g. parameters[“incidence”]=[“reporting_period”, “number_of_cases”, “population”]
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

    def plan_calculate(self, list_of_tuples, job):
        # getting tuples from cache
        list_of_tuples = list_of_tuples.value()['tuples']
        
        var_dic = self._dict_of_variables
        modifiers = self._modifiers
        params_set = self._params_set
        parameters = self._parameters
        obs_keys = {}
        obs_aliases = {}
        next_keys = {}
        for t in list_of_tuples:
          if "obs" in t and "attrs" in t: 
            base_name = next(iter(t["obs"].keys()))
            var_names = [base_name]
            # evaluating if current tuple is a derived variable (to allow proper detection of derivated user provided variables)
            if 'aliases' in  var_dic[base_name]:
              for alias in var_dic[base_name]['aliases']:
                if all(m['variable'] in t['attrs'] and t['attrs'][m['variable']] == m['value'] for m in alias['modifiers']):
                  var_names.append(alias['alias'])
            
            for var_name in var_names:
              if var_name in params_set or var_name in parameters.keys():
                if var_name not in obs_keys:
                  obs_keys[var_name] = {
                    "comb":set(),
                    "dates":set()
                  }
                date_attrs = set(vn for vn in t["attrs"].keys() if var_dic[vn]['type'] == 'date' and t["attrs"][vn] is not None)
                independent_attrs = [t for t in t["attrs"].keys() if t not in modifiers[var_name] or  modifiers[var_name][t] is not None]
                sorted_attrs = list(sorted(vn for vn in independent_attrs if var_dic[vn]['type'] not in ['not_characteristic', 'date'] and t["attrs"][vn] is not None))
                if len(sorted_attrs) > 0 and len(date_attrs)>0:
                  obs_keys[var_name]["comb"].add(tuple((vn, t["attrs"][vn]) for vn in sorted_attrs))
                  obs_keys[var_name]["dates"].add(tuple((vn, t["attrs"][vn]) for vn in date_attrs))


        var_obs = {}

        indicators_to_cal = {}
        step = 0
        stop = False
        while not stop:
            stop = True
            for ind, params in parameters.items():
                # not trying a varibale being already present
                if not ind in obs_keys:
                    # testing the tuples than satisfy the provided parameters
                    mod = modifiers[ind]
                    date_par = next(p for p in params if var_dic[p]['type'] == 'date')
                    base_date = var_dic[date_par]['variable']
                    no_date_pars = [p for p in params if p != date_par]
                    attr_pars =  [p for p in no_date_pars if var_dic[p]['type'] not in {'observation', 'indicator', 'resource'}]
                    obs_pars =  list([p for p in params if var_dic[p]['type'] in {'observation', 'indicator', 'resource'}])
                    base_pars =  list([var_dic[p]["variable"] for p in obs_pars])
                    # in order to test this indicator we need to find at least the first observation on the current values
                    if len(obs_pars) > 0 and  base_pars[0] in obs_keys:
                      main_obs = obs_pars[0]
                      main_base = base_pars[0]
                      # We need to identify all tuples present on all obs_pars that respect the attr pars  
                      comb = list(obs_keys[main_base]["comb"])
                      dates = obs_keys[main_base]["dates"]
                      date_filter = {base_date: {v for date_comb in dates for k, v in date_comb if k == base_date}}
                      #date_filter.update(mofifiers[date_par])
                      # we can proceed the date_par has been found
                      if len(date_filter[base_date]) > 0:
                        # Iterating over all obs_par in formula
                        for i in range(0, len(obs_pars)):
                            obs_to_test = obs_pars[i]
                            base_to_test = base_pars[i]
                            # if the current obs base variable is not on obs key we have to look for it on published data
                            if len(comb) > 0 and base_to_test not in obs_keys:
                               # if the current obs has null mofifiers we have to delete those from lookup key
                               pub_comb = self._variables_proxy.lookup([base_to_test], comb, job['dls_json']['scope']['source'], date_filter, include_source = False, include_tag = True).get()
                               obs_keys[base_to_test] = {
                                 "comb":set(pub_comb.keys()),
                                 "dates":dates
                               }
                            # iterating over current possible combinations
                            j = 0
                            #if ind == "incidence" and obs_to_test == "population":
                            #    breakpoint()
                            while j < len(comb):
                                # the current combination should exists on obs_keys for the current parameter base observation
                                # tuple must contain the attrs_pars unless the current observation overrides it with a modifier
                                attr_pars_ok = True
                                # testing independent keys which is attributes not modified by the variable
                                key = comb[j]
                                for attr_par in attr_pars:
                                  ignore_attr = attr_par in modifiers[obs_to_test]
                                  if not ignore_attr and not any(k == attr_par for k, v in key):
                                    attr_pars_ok = False
                                    break
                                if attr_pars_ok:
                                  # checking that tuple contains the modifiers of the current observation unless is null
                                  attrs_obs_ok = True
                                  for obs_mod_var, obs_mod_value in modifiers[obs_to_test].items():
                                    if obs_mod_value is not None:
                                      if not any(k == obs_mod_var and v == obs_mod_value for k, v in key):
                                        attrs_obs_ok = False
                                        break
                                  if attrs_obs_ok:
                                    # checking if the current combination exists for the expected base variable
                                    # exluding modifiers
                                    indep_key = tuple([(k, v) for k, v in key if k not in modifiers[obs_to_test] or modifiers[obs_to_test][k] is not None])
                                    if base_to_test in obs_keys and indep_key in obs_keys[base_to_test]["comb"]:
                                      j = j + 1
                                    else:
                                      comb.pop(j)
                                  else:
                                    comb.pop(j)
                                else:
                                  comb.pop(j)
                        # The remaining combination have passed all validations to calculate the candidate indicator
                        if len(comb) > 0:
                          next_keys[ind] = {
                            "comb":set(comb),
                            "dates":dates
                          }
                          # adding the found indicator to the list to calculate
                          indicators_to_cal[ind] = {
                            "step":step + 1,
                            "comb":set(comb),
                            "dates":dates,
                            "date_par":date_par
                          }
                          # we have found something new so we will try to performa a new step
                          stop = False
            step = step + 1
            obs_keys.update(next_keys)
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

    def calculate(self, indicators_to_cal, job):
        try:
            vars_dic =  self._dict_of_variables
            params = self._parameters
            source =  job['dls_json']['scope']['source']
            result = {"tuples": []}
            # mixing indicators to calculate on all paths
            if indicators_to_cal and len(indicators_to_cal) > 0:
                # looping trough all indicators
                for ind, ind_map in indicators_to_cal.items():
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
                    if len(combis) > 0: 
                      data = self._variables_proxy.lookup(list(obs.values()), combis, source, {base_date:None} , include_source = True, include_tag = True).get()
                      # getting sorted dates
                      dates = sorted({v["attrs"][base_date] for row in data.values() for v in row[main_base] })
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
                            param_values = [self._get_param_value(p, comb, date, data, obs, attrs, main_par, base_date) for comb in combis]
                            jsonf.write(sep)
                            sep = ","
                            for v in param_values:
                              if v is not None:
                                all_none = False
                            # writing the values on the parameter file 
                            jsonf.write(json.dumps(param_values))
                          jsonf.write("]")
                          if all_none:
                            can_run = False

                      # executing the script
                      exec_file_path = os.path.join(staging_dir, 'exec.R')
                      result_path = os.path.join(staging_dir, 'result.json')
                      if can_run:
                        subprocess.run (f'/usr/bin/Rscript --vanilla {exec_file_path}', shell=True, cwd=staging_dir)
                        if os.path.exists(result_path):
                            modifiers = {t["variable"]:t["value"] for t in vars_dic[ind]["modifiers"] } if "modifiers" in vars_dic[ind] else {}
                            with open(self.pandem_path(result_path)) as f:
                                r = json.load(f)
                            assert(len(r) == len(combis))
                            for combi_res, comb in zip(r, combis):
                              for date, value in zip(dates, combi_res):
                                ind_date_tuple = {'obs': {ind:value if value != "NA" else None},
                                                'attrs':{**{base_date:date, "source":source},
                                                         **{k:v for k,v in comb},
                                                         ** modifiers
                                                         }
                                                }
                                result['tuples'].append(ind_date_tuple)
                        else:
                            l.warning(f'result file {result_path} not found')
            
            result['scope'] = {}            
            result['scope']['update_scope'] = [{'variable':'source', 'value':[source]}]            

            ret = self._storage_proxy.to_job_cache(job["id"], f"calc", result).get()
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
            self._pipeline_proxy.fail_job(job, issue = issue).get()

    def _get_param_value(self, param, comb, date, data, obs, attrs, main_par, base_date):
        if comb in data:
          row = data[comb]
          # if the parameter is an observation we will look into the associated row attribute getting the date from attrs 
          if param in obs:
            for v in row[obs[param]]:
              if v["attrs"][base_date] == date:
                return v["value"]
          # if not an observation then the result is expected to be on attrs or in combination
          else:
            for v in row[obs[main_par]]:
              if v["attrs"][base_date] == date:
                if attrs[param] in v["attrs"]:
                  return v["attrs"][attrs[param]]
                else:
                  comb_map = {k:v for k, v in comb if k == attrs[param]}
                  return comb_map[attrs[param]]
        return None

