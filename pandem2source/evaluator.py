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
        self._parameters, self._indicators = self.get_indicators(self._dict_of_variables)


    def get_indicators(self, dic_of_variables):
        # Read the formulas of all indicators 
        formulas = {var:var_dic['formula'] for var, var_dic in dic_of_variables.items() if var_dic['formula']} 
        # Parse parameters from the functions/formula text and store them in parameters dict: for each indicator, returning a list for each parameters. e.g. parameters[“incidence”]=[“reporting_period”, “number_of_cases”, “population”]
        parameters = {ind:re.findall (r'([^(, )]+)(?!.*\()', formula) for ind, formula in formulas.items()}
        # Build indicators dict that returns for each variable used on an indicator a list of indicators where this variable is used. eg. indicators[“population”]=[“Incidence”, “prevalence, …”]
        indicators = dict()
        params_set = {params[i] for ind, params in parameters.items() for i in range(len(params))}
        for param in params_set:
            ind_list = [ind for ind, params in parameters.items() if param in params]
            indicators[param] = ind_list
        return parameters, indicators

    def calculate(self, list_of_tuples, path, job):
        tuples_with_ind = deepcopy(list_of_tuples)
        obs_attrs = dict() 
        for tuple in list_of_tuples['tuples']:
            if 'obs' in tuple:
                if list(tuple['obs'])[0] in obs_attrs:
                    obs_attrs[list(tuple['obs'])[0]] = list(set(obs_attrs[list(tuple['obs'])[0]] + list(tuple['attrs'])))
                else:
                    obs_attrs[list(tuple['obs'])[0]] = list(tuple['attrs'])
        attr_set = {attr  for tuple in list_of_tuples['tuples'] for attr in list(tuple['attrs'])} 
        attr_set_filtered = {attr for attr in attr_set if self._dict_of_variables[attr]['type']!='not_characteristic'}
        attr_values = dict()
        for attr in attr_set_filtered:
            attr_values[attr] = list(set([tuple['attrs'][attr] for tuple in list_of_tuples['tuples'] if attr in tuple['attrs']]))
        indicators_to_calculate = []
        if obs_attrs:
            for ind, params in self._parameters.items(): # ? Should we suppose here that reposting period is the only attribute type parameter for an indicator
                if len(set(params[1:]).intersection(set(obs_attrs)))== len(params)-1:
                    indicators_to_calculate.append(ind)
        print(f'indicators to calculate: {indicators_to_calculate}')
        if indicators_to_calculate:
            date_var = self._parameters[indicators_to_calculate[0]][0]# ? Why should we find date_var in parameters since it is alwayas 'reporting period
            dates_in_tuples = list({tuple['attrs'][date_var] for tuple in list_of_tuples['tuples'] if date_var in tuple['attrs']})
            for ind in indicators_to_calculate:
                # read the functin code from indcators directory
                function_path = self.pandem_path(f'files/indicators/{ind}', 'function.R')
                with open(function_path) as f:
                    function_code = f.read()
                staging_dir = self.pandem_path(f'files/staging/{ind}')
                if not os.path.exists(staging_dir):
                    os.makedirs(staging_dir)
                exec_file_path = os.path.join(staging_dir, 'exec.R')
                result_path = os.path.join(staging_dir, 'result.json')
                params = self._parameters[ind]
                # write the R script within the exec.R file
                execf = open(exec_file_path, 'w+')
                for i, param in enumerate(params[1:]):
                    param_file_path = os.path.join(staging_dir, param+'.json')
                    execf.write(f'p{i+1} <- jsonlite::fromJSON("{param_file_path}")'+'\n')
                execf.write(f'res <- {function_code}'+'\n')
                execf.write(f'jsonlite::write_json(res, "{result_path}")'+'\n')
                execf.close()
                # find attributes related to an indicator parameters other than reporting_period and period_type
                attrs_ind = list(set(chain.from_iterable([obs_attrs[param] for param in params[1:]])).intersection(attr_set_filtered) - {date_var, 'period_type'})
                # retrieve the list of values combinations of attributes related to an indicator other than reporting_period
                combinations = list(it.product(*(attr_values[attr] for attr in attrs_ind)))
                # for each attributes values combination, build a dict where for each date in dates_in_tuples associate parameters values if any
                for attr_comb in combinations:
                    non_charac_attrs = dict()
                    params_values = defaultdict(dict)
                    for tuple in list_of_tuples['tuples']:
                        if 'obs' in tuple:
                            for date in dates_in_tuples:
                                if tuple['attrs'][date_var]==date: 
                                    for param in params[1:]:
                                        if param in tuple['obs'] : 
                                            if all(tuple['attrs'][attr]==val for attr,val in zip(attrs_ind, attr_comb) if attr in obs_attrs[param]):
                                                non_charac_attrs['source'] = tuple['attrs']['source']
                                                non_charac_attrs['file'] = tuple['attrs']['file']
                                                non_charac_attrs['line_number'] = tuple['attrs']['line_number']
                                                if type(tuple['obs'][param]) in [int64]:
                                                    params_values[date][param] = int(tuple['obs'][param])
                                                else:
                                                    params_values[date][param] = tuple['obs'][param]
                

                    # write params values within temporary files
                    for i, param in enumerate(params[1:]):
                        param_values_list = [params_values[date][param] if param in params_values[date] else 'NA' for date in params_values.keys()]
                        param_file_path = os.path.join(staging_dir, param+'.json')
                        with open(param_file_path, 'w+') as jsonf:
                            jsonf.write(json.dumps(param_values_list))
                    if params_values:
                        subprocess.run (f'/usr/bin/Rscript --vanilla {exec_file_path}', shell=True, cwd=staging_dir)
                        if os.path.exists(result_path):
                            with open(self.pandem_path(result_path)) as f:
                                result = f.read()
                            result = re.findall (r'([^\[," \]\n]+)', result)
                        else:
                            print('result file not found')
                        for i, res in enumerate(result):
                            date = list(params_values)[i]
                            ind_date_tuple = {'obs': {ind:res},
                                            'attrs':{**{'reporting_period':date, 'period_type':'date'},
                                                     **{k:v for k,v in zip(attrs_ind, attr_comb)},
                                                     **non_charac_attrs}
                                            }
                            tuples_with_ind['tuples'].append(ind_date_tuple)
        self._pipeline_proxy.calculate_end(tuples = tuples_with_ind, path = path, job = job)
    
        # It will identify the indicators to calculate by looking for parameters on the formulas using the indicators map for each variable on the list of tuples.
        # For each indicator to calculate it will determine the list of parameters to obtain using the parametet map.
        # The first parameter of the indicator is expected to be a date, it wil build a list of all dates (it must be present on the tuple). 
        # For all other parameters it will try to find the values for each date. on the list of tuples.
        # For each combination of attributes (e.g. agre_group, location_code):
        #     - It will build a vector for each variable with as many elements as dates found
        #     - It will calculate the results as follow
        #     - It will create a temporary folder
        #     - The parameters are written as json files on a temp folder p0.json, p1.json, p2.json …
        #     - Write the temporary_folder/exec.R script to calculate the value as follow
        #         _ Read the parameter from json files and load the results on varialbles p0, p1, p2…. (this code is produced by the calculate actor)
        #             p0 ← jsonlite::fromJSON(“p0.json“)
        #         _ Evaluate the expression on the formula res <- { code on function.R here }
        #         _ write the result as result.json jsonlite::writeJSON(res, “result.json') (this code is produced by the actor)
        #     - Execute the scrupt with Rscript temporary_folder/exec.R
        #     - If the results.json file has been produced read the file with the results
        #     - Add the obtained result to each tuple
 