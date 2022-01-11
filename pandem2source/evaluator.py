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
        print(f' The indicators map is: {self._parameters}')


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
        date_var = 'reporting_period'
        dates_in_tuples = set()
        ind_tuples = defaultdict(lambda: defaultdict(list))
        obs_attrs = defaultdict(dict)
        for tuple in list_of_tuples['tuples']:
            if 'obs' in tuple:
                dates_in_tuples.add(tuple['attrs'][date_var])
                for ind, params in self._parameters.items():
                    obs_var = list(tuple['obs'])[0]
                    base_vars = [self._dict_of_variables[param]['variable'] for param in params[1:]]
                    if obs_var in params[1:]:
                        ind_tuples[ind]['tuples'].append(tuple)
                        ind_tuples[ind]['vars'].append(obs_var)
                        ind_tuples[ind]['attrs'].extend(list(tuple['attrs']))
                        if obs_var in obs_attrs[ind]:
                            obs_attrs[ind][obs_var] = list(set(obs_attrs[ind][obs_var] + list(tuple['attrs'])))
                        else:
                            obs_attrs[ind][obs_var] = list(tuple['attrs'])
                    elif obs_var in base_vars:
                        i = base_vars.index(obs_var)+1
                        if all(tuple['attrs'][modif['variable']]==modif['value'] for modif in self._dict_of_variables[params[i]]['modifiers'] if modif['variable'] in tuple['attrs']):
                        
                            ind_tuples[ind]['tuples'].append(tuple)
                            ind_tuples[ind]['vars'].append(obs_var)
                            ind_tuples[ind]['attrs'].extend(list(tuple['attrs']))
                            if obs_var in obs_attrs[ind]:
                                obs_attrs[ind][obs_var] = list(set(obs_attrs[ind][obs_var] + list(tuple['attrs'])))
                            else:
                                obs_attrs[ind][obs_var] = list(tuple['attrs'])
        if 'age' in path:
            print(f' The ind_tuples is: {ind_tuples["rt_number"]}')
            print(f' The obs_attrs is: {obs_attrs}')
        #print(f' Dates in tuples are: {dates_in_tuples}')
                            
        indicators_to_calculate = []
        dls_variables = [col['variable'] for col in job['dls_json']['columns']]
        if ind_tuples:
            for ind, params in self._parameters.items(): 
                base_vars = [self._dict_of_variables[param]['variable'] for param in params[1:]]
                if ind not in dls_variables :
                    if len(set(base_vars).intersection(set(ind_tuples[ind]['vars'])))== len(params)-1:
                        indicators_to_calculate.append(ind)
        print(f'indicators to calculate: {indicators_to_calculate}')
        if indicators_to_calculate:
            sorted_dates = sorted(list(dates_in_tuples))
            for ind in indicators_to_calculate:
                attr_set = {attr  for tuple in ind_tuples[ind]['tuples'] for attr in list(tuple['attrs'])} 
                attr_set_filtered = {attr for attr in attr_set if self._dict_of_variables[attr]['type']!='not_characteristic'}
                params = self._parameters[ind]
                # read the functin code from indcators directory
                function_path = self.pandem_path(f'files/indicators/{ind}', 'function.R')
                with open(function_path) as f:
                    function_code = f.readlines()
                staging_dir = self.pandem_path(f'files/staging/{ind}')
                if not os.path.exists(staging_dir):
                    os.makedirs(staging_dir)
                exec_file_path = os.path.join(staging_dir, 'exec.R')
                result_path = os.path.join(staging_dir, 'result.json')
                # write the R script within the exec.R file
                base_vars = [self._dict_of_variables[param]['variable'] for param in params[1:]]
                execf = open(exec_file_path, 'w+')
                for i, param in enumerate(base_vars):
                    param_file_path = os.path.join(staging_dir, param+'.json')
                    execf.write(f'p{i+1} <- jsonlite::fromJSON("{param_file_path}")'+'\n')
                if len (function_code)>1:
                    for code_line in function_code[:-1]:
                        execf.write(code_line)
                execf.write(f'res <- {function_code[-1]}'+'\n')
                execf.write(f'jsonlite::write_json(res, "{result_path}")'+'\n')
                execf.close()
                # retrieve attributes values for tuples related to an indicator
                attr_values = dict()
                for attr in attr_set_filtered:
                    attr_values[attr] = list(set([tuple['attrs'][attr] for tuple in ind_tuples[ind]['tuples'] if attr in tuple['attrs']]))
                #print(f' The attr_values are: {attr_values}')
                # find attributes related to an indicator parameters other than reporting_period and period_type
                attrs_ind = list(set(ind_tuples[ind]['attrs']).intersection(attr_set_filtered) - {date_var, 'period_type'})
                #print(f' The arrs_id are: {attrs_ind}')
                # retrieve the list of values combinations of attributes related to an indicator other than reporting_period
                combinations = list(it.product(*(attr_values[attr] for attr in attrs_ind)))
                #print(f' The combinations are: {combinations}')
                print(f' Dates are: {dates_in_tuples}')
                for attr_comb in combinations:
                    non_charac_attrs = dict()
                    params_values = defaultdict(dict)
                    for tuple in ind_tuples[ind]['tuples']:
                        for date in sorted_dates:
                            if tuple['attrs'][date_var]==date: 
                                for param in set(ind_tuples[ind]['vars']):
                                    if param in tuple['obs']:
                                        if all(tuple['attrs'][attr]==val for attr,val in zip(attrs_ind, attr_comb) if attr in obs_attrs[ind][param]):
                                            non_charac_attrs['source'] = tuple['attrs']['source']
                                            non_charac_attrs['file'] = tuple['attrs']['file']
                                            non_charac_attrs['line_number'] = tuple['attrs']['line_number']
                                            if type(tuple['obs'][param]) in [int64]:
                                                params_values[date][param] = int(tuple['obs'][param])
                                            else:
                                                params_values[date][param] = tuple['obs'][param]
                    print(f' Tha param values for comb: {attr_comb} are: {params_values}')
                    # write params values within temporary files
                    base_vars = [self._dict_of_variables[param]['variable'] for param in params[1:]]
                    for i, param in enumerate(base_vars):
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
                            date = sorted_dates[i]
                            ind_date_tuple = {'obs': {ind:res},
                                            'attrs':{**{'reporting_period':date, 'period_type':'date'},
                                                     **{k:v for k,v in zip(attrs_ind, attr_comb)},
                                                     **non_charac_attrs}
                                            }
                            tuples_with_ind['tuples'].append(ind_date_tuple)
        self._pipeline_proxy.calculate_end(tuples = tuples_with_ind, path = path, job = job)
 