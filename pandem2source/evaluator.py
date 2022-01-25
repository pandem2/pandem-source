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
import logging as l




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

 
    def pre_calculate(self, list_of_tuples, path, job):
        date_var = 'reporting_period'
        ind_tuples = defaultdict(lambda: defaultdict(list))
        obs_attrs = defaultdict(dict)
        ind_values_in_tuples = dict()
        for ind, params in self._parameters.items():
            ind_values_in_tuples[ind] = []
        for tuple in list_of_tuples['tuples']:
            if 'obs' in tuple:
                for ind, params in self._parameters.items():
                    obs_var = list(tuple['obs'])[0]
                    if obs_var == ind : #and tuple['obs'][obs_var]
                        ind_values_in_tuples[ind].append(tuple['obs'][obs_var])
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
        indicators_to_calculate = defaultdict(dict)
        dls_variables = [col['variable'] for col in job['dls_json']['columns']]
        if ind_tuples:
            for ind, params in self._parameters.items(): 
                base_vars = [self._dict_of_variables[param]['variable'] for param in params[1:]]
                if ind not in dls_variables or not ind_values_in_tuples[ind]:
                    if len(set(base_vars).intersection(set(ind_tuples[ind]['vars'])))== len(params)-1:
                        indicators_to_calculate[ind]['base_vars'] = base_vars    
        attrs_ind = []
        combinations = []
        if indicators_to_calculate:
            for ind in indicators_to_calculate.keys():
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
                base_vars = indicators_to_calculate[ind]['base_vars']
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
                # find attributes related to an indicator parameters other than reporting_period and period_type
                attrs_ind = list(set(ind_tuples[ind]['attrs']).intersection(attr_set_filtered) - {date_var, 'period_type'})
                # retrieve the list of values combinations of attributes related to an indicator other than reporting_period
                combinations = list(it.product(*(attr_values[attr] for attr in attrs_ind)))
                indicators_to_calculate[ind]['attrs'] = attrs_ind
                indicators_to_calculate[ind]['comb_values'] = combinations
                indicators_to_calculate[ind]['obs_attrs'] = obs_attrs[ind]
            indicators_to_calculate['update_scope'] = list_of_tuples['scope']['update_scope']
        l.debug(f'indicators to calculate for source: {job["source"]} are: {indicators_to_calculate}')
        self._pipeline_proxy.precalculate_end(indicators_to_calculate, tuples=list_of_tuples, path=path, job=job)


    def calculate(self, indicators_to_cal, path, job):
        l.warning("delete path since it is ignored on this step")
        date_var = 'reporting_period'
        indicators = {"tuples": [],
                      "scope": {}}
        if 'datahub' in job['source']:
            print(f' Path : {path} is now processed')

        if indicators_to_cal:
            indicators["scope"]['update_scope'] = indicators_to_cal['update_scope']
            indicators_to_cal.pop('update_scope')
            l.warning("put tuples on a different attribure 'tuples'")
            for ind, ind_map in indicators_to_cal.items():
                for attr_comb in ind_map['comb_values']:
                    ind_tuples = dict()
                    dates_in_tuples =dict()
                    filter = dict(zip(ind_map['attrs'], attr_comb))
                    filter['source'] = job['source']
                    for obs in ind_map['base_vars']:
                        obs_filter = {k:v for k,v in filter.items() if k in ind_map['obs_attrs'][obs]}
                        
                        for attr in ind_map['obs_attrs'][obs]:
                            # TODO temporary solution until standarsation modified
                            if attr in ['geo_name', 'country_name']:
                                l.warning("FIX stadardisation and remove me")
                                obs_filter.pop(attr)
                            # replace referential_aliases by linked_attributes here (look at dict of variables) 
                            # if self._dict_of_variables[attr]['type']=='referential_alias':
                            #     attr_alias = self._dict_of_variables[attr]['linked_attributes'][0]
                            #     attr_alias_val = [tuple['attrs'][attr_alias] for tuple in  self._variables_proxy.get_referential(attr).get() if tuple['attr'][attr]==filter[attr].upper()][0]
                            #     obs_filter.pop(attr)
                            #     obs_filter[attr_alias] = attr_alias_val
                        ind_tuples[obs] = self._variables_proxy.read_variable(obs, obs_filter).get()
                        dates_in_tuples[obs] = [tuple['attrs'][date_var] for tuple in ind_tuples[obs]]
                    
                    ind_dates = set.intersection(*[set(dates_in_tuples[obs]) for obs in dates_in_tuples.keys()])
                    sorted_ind_dates = sorted(list(ind_dates))
                    if not sorted_ind_dates:
                        source_tags = job['dls_json']['scope']['tags']
                        if source_tags:
                            tagsource = self._variables_proxy.get_referential('tagsource').get()
                            
                            extended_sources = set([tuple['attrs']['source'] for tuple in tagsource if tuple['attrs']['tag'] in source_tags])
                            if len(extended_sources)>1:
                                filter['source'] = list(extended_sources)
                                for obs in ind_map['base_vars']:
                                    ind_tuples[obs] = self._variables_proxy.read_variable(obs, filter).get()
                                    dates_in_tuples[obs] = [tuple['attrs'][date_var] for tuple in ind_tuples[obs]]
                                ind_dates = set.intersection(*[set(dates_in_tuples[obs]) for obs in dates_in_tuples.keys()])
                                sorted_ind_dates = sorted(list(ind_dates))                    
                    if sorted_ind_dates:
                        params_values = defaultdict(dict)
                        for date in sorted_ind_dates:
                            for obs in ind_map['base_vars']:
                                obs_val = [tuple['obs'][obs] for  tuple in ind_tuples[obs] if tuple['attrs'][date_var]==date][0]
                                if type(obs_val) in [int64, str]:
                                    l.warning('Fix df reader and remove me')
                                    params_values[date][obs] = int(obs_val)
                                else:
                                    params_values[date][obs] = obs_val
                        #write params values within temporary files
                        staging_dir = self.pandem_path(f'files/staging/{ind}')
                        exec_file_path = os.path.join(staging_dir, 'exec.R')
                        result_path = os.path.join(staging_dir, 'result.json')
                        for i, obs in enumerate(ind_map['base_vars']):
                            param_values_list = [params_values[date][obs]  for date in params_values.keys() if obs in params_values[date]]
                            param_file_path = os.path.join(staging_dir, obs+'.json')
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
                                date = sorted_ind_dates[i]
                                l.warning("take out period type from here!!")
                                ind_date_tuple = {'obs': {ind:res},
                                                'attrs':{**{date_var:date, 'period_type':'date'},
                                                            **{k:v for k,v in zip(ind_map['attrs'], attr_comb)}
                                                            }
                                                }
                                indicators['tuples'].append(ind_date_tuple)
                    

        self._pipeline_proxy.calculate_end(indicators, path = path, job = job).get()
