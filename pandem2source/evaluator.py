import os
from . import worker
from abc import ABC, abstractmethod, ABCMeta
from datetime import datetime, timedelta
import time
import re
from collections import defaultdict

class Evaluator(worker.Worker):
    __metaclass__ = ABCMeta  
    def __init__(self, name, orchestrator_ref, settings): 
        super().__init__(name = name, orchestrator_ref = orchestrator_ref, settings = settings)    
         
    def on_start(self):
        super().on_start()
        self._storage_proxy = self._orchestrator_proxy.get_actor('storage').get().proxy()
        self._pipeline_proxy = self._orchestrator_proxy.get_actor('pipeline').get().proxy() 
        self._variables_proxy = self._orchestrator_proxy.get_actor('variables').get().proxy() 
        self._parameters, self._indicators = self.get_indicators(self._variables_proxy.get_variables().get())

    def source_path(self, dls, *args):
        if dls['acquisition']['channel']['name']=='input-local':
            return self.pandem_path(f'files/{self.channel}', *args)
        else:
            return self.pandem_path(f'files/{self.channel}', dls['scope']['source'], *args)

    def get_indicators(self, dic_of_variables):
        formulas = {var['variable']:var['formula'] for var in dic_of_variables if var['type']=='indicator' and var['formula']} #only indicators with formula not None
        parameters = {ind:re.findall (r'([^(, )]+)(?!.*\()', formula) for ind, formula in formulas.items()}
        indicators = dict()
        params_set = {params[i] for ind, params in parameters.items() for i in range(len(params))}
        for param in params_set:
            ind_list = [ind for ind, params in parameters.items() if param in params]
            indicators[param] = ind_list
        return parameters, indicators
        # It will read the formulas of all indicators
        # It will parse the functions spliting the text and getting the variables that are used on the formula
        # It will store two maps:
        #     - indicators: an entry for each variable used on an indicator as a parameter and returning a list of indicators where this variable is used. eg. indicators[“population”]=[“Incidence”, “prevalence, …”]
        #     - parameters: an entry for each indicator, returning a list for each parameters. e.g. parameters[“incidence”]=[“reporting_period”, “number_of_cases”, “population”]
        # This function will be called on start





    def calculate(self, list_of_tuples):
        indicators_to_calculate = []
        obs_set = {tuple['obs'] for tuple in list_of_tuples and 'obs' in tuple}
        attr_set = {tuple['attr'] for tuple in list_of_tuples and 'attr' in tuple}
        indicators_to_calculate = []
        for obs in obs_set:
            if obs  in self._indicators:
                indicators_to_calculate.extend(self._indicators[obs])
        for attr in attr_set:
            if attr in self._indicators:
                indicators_to_calculate.extend(self._indicators[attr])
        indicators_to_calculate = set(indicators_to_calculate)
        params_values = dict()   #defaultdict(dict)
        for ind in indicators_to_calculate:
            params_values[ind]['reporting_period'] = 
            #for param in self._parameters[ind]:
         

        pass
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
 