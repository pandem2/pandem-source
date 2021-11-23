import os
import time
from datetime import datetime
import threading
from . import worker
from . import util
from abc import ABC, abstractmethod, ABCMeta
import pandas as pd
import numpy as np
import json

class DataframeReader(worker.Worker):
    __metaclass__ = ABCMeta

    def __init__(self, name, orchestrator_ref, storage_ref, settings):
        super().__init__(name = name, orchestrator_ref = orchestrator_ref, settings = settings)


    def on_start(self):
        super().on_start()
        self._variables_proxy=self._orchestrator_proxy.get_actor('variables').get().proxy()
        self._pipeline_proxy=self._orchestrator_proxy.get_actor('pipeline').get().proxy()

    def get_variables(self):
        return self._variables_proxy.get_variables().get()

    def check_df_columns(self, df, job, dls, file_name, variables):
        """Checks if the DLS columns names exist in dataframe
        and if the variable indicated in the column element from dls exists in variables.json files
        and if yes, checks the dataframe variables types, if not as expected in variables.json, raises an issue"""

        var=variables
        dls_col_list=[]
        issues = {}

        if 'columns' in dls.keys() :
            dls_col_list = dls['columns']
        else :
            message=("DLS file not conform, 'columns' element is missing.")
            issue={ 'step': job['step'],                       
                    'line' : 0,                        
                    'source' : dls['scope']['source'],
                    'file' : file_name,
                    'message' : message,
                    'raised_on' : datetime.now(),
                    'job_id' : job['id'],
                    'issue_type' : "dls-not-conform"}
            issues["DLS file"] = issue

        
        for item in dls_col_list:
            if "name" not in item or "variable" not in item: 
              message=("DLS file not conform, 'columns' must contain a list of objects containint elements 'name' and 'variable'")
              issue={ 'step': job['step'],                       
                    'line' : 0,                        
                    'source' : dls['scope']['source'],
                    'file' : file_name,
                    'message' : message,
                    'raised_on' : datetime.now(),
                    'job_id' : job['id'],
                    'issue_type' : "dls-not-conform"}
              issues["DLS file"] = issue
            elif item['name'] not in df.columns :
                message=(f"Column '{item['name']}' not found in dataframe ")
                issue={ 'step' : job['step'],
                        'line' : 0,
                        'source' : dls['scope']['source'],
                        'file' : file_name,
                        'message': message,
                        'raised_on' : datetime.now(),
                        'job_id' : job['id'],
                        'issue_type' : "col-not-found"}
                issues[item["name"]] = issue
            elif item['variable'] in var:
                if var[item['variable']]['unit'] == 'date':
                    if df[item['name']].dtypes in ['date', 'object', 'str'] :
                        issues[item["name"]] = None
                    else :
                        message = (f"Type '{df[item['name']].dtypes}' in source file is not compatible with variable {item['variable']} with unit 'date' ")
                        issue={ 'step' : job['step'],
                                'line' : 0,
                                'source' : dls['scope']['source'],
                                'file' : file_name,
                                'message' : message,
                                'raised_on' : datetime.now(),
                                'job_id' : job['id'],
                                'issue_type' : "ref-not-found"}
                        issues[item["name"]] = issue
                elif var[item['variable']]['unit'] == 'people':
                    if df[item['name']].dtypes in ['integer', 'str', "obj", "float64", "int64"] :
                        issues[item["name"]] = None
                    else :
                        message = (f"Type '{df[item['name']].dtypes}' in source file is not compatible with variable {item['variable']} with unit 'integer' ")
                        issue = {'step' : job['step'],
                                'line' : 0,
                                'source' : dls['scope']['source'],
                                'file' : file_name,
                                'message' : message,
                                'raised_on' : datetime.now(),
                                'job_id' : job['id'],
                                'issue_type' : "ref-not-found"}
                        issues[item["name"]] = issue

                elif var[item['variable']]['unit'] == None :
                    issues[item["name"]] = None
                elif var[item['variable']]['unit'] == 'string' :
                    issues[item["name"]] = None
            else :
                message = (f"Variable {item['variable']} defined on source definition file is unknown")
                issue = {'step' : job['step'],
                        'line' : 0,
                        'source' : dls['scope']['source'],
                        'file' : file_name,
                        'message' : message,
                        'raised_on' : datetime.now(),
                        'job_id' : job['id'],
                        'issue_type' : "ref-not-found"
                }
                issues[item["name"]] = issue
        return issues


    def translate(self, value, dtype, unit, parse_format = None):   # parameters to pass after : df, dls, file_name

        """Convert the format of dataframe column if expected unit is different"""

        if unit is None or unit in ["str"] :
            if dtype in ["str"]:
                return value
            else:
                return str(value)
        elif unit == 'date' :
            if dtype == 'date':
                return value
            else:
              return datetime.strptime(value, format)

        elif unit in ["people", "int"] :
            if dtype in ["int", "int64"]:
                return value
            elif np.isnan(value):
                return None
            else:
                return int(value)
        else: 
          raise ValueError("undefined case for casting")
    

    def df2tuple(self, df, path, job, dls): 
        variables = self.get_variables()
        dls_col_list=[]
        file_name = util.absolute_to_relative(path, "files/staging/")
       
        # adding column line number if it does not exists
        if 'line_number' not in df.columns :
            df['line_number'] = range(1, len(df)+1) 

        # Checking column names and compatible types
        col_issues = self.check_df_columns(df = df, job = job, dls = dls, file_name = path, variables = variables)
        issues = list([i for i in col_issues.values() if i is not None]) 

        types_ok = dict((t["name"],  t["name"] in col_issues and col_issues[t["name"]] is None) for t in  dls['columns'])
        col_vars = dict((t["name"], t["variable"]) for t in  dls['columns'])
        col_types = dict((k,variables[v]["type"]) for k,v in col_vars.items() if v in variables)
        insert_cols = dict((t["name"], ("obs" if col_types[t["name"]] in ["observation", "indicator"] else "attr")) 
          for t in  dls['columns'] 
          if types_ok[t["name"]] and
            ("action" in t and t["action"] == "insert" or col_types[t["name"]] in ["observation", "indicator"])
        )
        attr_cols = {}
        for col, typ in insert_cols.items():
          if typ in ["observation", "indicator"]:
            attr_cols[col] =  [k for k, v in col_types.items() if v in  ["observation", "indicator"] and types_ok[k]]
          else:
            attr_cols[col] =  [k for k, v in col_vars.items() if 
              types_ok[k] and  
              (
                (variables[col_vars[col]]["linked_attributes"] is not None and  
                v in variables[col_vars[col]]["linked_attributes"]) or
                  col_types[k] in ["characteristic"]
              )
            ]

        tuples = []
        for row in range(len(df)):
            for col, group in insert_cols.items():
                tup = {"attrs":{
                    "line_number":df["line_number"][row]
                    ,"source":dls["scope"]["source"]
                    ,"file":file_name
                  }
                }
                try:
                  tup[group] = {
                      col_vars[col]:self.translate(df[col][row], df[col].dtypes, variables[col_vars[col]]["unit"])
                  }
                except Exception as e:
                  issues.append({
                    "step":job['step'],
                    "line":df["line_number"][row],
                    "source":dls['scope']['source'],
                    "file":file_name,
                    "message":f"Cannot cast value {df[col][row]} into unit {variables[col_vars[col]]['unit']} \n {e}",
                    "raised_on":datetime.now(),
                    "job_id":job['id'],
                    "issue_type":"cannot-cast"
                  })
                for attr_col in attr_cols[col]:
                     val = None
                     try:
                       val = self.translate(df[attr_col][row], df[attr_col].dtypes, variables[col_vars[attr_col]]["unit"])
                     except Exception as e:
                       issues.append({
                         "step":job['step'],
                         "line":df["line_number"][row],
                         "source":dls['scope']['source'],
                         "file":file_name,
                         "message":f"Cannot cast value {df[attr_col][row]} into unit {variables[col_vars[attr_col]]['unit']} \n {e}",
                         "raised_on":datetime.now(),
                         "job_id":job['id'],
                         "issue_type":"cannot-cast"
                       })
                     if val is not None:
                       tup["attrs"][col_vars[attr_col]] = val
                tuples.append(tup)
        
        ret = {"scope":dls["scope"].copy()}
        ret["scope"]["file_name"] = file_name
        # validating that globals are property instantiated
        #if "scope" in dls and "globals" in dls["scope"] : 
        #  ret["scope"]["globals"] = self.add_values(dls["scope"]["globals"], tuples = [], issues = issues, dls = dls, job = job, file_name = file_name)
        # instantiating update scope based on existing values on tuple
        if "scope" in dls and "update_scope" in dls["scope"]:
          ret["scope"]["update_scope"] = self.add_values(dls["scope"]["update_scope"], tuples = tuples, issues = issues, dls = dls, job = job, file_name = file_name)
        ret["tuples"] = tuples
        
        self._pipeline_proxy.read_df_end(tuples = ret, issues = issues, path = path, job = job)
    

    def add_values(self, var_list, tuples, issues, dls, job, file_name):
        ret = []
        for var in var_list:
          if "variable" in var and "value" in var :
            # all good no need to get distinct values
            if not isinstance(var["value"], list):
              r = var.copy()
              r["value"] = [var["value"]]
              ret.append(r)
            else :
              ret.append(var)
          elif "variable" in var :
            # value is midding we are going to try to get it from tuples
            r = var.copy()
            r["value"] = list(set(x["attrs"][var["variable"]] for x in tuples if var["variable"] in x["attrs"]))
            ret.append(r)
          else :
            message = (f"Variable {var['variable']} needs to be instantiated ad per DLS but no valuer where found on dataset ")
            issue = {'step' : job['step'],
                    'line' : 0,
                    'source' : dls['scope']['source'],
                    'file' : file_name,
                    'message' : message,
                    'raised_on' : datetime.now(),
                    'job_id' : job['id'],
                    'issue_type' : "cannot-instantiate"
            }
            issues.append(issue)
        return ret


