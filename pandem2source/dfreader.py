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
import isoweek
import logging
import re
l = logging.getLogger("pandem.dfreader")

class DataframeReader(worker.Worker):
    __metaclass__ = ABCMeta

    def __init__(self, name, orchestrator_ref, storage_ref, settings):
        super().__init__(name = name, orchestrator_ref = orchestrator_ref, settings = settings)


    def on_start(self):
        super().on_start()
        self._variables_proxy=self._orchestrator_proxy.get_actor('variables').get().proxy()
        self._pipeline_proxy=self._orchestrator_proxy.get_actor('pipeline').get().proxy()
        self._storage_proxy = self._orchestrator_proxy.get_actor('storage').get().proxy()


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
                    'issue_type' : "dls-not-conform",
                    'issue_severity':"error"}
            issues["DLS file"] = issue

        
        for item in dls_col_list:
            if "name" not in item: 
              message=("DLS file not conform, 'columns' must contain a list of objects containint elements 'name'")
              issue={ 'step': job['step'],                       
                    'line' : 0,                        
                    'source' : dls['scope']['source'],
                    'file' : file_name,
                    'message' : message,
                    'raised_on' : datetime.now(),
                    'job_id' : job['id'],
                    'issue_type' : "dls-not-conform",
                    'issue_severity':"error"
                    }
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
                        'issue_type' : "col-not-found",
                        'issue_severity':"error"}                       
                issues[item["name"]] = issue
            elif 'variable' in item and item['variable'] in var:
                if var[item['variable']]['unit'] == 'date':
                    if df[item['name']].dtypes in ['date', 'object', 'str', 'datetime64[ns]', 'int', 'int64'] :
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
                                'issue_type' : "ref-not-found",
                                'issue_severity':"warning"}
                        issues[item["name"]] = issue
                elif self.is_numeric_unit(var[item['variable']]['unit']):
                    if df[item['name']].dtypes in ['integer', 'str', "object", "float", "float64", "int64", 'int'] :
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
                                'issue_type' : "ref-not-found",
                                'issue_severity':"warning"}
                        issues[item["name"]] = issue

                elif var[item['variable']]['unit'] == None :
                    issues[item["name"]] = None
                elif var[item['variable']]['unit'] in ['string', 'range'] :
                    issues[item["name"]] = None
            elif "variable" in item:
                message = (f"Variable {item['variable']} defined on source definition file is unknown")
                issue = {'step' : job['step'],
                        'line' : 0,
                        'source' : dls['scope']['source'],
                        'file' : file_name,
                        'message' : message,
                        'raised_on' : datetime.now(),
                        'job_id' : job['id'],
                        'issue_type' : "ref-not-found",
                        'issue_severity':"error"
                }
                issues[item["name"]] = issue
        return issues


    def translate(self, value, dtype, unit, format = {}):   # parameters to pass after : df, dls, file_name
        """Convert the format of dataframe column if expected unit is different"""
        if pd.isna(value):
            return None
        if unit is None or unit in ["str"] :
            if dtype in ["str"]:
                return value
            else:
                return str(value)
        elif unit == 'date':
            if np.issubdtype(dtype, np.datetime64):
                return value
            elif type(value) == datetime:
                return value
            elif format.get("date_format") is None:
                raise AssertionError("You have to provide a format to parse a date on Data source definition, on ['acquisition']['format']['date_format']")
            elif format.get("date_format") =='isoweek':
                if dtype in ["int", "int64", "float", "float64"]:
                    return isoweek.Week(int(int(value)/100),int(value)%100).thursday()
                else:
                    year = [int(v) for v in re.findall("[0-9]+", value) if int(v) > 100][0]
                    week = [int(v) for v in re.findall("[0-9]+", value) if int(v) < 100][0]
                    return isoweek.Week(year,week).thursday()
            else:
                return datetime.strptime(value, format.get('date_format'))
        elif self.is_numeric_unit(unit) :
            if dtype in ["int", "int64", "float", "float64"]: #, "object"
              if np.isnan(value):
                return None
              else:
                return value
            else:
                value = str(value).replace(" ","")
                if "thousands_separator" in format and format["thousands_separator"]!=".":
                  value = value.replace(format["thousands_separator"], "") 
                if "decimal_separator" in format and format["decimal_separator"]!=".":
                  value = value.replace(format["decimal_separator"], ".") 
                return float(value)
        elif unit=='range':
            return self.transform_range(value)
        else: 
          raise ValueError("undefined case for casting")
    

    def df2tuple(self, df, path, job, dls):
        variables = self.get_variables()
        # getting df from cache
        df = df.value()
        dls_col_list=[]
        file_name = util.absolute_to_relative(path, "files/staging/")

        # Checking column names and compatible types
        col_issues = self.check_df_columns(df = df, job = job, dls = dls, file_name = path, variables = variables)
        issues = list([i for i in col_issues.values() if i is not None]) 
        types_ok = dict((t["name"],  t["name"] in col_issues and col_issues[t["name"]] is None) for t in  dls['columns'])
        col_vars = dict((t["name"], t["variable"]) for t in  dls['columns'] if 'variable' in t)
        formats = {v:self.format_for(v, dls, variables) for v in col_vars.values()}
        col_types = dict((k,variables[v]["type"]) for k,v in col_vars.items() if v in variables)
        insert_cols = dict((t["name"], ("obs" if col_types[t["name"]] in ["observation", "indicator", "resource"] else "attr")) 
          for t in  dls['columns'] 
          if types_ok[t["name"]] and
            'variable' in t and
            ("action" in t and t["action"] == "insert" or col_types[t["name"]] in ["observation", "indicator", "resource"])
        )
        attr_cols = {}
        for col  in insert_cols.keys():
          typ = col_types[col]
          if typ in ["observation", "indicator", "resource"]:
            attr_cols[col] =  [k for k, v in col_types.items() if v not in  ["observation", "indicator", "resource"] and types_ok[k]]
          else:
            attr_cols[col] =  [k for k, v in col_vars.items() if 
              types_ok[k] and  
              (
                (variables[col_vars[col]]["linked_attributes"] is not None and  
                v in variables[col_vars[col]]["linked_attributes"]) or
                  col_types[k] in ["characteristic", "not_characteristic", "private", "date", "referential_parent"]
              )
            ]
        tuples = []
        for row in range(len(df)):
            for col, group in insert_cols.items():
                tup = {
                  "attrs":{
                    "line_number":df["line_number"][row]
                    ,"source":dls["scope"]["source"]
                    ,"file":file_name
                  },
                }
                self.translate_or_issue(
                    tup = tup,
                    group = group,
                    val = df[col][row], 
                    format = formats[col_vars[col]],
                    issues = issues, 
                    dtype = df[col].dtypes, 
                    unit = variables[col_vars[col]]["unit"], 
                    dls = dls, 
                    job = job, 
                    line_number = df["line_number"][row], 
                    file_name = file_name,
                    var_name = col_vars[col],
                    variables = variables
                )
                for attr_col in attr_cols[col]:
                    self.translate_or_issue(
                        tup = tup,
                        group = "attrs",
                        val = df[attr_col][row], 
                        format = formats[col_vars[attr_col]],
                        issues = issues, 
                        dtype = df[attr_col].dtypes, 
                        unit = variables[col_vars[attr_col]]["unit"], 
                        dls = dls, 
                        job = job, 
                        line_number = df["line_number"][row], 
                        file_name = file_name,
                        var_name = col_vars[attr_col],
                        variables = variables
                    )
                tuples.append(tup)
        ret = {"scope":dls["scope"].copy()}
        ret["scope"]["file_name"] = file_name

        # validating that globals are property instantiated
        #if "scope" in dls and "globals" in dls["scope"] : 
        #  ret["scope"]["globals"] = self.add_values(dls["scope"]["globals"], tuples = [], issues = issues, dls = dls, job = job, file_name = file_name)
        # instantiating update scope based on existing values on tuple
        if "scope" in dls and "update_scope" in dls["scope"]:
          ret["scope"]["update_scope"] = self.add_values(dls["scope"]["update_scope"], tuples = tuples, variables = variables, issues = issues, dls = dls, job = job, file_name = file_name)
        ret["tuples"] = tuples

        ret = self._storage_proxy.to_job_cache(job["id"], f"tup_{path}", ret).get()
        self._pipeline_proxy.read_df_end(tuples = ret, n_tuples = len(tuples), issues = issues, path = path, job = job)
    

    def add_values(self, var_list, tuples, variables, issues, dls, job, file_name):
        ret = []
        for var in var_list:
          if "variable" not in var:
            pass
          else :
            # Getting base varabe if a derivated variabe
            var_name = var["variable"]
            base_var = variables[var_name]["variable"]
            r = {"variable": base_var}
            modifiers  = self.add_alias_context({"attrs":{}}, var_name, variables)
            for k, v in modifiers["attrs"].items():
              ret.append({"variable":k, "value":v})
            
            if "value" in var :
              # all good no need to get distinct values just making sure the value is put as an array
              if not isinstance(var["value"], list):
                r["value"] = [var["value"]]
              else :
                r["value"] = var["value"]
            else :
              # value is missing we are going to try to get it from tuples
              r["value"] = list(set(x["attrs"][base_var] for x in tuples if base_var in x["attrs"]))
            ret.append(r)
            
            if len(r["value"])==0:
              message = (f"Variable {var['variable']} needs to be instantiated ad per DLS but no values where found on dataset")
              issue = {'step' : job['step'],
                      'line' : 0,
                      'source' : dls['scope']['source'],
                      'file' : file_name,
                      'message' : message,
                      'raised_on' : datetime.now(),
                      'job_id' : job['id'],
                      'issue_type' : "cannot-instantiate",
                      'issue_severity':"warning"
              }
              issues.append(issue)
        return ret
    def translate_or_issue(self, tup, group, val, format, issues, dtype, unit, dls, job, line_number, file_name, var_name, variables):
        trans = None
        try:
          trans = self.translate(val, dtype, unit, format)
        except AssertionError as e:
          issues.append({
            "step":job['step'],
            "line":line_number,
            "source":dls['scope']['source'],
            "file":file_name,
            "message":f"Cannot cast value {val} into unit {unit} as {dtype}\n {e}",
            "raised_on":datetime.now(),
            "job_id":job['id'],
            "issue_type":"cannot-cast",
            'issue_severity':"error"
          })
        except Exception as e:
          issues.append({
            "step":job['step'],
            "line":line_number,
            "source":dls['scope']['source'],
            "file":file_name,
            "message":f"Cannot cast value {val} into unit {unit} as {dtype}\n {e}",
            "raised_on":datetime.now(),
            "job_id":job['id'],
            "issue_type":"cannot-cast",
            'issue_severity':"warning"
          })
        if trans is not None:
            if group not in tup:
              tup[group] = {}
            tup[group][variables[var_name]["variable"]] = trans
            self.add_alias_context(tup, var_name, variables)

    def add_alias_context(self, tup, var_name, variables):
        if var_name in variables and "modifiers" in variables[var_name]:
            for mod in variables[var_name]["modifiers"]:
              mod_var = mod["variable"]
              if not "attrs" in tup:
                tup["attrs"] = {}
              if mod_var not in tup["attrs"] or tup["attrs"][mod_var] is None:
                tup["attrs"][mod_var] = mod["value"]
        return tup

    def transform_range(self, value):
      if '>' in value:
        a = value.strip().split('>')[1].split('y')[0]
        b = ''
      elif '-' in value:
        a = value.strip().split('-')[0]
        b = value.strip().split('-')[1].split('y')[0]
      else:
        a = 0
        b = value.strip().split('y')[0]
      return f'{a}-{b}'

    def is_numeric_unit(self, unit):
      if unit is None:
        return False 

      unit = unit.lower().replace(" ", "")
      if unit in ['people', 'number', 'qty', 'days', 'people/people', 'units/time']:
        return True
      return False

    def format_for(self, var_name, dls, variables):
      col_def = list([col for col in dls["columns"] if "variable" in col and col["variable"]==var_name])
      format = {}
      for attr in ["decimal_sign", "thousands_separator", "date_format"]:
        if len(col_def) > 0 and attr in col_def[0]:
          format[attr] = col_def[0][attr]
        elif "acquisition" in dls and "format" in dls["acquisition"] and attr in dls["acquisition"]["format"]: 
          format[attr] =  dls["acquisition"]["format"][attr]
      return format
