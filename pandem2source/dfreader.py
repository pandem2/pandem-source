import os
import time
from datetime import datetime
import threading
from . import worker
from abc import ABC, abstractmethod, ABCMeta
import pandas as pd
import json


class DataframeReader(worker.Worker):
    #Temporary delete of the Worker class to develop the DF2variables function
    __metaclass__ = ABCMeta

    def __init__(self, name, orchestrator_ref, storage_ref, settings):
        super().__init__(name = name, orchestrator_ref = orchestrator_ref, settings = settings)
        self._orchestrator_proxy = orchestrator_ref.proxy()
        self._storage_proxy = storage_ref.proxy()
        print(f'here in {self.name} __init__')


    def on_start(self):
        super().on_start()
        self._variables_proxy=self._orchestrator_proxy.get_actor('variables').get().proxy()
        print(f'here in {self.name} on-start')

    def read_df_start(self, dls):
        pass


    def send_heartbeat(self):
        #already implemented for parent class. To delete
        self._orchestrator_proxy.get_heartbeat(self.name)


    def actor_loop(self):
        while True:
            self.send_heartbeat()
            time.sleep(20)

    def get_df(self):
        if os.path.exists(os.path.join(os.getenv('PANDEM_HOME'), 'files/variables')):
            df = pd.read_csv(os.path.join(os.getenv('PANDEM_HOME'), 'files/variables/18725.csv'))
            return df
        else:
            print("NO CSV FILE FOUND")
            return ''


    def get_dls(self):
        if os.path.exists(os.path.join(os.getenv('PANDEM_HOME'), 'files/variables')):
            dls = json.load(open(os.path.join(os.getenv('PANDEM_HOME'), 'files/variables/covid19-datahub.json')))
            return dls
        else:
            print("NO JSON FILE FOUND")
            return ''

    def get_variables(self):
            variables = self._variables_proxy.get_variables().get()
            return variables


    def check_df_columns(self):                              #to pass in parameters : df, dls, file_name

        """Checks if the DLS columns names exist in dataframe"""

        df = self.get_df()
        dls = self.get_dls()
        dls_col_list=[]
        issues = {}
        job={'job_id':123, 'step':'step-job'}                 # job ref ?
        file_name = "AAA"                                     # should be passed in parameters

        if 'columns' in dls.keys() :
            dls_col_list = dls['columns']

        else :
            message=("DLS file not conform, 'columns' element is missing.")
            issue={ 'step': job['step'],                       #id to add ? auto-increment ?
                    'line' : "unknown",                        #the column element is not found, no line to raise
                    'source' : dls['scope']['source'],
                    'file' : dls['scope']['source'],           #file_name DLS à passer en paramètre ? l'attribut n'existe pas dans le dls
                    'message' : message,
                    'raised_on' : datetime.now(),
                    'job_id' : job['job_id'],
                    'issue_type' : "dls-not-conform"}
            issues["DLS file"] = issue

        for item in dls_col_list:
            if item['name'] not in df.columns :
                message=(f"Column '{item['name']}' not found in dataframe ")
                issue={ 'step' : job['step'],                   #id to add ? auto-increment ?
                        'line' : 1,
                        'source' : dls['scope']['source'],
                        'file' : dls['scope']['source'],        #file_name DLS à passer en paramètre ? l'attribut n'existe pas dans le dls
                        'message': message,
                        'raised_on' : datetime.now(),
                        'job_id' : job['job_id'],
                        'issue_type' : "col-not-found"}
                issues[item["name"]] = issue

        return issues



    def check_df_var_type(self):                                  # parameters to pass after : df, dls, file_name

        """Checks if the variable indicated in the column element from dls exists in variables.json files
        and if yes, checks the dataframe variables types, if not as expected in variables.json, raises an issue"""

        df = self.get_df()
        dls = self.get_dls()
        var=self.get_variables()

        dls_col_list=[]
        job={'job_id':123, 'step':'step-job'}
        line_number=12
        file_name = "AAA"
        issues = {}

        if dls['columns']:
            dls_col_list = dls['columns']


        for item in dls_col_list :
            if item['variable'] in var:
                #print("unit for variable", item['variable'], "is", var[item['variable']]['unit'])  # TO DELETE

                if var[item['variable']]['unit'] == 'date':

                    if df[item['name']].dtypes in ['date', 'object', 'str'] :
                        issues[item["name"]] = None

                    else :
                        message = (f"Type '{df[item['name']].dtypes}' in source file is not compatible with variable {item['variable']} with unit 'date' ")
                        issue={ 'step' : job['step'],                   #id to add ? auto-increment ?
                                'line' : 1,
                                'source' : dls['scope']['source'],
                                'file' : dls['scope']['source'],         #file_name à passer en paramètre de la fonction à ajouter
                                'message' : message,
                                'raised_on' : datetime.now(),
                                'job_id' : job['job_id'],
                                'issue_type' : "ref-not-found"}
                        issues[item["name"]] = issue

                if var[item['variable']]['unit'] == 'people':

                    if df[item['name']].dtypes in ['integer', 'str', "obj"] :
                        issues[item["name"]] = None

                    else :   #issue raised if type is float
                        message = (f"Type '{df[item['name']].dtypes}' in source file is not compatible with variable {item['variable']} with unit 'integer' ")
                        issue = {'step' : job['step'],                     #id to add ? auto-increment ?
                                'line' : 1,
                                'source' : dls['scope']['source'],
                                'file' : dls['scope']['source'],         #file_name à passer en paramètre de la fonction à ajouter
                                'message' : message,
                                'raised_on' : datetime.now(),
                                'job_id' : job['job_id'],
                                'issue_type' : "ref-not-found"}
                        issues[item["name"]] = issue

                if var[item['variable']]['unit'] == None :
                    issues[item["name"]] = None

                if var[item['variable']]['unit'] == 'string' :
                    issues[item["name"]] = None
            else :
                message = ("Variable defined on source definition file is unknown")
                issue = {'step' : job['step'],                   #id to add ? auto-increment ?
                        'line' : "unknown",
                        'source' : dls['scope']['source'],
                        'file' : dls['scope']['source'],         #file_name à passer en paramètre de la fonction à ajouter
                        'message' : message,
                        'raised_on' : datetime.now(),
                        'job_id' : job['job_id'],
                        'issue_type' : "ref-not-found"}
                issues[item["name"]] = issue

        return issues


    def translate(self, value, dtype, unit, parse_format):   # parameters to pass after : df, dls, file_name

        """Convert the format of dataframe column if expected unit is different"""

        if unit is None or unit in ["str"] :
            if dtype in ["str"]:
                return value
            else:
                return str(value)
        if unit == 'date' :
            if dtype == 'date':
                return value
            else:
              return datetime.strptime(value, format)

        elif unit in ["person", "int"] :
            if dtype in ["int"]:
                return value
            else:
                return int(value)
        else : raise ValueError("undefined case for casting")



    def df2var(self):   # parameters to add after df=None, dls=None, job=None, file_name=None

        var = self.get_variables()
        df = self.get_df()
        dls_col_list=[]
        dls= self.get_dls()
        issues = self.check_df_columns() # Parameters to add later : df, dls, file_name
        type_issues = self.check_df_var_type() # Parameters to add later : df, dls, file_name

        if dls['columns']:
            cols_vars = dict((t["name"],t["variable"]) for t in  dls['columns'])
            cols_types = dict((k,var[v]["type"]) for k,v in cols_vars.items())
            types_ok = dict((t["name"],  t["name"] in type_issues and type_issues[t["name"]] is None) for t in  dls['columns'])
        print(cols_types)
         # df$line_number (creer la colonne si elle n'existe pas)
         # if 'line_number' not in df.columns : #add control to check if an identical column content exists. une colonne vide existe démarre à 1 et non 0
         # df.insert(0, 'line_number', range(0, len(df))) #numérotation qui démarre à 0 ou 1 ?

        ret = {"scope":dls["scope"]}
        tuples = []
        #TODO: make sure that index is 0-(N-1) => yes 0 to N-1

        for row in range(len(df)):

            for col in df.columns:
                if col not in cols_vars.keys():
                    df = df.drop(col, axis=1)           #permet la suppression des colonnes non retrouvées comme la colonne vide du df . Conflit car non présente dans le dict col_types.

                elif cols_types[col] in ["observation", "indicator"] and types_ok[col]==False:
                    #for col in cols_vars.keys():
                    tuple = {"obs":{cols_vars[col]:df[col][row]}, "attrs":{"line_number":df.index[df[col][row]]}}
                    print(tuple)
                    for attr_col in df.columns:
                        if col_vars[attr_col] not in ["observation", "indicator"] and types_ok[attr_col]:
                            try:
                              val = self.translate(df[col][row], df[col].dtypes, variables[col_vars[col]]["unit"])
                            except e:
                              issues.append(
                                {job['step'],
                                        "unknown",
                                        dls['scope']['source'],
                                        filename,         #file_name à passer en paramètre de la fonction à ajouter
                                        f"Cannot cast value {df[col][row]} into unit {variables[col_vars[col]]['unit']}",
                                        datetime.now(),
                                        job['job_id'],
                                        "cannot-cast"}
                              )

                            #iso-country-code2 => characteristic
                            # confirmed-cases => observation
                            # reporting-date => characteristic
                            #location-code => characteristic

        return {"tuples":tuples, "issues":issues, "type_issues":type_issues}
