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


    def check_df_columns(self): #to pass in parameters : df, dls, file_name

        """Checks if the DLS columns names exist in dataframe"""

        df = self.get_df()
        dls = self.get_dls()
        dls_col_list=[]
        list_issues=[]
        job={'job_id':123, 'step':'step-job'}
        file_name = "AAA"

        if dls['columns']:
            dls_col_list = dls['columns']
        else :
            message=("DLS file not conform, columns element is missing.")
            print(message)
            issue={ job['step'],
                    "unknown",              #the column element is not found
                    dls['scope']['source'],
                    dls['scope']['source'], #file_name DLS à passer en paramètre ? l'attribut n'existe pas dans le dls
                    message,
                    datetime.now(),
                    job['job_id'],
                    "ref-not-found"}
            list_issues.append(issue)

        for item in dls_col_list:            #à voir si peut être remplacé par un check dans dls directement
            if item['name'] not in df.columns :
                message=("Column", item['name'], "not found in dataframe ")
                print(message)
                issue={ job['step'],
                        1,
                        dls['scope']['source'],
                        dls['scope']['source'], #file_name DLS à passer en paramètre ? l'attribut n'existe pas dans le dls
                        message,
                        datetime.now(),
                        job['job_id'],
                        "ref-not-found"}
                list_issues.append(issue)
            #else :                             # To delete, remains just for testing
            #    print("column found :", item['name'], "in dataframe")

        return list_issues



    def check_df_var_type(self):   # parameters to pass after : df, dls, file_name

        """Checks if the variable indicated in the column element from dls exists in variables.json files
        and if yes, checks the dataframe variables types, if not as expected in variables.json, raises an issue"""

        df = self.get_df()
        dls = self.get_dls()
        var=self.get_variables()

        dls_col_list=[]
        list_issues=[]
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

                    if df[item['name']].dtypes == 'date' :
                        issues[item["name"]] = None

                    if df[item['name']].dtypes != 'date' :
                        message = (f"Type '{df[item['name']].dtypes}' in source file is not compatible with variable {item['variable']} with unit 'date' ")
                        issue={ job['step'],
                                1,
                                dls['scope']['source'],
                                dls['scope']['source'],         #file_name à passer en paramètre de la fonction à ajouter
                                message,
                                datetime.now(),
                                job['job_id'],
                                "ref-not-found"}
                        issues[item["name"]] = issue

                if var[item['variable']]['unit'] == 'people':

                    if df[item['name']].dtypes == 'integer' :
                        issues[item["name"]] = None

                    if df[item['name']].dtypes != 'integer' :
                        message = (f"Type '{df[item['name']].dtypes}' in source file is not compatible with variable {item['variable']} with unit 'integer' ")
                        issue = {job['step'],
                                1,
                                dls['scope']['source'],
                                dls['scope']['source'],         #file_name à passer en paramètre de la fonction à ajouter
                                message,
                                datetime.now(),
                                job['job_id'],
                                "ref-not-found"}
                        issues[item["name"]] = issue

                if var[item['variable']]['unit'] == None :
                    issues[item["name"]] = None

                if var[item['variable']]['unit'] == 'string' :
                    issues[item["name"]] = None
            else :
                message = ("Variable defined on source definition file is unknown")
                issue = {job['step'],
                        "unknown",
                        dls['scope']['source'],
                        dls['scope']['source'],         #file_name à passer en paramètre de la fonction à ajouter
                        message,
                        datetime.now(),
                        job['job_id'],
                        "ref-not-found"}
                issues[item["name"]] = issue


        return issues




    def df2var(self):   # parameters to add after df=None, dls=None, job=None, file_name=None

        var = self.get_variables()
        df = self.get_df()
        dls_col_list=[]
        dls= self.get_dls()
        issues = self.check_df_columns() # Parameters to add later : df, dls, file_name
        type_issues = self.check_df_var_type() # Parameters to add later : df, dls, file_name

        #df$line_number (creer la colonne si elle n'existe pas)
        if 'line_number' not in df.columns : #add control to check if an identical column content exists. une colonne vide existe démarre à 1 et non 0
            df.insert(0, 'line_number', range(0, len(df))) #numérotation qui démarre à 0 ou 1 ?

        if dls['columns']:
            dls_col_list = dls['columns']
            print(dls_col_list)
        for key in dls_col_list:
            print(key['name'])


        ret = {"scope":dls["scope"]}
        tuples = []

        for row in range(len(df)):
            tuple = {}
            for col in df.columns:
                if col in type_issues and type_issues[col] is None:
                    for keys in dls_col_list:
                        if col in keys['name']:
                            #print("Variable known")
                            var_name = keys['variable']
                            print("Var name", var_name)
                            if var_name in var and var[keys['variable']]['type'] == 'characteristic' :
                                print("characteristic found")
                                tuple["obs"]={f"'{var_name}': {df[col][row]} "}
                                tuple["attrs"]={f"line_number :{df['line_number'][row]} "}
                                print(tuple)
                                tuples.append(tuple)


#location-code => characteristic
#iso-country-code2 => characteristic
# confirmed-cases => observation
# reporting-date => characteristic



                    #print("col:",col,"col_value:",df[col][row])

                    #if any(col in keys['name'] for keys in dls_col_list):

                    #if col in dls_col_list['name'] :
                        #print("Variable known")
                        #keys['variable']

                        #if any(col in keys['name'] for keys in dls_col_list):
                    #if var[item['variable']]['unit'] == None :

        return {"tuples":tuples, "issues":issues, "type_issues":type_issues}

"""
                    # transformer le type si necesaire
                    # ou creer issue si la transformation echoue e.g. try {vale.dateparse(v, format)} catch {issues.append... for line and column}
                    # tuple["attrs"][var_name] = transformed value0


                     #take into account the date format expected in the DLS file


                     # """" if unit is None then convert systematically into string --> no issue to raise """
"""
                elif col in type_issues :
                    issues.add(type_issues[col])
            tuples.add(tuple)
        ret["tuples"] = tuples
"""
#    return {"tuples":tuples, "issues":issues, "type_issues":type_issues} #Add of type_issues
"""
"tuples":[
    {
      "obs":{"confirmed_cases":value},
      "attrs":{"line_number":"12", "date":"2021-09-01", "geo":"DE12C", "coutry_code_2":"DE"}
    },
    {
      "obs":{"deads_with":12},
      "attrs":{"line_number":"12", "date":"2021-09-01", "reporting_date":"2021-09-01", "geo":"FRJ12", "country_code_2":"FR"}
    },
"""
"""
    if var[item['variable']]['unit'] == 'date':
        print("This is a date !")
        print(df[item['name']].dtypes)
        #if df[item['name']].dtypes == 'date'


    if var[item['variable']]['unit'] == 'people':
        print("This has to be an integer(people) !")
        if df[item['name']].dtypes != 'integer' :
            print(df[item['name']], "is not in an integer format")
            if df[item['name']].dtypes == 'float' :
                #df[item['name']].astype('int64')
                print("new type after no conversion", df[item['name']].dtypes )

    if var[item['variable']]['unit'] == None :
        print("Unit is null !")
"""
