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

    def check_df_str(self):
        #Checks if the DLS columns names exist in dataframe
        df = self.get_df()
        dls = self.get_dls()
        dls_col_list=[]
        list_issues=[]
        job={'job_id':123, 'step':'step-job'}
        line_number=12

        if dls['columns']:
            dls_col_list = dls['columns']
        for item in dls_col_list:  #à voir si peut être remplacé par un check dans dls directement
            if item['name'] in df.columns : # if à remplacer par ;        if item['name'] not in df.columns : contenu du 'else'
                print("column found :", item['name'], "in dataframe")
            else :
                message=(f"Column", item['name'], "not found in dataframe ")
                print(message)
                issue={ job['step'],
                        line_number,
                        "unknown source",
                        "18725.csv",
                        message,
                        datetime.now(),
                        job['job_id'],
                        "ref-not-found"}
                list_issues.append(issue)


    def check_df_var_type(self):
        #Check and convert dataframe variables types, if not as expected in variables.json, convert it
        df = self.get_df()
        dls = self.get_dls()
        var=self.get_variables()

        dls_col_list=[]
        list_issues=[]
        job={'job_id':123, 'step':'step-job'}
        line_number=12

        if dls['columns']:
            dls_col_list = dls['columns']
        print(dls_col_list)

        # Checks if the variable name included in DLS file exists in Variables.json file
        #If yes, check the type
        #Else, raise an issue
        for item in dls_col_list :
            if item['variable'] in var:
                print("unit for variable", item['variable'], "is", var[item['variable']]['unit'])
                if var[item['variable']]['unit'] == 'date':
                    print("This is a date !")
                    print(df[item['name']].dtypes)
                    if df[item['name']].dtypes != 'date' :
                        print(df[item['name']], "is not in a date format")
                        # apply different treatment to convert variable into date types
                        # take into account the date format expected in the DLS file


                if var[item['variable']]['unit'] == 'people':
                    print("This has to be an integer(people) !")
                    if df[item['name']].dtypes != 'integer' :
                        print(df[item['name']], "is not in an integer format")
                        if df[item['name']].dtypes == 'float' :
                            df[item['name']].astype('int64')
                            print("new type after conversion", df[item['name']].dtypes )


                if var[item['variable']]['unit'] == None :
                    print("Unit is null !")




    def df2var(self):

        var = self.get_variables()
        df = self.get_df()
        dls= self.get_dls()
