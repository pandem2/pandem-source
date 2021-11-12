import os
import time
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
        self.orchestrator_proxy = orchestrator_ref.proxy()
        self.storage_proxy = storage_ref.proxy()
        print(f'here in {self.name} __init__')




    def on_start(self):
        super().on_start()
        print(f'here in {self.name} on-start')


    def read_df_start(self, dls):

        pass


    def send_heartbeat(self):
        #already implemented for parent class. To delete
        self.orchestrator_proxy.get_heartbeat(self.name)


    def actor_loop(self):
        while True:

            self.send_heartbeat()
            time.sleep(20)

    def get_df():
        #Create a dataframe from the csv file uploaded in the PANDEM_HOME/datapub/pandem/files/reset_variables
        if os.path.exists(os.path.join(os.getenv('PANDEM_HOME'), 'files/variables')):
            df = pd.read_csv(os.path.join(os.getenv('PANDEM_HOME'), 'files/variables/18725.csv'))
            print("-----------------------------------------------------------------------------------------")
            print("Dataframe :")
            return df
        else:
            print("NO CSV FILE FOUND")
            return ''

    def get_dls():
        #Get back the json  file covid19-datahub sample.json  from C:\Users\Charline CLAIN\Documents\PANDEM\datapub\pandem\files\variables
        if os.path.exists(os.path.join(os.getenv('PANDEM_HOME'), 'files/variables')):
            dls = json.load(open(os.path.join(os.getenv('PANDEM_HOME'), 'files/variables/covid19-datahub.json')))
            print("-----------------------------------------------------------------------------------------")
            print("DLS from JSON :")
            return dls
        else:
            print("NO JSON FILE FOUND")
            return ''

    def get_variables():
            #Get back the json  file variables.json  from C:\Users\Charline CLAIN\Documents\PANDEM\datapub\pandem\files\variables
            if os.path.exists(os.path.join(os.getenv('PANDEM_HOME'), 'files/variables')):
                var = json.load(open(os.path.join(os.getenv('PANDEM_HOME'), 'files/variables/variables.json')))
                print("-----------------------------------------------------------------------------------------")
                print("Variables :")
                return var
            else:
                print("NO VARIABLES.JSON FILE FOUND")
                return ''

    def df2var(df,dls,var):
        print("-----------------------------------------------------------------------------------------")
        print(df)
        print("-----------------------------------------------------------------------------------------")
        print("--DLS IS-----------------------------------------------------------------------")
        print (dls)
        print("-----------------------------------------------------------------------------------------")
        print(var)
        #For each column in DLS

        dls_col_list=[]
        df_col_list=[]
        #for key in range (len(dls)):
        if dls['columns']:
            dls_col_list = dls['columns']
        #print("DLS_COL_LIST : ", dls_col_list)
        for item in dls_col_list:
            #print(item['name'])
            if item['name'] in df.columns :
                print("column found")

            else :
                print ("Column not found in dataframe")

#  [{'name': 'date', 'variable': 'reporting-date'}, {'name': 'confirmed', 'variable': 'confirmed-cases'}, {'name': 'iso_alpha_2', 'variable': 'iso-country-code2'}, {'name': 'key_nuts', 'variable': 'location-code'}]
        return df, dls, var
