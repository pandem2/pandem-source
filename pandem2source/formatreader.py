import pykka
import os
import time
import threading
from io import BytesIO
import pandas as pd
import re
from lxml import etree


class FormatReader(pykka.ThreadingActor):
    

    def __init__(self, name, orchestrator_ref, storage_ref, settings):
        super(FormatReader, self).__init__()
        self.name = name
        self.orchestrator_proxy = orchestrator_ref.proxy()
        self.storage_proxy = storage_ref.proxy()
        self.settings = settings
      
       
    # def on_start(self):
    #     threading.Thread(target=self.actor_loop).start()
        

    # def read_format_start(self, file_path, dls):
    #     file_bytes = self.storage_proxy.read_files(file_path).get()
    #     if file_bytes != '':
    #         if dls['acquisition']['format']['name']=='csv':
    #             df = pd.read_csv(file_bytes)
    #             pipeline_proxy = self.orchestrator_proxy.get_actor('pipeline').get().proxy()
    #             job_id = pipeline_proxy.read_format_end(file_path, df).get() #dls['source_name'], 
        

    # def send_heartbeat(self):
    #     self.orchestrator_proxy.get_heartbeat(self.name)
    
    
    # def actor_loop(self):
    #     while True:
    #         self.send_heartbeat()
    #         time.sleep(20)


#######################################################################################
#######################################################################################

class FormatReaderXML(FormatReader):
    

    def __init__(self, name, orchestrator_ref, storage_ref, settings): 
        super().__init__(name, orchestrator_ref, storage_ref, settings)
      
       
    def on_start(self):
        threading.Thread(target=self.actor_loop).start()
        

    def read_format_start(self, file_path, dls):
        if file_path.split('.')[-1] != "md":
            print(file_path)
            tree = etree.parse(os.path.join(os.getenv('PANDEM_HOME'), 'files', file_path))
            root = tree.getroot()
            nsmap = root.nsmap
            cols = [col['name'] for col in dls['columns'] ]
            rows = []
            for row in root.xpath(dls['acquisition']['format']['row'], namespaces=nsmap):
                dict_col = dict()
                for col in dls['columns']:
                    ltext = row.xpath(col['xpath'], namespaces=nsmap)
                    if ltext: 
                        if 'find' in col:
                            
                            subtext = [re.findall( col['find'], text)[0] for text in ltext]
                            if len(subtext)==1:
                                dict_col[col['name']] = subtext[0]
                            else:
                                dict_col[col['name']] = subtext
                        else:
                            dict_col[col['name']] = ltext[0]
                    else:
                        dict_col[col['name']] = None
                rows.append(dict_col)
            df = pd.DataFrame(rows, columns = cols)
            print(f'the data frame for the xml source: {df}')
            pipeline_proxy = self.orchestrator_proxy.get_actor('pipeline').get().proxy()
            job_id = pipeline_proxy.read_format_end(file_path, df).get()







        # file_bytes = self.storage_proxy.read_filese(file_path).get()
        # if file_bytes != '':
        #     if dls['acquisition']['format']['name']=='csv':
        #         df = pd.read_csv(file_bytes)
        #         pipeline_proxy = self.orchestrator_proxy.get_actor('pipeline').get().proxy()
        #         job_id = pipeline_proxy.read_format_end(file_path, df).get() #dls['source_name'], 
        

    def send_heartbeat(self):
        self.orchestrator_proxy.get_heartbeat(self.name)
    
    
    def actor_loop(self):
        while True:
            self.send_heartbeat()
            time.sleep(20)

#############################################################################################
#############################################################################################

class FormatReaderCSV(FormatReader):
    

    def __init__(self, name, orchestrator_ref, storage_ref, settings):
        super(FormatReader, self).__init__()
        self.name = name
        self.orchestrator_proxy = orchestrator_ref.proxy()
        self.storage_proxy = storage_ref.proxy()
        self.settings = settings
      
       
    def on_start(self):
        threading.Thread(target=self.actor_loop).start()
        

    def read_format_start(self, file_path, dls):
        file_bytes = self.storage_proxy.read_files(file_path).get()
        if file_bytes != '':
            df = pd.read_csv(file_bytes)
            pipeline_proxy = self.orchestrator_proxy.get_actor('pipeline').get().proxy()
            job_id = pipeline_proxy.read_format_end(file_path, df).get()
        

    def send_heartbeat(self):
        self.orchestrator_proxy.get_heartbeat(self.name)
    
    
    def actor_loop(self):
        while True:
            self.send_heartbeat()
            time.sleep(20)
