import pykka
import os
import time
import threading
from io import BytesIO
import pandas as pd

class FormatReader(pykka.ThreadingActor):
    

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
            if dls['acquisition']['format']['name']=='csv':
                df = pd.read_csv(file_bytes)
                pipeline_proxy = self.orchestrator_proxy.get_actor('pipeline').get().proxy()
                job_id = pipeline_proxy.read_format_end(file_path, df).get() #dls['source_name'], 
        

    def send_heartbeat(self):
        self.orchestrator_proxy.get_heartbeat(self.name)
    
    
    def actor_loop(self):
        while True:
            self.send_heartbeat()
            time.sleep(20)
