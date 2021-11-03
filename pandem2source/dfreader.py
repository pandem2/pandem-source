import pykka
import os
import time
import threading


class DataframeReader(pykka.ThreadingActor):
    

    def __init__(self, name, orchestrator_ref, storage_ref, settings): 
        super(DataframeReader, self).__init__()
        self.name = name
        self.orchestrator_proxy = orchestrator_ref.proxy()
        self.storage_proxy = storage_ref.proxy()
        self.settings = settings
        print(f'here in {self.name} __init__')
      
       
    def on_start(self):
       
        threading.Thread(target=self.actor_loop).start()
        print(f'here in {self.name} on-start')


    def read_df_start(self, dls):
       
        pass
        

    def send_heartbeat(self):
        self.orchestrator_proxy.get_heartbeat(self.name)
    
    
    def actor_loop(self):
        while True:
            
            self.send_heartbeat()
            time.sleep(20)