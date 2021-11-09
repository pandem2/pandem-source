import subprocess
import os
import time
import threading
from . import worker


class Standardizer(worker.Worker):
    def __init__(self, name, orchestrator_ref, storage_ref, settings): 
        super().__init__(name = name, orchestrator_ref = orchestrator_ref, settings = settings)

        self.orchestrator_proxy = orchestrator_ref.proxy()

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