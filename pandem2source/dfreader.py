import os
import time
import threading
from . import worker
from abc import ABC, abstractmethod, ABCMeta


class DataframeReader(worker.Worker):
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
