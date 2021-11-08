from abc import ABC, abstractmethod, ABCMeta
import pykka
import os
import time
import threading

class Worker(pykka.ThreadingActor):
    __metaclass__ = ABCMeta  
    def __init__ (self, name, orchestrator_ref, settings):
        super().__init__()
        self.name = name
        self.settings = settings
        self._orchestrator_proxy = orchestrator_ref.proxy()
        self._self_proxy = self.actor_ref.proxy()
    
    def on_start(self):
        threading.Thread(target=self.actor_loop).start()

    def send_heartbeat(self):
        self._orchestrator_proxy.get_heartbeat(self.name)
   
    def actor_loop(self):
        while True:
            self.send_heartbeat()
            self.loop_actions()
            time.sleep(20)

    def pandem_path(self, *args):
        return os.path.join(os.getenv('PANDEM_HOME'), *args)

    @abstractmethod
    def loop_actions(self):
        pass
  
