from abc import ABC, abstractmethod, ABCMeta
import pykka
import os
import time
import threading

class Worker(pykka.ThreadingActor):
    '''
    The Worker abstract class implements a Pykka actor and gives a blueprint for actors that are launched by the orchestrator actor.

    Args:
        name (str): The name given by the orchestration actor to this worker.
        orchestrator_ref: The orchestrator reference.
        setting: The package default configuration values.
    '''
    __metaclass__ = ABCMeta  
    def __init__ (self, name, orchestrator_ref, settings):
        super().__init__()
        self.name = name
        self.settings = settings
        self._orchestrator_proxy = orchestrator_ref.proxy()
        self._self_proxy = self.actor_ref.proxy()
    
    def on_start(self):
        '''Extends the Pykka on_start method for optional setup after initialisation and before processing messages.'''
        threading.Thread(target=self.actor_loop).start()

    def send_heartbeat(self):
        '''A class method that sends heartbeat meassages to the orchestrator'''
        self._orchestrator_proxy.get_heartbeat(self.name)
   
    def actor_loop(self):
        '''A class method that sends heartbeat mesaages and implements other loop actions at regular intervals'''
        while True: 
            self.send_heartbeat()
            self.loop_actions()
            time.sleep(2)

    def pandem_path(self, *args):
        '''A class method to get the absolute path from relative paths components'''
        return os.path.join(os.getenv('PANDEM_HOME'), *args)

    @abstractmethod
    def loop_actions(self):
        pass
  
