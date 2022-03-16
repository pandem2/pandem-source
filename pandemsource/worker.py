from abc import ABC, abstractmethod, ABCMeta
import pykka
import os
import time
import threading
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

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
        self._actions = []
        
    
    def on_start(self):
        '''Extends the Pykka on_start method for optional setup after initialisation and before processing messages.'''
        heartbeat_repeat = Repeat(timedelta(seconds=20))
        self.register_action(heartbeat_repeat, self.send_heartbeat)
        threading.Thread(target=self.actor_loop).start()

    def send_heartbeat(self):
        '''A class method that sends heartbeat meassages to the orchestrator'''
        self._orchestrator_proxy.get_heartbeat(self.name)


    def actor_loop(self):
        while True:
            time.sleep(0.01)
            #print(f'last execution list is: {last_executions}')
            next_executions = [action['repeat'].next_exec() for action in self._actions]
            #print(f'next execution list is: {next_executions}')
            next_deltas = [(next_time - datetime.now()).total_seconds() for next_time in next_executions]
            #print(f'next deltas list is: {next_deltas}')
            next_action_index = next_deltas.index(min(next_deltas)) #return the index of the first min value
            #print(f'next action index is: {next_action_index}')

            next_action = self._actions[next_action_index]
            #print(f'next action is: {next_action}')
            if datetime.now() > next_executions[next_action_index]:
                next_action["repeat"].last_exec = datetime.now()
                next_action["func"]()
                if next_action["oneshot"]:
                  self._actions.pop(next_action_index)
                else:
                  self._actions[next_action_index] = next_action 


    def pandem_path(self, *args):
        '''A class method to get the absolute path from relative paths components'''
        return os.path.join(os.getenv('PANDEM_HOME'), *args)

    def register_action(self, repeat, action, oneshot = False):
        self._actions.append({'repeat': repeat, 'func': action, 'oneshot':oneshot })
        
    def staging_path(self, job_id, *args):
        return self.pandem_path(f'files/staging', str(int(job_id)), *args)


class Repeat:
        def __init__(self, tdelta, start_hour=None, end_hour=None, last_exec=None):
            self.tdelta = tdelta
            self.start_hour = start_hour
            self.end_hour = end_hour
            self.last_exec = last_exec
        
        def next_exec(self): #last_exec:datetime.datetime()
            if pd.isna(self.last_exec):
              self.last_exec = None
            if self.last_exec is None:
              next_exec = datetime.now()
            else :
              next_exec = self.last_exec + self.tdelta

              if self.start_hour is None:
                st = 0
              else:
                st = self.start_hour

              if self.end_hour is None:
                en = 25
              else:
                en = self.end_hour

              # if next execution is outside of the hour window, next execution will be next day at the beginning of the period
              if next_exec.hour < st or next_exec.hour > en:
                next_exec = datetime.combine(next_exec.date(), datetime.min.time()) + timedelta(days = 1, hours = st)
            return next_exec




    
  
