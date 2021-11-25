from abc import ABC, abstractmethod, ABCMeta
import pykka
import os
import time
import threading
from datetime import datetime, timedelta

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
            time.sleep(1)
            last_executions = [action['last_exec'] if action['last_exec'] is not None else datetime.now()-timedelta(days=2) \
                               for action in self._actions]
            #print(f'last execution list is: {last_executions}')
            next_executions = [action['repeat'].next_execution(last_exec) for action, last_exec in zip(self._actions, last_executions)]
            #print(f'next execution list is: {next_executions}')
            next_deltas = [(next_time - datetime.now()).total_seconds() for next_time in next_executions]
            #print(f'next deltas list is: {next_deltas}')
            next_action_index = next_deltas.index(min(next_deltas)) #return the index of the first min value
            #print(f'next action index is: {next_action_index}')

            next_action = self._actions[next_action_index]
            #print(f'next action is: {next_action}')
            if datetime.now() > next_executions[next_action_index]:
                if next_action['repeat'].start is not None and  next_action['repeat'].end is not None:
                    if next_action['repeat'].start <= datetime.now().time() <= next_action['repeat'].end:
                        next_action["func"]()
                        next_action['last_exec'] = datetime.now()
                        self._actions[next_action_index] = next_action
                else:
                    next_action["func"]()
                    next_action['last_exec'] = datetime.now()
                    self._actions[next_action_index] = next_action 


    def pandem_path(self, *args):
        '''A class method to get the absolute path from relative paths components'''
        return os.path.join(os.getenv('PANDEM_HOME'), *args)

    def register_action(self, repeat, action, last_exec=None, id_source=None):
        self._actions.append({'repeat': repeat, 'func': action, 'last_exec': last_exec })
        


class Repeat:
        def __init__(self, tdelta, start=None, end=None):
            self.tdelta = tdelta #:datetime.timedelta()
            self.start = start #:datetime.time()
            self.end = end #:datatime.time()
        
        def next_execution(self, last_excec): #last_exec:datetime.datetime()
            return last_excec + self.tdelta #return:




    
  
