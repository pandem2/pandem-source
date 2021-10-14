import pykka
from . import storage
import datetime

class Orchestration(pykka.ThreadingActor):
    
    def __init__(self, settings):
        super().__init__()
        self.settings = settings
        self.current_actors = dict()
        print('here in __init__ orchestration \n')
        
   
    def on_start(self):
        
        storage = storage.Storage.start('storage', self.actor_ref, self.settings) 
        print('here in on_start orchestration \n')
        self.current_actors['storage'] = {'ref': storage.actor_ref} #'sources': [], ???
        
        
                                      


    def get_heartbeat(self, actor_name, message):
        now = datetime.datetime.now()
        self.current_actors[actor_name]['heartbeat'] = now
        print(f'heartbeat from {actor_name} at: {now}')

        
    
    def on_stop(self):
        self.current_actuators['storage']['ref'].stop()


    
