import time
from . import worker
from abc import ABC, abstractmethod, ABCMeta
import logging as l

class Aggregator(worker.Worker):
    __metaclass__ = ABCMeta  
    def __init__(self, name, orchestrator_ref, settings): 
        super().__init__(name = name, orchestrator_ref = orchestrator_ref, settings = settings)    

    def on_start(self):
        super().on_start()
        self._storage_proxy = self._orchestrator_proxy.get_actor('storage').get().proxy()
        self._pipeline_proxy = self._orchestrator_proxy.get_actor('pipeline').get().proxy() 
        self._variables_proxy = self._orchestrator_proxy.get_actor('variables').get().proxy()

    def aggregate(self, list_of_tuples, path, job):
      # This method will aggregates all provided tuples based on well known variables
      # CASE 1: 
      # - article_id is the observation and will be grouped as an array
      # - text is ignored
      # - other characteristics are grouped
      # - non characteristics are aggregated as max 
        to_annotate = [ t for t in list_of_tuples['tuples'] if "attrs" in t and "article_text" in t["attrs"] and "article_language" in t["attrs"] and t["attrs"]["article_language"]==lang ]
        if len(to_annotate) > 0:
          for m in categories.keys():
            if m in self._model_languages[lang]: 
              texts = [t["attrs"]["article_text"] for t in to_annotate]
              data = json.dumps({"instances": [[t] for t in texts]})
              result = requests.post(f"{endpoints[m]}:predict", data = data, headers = {'content-type': "application/json"}).content
              annotations = json.loads(result)["predictions"]
              for t, pred in zip(to_annotate, annotations):
                best = functools.reduce(lambda a, b: a if a[1]>b[1] else b, enumerate(pred))[0]
                t["attrs"][f"article_cat_{m}"] = categories[m][best]
      
      self._pipeline_proxy.annotate_end(list_of_tuples, path = path, job = job)


