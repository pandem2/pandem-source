import os
from . import worker
from abc import ABC, abstractmethod, ABCMeta
import logging
import functools
import requests
import json
import re
import copy
import itertools

l = logging.getLogger("pandem-nlp")

class NLPAnnotator(worker.Worker):
    __metaclass__ = ABCMeta  
    def __init__(self, name, orchestrator_ref, settings): 
        super().__init__(name = name, orchestrator_ref = orchestrator_ref, settings = settings)    
        self._models_path = settings["pandem"]["source"]["nlp"]["models_path"]
        self._tf_port = settings["pandem"]["source"]["nlp"]["tensorflow_server_port"]
        self._tf_url = f"{settings['pandem']['source']['nlp']['tensorflow_server_protocol']}://{settings['pandem']['source']['nlp']['tensorflow_server_host']}:{settings['pandem']['source']['nlp']['tensorflow_server_port']}"
        self._tf_version = settings["pandem"]["source"]['nlp']["tensorflow_server_version"]
        self._model_categories = settings["pandem"]["source"]["nlp"]["categories"]
        self._model_languages = settings["pandem"]["source"]["nlp"]["languages"]

    def on_start(self):
        super().on_start()
        self._storage_proxy = self._orchestrator_proxy.get_actor('storage').get().proxy()
        self._pipeline_proxy = self._orchestrator_proxy.get_actor('pipeline').get().proxy() 
        self._variables_proxy = self._orchestrator_proxy.get_actor('variables').get().proxy()
        #self._models = self.get_models()
        self._models = None

    def annotate(self, list_of_tuples, path, job):
      # getting tuples from cache
      list_of_tuples = list_of_tuples.value() 
      if self._models is None:
          self._models = self.get_models()
      # gathering information about nlp categories
      endpoints = self.model_endpoints()
      categories = {m:self._model_categories[m] for m in endpoints.keys() if (m in self._model_categories) }
      
      #gatherint information for geo annotation
      variables = self._variables_proxy.get_variables().get()
      geos = {var["variable"] for var in variables.values() if var["type"] == "geo_referential"}
      alias_vars = {
        var["variable"]:var["linked_attributes"][0] 
        for var in variables.values() 
        if var["type"] in ["referential_alias", "referential_label"] and var["linked_attributes"] is not None and var["linked_attributes"][0] in geos
      }
      aliases = {}
      for alias_var, code_var in alias_vars.items():
        alias_values = self._variables_proxy.read_variable(alias_var, {}).get()
        if alias_values is not None:
          alias_map = {t["attr"][alias_var].lower():t["attrs"][code_var] for t in alias_values if "attr" in t and "attrs" in t and alias_var in t["attr"] and code_var in t["attrs"]} 
          if code_var not in aliases:
            aliases[code_var] = alias_map
          else :
            aliases[code_var].update(alias_map)
      
      alias_regex = {code_var:re.compile('|'.join([f"\\b{re.escape(alias)}\\b" for alias in aliases[code_var]])) for code_var in aliases}

      text_field = "article_text"
      lang_field = "article_language"
     
      
      l.debug("Evaluatig NLP models")
      annotated = []
      count = 0
      for lang in self._model_languages:
        for to_annotate in self.slices((t for t in list_of_tuples['tuples'] if "attrs" in t and text_field in t["attrs"] and lang_field in t["attrs"] and t["attrs"][lang_field]==lang), 10000):
          # Annotating using tensorflow categories
          for m in categories.keys():
            if m in self._model_languages[lang]: 
              texts = (t["attrs"][text_field] for t in to_annotate)
              data = json.dumps({"instances": [[t] for t in texts]})
              result = requests.post(f"{endpoints[m]}:predict", data = data, headers = {'content-type': "application/json"}).content
              annotations = json.loads(result)["predictions"]
              for t, pred in zip(to_annotate, annotations):
                best = functools.reduce(lambda a, b: a if a[1]>b[1] else b, enumerate(pred))[0]
                at = copy.deepcopy(t)
                # creating a new tuple with the best category for this model
                at["attrs"][f"article_cat_{m}"] = categories[m][best]
                annotated.append(at)
                # updating exitsting tuple with category ALL
                t["attrs"][f"article_cat_{m}"] = "All"
          count = count + len(to_annotate)
          l.debug(f"{count} articles annotated")
      l.debug("Evaluating geolocation")

      # Annotating geographically using extra simplistic approach
      count = 0
      geo_annotated = []
      geo_annotation = {}
      to_annotate = [ t for t in list_of_tuples['tuples'] if "attrs" in t and text_field in t["attrs"]]
      for geo_var, regex in alias_regex.items():
        texts = [t["attrs"][text_field] for t in to_annotate]
        for t in to_annotate + annotated:
          if not geo_var in t["attrs"]:
            text = t["attrs"][text_field].lower()
            if text not in geo_annotation:
              match = re.search(alias_regex[geo_var], text)
              geo_annotation[text] = match
            else :
              match = geo_annotation[text]

            if match is not None:
              matched_alias = match.group()
              at = copy.deepcopy(t)
              at["attrs"][geo_var] = aliases[geo_var][matched_alias]
              geo_annotated.append(at)
            t["attrs"][geo_var] = "All"
          count = count + 1
          if count % 10000 == 0:
            l.debug(f"{count} articles geo annotated")
      l.debug(f"{count} articles geo annotated")
      l.debug("Removing language from tuples")
      
      # Adding annotated tuples
      list_of_tuples['tuples'].extend(annotated)
      list_of_tuples['tuples'].extend(geo_annotated)
      
      # Removing language field since it is not interesting for generating separated time series
      for t in list_of_tuples['tuples']:
        if 'attrs' in t and lang_field in t['attrs']:
          t['attrs'].pop(lang_field)
            
      ret = self._storage_proxy.to_job_cache(job["id"], f"std_{path}", list_of_tuples).get()
      self._pipeline_proxy.annotate_end(ret, path = path, job = job)

    def slices(self, iterable, size):
       head = list(itertools.islice(iterable, size))
       while len(head) > 0:
         yield head
         head = list(itertools.islice(iterable, size))

    def get_models(self):
      if os.path.exists(self._models_path):
        return list(filter(lambda v: not v.startswith("."), next(os.walk(self._models_path))[1]))
      else: 
        raise FileNotFoundError(f"Cannot find the NLP models folder {self._models_path}")

    def model_endpoints(self):
      return {m:f"{self._tf_url}/v1/models/{m}" for m in self.get_models()}

       

