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
from . import util
from pprint import pprint

l = logging.getLogger("pandem-nlp")

class NLPAnnotator(worker.Worker):
    __metaclass__ = ABCMeta  
    def __init__(self, name, orchestrator_ref, settings, run_nlp): 
        super().__init__(name = name, orchestrator_ref = orchestrator_ref, settings = settings)    
        self.run = run_nlp
        if self.run:
          self._models_path = settings["pandem"]["source"]["nlp"]["models_path"]
          self._tf_port = settings["pandem"]["source"]["nlp"]["tensorflow_server_port"]
          self._tf_url = f"{settings['pandem']['source']['nlp']['tensorflow_server_protocol']}://{settings['pandem']['source']['nlp']['tensorflow_server_host']}:{settings['pandem']['source']['nlp']['tensorflow_server_port']}"
          self._tf_version = settings["pandem"]["source"]['nlp']["tensorflow_server_version"]
          self._models_info = settings["pandem"]["source"]["nlp"]["models"]
          self._chunk_size = settings["pandem"]["source"]["nlp"]["chunk_size"]
          self._evaluation_steps = settings["pandem"]["source"]["nlp"]["evaluation_steps"]
    def on_start(self):
        super().on_start()
        if self.run:
          self._storage_proxy = self._orchestrator_proxy.get_actor('storage').get().proxy()
          self._pipeline_proxy = self._orchestrator_proxy.get_actor('pipeline').get().proxy() 
          self._variables_proxy = self._orchestrator_proxy.get_actor('variables').get().proxy()
          self._models = None

    def annotate(self, list_of_tuples, path, job):
      if self.run:
        # getting tuples from cache
        list_of_tuples = list_of_tuples.value() 
        if self._models is None:
            self._models = self.get_models()
            self._model_aliases = {}
            for mn, m in self._models.items():
              if "alias" in m:
                if isinstance(m["alias"], str):
                  self._model_aliases[mn] = m["alias"]
                elif isinstance(m["alias"], dict):
                  for k, v in m["alias"].items():
                    self._model_aliases[f"{mn}.{k}"] = v
                else:
                  raise ValueError(f"Unexpected type for alias in model {mn} it should be a str or dict")

        breakpoint()
        self._model_languages = {l for info in self._models_info.values() for l in info["languages"]}
        # gathering information about nlp categories
        endpoints = self.model_endpoints()
        steps = self._evaluation_steps
        #gathering information for geo annotation
        variables = self._variables_proxy.get_variables().get()
        geos = {var["variable"] for var in variables.values() if var["type"] == "geo_referential"}
        alias_vars = {
          var["variable"]:var["linked_attributes"][0] 
          for var in variables.values() 
          if var["type"] in ["referential_alias", "referential_label"] and var["linked_attributes"] is not None and var["linked_attributes"][0] in geos
        }
        geo_aliases = {}
        for alias_var, code_var in alias_vars.items():
          alias_values = self._variables_proxy.read_variable(alias_var, {}).get()
          if alias_values is not None:
            alias_map = {t["attr"][alias_var].lower():t["attrs"][code_var] for t in alias_values if "attr" in t and "attrs" in t and alias_var in t["attr"] and code_var in t["attrs"]} 
            if code_var not in geo_aliases:
              geo_aliases[code_var] = alias_map
            else :
              geo_aliases[code_var].update(alias_map)
        
        alias_regex = {code_var:re.compile('|'.join([f"\\b{re.escape(alias)}\\b" for alias in geo_aliases[code_var]])) for code_var in geo_aliases}

        text_field = "article_text"
        lang_field = "article_language"
     
        
        annotated = []
        count = 0
        l.debug(f"{len(list_of_tuples['tuples'])} articles to annotate ")
        for lang in self._model_languages:
          for to_annotate in util.slices((t for t in list_of_tuples['tuples'] if "attrs" in t and text_field in t["attrs"] and lang_field in t["attrs"] and t["attrs"][lang_field]==lang), self._chunk_size):
            # Getting annotations for chunks using tensorflow server for all categories in language
            predictions = {}
            for m, info in self._models_info.items():
              if m in info["languages"]: 
                # setting default value for predictions
                predictions[m] = ["None" for i in range(0, len(to_annotate))]
                
                texts = (t["attrs"][text_field] for t in to_annotate)
                if info["source"] == "tf_server":
                  data = json.dumps({"instances": [[t] for t in texts]})
                  result = requests.post(f"{endpoints[m]}:predict", data = data, headers = {'content-type': "application/json"}).content
                  #print(f"++++++++++++++++++++++++++++++  {m}")
                  #pprint(json.loads(result))
                  res =  json.loads(result)
                  if "predictions" not in res:
                    raise ValueError(f"Not prediction found on tensorflow response: {res}")
                  annotations = res["predictions"]
                  
                  # catogory classifier 
                  if "categories" in info:
                    categories = info["categories"]
                    #getting positive predictions on step models
                    for i in range(0, len(to_annotate)):
                      positivies = [categories[j] for j, score in enumerate(annotations[i]) if score >=0.5] 
                      if len(positives) > 0:
                        predictions[m][i] == positives
                  elif "bio" in info:
                    bio = info["bio"]
                    if "token" not in info["bio"] or "class" not in info["bio"]:
                      raise ValueError(f"Invalid model configuration: Model {m} is an bio model but it does not contains either 'token' or 'class' accessor")
                    attr_tok = info["bio"]["token"]
                    attr_cla = info["bio"]["class"]
                    predictions[m] = ["None" for i in range(0, len(annotations))]
                    for i in range(0, len(to_annotate)):
                      tagged = [j for j, t in enumerate(annotations[i][attr_cla]) if t != 'O' and annotations[i][attr_tok][j]!= "[PAD]"]
                      if len(tagged) > 0:
                        entities = []
                        within = False
                        eoent = False
                        newent = False
                        entity = []
                        for j in tagged:
                          cl = annotations[i][attr_cla][j]
                          t = annotations[i][attr_tok][j]
                          if cl.startswith("B"):
                            if j == tagged[-1] or within: #last element we are on the end of the entity
                              eoent = True
                            else:
                              eoent = False
                            within = True
                            newent = True
                          elif cl.startswith("I"):
                            entity.append(t)
                            if j == tagged[-1]: #last element we are on the end of the entity
                              eoent = True
                            else:
                              eoent = False
                            newent = False
                          else:
                            raise ValueError(f"Entity annotation is expected to start with B, I, O or [PAD], which is not True in {cl}")
                          if eoent & newent:
                            within = False
                            current_class = cl.split("-")[-1]
                            entities.append({"class":current_class, "entity":t})
                          elif eoent:
                            within = False
                            entities.append({"class":current_class, "entity":" ".join(entity)})
                          elif newent:
                            current_class = cl.split("-")[-1]
                            entity = [t]
                        predictions[m][i] = entities 
                  else:
                    raise ValueError(f"Invalid model configuration: Model {m} should have either bio or category properties")

              elif info["source"] == "script":
                raise NotImplementedError()
              else:
                raise ValueError(f"Invalid model configuration: Model {m} has invalid source {info['source']}")

            # creating annotated tuples per step
            model_classes = [[None for ss in s] for s in steps]
            for i in range(0, len(to_annotate)):
              for j in range(0, len(steps)):
                for k in range(0, len(steps[j])):
                  s = steps[j][k]
                  m = s.split(".")[0]
                  prop = s.split(".")[1] if "." in s else None
                  c = predictions[m][i]
                  if isinstance(c, str):
                    model_classes[j][k] = c
                  elif isintance(c, dict) and prop is not None:
                    model_classes[j][k] = c[prop]
                  else:
                    raise ValueError(f"Unexpected model step {s} for prediction {c}")
                    
                #generating a tuple for each combination of positive predictions
                
              class_combs = [*itertools.product(model_classes)]
              for classes in class_combs:
                at = copy.deepcopy(to_annotate[i])
                for m, c in allclasses:
                  # if dic and point then extract value
                  if isinstance(c, dict):
                    for k, v in c.items():
                      at["attrs"][model_aliases.get(f'{m}.{k}') or f'{m}-{k}'] = v
                  elif isinstance(c, str):
                    at["attrs"][model_aliases.get(m) or m] = c
                  else:
                    raise ValueError(f"Unexpected type for preciction {c} is is expected to be str or dic")
                breakpoint()
                annotated.append(at)
            count = count + len(to_annotate)
            l.debug(f"{count} articles after NLP")
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
                at["attrs"][geo_var] = geo_aliases[geo_var][matched_alias]
                geo_annotated.append(at)
              t["attrs"][geo_var] = "All"
            count = count + 1
            if count % 10000 == 0:
              l.debug(f"{count} articles geo annotated")
        l.debug(f"{count} articles after geo annotation")
        
        # Adding annotated tuples
        list_of_tuples['tuples'].extend(annotated)
        list_of_tuples['tuples'].extend(geo_annotated)
        
        # Removing language field since it is not interesting for generating separated time series
        for t in list_of_tuples['tuples']:
          if 'attrs' in t and lang_field in t['attrs']:
            t['attrs'].pop(lang_field)
            t['attrs'].pop(text_field)
        
        ret = self._storage_proxy.to_job_cache(job["id"], f"std_{path}", list_of_tuples).get()
        self._pipeline_proxy.annotate_end(ret, path = path, job = job)
        return ret


    def get_models(self):
      if os.path.exists(self._models_path):
        return list(filter(lambda v: not v.startswith("."), next(os.walk(self._models_path))[1]))
      else: 
        raise FileNotFoundError(f"Cannot find the NLP models folder {self._models_path}")

    def model_endpoints(self):
      return {m:f"{self._tf_url}/v1/models/{m}" for m in self.get_models()}

       

