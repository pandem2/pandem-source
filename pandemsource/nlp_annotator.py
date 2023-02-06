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
          self._point_storage = settings["pandem"]["source"]["nlp"].get("point_storage") or []
          self._chunk_size = settings["pandem"]["source"]["nlp"]["chunk_size"]
          self._evaluation_steps = settings["pandem"]["source"]["nlp"]["evaluation_steps"]
    def on_start(self):
        super().on_start()
        if self.run:
          self._storage_proxy = self._orchestrator_proxy.get_actor('storage').get().proxy()
          self._pipeline_proxy = self._orchestrator_proxy.get_actor('pipeline').get().proxy() 
          self._variables_proxy = self._orchestrator_proxy.get_actor('variables').get().proxy()
          self._models = None
          self._model_stats = {}
          self._stats_path = util.pandem_path("files", "nlp")
          if not os.path.exists(self._stats_path):
            os.makedirs(self._stats_path)
          self._p_storage_path = util.pandem_path("files", "nlp", "points")
          if not os.path.exists(self._p_storage_path):
            os.makedirs(self._p_storage_path)

    def annotate(self, list_of_tuples, path, job, last_in_job):
      if self.run:
        # getting tuples from cache
        list_of_tuples = list_of_tuples.value() 
        if self._models is None:
            self._models = self.get_models()
            self._model_aliases = {}
            for mn, m in self._models_info.items():
              if "alias" in m:
                if isinstance(m["alias"], str):
                  self._model_aliases[mn] = m["alias"]
                elif isinstance(m["alias"], dict):
                  for k, v in m["alias"].items():
                    self._model_aliases[f"{mn}.{k}"] = v
                else:
                  raise ValueError(f"Unexpected type for alias in model {mn} it should be a str or dict")
              self.load_stats(mn)
            self._alias_models = {v:k for k,v in self._model_aliases.items()}

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
        # checking if the job has been already processed
        processed_jobs_path = os.path.join(self._stats_path, f"jobs.json")
        if os.path.exists(processed_jobs_path):
          job_ids = util.load_json(processed_jobs_path, new_lined = True)
          update_stats = int(job["id"]) not in job_ids
        else:
          update_stats = True
          job_ids = []
        
        annotated = []
        count = 0
        l.debug(f"{len(list_of_tuples['tuples'])} articles to annotate ")
        ordered_models = sorted(self._models_info.items(), key = lambda p: p[1].get("order") or 0)
        for lang in self._model_languages:
          for to_annotate in util.slices((t for t in list_of_tuples['tuples'] if "attrs" in t and text_field in t["attrs"] and lang_field in t["attrs"] and t["attrs"][lang_field]==lang), self._chunk_size):
            # Getting annotations for chunks using tensorflow server for all categories in language
            predictions = {}
            for m, info in ordered_models:
              if lang in info["languages"]: 
                # setting default value for predictions
                predictions[m] = [None for i in range(0, len(to_annotate))]
                
                texts = [t["attrs"][text_field] for t in to_annotate]
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
                      positives = [categories[j] for j, score in enumerate(annotations[i]) if score >=0.5] 
                      if len(positives) > 0:
                        predictions[m][i] = positives
                  elif "bio" in info:
                    bio = info["bio"]
                    if "token" not in info["bio"] or "class" not in info["bio"]:
                      raise ValueError(f"Invalid model configuration: Model {m} is an bio model but it does not contains either 'token' or 'class' accessor")
                    attr_tok = info["bio"]["token"]
                    attr_cla = info["bio"]["class"]
                    predictions[m] = [None for i in range(0, len(annotations))]
                    current_class = ''
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
                  if "script" in info and "name" in info["script"] and "function" in info["script"] and "type" in info["script"]:
                    if info["script"]["type"] != "python":
                      raise NotImplementedError(f"Invalid configuration for model {m}. Only python scruipts are supported")
                    # calling annotation function
                    custom_annotate = util.get_custom(info["script"]["name"],info["script"]["function"])
                    if custom_annotate is not None:
                      args = info["script"].get("args") or ["text"]
                      argvals = [None]* len(args)
                      for iar in range(0, len(args)):
                        if args[iar] == "text":
                          argvals[iar] = texts
                        elif args[iar] == "lang":
                          argvals[iar] = [lang for t in texts]
                        elif (self._alias_models.get(args[iar]) or args[iar]) in predictions:
                          argvals[iar] = predictions[self._alias_models.get(args[iar]) or args[iar]]
                        else:
                          raise ValueError(f"The argument of models can only be 'text', 'lang' or a model/alias with a lower evaluation order. Cannot evaluate {args[iar]} for model '{m}'")
                      annotations = custom_annotate(*argvals)
                      ann = annotations.copy()
                      for ia in range(0, len(ann)):
                        if ann[ia] is None or ann[ia]== '':
                          ann[ia] = None
                        elif isinstance(ann[ia], str):
                          ann[ia] = [ann[ia]]
                        elif isinstance(ann[ia], list):
                          pass
                        else:
                          raise ValueError(f"Invalid return type for model {m}: {ann[ia]}")
                      predictions[m] = ann
                    else:
                      raise ValueError(f"Could not find function {info['script']['function']} in script {info['script']['name']} as defined on model {m}")
                  else:
                    raise ValueError(f"Invalid configuration for model {m}. When type is script a scipt attribute needs to be set with name, function and type defined")
                
                else:
                  raise ValueError(f"Invalid model configuration: Model {m} has invalid source {info['source']}")

            # updating stats
            if update_stats:
              for m, preds in predictions.items():
                self.update_stats(m, preds)

            # creating annotated tuples per step
            model_classes = [[None for ss in s] for s in steps]
            for i in range(0, len(to_annotate)):
              for j in range(0, len(steps)):
                for k in range(0, len(steps[j])):
                  s = steps[j][k]
                  m = s.split(".")[0]
                  prop = s.split(".")[1] if "." in s else None
                  if predictions[m][i] is None:
                    c = ['None']
                  else:
                    c = predictions[m][i].copy()
                  # filtering to top N categories if defined on algorithl
                  if self._models_info[m].get("limit_top"):
                    if c != ["None"]:
                      c = [cc for cc in c if 
                        self.get_pred_key(cc) in self._model_stats[m] 
                          and self._model_stats[m][self.get_pred_key(cc)][1] < self._models_info[m]["limit_top"] 
                          and self._model_stats[m][self.get_pred_key(cc)][0] > 1
                      ]
                    
                  model_classes[j][k] = c
                  for h in range(0, len(c)):
                    if isinstance(c[h], str):
                      pass #c[h] = c[h]
                    elif isinstance(c[h], dict) and prop is not None and prop in c[h]:
                      c[h] = c[h][prop]
                    else:
                      raise ValueError(f"Unexpected model step {s} for prediction {c[h]}")
                    
              #generating a tuple for each combination of positive predictions
                
              for j in range(0, len(steps)):
                class_combs = [*itertools.product(*model_classes[j])]
                for classes in class_combs:
                  at = copy.deepcopy(to_annotate[i])
                  for k in range(0, len(steps[j])):
                    s = steps[j][k]
                    m = s.split(".")[0]
                    prop = s.split(".")[1] if "." in s else None
                    c = classes[k]
                    # if dic and point then extract value
                    if prop is not None:
                      at["attrs"][self._model_aliases.get(f'{m}.{prop}') or f'{m}-{prop}'] = c
                    else:
                      at["attrs"][self._model_aliases.get(m) or m] = c
                  annotated.append(at)
                  count = count + 1
            # hhh = 0
            # for aaa in annotated:
            #   hhh = hhh + 1
            #   xxx = {k:v for k,v in aaa["attrs"].items() if (v != "None" and v != "non-suggestion")  and k in ["is_suggestion", "suggestion"]}#["aspect", "sub-topic", "suggestion", "emotion", "sentiment"]}
            #   if len(xxx) > 1:
            #     print(f'{hhh} - {xxx}')
            l.debug(f"{count} articles after NLP")
        # Saving stats
        if update_stats:
          self.save_stats()
          if last_in_job:
            job_ids.append(int(job["id"]))
            util.save_json(job_ids, processed_jobs_path, new_lined = True)

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
                #TODO remove the country limit of two first after performance fixes
                at["attrs"][geo_var] = geo_aliases[geo_var][matched_alias][0:2]
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


        # Splitting out tuples to be stored as points and those to be sent for aggregation
        point_storage = self._point_storage
        to_ts = [t for t in list_of_tuples["tuples"] if t["attrs"].keys().isdisjoint(point_storage)]
        to_point = [t for t in list_of_tuples["tuples"] if not t["attrs"].keys().isdisjoint(point_storage)]
        list_of_tuples["tuples"] = to_ts
        if update_stats:
          self.to_point_storage(to_point)
        ret = self._storage_proxy.to_job_cache(job["id"], f"std_{path}", list_of_tuples).get()
        self._pipeline_proxy.annotate_end(ret, path = path, job = job)
        return ret

    def to_point_storage(self, tuples):
      lines = [self.tuples_to_line(t) for t in tuples]
      periods = {l["reporting_period"] for l in lines}
      for p in periods:
        path = os.path.join(self._p_storage_path, f"{p.strftime('%Y-%m-%d')}.json")
        util.append_json([l for l in lines if l["reporting_period"] == p], path)

    def tuples_to_line(self, t):
      return {
         **{"indicator":next(iter(t["obs"].keys())), "value":next(iter(t["obs"].values()))}, 
         **{k:t["attrs"][k] for k in {*t["attrs"].keys()}.difference({"line_number", "article_created_at", "file", "period_type", "created_on"})}
      }
    def get_models(self):
      if os.path.exists(self._models_path):
        return list(filter(lambda v: not v.startswith("."), next(os.walk(self._models_path))[1]))
      else: 
        raise FileNotFoundError(f"Cannot find the NLP models folder {self._models_path}")

    def model_endpoints(self):
      return {m:f"{self._tf_url}/v1/models/{m}" for m in self.get_models()}

    def load_stats(self, model):
      stats_path = os.path.join(self._stats_path, f"{model}.json")
      if os.path.exists(stats_path):
        ml = util.load_json(stats_path, new_lined = True)
      else:
        ml = []
      self._model_stats[model] = {}
      for i, r in enumerate(ml):
        cat = r[0]
        if isinstance(cat, list):
          for j in range(0, len(cat)):
            if isinstance(cat[j], list):
              cat[j] = tuple(cat[j])
          cat = tuple(cat)
        rank = i
        freq = r[1]
        self._model_stats[model][cat] = (freq, rank)
    
    def update_stats(self, m, preds):
      # Updatibng counters
      for cat in preds:
        if cat is not None:
          for p in cat:
            p = self.get_pred_key(p) 
            if p not in self._model_stats[m]:
              self._model_stats[m][p] = (1, None)
            else:
              self._model_stats[m][p] = (self._model_stats[m][p][0] + 1, None)
      
      # calculating stats rankings
      ranked = [*enumerate(sorted(self._model_stats[m].items(), key = lambda p:-p[1][0]))]
      self._model_stats[m] = {cat:(freq, rank) for rank, (cat, (freq, old_rank)) in ranked[0:10000]}

    def save_stats(self):
      for m, stats in self._model_stats.items():
        stats_path = os.path.join(self._stats_path, f"{m}.json")
        ranked = [*enumerate(sorted(self._model_stats[m].items(), key = lambda p:-p[1][0]))]
        stat_list = [[cat, freq, rank] for rank, (cat, (freq, old_rank)) in ranked[0:10000]]
        util.save_json(stat_list, stats_path, new_lined = True)
    
    def get_pred_key(self, p):
      if isinstance(p, dict):
        return tuple(sorted(p.items(), key = lambda p:p[0]))
      else: 
        return p
      
