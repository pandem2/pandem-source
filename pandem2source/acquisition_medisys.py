from . import acquisition
from . import util
import time
import json
from .util import JsonEncoder
import gzip
from datetime import datetime, timedelta
import urllib
import requests
import os
from math import inf
import re
import logging
import xml.etree.ElementTree
from email.utils import parsedate_to_datetime
import pytz

l = logging.getLogger("pandem.medysis")

class AcquisitionMedisys(acquisition.Acquisition):
    def __init__(self, name, orchestrator_ref, settings): 
        super().__init__(name = name, orchestrator_ref = orchestrator_ref, settings = settings, channel = "twitter")
    
    def on_start(self):
        super().on_start()
    
    def add_datasource(self, dls, force_acquire):
      if len(self.current_sources) > 0:
        raise ValueError("Medisys aqquisition support only a singlr DLS, others will be ignored")
      if "acquisition" in dls and "channel" in dls["acquisition"] and "topics" in dls["acquisition"]["channel"]:
        self._topics  = dls["acquisition"]["channel"]["topics"].keys()
        self._maingroup  = dls["acquisition"]["channel"]["main_group"]
        self._topic_categories = {}
        self._included_regex = {}
        self._topic_groups = {}
        self._current_dir = util.pandem_path("files", "medisys", "current")
        self._arc_dir = util.pandem_path("files", "medisys", "archive")
        if not os.path.exists(self._current_dir):
          os.makedirs(name = self._current_dir)
        if not os.path.exists(self._arc_dir):
          os.makedirs(name = self._arc_dir)

        for topic in self._topics:
          self._included_regex[topic] = ""

          in_maingroup = dls["acquisition"]["channel"]["topics"][topic]["group"] == self._maingroup
          has_cat = False
          if "categories" in dls["acquisition"]["channel"]["topics"][topic]:
            self._topic_categories[topic] = set(dls["acquisition"]["channel"]["topics"][topic]["categories"])
            has_cat = True
          has_phrase = False
          if "phrases" in dls["acquisition"]["channel"]["topics"][topic]:
            phrases = dls["acquisition"]["channel"]["topics"][topic]["phrases"]
            self._included_regex[topic] = "|".join(map(lambda v: re.escape(v.lower()), phrases))  
            has_phrase = True
          if not has_phrase and not has_cat:
            raise ValueError("Medysis DLS topics need to have either  phrases property with a list of phrases to use or a categories property with a list of properties to follow")
          if in_maingroup and not has_cat:
            raise ValueError("Medysis DLS main topucs need to have a category")
          if "group" in dls["acquisition"]["channel"]["topics"][topic]:
            self._topic_groups[topic] =  dls["acquisition"]["channel"]["topics"][topic]["group"]
      else: 
        raise ValueError("If a Medysis DLS is found it should contain a (possible empty) list of topics under ['acquisition']['channel']")
      
      if "acquisition" in dls and "channel" in dls["acquisition"] and "languages" in dls["acquisition"]["channel"]:
        self._languages = [l.lower() for l in dls["acquisition"]["channel"]["languages"]]
      else:
        self._languages = None

      if "acquisition" in dls and "channel" in dls["acquisition"] and "excluded_phrases" in dls["acquisition"]["channel"]:
        self._excluded_regex =  "|".join(map(lambda v: re.escape(v.lower()), dls["acquisition"]["channel"]["excluded_phrases"]))
      else:
        self._excluded_regex = None

      super().add_datasource(dls, force_acquire)


    def new_files(self, dls, last_hash):
        # last_hash is the last until timespamp we captured
        if last_hash is None or last_hash == '':
          # If no last_hash is provided then 'until' = now and 'old_target' = a very early date
          until = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
          old_target = "2000-00-00T00:00:00Z"
        else: 
          # If a hash is provided then then 'until' = now and 'old_target' = last_hash + 1sec
          until = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
          old_target = datetime.strftime(datetime.strptime(last_hash, "%Y-%m-%dT%H:%M:%SZ") + timedelta(0,1), "%Y-%m-%dT%H:%M:%SZ")

        # looping over results until the endpoint produces no more articles
        # until will be the new hash after a successful iteration
        current_hash = until
        stop = False
        retries = 10
        retried = 0
        articles = []
        while(not stop):
          #Building the query url
          if self._languages is None:
            lang = "?"
          else:
            lang = f"?language={urllib.parse.quote(','.join(self._languages))}"
          dateto = f"dateto={urllib.parse.quote(until)}"
          datefrom = f"datefrom={urllib.parse.quote(old_target)}"
          categories = {v for vals in self._topic_categories.values() for v in vals}
          category = f"category={urllib.parse.quote(','.join(categories))}"
          url = f"https://medisys.newsbrief.eu/rss/{lang}&type=search&mode=advanced&{dateto}&{datefrom}&{category}"
          l.debug(url) 
          # getting the xml for the feed url
          res = requests.get(url)
          if res.status_code != 200:
            # request failed retrying on a arithmetic progression fashion
            l.warning(f"Medysis request failed with status {res.status_code} and reason: '{res.reason}'")
            if retried < retries:
              retried = retried + 1
              to_sleep = 10 * retried * retried
              l.info(f"Retrying again in {to_sleep} seconds")
              time.sleep(to_sleep)
            else:
              l.error("Cannot connect to the medisys endpoint, all retries has been exhausted")
              raise ConnectionError("Cannot connect to the medisys endpoint")
          else:
            # we have a succesfull connection we can parse the xml and get the articles
            tree = xml.etree.ElementTree.fromstring(res.content)
            nsmap = {"emm":"http://emm.jrc.it", "iso":"http://www.iso.org/3166", "georss":"http://www.georss.org/georss", "content":"http://purl.org/rss/1.0/modules/content/"} 
            new_art = []
            # Getting elements to obtain from tree
            if tree.find("channel") is not None:
              for item in tree.find("channel").findall("item"):
                art = {}
                new_art.append(art)
                # Getting single elements
                art["guid"] = [(node.text if node is not None else None) for node in [item.find("guid", nsmap)]][0]
                art["title"] = [(node.text if node is not None else None) for node in [item.find("title", nsmap)]][0]
                art["link"] = [(node.text if node is not None else None) for node in [item.find("link", nsmap)]][0]
                art["keywords"] = [(node.text if node is not None else None) for node in [item.find("emm:keywords", nsmap)]][0]
                art["pub_date"] = [(node.text if node is not None else None) for node in [item.find("pubDate", nsmap)]][0]
                art["lang"] = [(node.text if node is not None else None) for node in [item.find("iso:language", nsmap)]][0]
                art["article_count"] = 1

                # getting categories
                art["categories"] = [node.text for node in item.findall("category", nsmap)]
                
                # parsing the publication date to datetime
                art["pub_date"]= parsedate_to_datetime(art["pub_date"]).astimezone(pytz.utc)
                art["rep_date"]= datetime.strptime(datetime.strftime(art["pub_date"], "%Y-%m-%dT%H:00:00Z"), "%Y-%m-%dT%H:%M:%SZ")
 
                # getting full text of article (on this case is just the concatenation of the title and keywords
                art["text"] = f"{art['title']}. {art['keywords']}"

                # associating topics to the article
                for t in self.matching_topics(art["text"], art["categories"]):
                  if t in self._topic_groups:
                    col_name = f"topic_{self._topic_groups[t]}"
                  else:
                    col_name = "topic"
                  if col_name in art:
                    art[col_name].append(t)
                  else:
                    art[col_name] = [t]

          if len(new_art) > 0:
            # keep trying with older articles
            # new until will be the oldest of all articles minus one second
            l.debug(f"{len(new_art)} article obtained from medisys on a request")
            until = min(datetime.strftime(art["pub_date"] - timedelta(0,1), "%Y-%m-%dT%H:%M:%SZ") for art in new_art)
            articles.extend(new_art)
          else :
            # stop trying to get articles
            stop = True

        # collection finished, we can write the file 
        l.debug(f"{len(articles)} article obtained in total from medisys")
        
        
        # archiving previous files
        for to_arc in filter(lambda f: f.endswith(".json.gz"), os.listdir(self._current_dir)):
          os.rename(os.path.join(self._current_dir, to_arc), os.path.join(self._arc_dir, to_arc))

        # writing data to json.gz file
        fname = f"{datetime.now().strftime('%Y.%m.%d.%H.%M.%S')}.json.gz"
        path = os.path.join(self._current_dir, fname)
        with gzip.GzipFile(path, 'a') as fout:
          fout.write(json.dumps(articles, cls=JsonEncoder, indent = 4).encode('utf-8')) 

        return {"hash":current_hash, "files":[path]}  
          
    def matching_topics(self, text, categories):
        return list(filter(
          lambda topic: 
            (len(self._included_regex[topic]) > 0 and re.search(self._included_regex[topic], text.lower()) is not None and re.search(self._excluded_regex, text.lower()) is None)
            or (topic in self._topic_categories and not self._topic_categories[topic].isdisjoint(categories)),
          self._topics
        ))
        



