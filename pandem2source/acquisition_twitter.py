from . import acquisition
from . import util
import tweepy
import threading
import time
import json
import gzip
from datetime import datetime
import os
from math import inf

class AcquisitionTwitter(acquisition.Acquisition):
    def __init__(self, name, orchestrator_ref, settings): 
        super().__init__(name = name, orchestrator_ref = orchestrator_ref, settings = settings, channel = "twitter")
    
    def on_start(self):
        super().on_start()
        # Getting twitter credentials
        self._api_key=util.get_or_set_secret("twitter-api-key") 
        self._api_key_secret=util.get_or_set_secret("twitter-api-key-secret") 
        self._access_token=util.get_or_set_secret("twitter-access-token") 
        self._access_token_secret=util.get_or_set_secret("twitter-access-token-secret") 
        self._filter_dir = util.pandem_path("files", "twitter", "v1.1", "tweets")
        if not os.path.exists(self._filter_dir):
          os.makedirs(name = self._filter_dir)

    def add_datasource(self, dls):
      if len(self.current_sources) > 0:
        raise ValueError("Twitter aqquisition support only a singlr DLS, others will be ignored")
      if "acquisition" in dls and "channel" in dls["acquisition"] and "topics" in dls["acquisition"]["channel"]:
        self._topics = dls["acquisition"]["channel"]["topics"].keys()
        self._phrases = []
        for topic in self._topics:
          if "phrases" in dls["acquisition"]["channel"]["topics"][topic]:
            for kw in dls["acquisition"]["channel"]["topics"][topic]["phrases"]:
              if len(kw.encode("utf-8")) > 60:
                raise ValueError(f"Twitter filter endpoint cannot contain phrases bigger than 60 bytes and {kw} has {len(kw.encode('utf-8'))}")
              if not kw in self._phrases:
                self._phrases.append(kw)
          else:
            raise ValueError("Twitter DLS topics needs to have a phrases property with a list of phrases to use")
      else: 
        raise ValueError("If a Twitter DLS is found it should contain a (possible empty) list of topics under ['acquisition']['channel']")
      
      if "acquisition" in dls and "channel" in dls["acquisition"] and "languages" in dls["acquisition"]["channel"]:
        self._languages = dls["acquisition"]["channel"]["languages"]
      else:
        self._languages = None

      if "acquisition" in dls and "channel" in dls["acquisition"] and "excluded_phrases" in dls["acquisition"]["channel"]:
        self._excluded_phrases = dls["acquisition"]["channel"]["excluded_phrases"]
      else:
        self._excluded_phrases = []

      if "acquisition" in dls and "channel" in dls["acquisition"] and "include_retweets" in dls["acquisition"]["channel"]:
        self._include_retweets = bool(dls["acquisition"]["channel"]["include_retweets"])
      else:
        self._include_retweets = False
      
      # launching the tweet collection
      self.tweet_filter = self.TwitterFilter(
        track = self._phrases,
        languages = self._languages,
        excluded_phrases = self._excluded_phrases,
        include_retweets = self._include_retweets,
        filter_dir = self._filter_dir,
        consumer_key = self._api_key, 
        consumer_secret = self._api_key_secret, 
        access_token = self._access_token, 
        access_token_secret = self._access_token_secret
      )
      self.create_new_gz()
      threading.Thread(target=self.tweet_filter.run).start()
      super().add_datasource(dls)

    def create_new_gz(self):
       path = util.pandem_path("files", "twitter", "v1.1", "tweets", f"{datetime.now().strftime('%Y.%m.%d.%H.%M.%S')}.json.gz")
       open(path, 'a').close()
    
    def new_files(self, dls, last_hash):
        return {"hash":'', "files":[]} 
        file_path = self.source_path(dls, dls['acquisition']['channel']['xls_file'])
        if os.path.exists(file_path):
            current_hash = hashlib.md5(open(file_path,'rb').read()).hexdigest()
            files_to_pipeline = []
            if not os.path.exists(file_path) or last_hash == "":
                files_to_pipeline.extend([file_path])
            # the file already exists and we know the last etag 
            elif current_hash != last_hash:
                files_to_pipeline.extend([file_path])   
            return {"hash":current_hash, "files":files_to_pipeline}  
        else:
            return {"hash":'', "files":[]} 

    class TwitterFilter(tweepy.Stream):
      def __init__(self, track, languages, excluded_phrases, include_retweets, filter_dir,
            consumer_key, consumer_secret, access_token, access_token_secret, *args, chunk_size=512, max_retries=inf, proxy=None, verify=True
          ): 
        self._track = track
        self._languages = languages
        self._excluded_phrases = excluded_phrases
        self._include_retweets = include_retweets
        self._filter_dir = filter_dir
        super().__init__(consumer_key, consumer_secret, access_token, access_token_secret, *args, chunk_size=chunk_size, daemon=False, max_retries=max_retries, proxy=proxy, verify=verify)

      def run(self):
        try:
          while(True) :
            print("Launching twitter track")
            print(self._languages)
            print(self._excluded_phrases)
            print(self._include_retweets)
            print(self._track)
            self.filter(follow=None, track=self._track, locations=None, filter_level=None, languages=self._languages, stall_warnings=False)
            print("Tweet filter failed, trying again in 1 minute")
            time.sleep(60)
        except Exception as e: 
          print(str(e))
      
      def on_status(self, status):
        
        if hasattr(status, "retweeted_status"):
          if not self._include_retweets:
            text = None
          elif hasattr(status.retweeted_status, "extended_tweet") and "full_text" in status.retweeted_status.extended_tweet:
            text = status.retweeted_status.extended_tweet["full_text"]
          else: 
            text = status.retweeted_status.text
        elif hasattr(status, "extended_tweet") and "full_text" in status.extended_tweet:
          text = status.extended_tweet["full_text"]
        else: 
          text = status.text
        if text is not None:
          res = {
            "text":text,
            "created_at":str(status.created_at),
            "id":status.id_str
          }
          with gzip.GzipFile(self.get_gz_file(), 'a') as fout:
            fout.write(f"{json.dumps(res)}\n".encode('utf-8')) 

      def get_gz_file(self):
        files = os.listdir(self._filter_dir)
        if len(files)==0:
          raise ValueError("Could not find any file to save the tweets")
        else:
          files.sort(reverse=True)
          return os.path.join(self._filter_dir, files[0])
     
           
