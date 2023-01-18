import numpy as np
import pandas as pd
import os
import gzip
import json
import re
from datetime import datetime
from datetime import timedelta
from pandemsource import util

chunk_size = 5000

def df_transform(df):
  df["article_count"] = 1
  df["reporting_time"] = df.apply(lambda row: datetime.strftime(datetime.strptime(row["date"], "%Y-%m-%d") + timedelta(seconds = int(row["chunk"])), "%Y-%m-%d %H:%M:%S"), axis = 1)
  return df
  
def join_files(files_hash, last_hash, dls, orchestrator, logger, **kwargs):
  files = files_hash["files"]
  current_hash = files_hash["hash"]
  dest_file = os.path.join(os.getcwd(), "joined.tsv.gz")

  change_detected = not os.path.exists(dest_file) or current_hash != last_hash

  if change_detected:
    with open(dest_file, 'wb') as outfile:
      for fname in files:
        logger.debug(f"Joining {fname}")
        with open(fname, 'rb') as infile:
          data = infile.read(1024*1024)
          while(len(data))>0:
            outfile.write(data)
            data = infile.read(1024*1024)

  else:
    logger.debug(f"Ignoring joining since no change was detected")

  return {"files":[dest_file], "hash":current_hash}


def index_tweets(files_hash, last_hash, dls, orchestrator, logger, **kwargs):
  # tweets ids from provided files and returning the ids of modified files
  files = files_hash["files"]
  current_hash = files_hash["hash"]
  cols = None
  current_date = None
  base_dir = os.path.join(os.getcwd(), "tweets")
  if not os.path.exists(base_dir):
    os.mkdir(base_dir)
  dest_dir = os.path.join(os.getcwd(), "tweets", "tweets_ids")
  if not os.path.exists(dest_dir):
    os.mkdir(dest_dir)
  stats = load_index_stats()

  change_detected = len(stats) == 0 or current_hash != last_hash
  if change_detected:
    # iterating over all files to index
    i = 0
    for file_path in files_hash["files"]:
      # opening the gziped file 
      with gzip.open(file_path,'rt') as f:
        # iterating over all lines in the file 
        for line in f:
          # first line will contain headers
          if cols is None:
            cols = {col:j for j, col in enumerate(re.split("\\s+", line))}
          # other lines will contain data
          else:
            i = i + 1
            # getting tweet data
            row = re.split("\\s+", line)
            date = row[cols["date"]]
            tweet_id = row[cols["tweet_id"]]
            tweet_chunk = str(int(tweet_id[-10:]) % chunk_size)
            
            # if new date in file we load the current knowns ids and update the index with previous day data
            if date != current_date:
              # saving previous day stats
              if current_date is not None and stats[current_date]["changed"]:
                update_index(current_date, stats, ids, i, logger)
              current_date = date
              # Adding the stats 
              if date not in stats:
                stats[date]= {"to_hydrate":dict()}
              # Setting stats for date to unchanged to detect if any change need to be saved
              stats[date].update({"changed":False})
              # loading ids from index (or creating a new one
              ids = load_date_index(date)
            # adding the tweet chunk if it does not exists
            if not tweet_chunk in ids:
              ids[tweet_chunk] = set()
            
            # a new tweet is found
            if not tweet_id in ids[tweet_chunk]:
            # Adding the new tweet id to the ids
              ids[tweet_chunk].add(tweet_id)
              # marking the day as changed
              stats[date].update({"changed":True})
              # ading the marking the chunk for hydration 
              if not tweet_chunk in stats[date]["to_hydrate"]:
                stats[date]["to_hydrate"][tweet_chunk] = 1
              else: 
                stats[date]["to_hydrate"][tweet_chunk] = stats[date]["to_hydrate"][tweet_chunk] + 1
        # index update for the last day
        if current_date is not None and stats[current_date]["changed"]:
          update_index(current_date, stats, ids, i,  logger)
  else:
    logger.debug(f"Ignoring indexing since no change was detected")

  # we need to return all files with the smallest chunk number 
  chunk_to_hydrate = str(min([min([int(chunk) for chunk in info["to_hydrate"].keys()]) for date, info in stats.items()]))
  dates_to_update = [date for date, info in stats.items() if chunk_to_hydrate in info["to_hydrate"]] 
  return {"files":[os.path.join(os.getcwd(), "tweets", "tweets_ids", f"{date}.json") for date in dates_to_update], "hash":current_hash}

            
def update_index(date, stats, ids, i, logger):
  stats_path = os.path.join(os.getcwd(), "tweets", "tweet_stats.json")
  ids_path = os.path.join(os.getcwd(), "tweets", "tweets_ids", f"{date}.json")
  ntweets = sum([len(d) for d in ids.values()])
  logger.debug(f"updating index for date {date} with {ntweets} a total of {i} tweets scanned")
  stats[date].update({"ntweets":ntweets})
  util.save_json(stats, stats_path)
  ids_to_save = {chunk:[*vals] for chunk, vals in ids.items()}
  util.save_json(ids_to_save, ids_path)

def load_date_index(date):
  stats_path = os.path.join(os.getcwd(), "tweets", "tweet_stats.json")
  ids_path = os.path.join(os.getcwd(), "tweets", "tweets_ids", f"{date}.json")
  if not os.path.exists(ids_path):
    return {}
  else:
    with open(ids_path) as f:
      ret = json.load(f)
      for chunk, vals in ret.items():
        ret[chunk] = {*vals}
      return ret

def load_index_stats():
  stats_path = os.path.join(os.getcwd(), "tweets", "tweet_stats.json")
  if not os.path.exists(stats_path):
    return {}
  else:
    with open(stats_path) as f:
      return json.load(f)


def hydrate(files_hash, last_hash, dls, orchestrator, logger, **kwargs):
  # list of files by day with list chunks of tweet ids
  files = files_hash["files"]

  # since the work to be performed is obtained directly from the stat file 
  stats = load_index_stats()
  
  chunk_to_hydrate = str(min([min([int(chunk) for chunk in info["to_hydrate"].keys()]) for date, info in stats.items()]))
  ntweets = sum([info["to_hydrate"][chunk_to_hydrate] for info in stats.values() if chunk_to_hydrate in info["to_hydrate"]])
  current_hash = f"{chunk_to_hydrate}:{ntweets}"
  base_dir = os.path.join(os.getcwd(), "tweets", "tweets_texts")
  if not os.path.exists(base_dir):
    os.mkdir(base_dir)

  new_files = []
  if current_hash != last_hash:
    logger.debug(f"going to hydrate {ntweets} tweets on chunk {chunk_to_hydrate}")
    i = 0
    
    # grouping by dates in monthly pachages
    months = sorted({date[0:7] for date in stats.keys()})
    twitter_proxy = orchestrator.get_actor('acquisition_twitter').get().proxy()
    for month in months:
      ids_to_hydrate = []
      ids_by_date = {}
      ntweets = sum([info["to_hydrate"][chunk_to_hydrate] for date, info in stats.items() if date[0:7]== month and chunk_to_hydrate in info["to_hydrate"]])
      logger.debug(f"processing {ntweets} twets for {month} in chunk {chunk_to_hydrate}")
      for date, info in stats.items():
        if chunk_to_hydrate in info["to_hydrate"] and date[0:7] == month:
          i = i + 1
          existing_tweets = load_tweet_date_chunk(date, chunk_to_hydrate)
          file_path = os.path.join(os.getcwd(), "tweets", "tweets_ids", f"{date}.json")
          tweet_index = load_date_index(date)
          tweets_ids = tweet_index[chunk_to_hydrate] - existing_tweets.keys()
          ids_to_hydrate.extend(tweets_ids)
          ids_by_date[date] = tweets_ids
      if len(ids_to_hydrate) > 0:
        # requesting tweet hydrate to twitter (using twitter actor)
        hydrated_tweets = twitter_proxy.hydrate_tweet_ids(ids_to_hydrate).get()
        # storing texts
        for date, info in stats.items():
          if chunk_to_hydrate in info["to_hydrate"] and date in ids_by_date and date[0:7] == month:
            res = load_tweet_date_chunk(date, chunk_to_hydrate)
            chunk_texts = {tweet_id:{**(hydrated_tweets.get(tweet_id) or {}), **{"date":date, "chunk":chunk_to_hydrate}} for tweet_id in ids_by_date[date]}
            chunk_texts.update(res)
            #writing changes
            save_tweet_date_chunk(chunk_texts, date, chunk_to_hydrate)

  else:
    logger.debug(f"Ignoring hydrate since no change was detected")
     
  # adding files to be processed
  for date, info in stats.items():
    if chunk_to_hydrate in info["to_hydrate"]:
      dest_dir = os.path.join(os.getcwd(), "tweets", "tweets_texts", f"{date}")
      dest_file = os.path.join(dest_dir, f"{chunk_to_hydrate}.json") 
      if os.path.exists(dest_file):
        new_files.append(dest_file)
    
  return {"files":new_files, "hash":current_hash}

def load_tweet_date_chunk(date, chunk):
  dest_dir = os.path.join(os.getcwd(), "tweets", "tweets_texts", f"{date}")
  if not os.path.exists(dest_dir):
    os.mkdir(dest_dir)
  dest_file = os.path.join(dest_dir, f"{chunk}.json") 
  res = {}
  if os.path.exists(dest_file):
    with open(dest_file, "rt") as f:
      res = {t["id"]:t for t in json.load(f)}
  return res

def save_tweet_date_chunk(data, date, chunk):
  dest_dir = os.path.join(os.getcwd(), "tweets", "tweets_texts", f"{date}")
  dest_file = os.path.join(dest_dir, f"{chunk}.json") 
  util.save_json([*data.values()], dest_file)

def chunk_done(files_hash, dls, logger, **kwargs):
  stats = load_index_stats()
  files = files_hash["files"]
  current_hash = files_hash["hash"]
  last_chunk = current_hash.split(":")[0]
  logger.debug(f"setting chunk {last_chunk} as processed")
  for date, info in stats.items():
    if last_chunk in info["to_hydrate"]:
      info["to_hydrate"].pop(last_chunk)
  stats_path = os.path.join(os.getcwd(), "tweets", "tweet_stats.json")
  util.save_json(stats, stats_path)

    
    
