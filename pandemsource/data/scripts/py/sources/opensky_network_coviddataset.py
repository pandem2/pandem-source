import pandas as pd
import json
import os

def df_transform(df):
  path = os.path.join(os.environ.get("PANDEM_HOME"), "files/variables/geo_airport_code/default.json")
  with open(path, "r") as f:
    m = json.load(f)
  air_map = {t["attr"]["geo_airport_code"]:t["attrs"]["geo_code"] for t in m["tuples"] if "geo_code" in t["attrs"]}
  
  path = os.path.join(os.environ.get("PANDEM_HOME"), "files/variables/geo_airport_origin_code")
  air_map_orig = {}
  for fn in os.listdir(path):
    if fn.endswith("json"):
      with open(os.path.join(path, fn), "r") as f:
        m = json.load(f)
        air_map_orig = {**air_map_orig, **{t["attr"]["geo_airport_origin_code"]:t["attrs"]["country_of_origin_code"] for t in m["tuples"] if "country_of_origin_code" in t["attrs"]}}

  df['origin'] = df['origin'].apply(lambda x:air_map_orig.get(x))
  df['destination'] = df['destination'].apply(lambda x:air_map.get(x))
  df = df[pd.notna(df['origin'])]
  df = df[pd.notna(df['destination'])]
  df = df.groupby(['day', 'destination', 'origin'])['callsign'].count()
  df = df.reset_index()
  df['line_number'] = range(1, len(df)+1) 
  df = df.rename(columns={"callsign": "flights"})
  return df
