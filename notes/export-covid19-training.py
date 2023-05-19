import requests
import pandas as pd
import os
import time

def export_covid19training_ds():

  res = requests.get(f"http://localhost:8000/timeseries?key=True&data=False").json()
  
  all_ts = res["timeseries"]
  
  res = requests.get(f"http://localhost:8000/variable_list").json()
  var_map = {v["variable"]:v for v in res["variables"]}
  
  renamings = {"indicence_1000":"estimated_incidence" }
  source_fields = ["source__reference_user", "source__source_description", "source__source_name", "source", "source__table"]
  indicator_fields = ["indicator", 'indicator__family', 'indicator__unit', 'indicator__description']
  excluded_endings = {"_alert"}
  excluded_fields = {"key"}
  
  print(f"{len(all_ts)} timeseries found on instance")
  available_sources = {t["source__source_name"] for t in all_ts}
  target_sources  = {"COVID-19 Datahub", "ECDC COVID-19", "SeroTracker", "Twitter datasets"}
  missing_sources = target_sources.difference(available_sources)
  sources = available_sources.intersection(target_sources)
  back = "\n"
  
  
  if len(missing_sources) > 0:
    print(f"Warning, the following sources are not present on this instance \n{back.join([('----- ' + s) for s in missing_sources])}") 
  if len(sources) == 0:
    print("No available sources were found so no data will be exported")
  else:
    dfs = {}
    ts = [t for t in all_ts if t["source__source_name"] in sources and "indicator__family" in t and len([ea for ea in excluded_endings if t["indicator"].endswith(ea)]) == 0]
    ind = {tuple(t[f] for f in indicator_fields) for t in ts}
    ind_map = {i: {"family":f, "indicator":i, "unit":u, "description":d} for i, f, u, d in ind}
    print(f"Going to export {len(ts)} time series for {len(ind)} indicators")
    
    ind_df = pd.DataFrame.from_records(list(ind_map.values())).sort_values(["family", "indicator"]).reset_index(drop = True)
    export_csv({"00_indicators":ind_df})
  
    fields = {v for t in ts for v in t.keys() if v not in excluded_fields.union(indicator_fields)}
    labels = {f for f in fields if f.endswith("_label")}
    refs = {l.replace("_label", ""):l for l in labels}
    chars = fields.difference(labels.union(refs.keys()).union(source_fields).union(indicator_fields))
    char_values = {c:{(t[c], None) for t in ts if c in t} for c in chars}
    ref_values = {c:{(t[c], t[l]) for t in ts if c in t and l in t} for c, l in refs.items()}
    all_values = {**char_values, **ref_values}
    charlist = []
    geo_codes_source = {i:{(its["geo_code"], its["source"]) for its in ts if its["indicator"] == i if "geo_code" in its} for i in ind_map.keys()}
    for c in chars.union(refs):
      v = c.replace("ref__", "")
      charlist.append({
        "data_family": var_map[v]["data_family"], 
        "characteristic": v, 
        "description":var_map[v]["description"]
      })
  
    char_df = pd.DataFrame.from_records(charlist)
  
    export_csv({"00_characteristics":char_df})
    
    dic_values = sorted([{"characteristic":c, "value":v, "label":l} for c, values in {**char_values, **ref_values}.items() for v, l in values], key = lambda v: (v["characteristic"], v["value"])) 
    dic_df = pd.DataFrame.from_records(dic_values)
    export_csv({"00_dictionary":dic_df})
   
    rows = []

    start = time.time()
    i = 1
    for ii, row in ind_df.iterrows():
      ind = row["indicator"]
      print(f"exporting {ind}                                                                                                                   ")
      rows = []
      for g, s in sorted(geo_codes_source[ind]):
        offset = 0
        res = requests.get(f"http://localhost:8000/timeseries?key=True&data=True&indicator={ind}&limit=200&offset={offset}&geo_code={g}&source_table={s}").json()
        its = res["timeseries"]
        while len(its)>0 :
          secs = time.time() - start
          print(f'{round(100*i/len(ts),1)}%, time elapsed: {get_time(secs)}, remaining {get_time((secs*len(ts))/i-secs)}                                                ', end='\r')
          for igts in its:
            for j in range(0, len(igts["data"])):
              rows.append({
                **{"ts_id":i, "indicator":ind, "date":igts["data"][j]["date"], "value":igts["data"][j]["value"]},
                **igts["key"],
                **{"source":igts["source__source_name"], "source_file":igts["source"], "geo_level":igts.get("geo_level"), "data_quality":igts["data_quality"]}
              })
            i = i + 1
          offset = offset + len(its)
          res = requests.get(f"http://localhost:8000/timeseries?key=True&data=True&indicator={ind}&limit=200&offset={offset}&geo_code={g}&source_table={s}").json()
          its = res["timeseries"]
      ind_df = pd.DataFrame.from_records(rows)
      export_csv({f"{ind_map[ind]['family']}_{ind}":ind_df})



def get_time(secs):
  return f"{round(secs/3600):02d}:{round(secs/60)%60:02d}:{round(secs % 60):02d} secs"

def export_csv(dfs):
  export_folder = os.path.join(os.environ.get("PANDEM_HOME"), "export", "covid_19_training_ds")
  os.makedirs(export_folder, exist_ok = True)
  for n, df in dfs.items():
    df.to_csv(os.path.join(export_folder, f"{n}.csv"), encoding='utf-8', index=False)

export_covid19training_ds()
