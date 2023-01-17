import pandas as pd
import pkg_resources
import json
import codecs
import threading
import os
import shutil
import subprocess
import time
import requests
import logging
import pickle
from . import util

l = logging.getLogger("pandem-admin")

def reset_variables():
  dir_path = util.pandem_path("files", "variables")
  home_path = util.pandem_path("files", "variables", "variables.csv")
  if not os.path.exists(dir_path):
    os.makedirs(dir_path)
  pkg_path = pkg_resources.resource_filename("pandemsource", "data/list-of-variables.csv")
  shutil.copy(pkg_path, home_path)
  # rewriting default variables
  reset_default_folders("variables", "indicators", delete_existing = False)

def reset_default_folders(*folders, delete_existing = True):
  # copy default data from data folder
  for folder in folders:
    if pkg_resources.resource_exists("pandemsource", f"data/{folder}"):
      var_from = pkg_resources.resource_filename("pandemsource", os.path.join("data", folder))
      var_to = util.pandem_path("files", folder)
      if os.path.exists(var_to) and delete_existing:
        shutil.rmtree(var_to)
      shutil.copytree(var_from, var_to, copy_function = shutil.copy, dirs_exist_ok = True)

def parseJsonShowError(j):
  try:
    return json.loads(j)
  except Exception as e:
    raise ValueError(f"Cannot interpret {j[0:300]} as JSON\n{e}")

def read_variables_definitions():
  path = util.pandem_path("files", "variables", "variables.csv")
  df = pd.read_csv(path, encoding = "ISO-8859-1")
  df = df.rename(columns = {
    "Variable":"variable", 
    "Data Family":"data_family", 
    "Linked Attributes":"linked_attributes", 
    "Partition":"partition", 
    "Aliases":"aliases", 
    "Description":"description", 
    "Type":"type", 
    "Unit":"unit", 
    "Datasets":"datasets"
    })

  for col in df.columns:
    if col not in ["description", "modifiers", "formula", "no_report", "synthetic_tag", "synthetic_blocker"] and not df[col].isnull().values.all():
      df[col] = df[col].str.lower().str.replace(", ", ",", regex=False).str.replace(".", "", regex=False).str.replace(" ", "_",regex=False)
    if col in ["linked_attributes", "partition", "synthetic_tag", "synthetic_blocker"]:
      df[col] = df[col].apply(lambda x : [v.strip() for v in str(x).split(",")] if pd.notna(x) else None)
    if col == "modifiers":
      df[col] = df[col].apply(lambda x : parseJsonShowError(x) if pd.notna(x) else [])
    if col == "no_report":
      df[col] = df[col].apply(lambda x : str(x).lower() == 'true' if pd.notna(x) else False)
  result = df.to_json(orient = "records")
  parsed = json.loads(result)
  return parsed

def reset_source(source_name, delete_data = False, reset_acquisition = False ):
  dls_from = pkg_resources.resource_filename("pandemsource", os.path.join("data", "DLS", f"{source_name}.json"))
  dls_to = util.pandem_path("files", "source-definitions", f"{source_name}.json")
  # copyint DLS files and deleting data if delete_data = True
  if os.path.exists(dls_from):
    dls_to_dir = util.pandem_path("files", "source-definitions")
    if not os.path.exists(dls_to_dir):
      os.makedirs(dls_to_dir)
    shutil.copyfile(dls_from, dls_to)
    if delete_data:
      delete_source_data(source_name)
  else:
    raise ValueError(f"Cannot find source definition {source_name} within pandem default sources")
  
  # resetting source hash if reset_acquisition = True
  path = util.pandem_path("database/sources.pickle")
  if reset_acquisition and os.path.exists(path):
    with open(path, "rb") as f:
      s = pickle.load(f)
    for i in s[s["name"] == source_name].index:
      s.at[i, "last_hash"] = ""

      util.save_pickle(s, path)
  
    
  if os.path.exists(dls_to):
    # reading DLS file
    with open(util.pandem_path("files", "source-definitions", f"{source_name}.json"), "r") as f:
      dls = json.load(f)
    
    # copying the changing script if any is defined
    if "changed_by" in dls["acquisition"]["channel"] and  "script_type" in dls["acquisition"]["channel"]["changed_by"]:
      script_type = dls["acquisition"]["channel"]["changed_by"]["script_type"]
      script_name = dls["acquisition"]["channel"]["changed_by"]["script_name"]
      script_from = pkg_resources.resource_filename("pandemsource", os.path.join("data", "scripts", script_type, f"{script_name}.{script_type}"))
      if os.path.exists(script_from):
        script_to = util.pandem_path("files", "scripts", script_type, f"{script_name}.{script_type}" )
        script_to_dir = util.pandem_path("files", "scripts", script_type)
        if not os.path.exists(script_to_dir):
          os.makedirs(script_to_dir)
        shutil.copyfile(script_from, script_to)

    # Copying sustom Python scripts from package to pandem-home
    pyscript_fol = pkg_resources.resource_filename("pandemsource", os.path.join("data", "scripts", "py", "sources"))
    dls_pyscript = dls["scope"]["source"].replace("-", "_").replace(" ", "_") + ".py"
    pyscript_to = util.pandem_path("files", "scripts", "py", "sources", dls_pyscript)
    if dls_pyscript in os.listdir(pyscript_fol):
      shutil.copyfile(os.path.join(pyscript_fol, dls_pyscript), pyscript_to)

def delete_source_data(source_name):
  ts_path = util.pandem_path("files/variables/time_series.pi" ) 
  var_path = util.pandem_path("files/variables" ) 
  ind_to_delete = []
  i = 0
  source_to_delete = [source_name]
  if os.path.exists(ts_path):
    with open(ts_path, "rb") as f:
      ts = pickle.load(f)

    for k in [t for t in ts.keys() if (
            (len(ind_to_delete) == 0 or len([k for k, v in t if k == "indicator" and v in ind_to_delete]) > 0 ) and
            (len(source_to_delete) == 0 or len([k for k, v in t if k == "source" and v in source_to_delete]) > 0 )
          )
        ]:
      ts.pop(k)
      i = i + 1
    util.save_pickle(ts, ts_path)

  l.info(f"{i} timeseries deleted for {source_name}")

  j = 0
  if os.path.exists(var_path):
    if len(source_to_delete) > 0:
      for root, dirs, files in  os.walk(var_path):
         for name in files:
           for s in source_to_delete:
             if s in name:
               p = os.path.join(root, name)
               if os.path.exists(p):
                 os.remove(p)
                 j = j + 1
    
  l.info(f"{j} files deleted for {source_name}")


def delete_all():
    if os.path.exists(util.pandem_path("settings.yml")):
      os.remove(util.pandem_path("settings.yml"))
    if os.path.exists(util.pandem_path("files")):
      shutil.rmtree(util.pandem_path("files"))
    if os.path.exists(util.pandem_path("database")):
      shutil.rmtree(util.pandem_path("database"))

def install_issues(check_nlp = True):
  settings = util.settings()
  ret = []
  FNULL = open(os.devnull, 'w') 
  if shutil.which("R") is None:
    ret.append("Cannot find R language. Please installe it. PANDEM2 needs it to calculate indecators")
  else:
    r_packages = '"dplyr", "shiny", "plotly", "DT", "jsonlite", "httr", "XML", "ggplot2", "epitweetr", "reticulate", "seqinr", "readr"'
    installed = subprocess.run(['R', '-e', f'if(length(setdiff(c({r_packages}), names(installed.packages()[,1])))> 0) stop("some packages are missing!!")'], stdout=FNULL, stderr=FNULL).returncode
    if installed == 1:
      ret.append(f'Cannot find some necessary R packages, please intall them from CRAN, by running install.packages(c({r_packages}))')
    installed = subprocess.run(['R', '-e', f'if(length(setdiff(c("COVID19"), names(installed.packages()[,1])))> 0) stop("some packages are missing!!")'], stdout=FNULL, stderr=FNULL).returncode
    if installed == 1:
      ret.append(f"""Cannot find COVID19 R packages necessary for getting COVID19 data hub data. 
      if you want to download data directly from sources you have to install it as follow:
        install.packages("devtools")
        devtools::install_github(repo = "covid19datahub/COVID19")
      If you prefer downloading data prepared by COVID data hub tem you can install it from CRAN, by running install.packages(c("COVID19"))""")
    installed = subprocess.run(['R', '-e', f'if(length(setdiff(c("Pandem2simulator"), names(installed.packages()[,1])))> 0) stop("some packages are missing!!")'], stdout=FNULL, stderr=FNULL).returncode
    if installed == 1:
      ret.append(f"""Cannot find Pandem2simulator R packages necessary for simulating COVID19 detailed data from ecdc datasets. 
      You can install this package with the following command:
        install.packages("devtools")
        
      devtools::install_github("maous1/Pandem2simulator")
      """)
  need_nlp = is_nlp_needed() 
  if settings["pandem"]["source"]["nlp"]["active"] and check_nlp and need_nlp:
     models_path = settings["pandem"]["source"]["nlp"]["models_path"]
     if os.path.exists(models_path):
       if not nlp_models_up():
         # Here if it will be to pandem2 to launch docker since models are not up   
         if shutil.which("docker") is None:
           ret.append("Cannot find docker and NLP models are not running, we need it to start the NLP server. Please install it.")
         else:
           l.info("Launching docker for serving NLP modles")
           retries = 10
           threading.Thread(target=nlp_run_model_server).start()
           while retries > 0 and not nlp_models_up():
             l.info("Waiting 1 until models are running")
             time.sleep(1)
             retries = retries - 1
       if not nlp_models_up():
         ret.append(f"""Launching of docker NLP server failed. Please check any previous errors.
           If you cannot (or do not want to) fix this, you can deactivate the NLP models on the settings.json file by setting the property by changing pandem.source.nlp.active to False 
           un {util.pandem_path('settings.yml')}
           To fix this ensure that:
             - Docker is installed https://docs.docker.com/engine/install/
             - You have downloaded the PANDEM-2 SMA 2.1 components to a local folder https://drive.google.com/file/d/14C1BSmmq_ObB-OvSBpaS5EpsyZ5ueQME/view?usp=share_link
             - You have either set the PANDEM_NLP environment variable to the folder holding the models or set the value pandem.source.nlp.models.path on  {util.pandem_path('settings.yml')}
             - You can test the launching command as follow
               {nlp_docker_launch_command()}
          """
         )
     else:
       ret.append(f"""NLP Annotation is active as per settings but the models path has not been found which is necessary to detect the existing models (even when running the server outside PANDEM-2
             You have either set the PANDEM_NLP environment variable to the folder holding the models or set the value pandem.source.nlp.models.path on  {util.pandem_path('settings.yml')}
       """)
  if check_nlp and are_twitter_credentials_missing() and need_nlp:
    ret.append(f"""Twitter credentials are necessary since one of your sources uses twitter, but we could not find the credentials
      please try running PANDEM-2 again to provide the right credentials or remove the twitter data source definition on {util.pandem_path("files", "source-definitions")}
      To get twitter credentials please check you have to decralre it. Please check this: https://developer.twitter.com/en/docs/twitter-api/getting-started/getting-access-to-the-twitter-api
    """)

  return ret

def nlp_models_up():
  settings = util.settings()
  models_path = settings["pandem"]["source"]["nlp"]["models_path"]
  models = list(filter(lambda v: not v.startswith("."), next(os.walk(models_path))[1]))
  tf_url = f"{settings['pandem']['source']['nlp']['tensorflow_server_protocol']}://{settings['pandem']['source']['nlp']['tensorflow_server_host']}:{settings['pandem']['source']['nlp']['tensorflow_server_port']}"
  model_endpoints = {m:f"{tf_url}/v1/models/{m}" for m in models}

  for ep in model_endpoints.values():
    try:
      if requests.get(ep).status_code != 200:
        return False
    except Exception as err:
      return False
  return True

def nlp_docker_launch_command():
  settings = util.settings()
  tf_usesudo = settings["pandem"]["source"]["nlp"]["use_sudo"]
  tf_port = settings["pandem"]["source"]["nlp"]["tensorflow_server_port"]
  tf_version = settings["pandem"]["source"]['nlp']["tensorflow_server_version"]
  models_path = settings["pandem"]["source"]["nlp"]["models_path"]
  models = list(filter(lambda v: not v.startswith("."), next(os.walk(models_path))[1]))
  
  if os.name == "posix" and tf_usesudo:
    cmd = ["sudo"]
  else: 
    cmd = []
  cmd.extend(["docker", "run", "-p", f"{tf_port}:8501"]) 
  
  for model in models:
    cmd.extend(["-v",  f"{models_path}{os.sep}{model}:/models/{model}/1/"]) 

  cmd.extend(["-v", f"{models_path}{os.sep}model_config.config:/models/model_config.config"])
  cmd.extend(["--rm", "-t", f"tensorflow/serving:{tf_version}", "--model_config_file=/models/model_config.config"])
  return cmd

def nlp_run_model_server():
  #sudo_password = getpass.getpass(prompt='enter your sudo password: ')
  #print("pwd received!")
  cmd = nlp_docker_launch_command()
  l.debug(f"Launching docker comman for nlp model server: {cmd}")
  #p = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE,  stdin=subprocess.PIPE)
  #with open(os.devnull, 'w') as devnull:
  p = subprocess.run(cmd)
  
  #try:
  #  out, err = p.communicate(input=(sudo_password+'\n').encode(),timeout=20)
  #except subprocess.TimeoutExpired:
  #  print("\n\n\n\n\n\n BUAAAAAAAAAAAAAAAAAAAAAA")
  #  p.kill()

def is_nlp_needed():
  dls_path = util.pandem_path("files", "source-definitions")
  if not os.path.exists(dls_path):
    return False
  for f in os.listdir(dls_path):
   if f.endswith(".json"):
     with open(os.path.join(dls_path, f)) as fj:
       js = json.load(fj)
     if "columns" in js:
       for c in js["columns"]:
         if "variable" in c and c["variable"] == "article_text":
           return True
  return False

def are_twitter_credentials_missing():
   dls_path = util.pandem_path("files", "source-definitions")
   if len([
       p for 
       p in os.listdir(dls_path) 
       if p.endswith(".json") 
         and json.load(open(os.path.join(dls_path, p)))["acquisition"]["channel"]["name"] == "twitter"]
   ) > 0:
     l.info("Checking if twitter API are necessary")
     secrets = [
       util.get_or_set_secret("twitter-api-key"), 
       util.get_or_set_secret("twitter-api-key-secret"), 
       util.get_or_set_secret("twitter-access-token"),
       util.get_or_set_secret("twitter-access-token-secret")
     ]
     return any(filter(lambda s: s == "", secrets))
   return False

def run_pandem2app():
  settings = util.settings()
  cmd = ["Rscript", util.pandem_path("files", "scripts", "R", "pandem-source-app.R"), str(settings["pandem"]["source"]["app"]["port"]), util.pandem_path("files", "img", "Partner-map_PANDEM-2.jpg")]
  l.debug(f"Launching app server: {cmd}")
  def run_app(): 
    subprocess.run(cmd)
  threading.Thread(target=run_app).start()


def list_sources_dir(p):
  ret = dict()
  for f in os.listdir(p):
   if f.endswith(".json"):
     with open(os.path.join(p, f)) as fj:
       js = json.load(fj)
       if "scope" in js:
         s, t = (js["scope"].get("source"), js["scope"].get("tags"))
         ret[s] = t[0] if len(t) > 0 else s
  return ret

def list_jobs(source = None):
  job_paths = util.pandem_path("database", "jobs.pickle")
  with open(job_paths, 'rb') as f:
    df = pickle.load(f)
  if source is not None:
    df = df[df["source"]==source]
  return df

def list_sources(local = True, default = False, missing_local = False, missing_default = False):
  local_map = list_sources_dir(util.pandem_path("files", "source-definitions"))
  default_map = list_sources_dir(pkg_resources.resource_filename("pandemsource", os.path.join("data", "DLS")))
  
  if local and missing_local or default and missing_default:
    raise ValueError("Cannot lis both missing and existing sources")
  ret = list()
  if local:
    ret.extend(local_map.items())
  elif missing_local:
    ret.extend((k, v) for k, v in default_map.items() if k not in local_map)
  if default:
    ret.extend(local_map.items())
  elif missing_default:
    ret.extend((k, v) for k, v in local_map.items() if k not in defaul_map)
  return ret

