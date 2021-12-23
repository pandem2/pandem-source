import pandas as pd
import pkg_resources
import json
import codecs
import os
import shutil
from . import util

def reset_variables(in_package = False, in_home = True):
  if in_package:
    write_json_variables( pkg_resources.resource_filename("pandem2source", "data/DLS/variables.json"))
  if in_home:
    dir_path = util.pandem_path("files", "variables")
    file_path = util.pandem_path("files", "variables", "variables.json")
    if not os.path.exists(dir_path):
      os.makedirs(dir_path)
    write_json_variables(file_path)
    # copy variables json files, indicators scripts or input-local folder if any
    for folder in ["variables", "indicators", "input-local"]:
      if pkg_resources.resource_exists("pandem2source", f"data/{folder}"):
        for sub_path in pkg_resources.resource_listdir("pandem2source", f"data/{folder}"):
          if folder=="input-local":
            var_from = pkg_resources.resource_filename("pandem2source", os.path.join("data", folder))
            var_to = util.pandem_path("files", folder)
          else:
            var_from = pkg_resources.resource_filename("pandem2source", os.path.join("data", folder, sub_path))
            var_to = util.pandem_path("files", folder, sub_path)
          shutil.copytree(var_from, var_to, copy_function = shutil.copy)
    # # copy input-local folder if any
    # if pkg_resources.resource_exists("pandem2source", "data/input-local"):
    #     for sub_path in pkg_resources.resource_listdir("pandem2source", "data/input-local"):
    #       print(f'sub-path is: {sub_path}')
    #       var_from = pkg_resources.resource_filename("pandem2source", os.path.join("data", "input-local"))
    #       var_to = util.pandem_path("files", "input-local", sub_path)
    #       shutil.copytree(var_from, var_to, copy_function = shutil.copy)



def read_variables_xls():
  path = pkg_resources.resource_filename("pandem2source", "data/list-of-variables.xlsx")
  df = pd.read_excel(path)
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
    if col not in ["description", "modifiers", "formula"] and not df[col].isnull().values.all():
      df[col] = df[col].str.lower().str.replace(", ", ",", regex=False).str.replace(".", "", regex=False).str.replace(" ", "_",regex=False)
    if col in ["linked_attributes", "partition"]:
      df[col] = df[col].str.split(",")
    if col == "modifiers":
      df[col] = df[col].apply(lambda x : eval(x))
  return df

def write_json_variables(dest):
  df = read_variables_xls()
  path = dest
  result = df.to_json(orient = "records")
  parsed = json.loads(result)
  j = json.dumps(parsed, indent=2)

  file = codecs.open(path, "w", "utf-8")
  file.write(j)
  file.close()

def reset_source(source_name):
  dls_from = pkg_resources.resource_filename("pandem2source", os.path.join("data", "DLS", f"{source_name}.json"))
  if os.path.exists(dls_from):
    dls_to = util.pandem_path("files", "source-definitions", f"{source_name}.json")
    dls_to_dir = util.pandem_path("files", "source-definitions")
    if not os.path.exists(dls_to_dir):
      os.makedirs(dls_to_dir)
    shutil.copyfile(dls_from, dls_to)
  else:
    raise ValueError(f"Cannot find source definition {source_name} within pandem default sources")
  
  # copytin script if any
  with open(util.pandem_path("files", "source-definitions", f"{source_name}.json"), "r") as f:
    dls = json.load(f)
    
  if "changed_by" in dls["acquisition"]["channel"] and  "script_type" in dls["acquisition"]["channel"]["changed_by"]:
    script_type = dls["acquisition"]["channel"]["changed_by"]["script_type"]
    script_name = dls["acquisition"]["channel"]["changed_by"]["script_name"]
    script_from = pkg_resources.resource_filename("pandem2source", os.path.join("data", "scripts", script_type, f"{script_name}.{script_type}"))
    script_to = util.pandem_path("files", "scripts", script_type, f"{script_name}.{script_type}" )
    script_to_dir = util.pandem_path("files", "scripts", script_type)
    if not os.path.exists(script_to_dir):
      os.makedirs(script_to_dir)
    shutil.copyfile(script_from, script_to)

def delete_all():
    if os.path.exists(util.pandem_path("files")):
      shutil.rmtree(util.pandem_path("files"))
    if os.path.exists(util.pandem_path("database")):
      shutil.rmtree(util.pandem_path("database"))


