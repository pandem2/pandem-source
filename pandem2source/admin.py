import pandas as pd
import pkg_resources
import json
import codecs
import os
from . import util

def reset_variables(in_package = False, in_home = True):
  if in_package:
    write_json_variablesi( pkg_resources.resource_filename("pandem2source", "data/DLS/variables.json"))
  if in_home:
    dir_path = util.pandem_path("files", "variables")
    file_path = util.pandem_path("files", "variables", "variables.json")
    if not os.path.exists(dir_path):
      os.makedirs(dir_path)
    write_json_variables(file_path)

def read_variables_xls():
  path = pkg_resources.resource_filename("pandem2source", "data/list-of-variables.xlsx")
  df = pd.read_excel(path)
  df = df.rename(columns = {
    "Variable":"variable", 
    "Data Family":"data_family", 
    "Linked Attributes":"linked_attributes", 
    "Aliases":"aliases", 
    "Description":"description", 
    "Type":"type", 
    "Unit":"unit", 
    "Datasets":"datasets"
    })

  for col in df.columns:
    if col != "description":
      df[col] = df[col].str.lower().str.replace(", ", ",").str.replace(".", "").str.replace(" ", "-")
    if col == "linked_attributes" or col == "datasets":
      df[col] = df[col].str.split(",")
    if col == "aliases":
      df[col] = df[col].apply(json.loads)
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

