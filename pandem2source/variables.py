import pandas as pd
import pkg_resources
import json

def read_variables_xls():
  path = pkg_resources.resource_filename("pandem2source", "data/list-of-variables.xlsx")
  df = pd.read_excel(path)
  df = df.rename(columns = {"Variable":"variable", "Data Family":"data_family", "Linked Attributes":"linked_attributes", "Aliases":"aliases", "Description":"description", "Type":"type", "Unit":"unit"})
  for col in df.columns:
    if col != "description":
      df[col] = df[col].str.lower().str.replace(", ", ",").str.replace(".", "").str.replace(" ", "-")
    if col == "linked_attributes":
      df[col] = df[col].str.split(",")
    if col == "aliases":
      df[col] = df[col].apply(json.loads)
  return df

def write_json_variables():
  df = read_variables_xls()
  path = pkg_resources.resource_filename("pandem2source", "data/list-of-variables.json")
  df.to_json(path, orient = "index")
