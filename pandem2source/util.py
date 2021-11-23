import os
import datetime
import json
import numpy

def check_pandem_home():
  if os.environ.get("PANDEM_HOME") is None:
    raise RuntimeError("The variable PANDEM_HOME needs to be set to a local folder in order to run pandem source")

def pandem_path(*path):
  if os.environ.get("PANDEM_HOME") is None:
    raise RuntimeError("The variable PANDEM_HOME needs to be set to a local folder in order to run pandem source")
  else:
    return os.path.join(os.environ.get("PANDEM_HOME"), *path)


def absolute_to_relative(path, inner_path):
    rel = os.environ.get("PANDEM_HOME")
    if path.startswith(rel):
      path = path[len(rel):len(path)]
    if path.startswith("/") or path.startswith("\\"):
      path = path[1:len(path)]
    rel = inner_path
    if path.startswith(rel):
      path = path[len(rel):len(path)]
    return path

def pretty(o):
  class JsonEncoder(json.JSONEncoder):
    def default(self, z):
      if isinstance(z, datetime.datetime) or isinstance(z, numpy.int64):
        return (str(z))
      else:
        return super().default(z)
  return json.dumps(o,cls=JsonEncoder, indent = 4)
