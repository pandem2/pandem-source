import yaml
import os

class config:
  _settings = {}

  def __init__(self, location = None):
    # loading default config
    pkg_dir, this_filename = os.path.split(__file__)
    defaults = os.path.join(pkg_dir, "data/defaults.yml") 
    with open(defaults, "r") as f:
      conf = yaml.safe_load(f)   
    if location is not None and os.path.exists(location):
      with open(location, "r") as f:
        userconf = yaml.safe_load(f)
      merge_config(conf, userconf)
    if "pandem" in conf and "sources" in conf["pandem"]: 
      self._settings = conf["pandem"]["sources"]
    else: 
      raise KeyError("Cannot find attribute pandem.sources on configuration")

  def get(self, path):
    setting = self._settings
    for p in path.split("."):
      if type(setting) is dict and p in setting:
        setting = setting[p]
      else:
        raise KeyError(f"Cannot find property '{p}' of '{path}' on settings")
    return setting

  def find(self, paths = None):
    if paths == None:
      return flatten_map(self._settings)
    else:
      if type(paths) is str:
        paths = [paths]
      ret = {}
      for path in paths:
        settings = self._settings
        base_path = ""
        for p in path.split("."):
          base_path = f"{base_path}.{p}" if len(base_path)>0 else p 
          if type(settings) is dict and p in settings:
            settings = settings[p]
          else:
            raise KeyError(f"Cannot find property '{p}' of '{path}' on settings")
        if type(settings) is dict:
          flatten_map(settings, path = base_path, curr = ret)
        else:
          ret[base_path] = setting
      return ret

def merge_maps(a, b, path = []):
  "merges map b into a"
  if path is None: path = []
  for key in b:
    if key in a:
      if isinstance(a[key], dict) and isinstance(b[key], dict):
        merge_maps(a[key], b[key], path + [str(key)])
      elif a[key] == b[key]:
        pass # same leaf value
      else:
        raise Exception('Conflict at %s' % '.'.join(path + [str(key)]))
    else:
      a[key] = b[key]
  return a

def flatten_map(m, path = None, curr = {}):
  assert type(m) is dict, f"To flatten map {type(m)} was expected to be a dictionary"
  for key, value in m.items():
    next_path = f"{path}.{key}" if path is not None else key
    if type(value) is dict:
      flatten_map(value, path = next_path, curr = curr)
    else: 
      curr[next_path] = value
  return curr

