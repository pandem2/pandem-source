import os
import datetime
import pickle
import json
import numpy
import io
import getpass
import yaml
import psutil
import logging
import hashlib
import itertools
l = logging.getLogger("pandem.perf")


def printMem(msg = ""):
  l.debug(f"{msg} Used Mem: {psutil.virtual_memory().used / (1024*1024*1024)}")

def check_pandem_home():
  if os.environ.get("PANDEM_HOME") is None:
    raise RuntimeError("The variable PANDEM_HOME needs to be set to a local folder in order to run pandem source")

def pandem_path(*path):
  if os.environ.get("PANDEM_HOME") is None:
    raise RuntimeError("The variable PANDEM_HOME needs to be set to a local folder in order to run pandem source")
  else:
    return os.path.join(os.environ.get("PANDEM_HOME"), *path)

def settings():
  config = pandem_path("settings.yml")
  with open(config, "r") as f:
    settings = yaml.safe_load(f)
  if os.environ.get("PANDEM_NLP") is not None:
     settings["pandem"]["source"]["nlp"]["models_path"] = os.environ.get("PANDEM_NLP")
  return settings

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
  return json.dumps(o,cls=JsonEncoder, indent = 4)

def get_or_set_secret(name):
  secret_dir = pandem_path("secrets")
  if not os.path.exists(secret_dir):
    os.makedirs(name = secret_dir, mode = 0o700)

  secret_path = os.path.join(secret_dir, name)
  if not os.path.exists(secret_path):
    p = getpass.getpass(prompt = f"Password {name} not found, please type it or put a file with its content (UTF-8) in {secret_path}")
    with open(secret_path, 'w', encoding='utf-8') as f:
      f.write(p)
    os.chmod(secret_path, 0o400)
  with io.open(secret_path, mode = "r", encoding = "utf-8") as f:
    return f.read()

def get_custom(path, function):
  if type(path) == str:
    path = [path]
  path = [*( p.replace('-', '_') for p in path )]
  script_path = pandem_path(*(["files", "scripts", "py"] + path))
  if not os.path.exists(script_path+".py"):
    return None
  else :
    exec(f"import {'.'.join(path)}")
    if not eval(f"hasattr({'.'.join(path)}, '{function}')"):
      return None
    else:
      return eval(f"{'.'.join(path)}.{function}")
 
def md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

 
class JsonEncoder(json.JSONEncoder):
  def default(self, z):
    if isinstance(z, datetime.datetime) or isinstance(z, datetime.date):
      return (str(z))
    elif  isinstance(z, numpy.int64):
      return int(z)
    else:
      return super().default(z)

def slices(iterable, size):
   head = list(itertools.islice(iterable, size))
   while len(head) > 0:
     yield head
     head = list(itertools.islice(iterable, size))

def save_pickle(o, path):
  fd, fn = os.path.split(path)
  tpath = os.path.join(fd, f'.{fn}.tmp')
  with open(tpath, "wb") as f:
    pickle.dump(o, f)
  os.rename(tpath, path)

def save_pickle_df(df, path):
  fd, fn = os.path.split(path)
  tpath = os.path.join(fd, f'.{fn}.tmp')
  df.to_pickle(tpath)
  os.rename(tpath, path)

def save_json(o, path, indent = None):
  fd, fn = os.path.split(path)
  tpath = os.path.join(fd, f'.{fn}.tmp')
  with open(tpath, 'w') as f:
    json.dump(o, f, cls=JsonEncoder, indent = indent)
  os.rename(tpath, path)


def compress(x, get_id = {}, get_val = {}, i = [0]):
  if type(x) == dict:
    items = [*x.items()]
    x.clear()
    for k, v in items:
      k = compress(k, get_id, get_val, i)
      v = compress(v, get_id, get_val, i)
      x[k] = v
    return x
  elif type(x) in (list, tuple):
    for j in range(0, len(x)):
      x[j] = compress(x[j], get_id, get_val, i)
    return x
  else:
    if x in get_val:
      x = get_val[x]
    else:
      get_val[x] = i[0]
      get_id[i[0]] = x
      x = i[0]
      i[0] = i[0] + 1
    return x

class NA():
  pass

#from dataclasses import dataclass
#@dataclass(frozen=True)
#class strdic:
#  _val: str
#  def __init__(self, dic):
#    t = sorted(tuple(val.items()), lambda t:t[0])
#    self._val = json.dumps(t,cls=JsonEncoder)
#  def val():
#    return dict(*json.loads(self._val,cls=JsonEncoder))

class tuples():
  def __init__(self, tuples):
    self._2id = {}
    self._2val = {}
    self._i = 0

    self._vars = [self.id(next(iter(t["obs" if "obs" in t else "attr"].keys()))) for t in tuples]
    self._group = "obs" if "obs" in tuples[0] else "attr"
    self._vals = [next(iter(t["obs"].values())) for t in tuples] if self._group == "obs" else [self.id(next(iter(t["attr"].values()))) for t in tuples]
    self._attrs = [self.id(t["attrs"]) for t in tuples]

  def id(self, val):
    if type(val) == dict:
      val = tuple(sorted(list(filter(lambda v: v[0]!="line_number", val.items())), key = lambda t:t[0]))
    if val in self._2id:
      return self._2id
    else:
      self._2id[val] = self._i
      self._2val[self._i] = val
      self._i = self._i + 1
      return self._i

  def val(self, id):
    v = self._2val[id]
    if type(v) == tuple:
      return dict(*v)
    else:
      return v
