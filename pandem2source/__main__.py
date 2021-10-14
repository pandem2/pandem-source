import sys, os
import yaml
import argparse

#from .config import config
#from . import api
from .orchestrator import Orchestration

def main(a):
  #conf = config()
  # Base argument parser
  parser = argparse.ArgumentParser()
  subs = parser.add_subparsers()
  
  # Launch pandem source start command
  start_parser = subs.add_parser("start", help = "Launch pandem a 2 source")
  '''
  driver_parser.add_argument(
    "-p", 
    "--port", 
    required=False, 
    help="Path to a file containing the extraction files to be encryped. If not provided it will be prompted", 
    default = conf.get("driver.port"), 
    type = int
  )'''
  start_parser.set_defaults(func = do_start)
  
  '''
  #calling handlers
  func = None
  try:
    args = parser.parse_args()
    func = args.func
  except AttributeError:
    parser.print_help()
  if func != None:
    args.func(args, parser, conf)
  '''

#calling handlers
  func = None
  try:
    args = parser.parse_args()
    func = args.func
  except AttributeError:
    parser.print_help()
  if func != None:
    args.func(args, parser)

# handlers
def do_start(args, *other):
  #read settings from defaults.yaml
  pkg_dir, this_filename = os.path.split(__file__)
  defaults = os.path.join(pkg_dir, "data/defaults.yml") 
  with open(defaults, "r") as f:
      settings = yaml.safe_load(f) #precise the setting key within the default yaml file
  orchestrator_ref = Orchestration.start(settings)
  
  #orchestrator_ref.stop()
  

 

if __name__ == "__main__":
  main(sys.argv[1] if len(sys.argv)>1 else None)


