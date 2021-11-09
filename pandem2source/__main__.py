import sys, os
import yaml
import argparse
import logging
from . import util 
from .orchestrator import Orchestration
from . import admin
def main(a):
  #conf = config()
  # Base argument parser
  parser = argparse.ArgumentParser()
  subs = parser.add_subparsers()
  
  # Launch pandem source start command
  start_parser = subs.add_parser("start", help = "Starts PANDEM source monitor")
  
  start_parser.add_argument(
    "-d", 
    "--debug", 
    action="store_true", 
    help="Whether to output debugging messages to the console", 
  )
  
  start_parser.set_defaults(func = do_start)
  
  # setup 
  reset_parser = subs.add_parser("reset", help = "reset configuration as system defaults")
  
  reset_parser.add_argument(
    "-v", 
    "--variables", 
    action="store_true", 
    help="Whether to rebuild variables based on las system defaults", 
  )
  
  reset_parser.set_defaults(func = do_reset)

  util.check_pandem_home()
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
  if args.debug:
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    root.addHandler(handler)
 
  pkg_dir, this_filename = os.path.split(__file__)
  defaults = os.path.join(pkg_dir, "data/defaults.yml") 
  with open(defaults, "r") as f:
      settings = yaml.safe_load(f)
  orchestrator_ref = Orchestration.start(settings)
  
def do_reset(args, *other):
  if args.variables:
    admin.reset_variables(in_home = True)
     

if __name__ == "__main__":
  main(sys.argv[1] if len(sys.argv)>1 else None)


