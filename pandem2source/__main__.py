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
  
  start_parser.add_argument(
    "--no-acquire", 
    action="store_true", 
    help="Whether to output debugging messages to the console", 
  )
  
  start_parser.add_argument(
    "--retry-failed", 
    action="store_true", 
    help="Whether to retry failed jobs", 
  )
  
  start_parser.set_defaults(func = do_start)
  
  # setup 
  reset_parser = subs.add_parser("reset", help = "reset configuration as system defaults")
  
  reset_parser.add_argument(
    "-v", 
    "--variables", 
    action="store_true", 
    help="Whether to rebuild variables based on last system defaults", 
  )
  reset_parser.add_argument(
    "--restore-factory-defaults", 
    action="store_true", 
    help="Delete all files and database elements and restore system defaults", 
  )
  reset_parser.add_argument(
    "--covid19-datahub", 
    action="store_true", 
    help="Reset covid19-datahub datasource to system defaults", 
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
def do_start_dev(debug = True, no_acquire = False, retry_failed = False):
  from types import SimpleNamespace
  return do_start(SimpleNamespace(**{"debug":True, "no_acquire":no_acquire, "retry_failed":retry_failed}))

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
  orchestrator_ref = Orchestration.start(settings, start_acquisition = not args.no_acquire, retry_failed = args.retry_failed)
  return orchestrator_ref.proxy()
  
def do_reset(args, *other):
  if args.restore_factory_defaults:
    admin.delete_all()
  if args.variables or args.restore_factory_defaults:
    admin.reset_variables(in_home = True)
  if args.covid19_datahub or args.restore_factory_defaults:
    admin.reset_source("nuts-eurostat")
    admin.reset_source("ICD-10-diseases-list")
    #admin.reset_source("covid19-datahub")
    admin.reset_source("covid19-variants")
    admin.reset_source("local-file-cases")
     

if __name__ == "__main__":
  main(sys.argv[1] if len(sys.argv)>1 else None)


