import sys
import argparse
from .config import config
from . import api

def main(a):
  conf = config()
  # Base argument parser
  parser = argparse.ArgumentParser()
  subs = parser.add_subparsers()
  
  # Launch pandem source driver
  driver_parser = subs.add_parser("driver", help = "Launch pandem a 2 source driver providing configuration, logging and health status")
  driver_parser.add_argument(
    "-p", 
    "--port", 
    required=False, 
    help="Path to a file containing the extraction files to be encryped. If not provided it will be prompted", 
    default = conf.get("driver.port"), 
    type = int
  )
  driver_parser.set_defaults(func = do_driver)
  
  #calling handlers
  func = None
  try:
    args = parser.parse_args()
    func = args.func
  except AttributeError:
    parser.print_help()
  if func != None:
    args.func(args, parser, conf)
  

# handlers
def do_driver(args, *other):
  api.run(other[1], args.port, "driver")

if __name__ == "__main__":
  main(sys.argv[1] if len(sys.argv)>1 else None)


