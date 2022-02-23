import sys, os
import argparse
import logging
import shutil
from . import util 
from .orchestrator import Orchestration
from . import admin

l = logging.getLogger("pandem")

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
    help="Whether to retry failed jobs" 
  )
  
  start_parser.add_argument(
    "--not-retry-active", 
    action="store_true", 
    help="Whether to do not retry active jobs" 
  )
  start_parser.add_argument(
    "--no-app", 
    action="store_true", 
    help="If present the pandem2-source app will not be lauched" 
  )

  start_parser.add_argument(
    "-r",
    "--restart-job", 
    type=int, 
    required = False,
    default = 0,
    help="Job id to re-run if its data is still stored" 
  )
  
  start_parser.add_argument(
    "-l",
    "--limit-collection", 
    help="Comma separated list of sources for limiting the collection" 
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
  reset_parser.add_argument(
    "--ecdc-atlas", 
    action="store_true", 
    help="Reset ecdc-atlas datasource to system defaults", 
  )
  reset_parser.add_argument(
    "--influenzanet", 
    action="store_true", 
    help="Reset influenza net datasource to system defaults", 
  )
  reset_parser.add_argument(
    "--ecdc-covid19-variants", 
    action="store_true", 
    help="Reset ecdc-covid19-variants datasource to system defaults", 
  )
  reset_parser.add_argument(
    "--pandem-partners-template", 
    action="store_true", 
    help="Reset pandem partner templates to system defaults", 
  )
  reset_parser.add_argument(
    "--twitter", 
    action="store_true", 
    help="Reset pandem twitter system defaults", 
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
def do_start_dev(debug = True, no_acquire = False, retry_failed = False, restart_job = 0, not_retry_active = False):
  from types import SimpleNamespace
  return do_start(SimpleNamespace(**{"debug":True, "no_acquire":no_acquire, "retry_failed":retry_failed, "limit_collection":None, "restart_job":restart_job, "no_app":False, "not_retry_active":not_retry_active}))

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
  config = util.pandem_path("settings.yml")
  if not os.path.exists(config):
    shutil.copy(defaults, config)

  # Adding python scripts on pandem_home to the system path
  sys.path.insert(1, util.pandem_path("files", "scripts", "py"))
  settings = util.settings()
  
  install_issues = admin.install_issues()
  if len(install_issues) > 0:
    eol = '\n'
    l.warning(f"""The following errors where found on current PANDEM-2 installation please fix before proceed
    {eol.join(install_issues)}
    """)
    return None
  
  if args.restart_job > 0:
    orchestrator_ref = Orchestration.start(settings, start_acquisition = False, retry_failed = False, restart_job = args.restart_job, retry_active = True)
    orch = orchestrator_ref.proxy()
  elif args.limit_collection is not None:
    orchestrator_ref = Orchestration.start(settings, start_acquisition = False, retry_failed = args.retry_failed, restart_job = args.restart_job, retry_active = not args.not_retry_active)
    orch = orchestrator_ref.proxy()
    storage_proxy = orch.get_actor("storage").get().proxy()
    dls_files = storage_proxy.list_files('source-definitions').get()
    dls_dicts = [storage_proxy.read_file(file_name['path']).get() for file_name in dls_files]
    
    for source_name in args.limit_collection.split(","):
      dls = list(filter(lambda dls: dls['scope']['source'] == source_name, dls_dicts))[0]
      acquisition_proxy = orch.get_actor(f"acquisition_{dls['acquisition']['channel']['name']}").get().proxy()
      acquisition_proxy.add_datasource(dls)
  else:
    orchestrator_ref = Orchestration.start(settings, start_acquisition = not args.no_acquire, retry_failed = args.retry_failed, retry_active = not args.not_retry_active)
    orch = orchestrator_ref.proxy()

  # launching pandem2source app
  if not args.no_app:
    admin.run_pandem2app()    

  return orch
  
def do_reset(args, *other):
  if args.restore_factory_defaults:
    admin.delete_all()
    admin.reset_default_folders("input-local", "input-local-defaults", "dfcustom", "scripts", "variables", "indicators", "img")
  if args.variables or args.restore_factory_defaults:
    admin.reset_variables(in_home = True)
  if args.covid19_datahub or args.ecdc_covid19_variants or args.restore_factory_defaults:
    admin.reset_source("nuts-eurostat")
    admin.reset_source("ICD-10-diseases-list")
  if args.pandem_partners_template or args.restore_factory_defaults:
    admin.reset_source("covid19-template-cases")
    admin.reset_source("covid19-template-cases-RIVM")
    admin.reset_source("covid19-template-cases-THL")
    admin.reset_source("covid19-template-beds-staff")
    admin.reset_source("covid19-template-contact-tracing")
    admin.reset_source("covid19-template-daily-voc-voi")
    admin.reset_source("covid19-template-daily-voc-voi-RIVM")
    admin.reset_source("covid19-template-hospitalised-deaths")
    admin.reset_source("covid19-template-hospitalised-deaths-THL")
    admin.reset_source("covid19-template-hospitalised-deaths-RIVM")
    admin.reset_source("covid19-template-ltcf-RIVM")
    admin.reset_source("covid19-template-outbreaks")
    admin.reset_source("covid19-template-participatory-sentinel-surveilla")
    admin.reset_source("covid19-template-sex-age-comorbidity")
    admin.reset_source("covid19-template-testing")
    admin.reset_source("covid19-template-testing-THL")
    admin.reset_source("covid19-template-testing-RIVM")
    admin.reset_source("covid19-template-vaccination")
    admin.reset_source("covid19-template-vaccination-THL")
    admin.reset_source("covid19-template-voc-voi")
    admin.reset_source("covid19-template-local-regions")
    admin.reset_source("covid19-template-local-regions-RIVM")
    admin.reset_source("covid19-template-local-regions-THL")
  if args.covid19_datahub or args.restore_factory_defaults:
    admin.reset_source("covid19-datahub")
  if args.ecdc_covid19_variants or args.restore_factory_defaults:
    admin.reset_source("ecdc-covid19-variants")    
  if args.ecdc_atlas or args.restore_factory_defaults:
    admin.reset_source("ecdc-atlas-influenza")    
  if args.influenzanet or args.restore_factory_defaults:
    admin.reset_source("influenza-net")    
  if args.twitter or args.restore_factory_defaults:
    admin.reset_source("twitter")    

if __name__ == "__main__":
  main(sys.argv[1] if len(sys.argv)>1 else None)


