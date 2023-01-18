import sys, os
import argparse
import logging
import shutil
from . import util 
from .orchestrator import Orchestration
from . import admin
import pandas as pd

l = logging.getLogger("pandem")

def main(a):
  # setting default PANDEM_HOME if not set
  if os.environ.get("PANDEM_HOME") is None:
    os.environ["PANDEM_HOME"] = os.path.expanduser('~/.pandemsource').replace('\\', '/')
    if not os.path.exists(os.environ.get("PANDEM_HOME")):
      os.mkdir(os.environ.get("PANDEM_HOME"))

  # conf = config()
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
    help="Prevent data acquisition from sources", 
  )
  
  start_parser.add_argument(
    "--no-nlp", 
    action="store_true", 
    help="Prevent NLP models and sources using them (Twitter, MediSys) pf being launched (Useful for instances without docker" 
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
    "--force-acquire", 
    action="store_true", 
    help="Reset the concerned data sources to force a fresh acquire" 
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
  setup_parser = subs.add_parser("setup", help = "reset configuration as system defaults")
  
  setup_parser.add_argument(
    "-v", 
    "--variables", 
    action="store_true", 
    help="Whether to restore variables defiitions to last system defaults", 
  )
  setup_parser.add_argument(
    "--install", 
    action="store_true", 
    help="Delete all files and database elements and install system defaults on the environment variable PANDEM_HOME", 
  )
  setup_parser.add_argument(
    "--scripts",
    action="store_true",
    help="Restore the default script folder in pandem-home"
  )
  setup_parser.add_argument(
    "--input-local",
    action="store_true",
    help="Restore the default input-local folder in pandem-home"
  )
  setup_parser.add_argument(
    "--notations", 
    action="store_true", 
    help="Reset interational standard notation datasources to system defaults", 
  )
  setup_parser.add_argument(
    "--covid19-datahub", 
    action="store_true", 
    help="Reset covid19-datahub datasource to system defaults", 
  )
  setup_parser.add_argument(
    "--ecdc-atlas", 
    action="store_true", 
    help="Reset ecdc-atlas datasource to system defaults", 
  )
  setup_parser.add_argument(
    "--influenzanet", 
    action="store_true", 
    help="Reset influenza net datasource to system defaults", 
  )
  setup_parser.add_argument(
    "--ecdc-covid19", 
    action="store_true", 
    help="Reset ecdc-covid19 datasource to system defaults", 
  )
  setup_parser.add_argument(
    "--ecdc-covid19-simulated", 
    action="store_true", 
    help="Reset ecdc-covid19 simulated datasource to system defaults", 
  )
  setup_parser.add_argument(
    "--serotracker", 
    action="store_true", 
    help="Reset serotracker datasource to system defaults", 
  )
  setup_parser.add_argument(
    "--pandem-partners-template", 
    action="store_true", 
    help="Reset pandem partner templates to system defaults", 
  )
  setup_parser.add_argument(
    "--twitter-covid19", 
    action="store_true", 
    help="Reset pandem twitter covid19 to system defaults", 
  )

  setup_parser.add_argument(
    "--twitter", 
    action="store_true", 
    help="Reset pandem twitter system defaults", 
  )
  setup_parser.add_argument(
    "--medisys", 
    action="store_true", 
    help="Reset pandem medisys system defaults", 
  )
  setup_parser.add_argument(
    "--flights",
    action="store_true",
    help="Reset pandem flight related information to system defaults", 
  )
  setup_parser.add_argument(
    "--airports",
    action="store_true",
    help="Reset pandem airports related information to system defaults", 
  )
  setup_parser.add_argument(
    "--owid",
    action="store_true",
    help="Reset pandem owid related information to system defaults"
  )
  setup_parser.add_argument(
    "--geonames",
    action="store_true",
    help="Reset pandem geonames related information to system defaults"
  )
  setup_parser.add_argument(
    "--health-resources-eurostat",
    action="store_true",
    help="Reset pandem health resources staff related information to system defaults"
  )
  setup_parser.add_argument(
    "--pandem-2-2023-fx",
    action="store_true",
    help="Reset pandem 2023 functional exercise related information to system defaults"
  )
  setup_parser.add_argument(
    "--oecd",
    action="store_true",
    help="Reset OECD related information to system defaults"
  )
  setup_parser.add_argument(
    "--delete-data",
    action="store_true",
    help="Delete associated data with the reseted data source"
  )
  
  setup_parser.add_argument(
    "--reset-acquisition",
    action="store_true",
    help="Dismiss source acquisition stamp, making pandemsource reload data on next execution"
  )

  setup_parser.set_defaults(func = do_setup)

  # Launch pandem source list command
  list_parser = subs.add_parser("list", help = "List elements on this Pandem-Source instance")
  list_parser.add_argument(
    "-s",
    "--source", 
    type=str, 
    required = False,
    default = None,
    help="Source for which to display the required information" 
  )
  list_parser.add_argument(
    "--jobs",
    action="store_true",
    help="List existing jobs in this instance", 
  )
  list_parser.add_argument(
    "--sources",
    action="store_true",
    help="List existing sources in this instance", 
  )
  
  list_parser.add_argument(
    "--missing-sources",
    action="store_true",
    help="List sources embedded on pandemSource package not present on this instance", 
  )
  list_parser.add_argument(
    "--package-sources",
    action="store_true",
    help="List sources embedded on pandemSource package", 
  )
  list_parser.add_argument(
    "--missing-package-sources",
    action="store_true",
    help="List existing sources in this instance not present on pandemSource package", 
  )

  list_parser.set_defaults(func = do_list)

  
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
def do_start_dev(debug = True, no_acquire = False, retry_failed = False, restart_job = 0, not_retry_active = False, no_app = True, force_acquire = False, no_nlp = False):
  from types import SimpleNamespace
  return do_start(SimpleNamespace(**{"debug":True, "no_acquire":no_acquire, "retry_failed":retry_failed, "limit_collection":None, "restart_job":restart_job, "no_app":no_app, "not_retry_active":not_retry_active, "force_acquire":force_acquire, "no_nlp": no_nlp}))

# handlers
def do_start(args, *other):
  if args.debug:
    loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict]

    for logger in [l for l in loggers if len([ni for ni in ["urllib", "tweepy"] if ni in l.name]) == 0]:
      logger.setLevel(logging.DEBUG)
    root = logging.getLogger()
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
  
  install_issues = admin.install_issues(check_nlp = not args.no_nlp)
  if len(install_issues) > 0:
    eol = '\n'
    l.warning(f"""The following errors where found on current PANDEM-2 installation please fix before proceed
    {eol.join(install_issues)}
    """)
    return None
  
  if args.restart_job > 0:
    orchestrator_ref = Orchestration.start(
      settings, 
      start_acquisition = False, 
      retry_failed = False, 
      restart_job = args.restart_job, 
      retry_active = True, 
      force_acquire = args.force_acquire, 
      no_nlp = args.no_nlp
    )
    orch = orchestrator_ref.proxy()
  elif args.limit_collection is not None:
    orchestrator_ref = Orchestration.start(
      settings, start_acquisition = False, 
      retry_failed = args.retry_failed, 
      restart_job = args.restart_job, 
      retry_active = 
      not args.not_retry_active, 
      force_acquire = args.force_acquire, 
      no_nlp = args.no_nlp
    )
    orch = orchestrator_ref.proxy()
    storage_proxy = orch.get_actor("storage").get().proxy()
    dls_files = storage_proxy.list_files('source-definitions').get()
    dls_dicts = [storage_proxy.read_file(file_name['path']).get() for file_name in dls_files if file_name['path'].endswith(".json")]
    for source_name in args.limit_collection.split(","):
      dlss = list(filter(lambda dls: dls['scope']['source'] == source_name or (source_name in dls['scope']['tags'] and (dls['acquisition'].get("active") != False)), dls_dicts))
      for dls in dlss:
        acquisition_proxy = orch.get_actor(f"acquisition_{dls['acquisition']['channel']['name']}").get().proxy()
        acquisition_proxy.add_datasource(dls, force_acquire = args.force_acquire, ignore_last_exec = True)
  else:
    orchestrator_ref = Orchestration.start(
      settings, 
      start_acquisition = not args.no_acquire, 
      retry_failed = args.retry_failed, 
      retry_active = not args.not_retry_active, 
      force_acquire = args.force_acquire, 
      no_nlp = args.no_nlp,
      ignore_last_exec = False
    )
    orch = orchestrator_ref.proxy()

  # launching pandem2source app
  if not args.no_app:
    admin.run_pandem2app()    

  return orch
  
def do_setup(args, *other):

  root = logging.getLogger()
  root.setLevel(logging.INFO)
  handler = logging.StreamHandler(sys.stdout)
  handler.setLevel(logging.INFO)
  formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
  handler.setFormatter(formatter)
  root.addHandler(handler)

  if args.install:
    admin.delete_all()
    admin.reset_default_folders("input-local", "input-local-defaults", "dfcustom", "scripts", "variables", "indicators", "img")
  if args.scripts:
    admin.reset_default_folders("scripts")
  if args.input_local:
    admin.reset_default_folders("input-local")
  if args.variables or args.install:
    admin.reset_variables()
  if args.notations or args.install:
    admin.reset_source("nuts-eurostat", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("ICD-10-diseases", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("isco-08-ilo", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
  if args.pandem_partners_template:
    admin.reset_source("covid19-template-cases", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("covid19-template-cases-RIVM", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("covid19-template-cases-THL", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("covid19-template-beds-staff", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("covid19-template-contact-tracing", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("covid19-template-daily-voc-voi", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("covid19-template-daily-voc-voi-RIVM", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("covid19-template-hospitalised-deaths", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("covid19-template-hospitalised-deaths-THL", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("covid19-template-hospitalised-deaths-RIVM", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("covid19-template-ltcf-RIVM", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("covid19-template-outbreaks", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("covid19-template-participatory-sentinel-surveilla", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("covid19-template-sex-age-comorbidity", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("covid19-template-testing", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("covid19-template-testing-THL", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("covid19-template-testing-RIVM", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("covid19-template-vaccination", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("covid19-template-vaccination-THL", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("covid19-template-voc-voi", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("covid19-template-local-regions", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("covid19-template-local-regions-RIVM", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("covid19-template-local-regions-THL", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
  if args.covid19_datahub:
    admin.reset_source("covid19-datahub", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("covid19-datahub-AUT", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("covid19-datahub-BEL", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("covid19-datahub-BGR", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("covid19-datahub-HRV", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("covid19-datahub-CYP", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("covid19-datahub-CZE", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("covid19-datahub-DNK", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("covid19-datahub-EST", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("covid19-datahub-FIN", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("covid19-datahub-FRA", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("covid19-datahub-DEU", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("covid19-datahub-GRC", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("covid19-datahub-HUN", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("covid19-datahub-IRL", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("covid19-datahub-ITA", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("covid19-datahub-LVA", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("covid19-datahub-LTU", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("covid19-datahub-LUX", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("covid19-datahub-MLT", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("covid19-datahub-NLD", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("covid19-datahub-POL", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("covid19-datahub-PRT", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("covid19-datahub-ROU", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("covid19-datahub-SVK", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("covid19-datahub-SVN", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("covid19-datahub-ESP", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("covid19-datahub-SWE", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("covid19-datahub-GBR", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)

  if args.ecdc_covid19_simulated:
    admin.reset_source("ecdc-covid19-age-group-variants", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("ecdc-covid19-weekly-hospital-occupancy-variants", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("ecdc-covid19-genomic-data", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
  if args.ecdc_covid19:
    admin.reset_source("ecdc-covid19-vaccination", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("ecdc-covid19-variants", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("ecdc-covid19-age-group", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("ecdc-covid19-measures", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("ecdc-covid19-weekly", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("ecdc-covid19-daily", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("ecdc-covid19-weekly-hospital-occupancy", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
  if args.serotracker:
    admin.reset_source("geonames-countries", delete_data = False)    
    admin.reset_source("serotracker", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)    
  if args.ecdc_atlas:
    admin.reset_source("ecdc-atlas-influenza", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)    
  if args.influenzanet:
    admin.reset_source("influenza-net", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("influenza-net-visits", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
  if args.twitter_covid19:
    admin.reset_source("twitter-covid19", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)    
  if args.twitter:
    admin.reset_source("twitter", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)    
  if args.medisys:
    admin.reset_source("medisys", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
  if args.flights:
    admin.reset_source("ourairports", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("ourairports-of-origin", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("opensky-network-coviddataset", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
  if args.airports:
    admin.reset_source("ourairports", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("ourairports-of-origin", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
  if args.owid:
    admin.reset_source("owid-covid19-excess-mortality", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
  if args.geonames:
    admin.reset_source("geonames-countries", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("geonames-countries-of-origin", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition) 
  if args.health_resources_eurostat:
    admin.reset_source("health-resources-national-eurostat", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("health-resources-nuts2-eurostat", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("health-resources-beds-eurostat", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
  if args.pandem_2_2023_fx:
    admin.reset_default_folders("input-local")
    admin.reset_source("pandem-2-2023-fx-DEU", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
    admin.reset_source("pandem-2-2023-fx-NLD", delete_data = args.delete_data, reset_acquisition = args.reset_acquisition)
  if args.oecd:
    admin.reset_default_folders("input-local")
    admin.reset_source("oecd-icu-beds", delete_data= args.delete_data, reset_acquisition= args.reset_acquisition)

def do_list(args, *other):
  if args.jobs:
    df = admin.list_jobs(args.source)
    print(df[["source", "step", "status"]])
  elif args.sources or args.missing_sources or args.package_sources or args.missing_package_sources:
    sources = admin.list_sources(local = args.sources, default = args.package_sources, missing_local = args.missing_sources, missing_default = args.missing_package_sources)
    for line in pd.DataFrame(sources,columns=["Source", "Tag"]).to_string().split("\n"):
        print(line)



if __name__ == "__main__":
  main(sys.argv[1] if len(sys.argv)>1 else None)


