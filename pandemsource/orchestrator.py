import pykka
from . import storage
from . import acquisition_url
from . import acquisition_git
from . import pipeline
from . import formatreader_xml
from . import formatreader_csv
from . import formatreader_xls
from . import formatreader_json
from . import unarchive
from . import acquisition_git_local
from . import acquisition_localFS
from . import acquisition_twitter
from . import acquisition_medisys
from . import script_executor
from . import nlp_annotator
from . import dfreader
from . import standardizer
from . import aggregator
from . import variables
from . import evaluator
from . import api
import datetime
import os
import pandas as pd
from itertools import chain

class Orchestration(pykka.ThreadingActor):

    def __init__(self, settings, start_acquisition = True, retry_failed = False, restart_job = 0, retry_active = True, force_acquire = True):
        super(Orchestration, self).__init__()
        self.settings = settings
        self.current_actors = dict()
        self.start_acquisition = start_acquisition
        self.retry_failed = retry_failed
        self.retry_active = retry_active
        self.restart_job = restart_job
        self.force_acquire = force_acquire


    def on_start(self):
        # Launching the storage actor which can be used by any actor
        storage_ref = storage.Storage.start('storage', self.actor_ref, self.settings)
        storage_proxy = storage_ref.proxy()
        self.current_actors['storage'] = {'ref': storage_ref}

        # Launching passive actors
        # launch dataframe reader actor
        dfreader_ref = dfreader.DataframeReader.start('dfreader', self.actor_ref, storage_ref, self.settings)
        self.current_actors['dfreader'] = {'ref': dfreader_ref}
        
        #launch xml format reader actor
        xmlreader_ref = formatreader_xml.FormatReaderXML.start('ftreader_xml', self.actor_ref, self.settings)
        self.current_actors['ftreader_xml'] = {'ref': xmlreader_ref}

        #launch csv format reader actor
        csvreader_ref = formatreader_csv.FormatReaderCSV.start('ftreader_csv', self.actor_ref, self.settings)
        self.current_actors['ftreader_csv'] = {'ref': csvreader_ref}

        #launch xls format reader actor
        xlsreader_ref = formatreader_xls.FormatReaderXLS.start('ftreader_xls', self.actor_ref, self.settings)
        self.current_actors['ftreader_xls'] = {'ref': xlsreader_ref}

        #launch json format reader actor
        jsonreader_ref = formatreader_json.FormatReaderJSON.start('ftreader_json', self.actor_ref, self.settings)
        self.current_actors['ftreader_json'] = {'ref': jsonreader_ref}
        
        #launch unarchiver actor
        unarchive_ref = unarchive.Unarchive.start('unarchiver', self.actor_ref, self.settings)
        self.current_actors['unarchiver'] = {'ref': unarchive_ref}

        # launch pipeline actor
        pipeline_ref = pipeline.Pipeline.start('pipeline', self.actor_ref, self.settings, self.retry_failed, self.restart_job, self.retry_active)
        self.current_actors['pipeline'] = {'ref': pipeline_ref}

        # launch script executor reader
        script_executor_ref = script_executor.ScriptExecutor.start('script_executor', self.actor_ref, self.settings)
        self.current_actors['script_executor'] = {'ref': script_executor_ref}

        # launch nlp annotator actor
        nlp_annotator_ref = nlp_annotator.NLPAnnotator.start('nlp_annotator', self.actor_ref, self.settings)
        self.current_actors['nlp_annotator'] = {'ref': nlp_annotator_ref}
        
        # launch standardizer actor
        standardizer_ref = standardizer.Standardizer.start('standardizer', self.actor_ref, self.settings)
        self.current_actors['standardizer'] = {'ref': standardizer_ref}

        # launch aggregator actor
        aggregator_ref = aggregator.Aggregator.start('aggregator', self.actor_ref, self.settings)
        self.current_actors['aggregator'] = {'ref': aggregator_ref}
        
        # launch variables actor
        variables_ref = variables.Variables.start('variables', self.actor_ref, self.settings)
        self.current_actors['variables'] = {'ref': variables_ref}

         # launch evaluator actor
        evaluator_ref = evaluator.Evaluator.start('evaluator', self.actor_ref, self.settings)
        self.current_actors['evaluator'] = {'ref': evaluator_ref}

         # launch api actor
        api_ref = api.apiREST.start('api', self.actor_ref, self.settings)
        self.current_actors['api'] = {'ref': api_ref}

        # Launching acquisition actors (active actors)
        # List source definition files within 'source-definitions' through storage actor to get 
        source_files = storage_proxy.list_files('source-definitions').get()
        # read json dls files into dicts
        dls_dicts = [storage_proxy.read_file(file_name['path']).get() for file_name in source_files]
        sources_labels = set([dls['acquisition']['channel']['name'] for dls in dls_dicts])
        
        for label in sources_labels:
            if label == "url":
                acquisition_ref = acquisition_url.AcquisitionURL.start(name = 'acquisition_'+label, orchestrator_ref = self.actor_ref, settings = self.settings)
            elif label == "git":
                acquisition_ref = acquisition_git.AcquisitionGIT.start(name = 'acquisition_'+label, orchestrator_ref = self.actor_ref, settings = self.settings)
            elif label == "git-local":
                acquisition_ref = acquisition_git_local.AcquisitionGITLocal.start(name = 'acquisition_'+label, orchestrator_ref = self.actor_ref, settings = self.settings)
            elif label == "input-local":
                acquisition_ref = acquisition_localFS.AcquisitionLocalFS.start(name = 'acquisition_'+label, orchestrator_ref = self.actor_ref, settings = self.settings)
            elif label == "twitter":
                acquisition_ref = acquisition_twitter.AcquisitionTwitter.start(name = 'acquisition_'+label, orchestrator_ref = self.actor_ref, settings = self.settings)
            elif label == "medisys":
                acquisition_ref = acquisition_medisys.AcquisitionMedisys.start(name = 'acquisition_'+label, orchestrator_ref = self.actor_ref, settings = self.settings)
            else:
                raise NotImplementedError(f"The acquisition channel {label} has not been implemented")

            acquisition_proxy = acquisition_ref.proxy()
            dls_label = [dls for dls in dls_dicts if dls['acquisition']['channel']['name'] == label]
            for dls in dls_label:
                if self.start_acquisition:
                   acquisition_proxy.add_datasource(dls, self.force_acquire)
        
            self.current_actors['acquisition_'+label] = {'ref': acquisition_ref, 'sources': dls_label}


    def get_heartbeat(self, actor_name):
        now = datetime.datetime.now()
        self.current_actors[actor_name]['heartbeat'] = now
        #print(f'heartbeat from {actor_name} at: {now}')

    def get_actor(self, actor_name):
        return self.current_actors[actor_name]['ref']
        

    def actors_df(self):
      map = self.current_actors
      df = pd.DataFrame(data = {
        "actor": map.keys(),
        "last_seen_seconds": [((datetime.datetime.now() - it["heartbeat"]).total_seconds() if "heartbeat" in it else pd.NA)  for it in map.values()],
        "heartbeat": [(it["heartbeat"] if "heartbeat" in it else pd.NA)  for it in map.values()]
      })
      return df
