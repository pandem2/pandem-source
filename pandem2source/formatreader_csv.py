from . import formatreader
import pandas as pd

class FormatReaderCSV(formatreader.FormatReader):

    def __init__(self, name, orchestrator_ref, settings): 
        super().__init__(name = name, orchestrator_ref = orchestrator_ref, settings = settings)
          
    def read_format_start(self, job, file_path):
        file_bytes = self._storage_proxy.read_files(file_path).get()
        if file_bytes != '':
            df = pd.read_csv(file_bytes) 
            self._pipeline_proxy.read_format_end(job, file_path, df).get()