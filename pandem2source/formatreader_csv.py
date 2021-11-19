from . import formatreader
import pandas as pd

class FormatReaderCSV(formatreader.FormatReader):

    def __init__(self, name, orchestrator_ref, settings): 
        super().__init__(name = name, orchestrator_ref = orchestrator_ref, settings = settings)
    
    def read_df(self, file_path, dls):
        file_bytes = self._storage_proxy.read_file(file_path).get()
        if file_bytes != '':
            df = pd.read_csv(file_bytes) 
        else:
            raise ValueError('can not read from empty bytes')
        return df
    
    


          
   
