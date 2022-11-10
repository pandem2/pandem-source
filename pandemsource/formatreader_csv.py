from . import formatreader
import pandas as pd

class FormatReaderCSV(formatreader.FormatReader):

    def __init__(self, name, orchestrator_ref, settings): 
        super().__init__(name = name, orchestrator_ref = orchestrator_ref, settings = settings)
    
    def read_df(self, file_path, dls):
        file_bytes = self._storage_proxy.read_file(file_path).get()
        params = {"filepath_or_buffer":file_bytes}
        if "decimal_sign" in dls["acquisition"]["format"]: 
          params["decimal"] =  dls["acquisition"]["format"]["decimal_sign"]
        if "skiprows" in dls["acquisition"]["format"]:
          params["skiprows"] =  dls["acquisition"]["format"]["skiprows"]
        if "sep" in dls["acquisition"]["format"]:
          params["sep"] =  dls["acquisition"]["format"]["sep"]
        if "thousands_separator" in dls["acquisition"]["format"] and len(dls["acquisition"]["format"]["thousands_separator"]) == 1:
          params["thousands"] =  dls["acquisition"]["format"]["thousands_separator"]
        if "encoding" in dls["acquisition"]["format"]:
          params["encoding"] =  dls["acquisition"]["format"]["encoding"]
        if "engine" in dls["acquisition"]["format"]:
          params["engine"] = dls["acquisition"]["format"]["engine"]
        
        if file_bytes != '':
            df = pd.read_csv(**params) 
        else:
            raise ValueError('can not read from empty bytes')
        return df
    
    


          
   
