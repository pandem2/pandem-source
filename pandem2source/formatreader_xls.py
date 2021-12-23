from . import formatreader
import pandas as pd

class FormatReaderXLS(formatreader.FormatReader):

    def __init__(self, name, orchestrator_ref, settings): 
        super().__init__(name = name, orchestrator_ref = orchestrator_ref, settings = settings)
    
    def read_df(self, file_path, dls):
        file_bytes = self._storage_proxy.read_file(file_path).get()
        if file_bytes != '':
            sheet = dls['acquisition']['channel']['sheet']
            col_names = [col['name'] for col in dls['columns']]
            df = pd.read_excel(file_bytes, sheet, header=None)
            df = df.dropna(how='all').dropna(how='all', axis=1)
            cols_row = 0
            while len(set(col_names).intersection(df.iloc[cols_row]))<len(col_names):
                cols_row += 1
            cols = df.iloc[cols_row]
            df  = pd.DataFrame(df.values[cols_row+1:], columns=cols)
        else:
            raise ValueError('can not read from empty bytes')
        return df