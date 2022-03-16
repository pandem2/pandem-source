from . import formatreader
import pandas as pd

class FormatReaderXLS(formatreader.FormatReader):

    def __init__(self, name, orchestrator_ref, settings): 
        super().__init__(name = name, orchestrator_ref = orchestrator_ref, settings = settings)
    
    def read_df(self, file_path, dls):
        file_bytes = self._storage_proxy.read_file(file_path).get()
        if file_bytes != '':
            sheet = dls['acquisition']['channel']['sheet']
            col_names = set([col['name'] for col in dls['columns']])
            df = pd.read_excel(file_bytes, sheet, header=None)
            df = df.dropna(how='all').dropna(how='all', axis=1)
            cols_row = 0
            best_row = 0
            missing = col_names
            while cols_row < len(df) and len(missing) > 0: 
                row_missing = col_names.difference(df.iloc[cols_row])
                if len(row_missing) < len(missing):
                    best_row = cols_row
                    missing = row_missing
                cols_row += 1
            if len(missing) > 0:
              raise ValueError(f"Cannot find columns {missing} on sheet {sheet}")
            cols = df.iloc[best_row]
            df  = pd.DataFrame(df.values[best_row+1:], columns=cols)
        else:
            raise ValueError('can not read from empty bytes')
        return df
