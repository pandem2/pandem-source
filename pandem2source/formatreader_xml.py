
from . import formatreader
import pandas as pd
import re
from lxml import etree

class FormatReaderXML(formatreader.FormatReader):

    def __init__(self, name, orchestrator_ref, settings): 
        super().__init__(name = name, orchestrator_ref = orchestrator_ref, settings = settings)
          
    def read_format_start(self, job, file_path):
        dls = job['dls_json']
        tree = etree.parse(file_path)
        root = tree.getroot()
        nsmap = root.nsmap
        cols = [col['name'] for col in dls['columns'] ]
        rows = []
        for row in root.xpath(dls['acquisition']['format']['row'], namespaces=nsmap):
            dict_col = dict()
            for col in dls['columns']:
                ltext = row.xpath(col['xpath'], namespaces=nsmap)
                if ltext: 
                    if 'find' in col:
                        subtext = [re.findall( col['find'], text)[0] for text in ltext]
                        if len(subtext)==1:
                            dict_col[col['name']] = subtext[0]
                        else:
                            dict_col[col['name']] = subtext
                    else:
                        dict_col[col['name']] = ltext[0]
                else:
                    dict_col[col['name']] = None
            rows.append(dict_col)
        df = pd.DataFrame(rows, columns = cols)
        self._pipeline_proxy.read_format_end(job, file_path, df).get()