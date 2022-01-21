
from . import formatreader
import pandas as pd
import re
import json
import itertools
import gzip

class FormatReaderJSON(formatreader.FormatReader):

    def __init__(self, name, orchestrator_ref, settings): 
        super().__init__(name = name, orchestrator_ref = orchestrator_ref, settings = settings)

    def read_df(self, file_path, dls):
        end_lined = "new_line_ended" not in dls["acquisition"]["format"] or bool(dls["acquisition"]["format"]["new_line_ended"])
        encoding = dls["acquisition"]["format"]["encoding"] if "encoding" in dls["acquisition"]["format"] else "UTF-8"
        gzipped = file_path.endswith(".gz")

        with (open(file_path,  'r', encoding=encoding) if not gzipped else gzip.open(file_path, 'rb')) as f:
           if end_lined :
              items = list(map(lambda l: json.loads(l), f.readlines())) 
           else :
              items = json.loads(f.read())

        row_steps = dls["acquisition"]["format"]["row"] if "row" in dls["acquisition"]["format"] else "*"
        row_steps = list(filter(lambda v: len(v)>0, row_steps.split("/")))

        #iterating over rows
        col_paths = {col['name']:list(filter(lambda v: len(v)>0, col['path'].split("/"))) for col in dls['columns'] }
        cols = list(col_paths.keys())
        rows = []
        for row_index in self.expand(items, row_steps):
          #Evaluating values for each row
          col_values = {c:self.values(items, row_index, col_paths[c]) for c in cols}
          combinations = itertools.product(*(col_values[c] for c in cols))
          for t in combinations:
            rows.append(t)

        df = pd.DataFrame(rows, columns = cols)
        return df

    def apply_index(self, items, index):
      x = items
      for i in index:
        x = x[i]
      return x

    def expand(self, items, steps, index = []):
      current = self.apply_index(items, index)
      if len(index) == len(steps):
        yield index
      else:
        i_step = len(index)

        step = steps[i_step]
        if step == "*":
          if not isinstance(current, list):
            raise ValueError(f"Cannot apply json path {step} since the current object is not a list")
          else:
            for i in range(0, len(current)):
              for y in self.expand(items, steps, index + [i]):
                yield y
        elif step.startswith("@"):
          prop = step[1:len(step)]
          if prop not in current:
            raise ValueError(f"Cannot apply json path {step} since the current object does not contains a property with that name")
          else:
            for y in self.expand(items, row_steps, index + [prop]):
              yield y
        else:
          raise ValueError(f"Cannot understund path {step}")

    def values(self, items, index, paths):
      current = self.apply_index(items, index)
      if len(paths) == 0:
        yield current
      else:
        step = paths[0]
        rest = paths[1:len(paths)]
        if step == "*":
          if not isinstance(current, list):
            raise ValueError(f"Cannot apply json path {step} of {paths} on since the current object is not a list")
          else:
            for i in range(0, len(current)):
              for y in self.values(items, index + [i], rest): 
                yield y
        elif step.startswith("@"):
          prop = step[1:len(step)]
          if prop not in current:
            yield None
          else:
            for y in self.values(items, index + [prop], rest):
              yield y
        elif step == "parent()":
          if len(index)==0:
            raise ValueError(f"Cannot apply json path {step} of {paths} since the current object is root")
          else:
            for y in self.values(items, index[0:len(index)-1], rest):
              yield y
        else:
          raise ValueError(f"Cannot understund path {step}")
