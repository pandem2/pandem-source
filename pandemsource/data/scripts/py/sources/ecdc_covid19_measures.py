import numpy as np
import pandas as pd
import datetime

def df_transform(df):
  data = list()
  starts = df["date_start"]
  starts = starts.replace(to_replace="NA", value=pd.NA)
  starts = pd.to_datetime(starts)
  
  ends = df["date_end"]
  ends = ends.replace(to_replace="NA", value=pd.NA)
  ends = pd.to_datetime(ends)

  last = max(ends)
  for i in df.index:
    start = starts[i]
    end = ends[i] if pd.notna(ends[i]) else last
    dates = [end - datetime.timedelta(days = x) for x in range((end - start).days + 1)]
    dates.reverse()
    data.append((df["line_number"][i], df["Country"][i].upper(), df["Response_measure"][i], min(dates) - datetime.timedelta(days = 1), 0))
    for d in dates:
      data.append((df["line_number"][i], df["Country"][i].upper(), df["Response_measure"][i], d, 1))
    data.append((df["line_number"][i], df["Country"][i].upper(), df["Response_measure"][i], max(dates) + datetime.timedelta(days = 1), 0))
  newdf = pd.DataFrame(data, columns = ["line_number", "Country", "Response_measure", "date", "count"])
  return newdf

