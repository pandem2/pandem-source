import pandas as pd
import numpy as np

def df_transform(df):
  dfvar = df.groupby(['country', 'country_code', 'year_week', 'variant']).agg(
    {"number_detections_variant":"sum", "percent_cases_sequenced":"max", "line_number":"min"}
  ).reset_index()
  dfvar["confirmed_cases"] = np.where(dfvar["percent_cases_sequenced"]>0, round(dfvar["number_detections_variant"] / (dfvar["percent_cases_sequenced"] / 100)), None)
  del dfvar["percent_cases_sequenced"]
  dfall = df.groupby(['country', 'country_code', 'year_week']).agg({"new_cases":"max", "number_sequenced":"max", "line_number":"min"}).reset_index()
  dfall["not_sequenced"] = dfall["new_cases"] - dfall["number_sequenced"]
  del dfall["new_cases"] 
  return pd.concat([dfvar, dfall], ignore_index = True)

