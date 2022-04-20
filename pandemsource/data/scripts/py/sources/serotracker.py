import numpy as np
import pandas as pd

def df_transform(df):
  def ag(i):
    if pd.isna(df['age_min'][i]) and pd.isna(df['age_min'][i]):
      return pd.NA
    elif pd.isna(df['age_min'][i]):
      return "-"+str(int(df['age_max'][i]))
      pd.isna()+ "-" + df['age_max'].map(str) 
    elif pd.isna(df['age_max'][i]):
      return str(int(df['age_min'][i]))+"-"
    else:
      return str(int(df['age_min'][i])) + "-" + str(int(df['age_max'][i]))
  
  df['age_group'] = [*map(ag, df.index)]
  return df
