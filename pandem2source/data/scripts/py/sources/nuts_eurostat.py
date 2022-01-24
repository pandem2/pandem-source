import numpy as np

def df_transform(df):
  df["parent"] = np.where(df["code"].str.len()==2, None, df["code"].str[0:-1])
  return df
