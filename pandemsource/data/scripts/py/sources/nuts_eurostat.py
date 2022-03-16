import numpy as np

def df_transform(df):
  df["parent"] = np.where(df["code"].str.len()==2, None, df["code"].str[0:-1])
  df["level"] =  np.where(
    df["code"].str.len()==2, "Country", np.where(
    df["code"].str.len()==3, "NUTS-1",  np.where(
    df["code"].str.len()==4, "NUTS-2", np.where(
    df["code"].str.len()==5, "NUTS-3", "Other"
    ))))
  return df
