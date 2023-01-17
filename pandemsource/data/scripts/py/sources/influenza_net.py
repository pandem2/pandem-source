import numpy as np

def df_transform(df):
  df["country"] = [* map(lambda v: v.split("_")[-2], df["file"])]
  df["incidence"] = df["incidence"] * 1000
  df["lower"] = df["lower"] * 1000
  df["upper"] = df["upper"] * 1000
  return df
