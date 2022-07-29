import numpy as np

def df_transform(df):
  df["country"] = [* map(lambda v: v.split("_")[-3], df["file"])]
  df["variable"] = df["variable"].apply(str.lower)
  return df