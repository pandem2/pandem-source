import numpy as np

def df_transform(df):
  df["country"] = [* map(lambda v: v.split("_")[-2], df["file"])]
  return df
