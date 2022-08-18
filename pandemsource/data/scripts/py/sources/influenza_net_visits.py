import numpy as np

def df_transform(df):
  df["country"] = [* map(lambda v: v.split("_")[-3], df["file"])]
  df["variable"] = df["variable"].apply(str.lower)
  for col in ["cum_prop_adj", "cum_prop_adj_low", "cum_prop_adj_up"]:
    df[col] = df[col].mul(100)
  return df