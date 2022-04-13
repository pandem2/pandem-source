import numpy as np

def df_transform(df):
  df["flights"] = [1 for _ in range(len(df["file"]))]
  return df