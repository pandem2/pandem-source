import numpy as np

def df_transform(df):
  df["Lineage + additional mutations"] =  df["Lineage + additional mutations"].str.replace(" (x)", "", regex = False).str.replace(" (z)", "", regex = False).str.replace("-like (a)", "", regex = False)
  df["is_variant_of_concern"] =  np.where(df["Lineage + additional mutations"].isin(["BA.2.75", "XBB.1.5", "XBB", "BQ.1"]), True, False)
  df["is_variant_of_interest"] = np.where(df["Lineage + additional mutations"].isin(["B.1.1.7", "B.1.351", "B.1.617.2", "P.1", "BA.1", "BA.2", "BA.3", "BA.4"]), True, False)
  return df
