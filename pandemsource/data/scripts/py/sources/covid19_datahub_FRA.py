import numpy as np

def df_transform(df):
  # we dont't need national data as we already have detailed data per region
  df = df[~(df["key_nuts"] == "FR")].reset_index()
  # contact tracing variables
  df["no_tracing"] = np.where(df["contact_tracing"].fillna(-1) == 0, 1, 0)
  df["limited_tracing"] = np.where(df["contact_tracing"].fillna(-1) == 1, 1, 0)
  df["comprehensive_tracing"] = np.where(df["contact_tracing"].fillna(-1) == 2, 1, 0)
  
  # testing variables
  df["no_testing"] = np.where(df["testing_policy"].fillna(-1) == 0, 1, 0)
  df["symptoms_limited_testing"] = np.where(df["testing_policy"].fillna(-1) == 1, 1, 0)
  df["symptoms_testing"] = np.where(df["testing_policy"].fillna(-1) == 2, 1, 0)
  df["open_testing"] = np.where(df["testing_policy"].fillna(-1) == 3, 1, 0)
  return df

