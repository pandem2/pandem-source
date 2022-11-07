import pandas as pd

def df_transform(df):
  filtered_df = df.groupby(['day', 'destination', 'origin'])['callsign'].count()
  filtered_df = filtered_df.reset_index()
  filtered_df['line_number'] = range(1, len(filtered_df)+1) 
  return filtered_df.rename(columns={"callsign": "flights"})