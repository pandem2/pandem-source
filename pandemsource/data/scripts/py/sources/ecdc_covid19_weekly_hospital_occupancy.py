import pandas as pd

def df_transform(df):
    df = rebuild_columns(add_same_indicator(df))
    df['country'] = df['country'].apply(str.upper)
    df['line_number'] = range(1, len(df)+1) 
    return df

def add_same_indicator(df: pd.DataFrame) -> pd.DataFrame:
    """Adds values of same indicators due to several measure for a week"""
    df1 = df[df['indicator'].isin(['Daily hospital occupancy', 'Daily ICU occupancy'])]
    df2 = df[df['indicator'].isin(['Weekly new hospital admissions per 100k', 'Weekly new ICU admissions per 100k'])]
     
    dff = pd.concat([
      df1.groupby(['indicator', 'year_week', 'country'])[['value']].last(),
      df2.groupby(['indicator', 'year_week', 'country'])[['value']].sum()
    ]).reset_index()
    return dff

def rebuild_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Creates 4 new columns for each inficator and transforms daily in weekly values"""
    indicators = ['Daily hospital occupancy', 'Daily ICU occupancy', 'Weekly new hospital admissions per 100k', 'Weekly new ICU admissions per 100k']
    new_indicators = list(map(lambda x: x.lower().replace(' ', '_'), indicators))
    weekly_indicators = dict()
    rows = df.to_dict(orient = "records")
    # Initialize new columns
    for row in rows:
      ind = row['indicator']
      value = row['value']
      for ni in new_indicators:
        row[ni] = None
      nind = ind.lower().replace(" ", "_")
      row[nind] = value
      row.pop('indicator')
      row.pop('value')
    df = pd.DataFrame.from_records(rows)
    return df
